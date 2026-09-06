# 架构重整交叉评审

| 属性 | 内容 |
|---|---|
| 日期与基线 | 2026-09-06；`f6d58a09` 与本提案工作树 |
| 评审对象 | 总 spec、三个领域子 spec、目录设计与机器附件、ADR-0009、文档导航 |
| 当前结论 | 设计交叉评审完成，未发现剩余设计阻断；不构成机器合同接受、实施通过或生产资格声明 |
| 本次范围 | 设计及文档，未修改 Rust、公开机器合同、migration 或部署资产 |

本记录保留决定及证据摘要，不复制机器 registry、schema、状态矩阵或原始证明产物。
当前行为继续由现有 authority 定义；本次设计完成与后续机器合同共同评审、实现验证和生产资格分别裁决。

## 评审方式

由主代理负责总架构、安全部署、目录与 ADR；执行/持久化审阅者（Bernoulli）负责执行子 spec；作者与产品审阅者（Parfit）
负责 authoring 子 spec。两个审阅者分别检查对方负责的设计，执行审阅者另外核对目录与安全边界，产品审阅者复核主代理修订。
主代理统一冲突决定并核对工作树与当前 owning authority。这里的交叉评审是代理之间的设计审阅，不代表维护者已经接受 ADR。

第一轮以当前代码、合同、migration 和 accepted ADR 校对目标；第二轮检查缺口修订与跨文档一致性。
评审关注可实现的业务规则与失败路径，不以篇幅、crate 数或理想依赖图代替正确性。

执行审阅者完成作者入口、安全与目录的交叉检查；产品审阅者完成总设计和最终执行修订的复核，确认下述问题均已关闭。
最终复核特别覆盖不可变 Job 创建排序/冻结 cutoff、跨事务扫描、锁序、非 Agent validator 版本，以及它们与机器提案附件的一致性。

## 实质发现与处理

| 发现 | 修订决定与对应位置 | 设计复核 |
|---|---|---|
| 新内容接口可能被现有 Run result 的 Inline 返回绕过 | [作者与集成 §9.3、§10](authoring-and-integration.md)：现有 result、Artifact、values 和 CLI 正文出口共用受限授权 port；无正文权限返回 403，成功 DTO 保持 | 已关闭 |
| Registry 重编译缺少 compiler semantic 路由，升级可能改变待验证定义 | [作者与集成 §3.4、§4](authoring-and-integration.md)：冻结 compiler identity 和策略输入 digest，validation Job 按兼容身份领取；退出窗口覆盖 Draft、Receipt 与编辑承诺；旧 immutable Plan 不在 admission 重编译 | 已关闭 |
| mandatory source 可能追溯要求旧 Plan，造成隐式破坏或伪造来源 | [作者与集成 §4.2](authoring-and-integration.md)：明确新发布合同；旧 immutable Plan 保持原读取执行合同，缺源 Draft 明确要求真实源重新编译，不造 fake source、不改旧 Receipt | 已关闭 |
| 只读 POST resolver 容易被 HTTP 方法分类误判，或成为 Receipt 旁路 | [作者与集成 §10](authoring-and-integration.md)：服务端 owning operation 声明 query 并生成到 Gateway/客户端；保留逐项授权、bounds、容量与审计，不接受客户端自声明豁免 | 已关闭 |
| Task 资格过滤若在 LIMIT 后丢弃结果，分页可能误报终点或遗漏任务 | [作者与集成 §9.1](authoring-and-integration.md)：索引粗筛加有界纯领域授权；cursor 按已扫描位置推进，允许空页继续，拒绝无界填页或复制数据库业务状态机 | 已关闭 |
| 撤权后若一概拒绝结果提交，已发生的副作用、quota 和清理会失去合法收敛路径 | [安全与部署](security-and-operations.md)、[执行 §4](execution-and-persistence.md)：区分新执行、正文披露、受限系统完成/核对/清理；撤销前两者，后者限定有效身份及 exact effect/owner/fence | 已关闭 |
| PKCE cleanup 耗尽后，终态 Job 与不变 current pointer 形成不可恢复状态 | [执行 §8](execution-and-persistence.md)：显式领域恢复命令创建新物理 Job 并原子 CAS current pointer，保留同一逻辑删除身份、旧证据与有界预算；不重开 Task、不自动无限续期 | 已关闭 |
| 有界候选窗口驱逐租户 deficit 会破坏公平；同租户前排拒绝还会遮挡后续任务 | [执行 §6](execution-and-persistence.md)、[ADR-0009](../../adr/0009-durable-kernel-and-agent-domain-boundaries.md)：新增租户调度状态表承接唯一 deficit authority 和新扫描 continuation，bounded 扫描持续推进并环回；明确新增表理由及 forward migration | 已关闭 |
| 单次扫描预算若触发回到队首，大 backlog 的后部仍会饥饿；持续新增租户也可能使轮转无法结束 | [执行 §6](execution-and-persistence.md)：Job 主扫描使用不可变数据库创建排序及冻结 creation cutoff；tenant round 冻结参与范围；跨事务保留位置，单次预算只 yield，due/priority probe 不重置 cursor | 已关闭 |
| 首个 Job 创建时补建公平行，可能与 claim 的公平行→tenant 锁序相反 | [执行 §4.2、§6](execution-and-persistence.md)：tenant 暴露前或受控 backfill 预建 closed work class 公平行，普通 Job 创建只验证；新增 work class 先 provision 再启用 | 已关闭 |
| 有效 SSE cursor 可能跨过已经清理的历史而静默丢事件 | [执行 §9](execution-and-persistence.md)、[作者与集成 §9.2](authoring-and-integration.md)：只删除连续前缀并原子推进 Run replay floor；每页校验下界，未过 TTL 也必须报告历史缺口 | 已关闭 |
| 非 Run Job 无法继承 Run 语义版本 | [执行 §7](execution-and-persistence.md)：Program、compiler、维护操作采用各自 typed execution requirement 与兼容路由，不创建 fake Run 或零值 sentinel | 已关闭 |
| 非 Agent 的 Registry validation 不运行 Agent compiler，不能强塞编译语义身份 | [执行 §7](execution-and-persistence.md)、[作者与集成 §4.2](authoring-and-integration.md)：仅 Agent/Plan 编译验证使用 compiler family，其余按实际 owner 选择领域 validator ABI 和冻结验证输入 | 已关闭 |
| 选择 JetStream ACK 投递后，目录和部署清单缺少发布角色及 transport 闭包 | [安全与部署](security-and-operations.md)、[目录清单](repository-layout.json)：独立 Outbox worker、最小数据库/subject 权限和 deployment-owned stream；ACK 不等于消费者业务完成 | 已关闭 |
| 联合恢复文档引用了未定义的数据库 epoch | [安全与部署](security-and-operations.md)：删除 epoch 假设；通过停旧环境、撤身份、终止 session、轮换凭证、新进程 generation 与受限配置隔离，再执行领域恢复 | 已关闭 |
| 目录抽取容易引入 Plan↔runtime、foundation↔tooling 循环，或放大 MCP/Sandbox 权限 | [目录设计](repository-and-documentation.md)：唯一 Plan 纯 owner、runtime predicate 与生成工具分离、物理执行抽出 adapter；移动同时更新依赖、镜像、CI 与权限闭包 | 已关闭 |
| fresh-only baseline 与新迁移目标相冲突；旧文档或静态测试可能被误认为生产证据 | [ADR-0009](../../adr/0009-durable-kernel-and-agent-domain-boundaries.md)、[总 spec](README.md)：新决策明确提议调整条款，现有 ADR/baseline 保持；受控向前迁移，Not run 保持，实际角色测试缺环境必须失败 | 已关闭 |

上述修订对应的行为用例已进入各子 spec 的验收矩阵；用例尚未实现或运行，不能把“设计关闭”解释为缺陷已经修复。
主代理统一的目录清单包含独立 Outbox role，并按新增表审查要求说明公平状态拆分的必要性，不从目录整洁推导新业务 authority。

## 完整性检查面

| 检查面 | 共同核对的决定 |
|---|---|
| Ownership | Resource、Run、Invocation、Job、Task、Artifact 与 Outbox 各自唯一 authority；调度租户状态迁出而非复制 |
| Identities | definition、Program/compiler semantic、build、logical effect、attempt、owner pointer 与 exact secret/object generation 分离 |
| Schemas | 唯一 IR；版本化 bounded payload；新增查询列/索引和租户调度表走向前迁移；baseline 不改写 |
| Errors | 配额背压、依赖故障、版本不支持、正文禁止、历史不可用、不确定副作用与 cleanup 耗尽各自可识别 |
| Transactions | owner/current pointer、quota、Event/Outbox、Receipt 在规定事务与锁序内提交；外部 I/O 不持锁 |
| Events | Event 已提交事实、有限 SSE 和 replay floor；Outbox 至少一次投递、稳定去重身份、无业务清理状态 |
| Security | 当前授权按操作目的裁决；全正文出口一致；角色最小权限、Sandbox activation/boot/orphan 边界保留 |
| Capacity | 固定分区与实际容量区分；有界且持续推进的扫描；只计费已准入任务；critical-control 保留资源 |
| Recovery | 不确定副作用不自动重做；旧语义 worker 路由、清理恢复、联合 restore 隔离与迁移停写闭环 |
| Evidence | 行为/并发/恢复/越权证据归属明确；本地静态检查、真实集成、生产资格不相互代替 |

## 本次实际验证

文档校验使用当前离线 Cargo metadata 和 Git tracked inventory，核对 package identity、源路径、目标唯一性、脚本覆盖、
机器附件标记、JSON 重复键、本地链接、代码围栏与空白。校验脚本为本次临时工具，不是新的仓库规则或 runtime authority。

| 检查 | 本次结果及范围 |
|---|---|
| 目录 inventory | 45 个当前 workspace package 全覆盖，77 个受版本控制脚本资产全覆盖；8 个目标新 package 各有职责理由；source/extract_from 存在、目标 package 路径与名字唯一 |
| 文档/JSON/链接 | 通过；12 个新增/修改文档资产仅在 docs 内，本地链接、围栏、JSON、提案标记与空白检查均通过 |
| `check-platform-v1-contracts.py` | 通过；核对当前 fixtures、limits、OpenAPI 与 protobuf 基础合同 |
| `check-platform-postgres-schema.py` | 通过；核对未改动的当前 baseline，不验证提议的新 schema |
| `check-platform-observability-redaction.py` | 通过；核对当前 tracing redaction 合同 |
| `check-cutover-residuals.sh` | 通过；未重新引入已删除的活动边界 |
| Rust/浏览器/真实数据库/故障与生产资格 | 本轮未运行；无实现变更，上述设计行为仍需后续证据 |

## 实施门禁与退出

ADR-0009 保持 Proposed。实施前应把对应切片的 owning types、生成合同草案、migration 和部署合同与本设计共同评审，
精确接受修改的 ADR 条款后再开始消费者实现或 schema 应用。该项是下一阶段交付边界，本轮不要求用户再批准文档写入。

实施交付必须是完整可运行的单一路径；禁止旧目录 wrapper、双写或没有退出条件的隐式兼容 fallback。已承诺的旧 Run、
Draft、Receipt、reader 与外部副作用按明确的兼容和保留合同处理，不能通过直接删数据完成 clean cut。
实现、合同、current 文档和证据一致后删除本提案，持久决定留在 accepted ADR，实际贡献流程进入 engineering。
