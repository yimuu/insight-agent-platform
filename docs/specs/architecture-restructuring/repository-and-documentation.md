# 目录、依赖与文档重整

本文是[目标架构 spec](README.md)的一部分。目录结构尚未实施；[repository-layout.json](repository-layout.json)
是唯一完整移动/抽取清单，覆盖评审基线的所有 workspace package 和工具文件。
本文解释目录职责、依赖规则和移动约束，不复制该清单，也不改变公共 wire identity。

## 目标布局

```text
apps/
  insight-cli/                  # 公共客户端与本地 supervisor
  console/                      # 静态浏览器客户端
  services/                     # 可信角色 composition roots
  sandbox/                      # fixed runner 与 launcher
crates/
  foundation/                   # 小型公共身份/合同与安全观测基础
  definitions/                  # Plan IR、纯验证与 Registry 领域
  authoring/                    # 共享编译核心与薄 WASM binding
  execution/                    # Run 决策、Job、调度算法
  domains/                      # Model、Invocation、Context、Task、数据、安全等语义
  application/                  # 用例协调与受限 worker 机制
  protocols/                    # public/RPC DTO、协议与 trace 适配
  adapters/                     # PostgreSQL、Provider、Broker 等物理实现
  deployment/                   # 角色配置与发行资格组合合同
contracts/
  platform-v1/                  # 当前公开与进程边界生成资产
  product-experience/           # 编译与用户旅程的机器语料
  proto/                        # protobuf source，package/RPC identity 保持
deploy/
  images/                       # 构建入口，保持容器内 ABI
  release/                      # 发行/profile 的源资产
  dev/  helm/  kind/             # 运行环境与声明式部署
tools/
  checks/  release/  qualification/  development/
  rust/                         # 独立的合同与存储运维 composition
  tests/  baselines/
tests/
  qualification/                # owning Rust 集成/用户旅程 package
  fixtures/                     # 测试载荷，不能进入生产镜像闭包
examples/                       # 可运行公开示例
docs/
  current/  engineering/  adr/  qualifications/  specs/
```

根目录保留 workspace 与仓库工具必须定位的入口。GitHub workflow、Cargo manifest/lock、toolchain、license、README 和 AGENTS
不因视觉整洁而移动。`.github`、本地凭证状态、vendor 镜像/来源证据等分别保持工具与安全边界。

保留现有 Cargo package 和 binary identity；改变的是源文件位置、模块归属和依赖。新 library/tooling package 对应独立依赖或
编译目标的抽取边界，不自动产生业务 aggregate、数据库或服务。新增的 Outbox worker 则拥有独立投递生命周期和受限权限，
由部署合同明确增加运行角色；目录分组本身不决定 runtime 进程数量。

## 关键模块抽取

### Plan 与 Agent 编译

将 `RuntimePlan`、`RuntimeNode`、typed expression 定义及纯验证从运行推进模块中抽取为 `platform-plan`。
它是唯一 IR owner；原 `platform-orchestrator` 保留 Run、node、scope 及控制流推进决策。
若定义校验当前调用运行时辅助，拆出纯类型/关系函数；不能让新 Plan 包反向依赖解释器。

将 CLI 的纯 compiler 抽为 `platform-agent-compiler`，薄 WASM 包只负责内存边界与结果转换。
CLI 文件 I/O、身份、发布 journal、Receipt/CAS 等仍在 app；Console TypeScript 保留 UI 和 API adapter，删除语义编译实现。
模型/Context/Capability 决策使用现有领域包，不新增一个把所有领域重新包裹起来的万能 `agent-execution` facade。
子 spec 中的 `agent-execution` 是这些领域处理器的逻辑称呼。

### Foundation、运行合同与部署合同

foundation 保留跨层必需的 nominal identity、当前运行合同、有限角色词汇和纯校验。IR、作者语义、发行证据和模板生成各自归属，
不得因为调用方便全部重新 export 到 foundation。

将 Candidate、qualification、发行/development profile 组合及配置生成逻辑从基础合同抽出；运行期确实需要的角色 identity
留在运行合同 owner。部署合同可以引用基础身份，基础身份不能反向依赖部署或发行工具。
具体 source module 的拆分位置由清单记录；涉及 owning type 移动时，在上游修改所有真实引用，不保留旧模块转发层。

现有 `machine.rs` 同时存在运行时 registry predicate 与生成逻辑，必须按职责拆分。运行时使用的闭集及验证仍由原领域/合同类型拥有；
生成 OpenAPI、JSON Schema、fixture manifest 和检查命令的 composition 移到独立 tooling。
生成器可以依赖 Plan、作者、部署与领域合同；runtime 不能为了引用一个 enum 依赖全部生成器。

Rust-to-wire 表示优先从 owning type 生成。独立 conformance 测试可以维护审查过的预期闭集，生成器与消费者的输出自比不能替代该证据。
现有 schema `$id`、protobuf package、gRPC method、公开路径和错误值不能由目录名自动推导并改变。

### Repository 与物理适配

`platform-postgres` 保持数据库结构与 SQL 的统一 owner，但其内部按业务 owner 拆分窄命令、读取和 transaction helper。
将当前 `repository.rs` 中不同领域的事务实现归回对应 owner 模块，集中保留确实共享的锁、Receipt、Event、quota 和错误设施。
既有 specialized repository 可以复用；不通过文件重命名创建第二套 SQL 实现。

应用层移除 SQLx、raw SQL、Provider codec 和具体 PgRepository 依赖，只消费窄 port。
存储层移除物理 model/capability adapter 依赖，其所需请求/结果类型移入纯领域边界。
进程入口拥有装配权限，不因此拥有额外的业务 mutation API。

将 provisioning/schema/bootstrap/check 的 binary composition 移到 storage tooling；migration、grants 和 schema owning code
仍保留在 PostgreSQL adapter。只有 provisioning artifact 拥有 DDL 权限；运行角色只做只读验证。
移动 binary source 不授权把运维 binary 加进普通 runtime 启动闭包，也不删除当前合法的发行诊断入口。

Sandbox 的纯决策/端口留在领域包，`dispatcher.rs` 与 OpenSandbox 物理生命周期实现归到独立 executor adapter。
runner 和 launcher 留在独立 app，仍只拥有其固定协议与签名验证权限。
MCP 领域包保留 OAuth、session/资源语义与纯端口；trace/RPC forwarding、网络、Token transport 等实现归相应 protocol/adapter，
不能为了满足路径分类而让 domain 继续携带物理依赖。

Outbox worker 的 app 入口只装配投递用例、受限 PostgreSQL port 和 JetStream adapter，不导入领域业务命令或敏感正文 port。
新增 role 与 deployment contract、数据库 grants、subject ACL、profile、镜像及资格入口作为同一完整切片交付。
Outbox 记录继续承载投递进度；worker 不为每条消息创建 shared Job，也不把 ACK 当作业务完成。

## 允许的依赖方向

| 模块 | 可依赖 | 必须拒绝的依赖 |
|---|---|---|
| Foundation / Plan | 更小的纯身份、边界类型与纯算法 | apps、SQLx、Provider、部署 tooling、网络与进程能力 |
| Authoring core | Foundation、Plan、纯解析与规范化 | 文件/网络/时间/随机/credential、UI、repository |
| 领域与执行决策 | Foundation、Plan、必要的纯领域端口 | 具体 PostgreSQL、RPC client、Provider SDK、composition root |
| Application | 领域与执行端口、worker 通用机制 | raw SQL、物理 Provider codec、工具生成器 |
| Protocol / Adapter | owning 类型与端口、其物理依赖 | app 入口；跨权限的全能凭证对象 |
| App composition | 对应角色实际需要的 adapter 和 application | 超出该角色权限的执行路径和任意动态插件 |
| Tooling / Qualification | 所需生成器、adapter、测试端口 | 被生产 runtime 反向依赖；把 fixture 变成 fallback |

依赖检查继续读取真实 Cargo metadata，覆盖重命名依赖、dev/build edge、传递依赖和 feature union。
文件路径只用于组织，不是安全证明。新的分组与 package role 必须更新原有边界检查，禁止通过清空 baseline 接受意外能力。

## 路径移动的完整约束

清单中的每个移动单元都必须同时更新引用，不能先合入已知不可编译的空目录骨架。
实施时重新生成 source inventory 并比较评审基线，新增用户文件不得因清单陈旧被漏掉或覆盖。

必须检查 Cargo members/default-members、workspace dependency path、build script、`include_str!`/`include_bytes!`、proto include root、
Rust test `#[path]`、fixture 载入、Node workspace/lockfile、Docker COPY/build context、CLI source discovery、release profile、
脚本根路径计算、CI changed-path classifier/required results、签名 provenance source 引用与文档链接。
shell 脚本不再假定自身永远位于根目录下一层；使用统一且只读的仓库定位规则，失败时清楚报错。

qualification 的产品旅程源文件移入其 owning test package，结束对远端根目录 Rust 源的 `#[path]` 拼接。
独立示例仍留 examples，测试载荷留 fixtures。测试 schema 和 fixture 不能因 Docker context 扩大进入 Platform/Sandbox 生产 image。

现有生成合同资产保留单份。纯路径更新只改指向 owner 的引用，不改公共序列化语义；生成产物发生变化时必须解释原因。
发行 bundle 内路径、binary 名称、Container ENTRYPOINT 与静态 Console 路径保持不变，除非另有明确合同变更及 release journey 验证。

清理旧路径必须是真正删除源位置。禁止 symlink、转发 crate、旧脚本 wrapper、双 COPY 或两份生成资产作为移动后的兼容层。
需要保留的公开契约不是旧源码路径；应在真实目标实现中保持。

## 文档职责与导航

`docs/README.md` 提供按读者目的组织的入口及 authority 关系，不维护阶段完成数量、测试计数或独立资格声明。
`docs/adr/README.md` 提供 accepted/proposed 的导航；具体状态仍以各 ADR 为准。
`docs/specs/README.md` 只链接活跃目标提案，并明确它们不是当前产品手册。

`docs/current` 保留现有产品入口，目标切片实现后重写受影响页面，而不是旁边新增一套“新架构 current”。
architecture 解释控制、执行、数据、身份、版本和恢复的职责；CLI/Console/API 页面面向可观察产品行为；operations 区分本地开发、
发行与生产；observability 的人工处置与实现中的 owner、错误及告警对应。
必要时增加 execution、authoring 的专题解释，但不从 owning type 复制字段表或状态矩阵。

`docs/engineering` 在实现目录切换后建立，记录模块边界、贡献者构建、测试分组、生成合同流程、迁移与发行开发流程。
开发规则仍以根 AGENTS 为准；engineering 解释如何遵守，不重复建立第二审批或版本规则。
尚未实际迁移的命令和路径不提前写成贡献者的可运行指导。

`docs/qualifications` 只说明证据强度、实际执行范围与 Not run 边界。raw evidence 和签名报告由 CI/artifact store 保存，
文档链接其归属并说明环境，不在 prose 手抄 checksum、全场景目录或成功数字。

本提案的目录和设计附件在完成后删除；accepted ADR 保存耐久决策，current 保存实际行为，engineering 保存贡献者指导。
中途变更目标时更新同一提案和审查记录，不另造并存的第二 spec。

## 文档一致性验收

本次 spec 交付可以立即整理文档导航，并验证所有本地链接、机器附件路径和目标目录唯一性。
这不移动源文件，不更新已接受 ADR 的状态，也不宣布实现行为改变。

实施完成后的验收还包括：从根 README 到首次 Run 的命令可执行；current 中无旧 owner 或不存在的发布器处置；
engineering 路径与实际 workspace 一致；所有机器合同与 migration 链接指向唯一 owner；
文档中的安全和资格声明由 corresponding evidence 支持；旧提案仅保留于 Git 历史。
