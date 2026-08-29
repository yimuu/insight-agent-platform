# 产品化收敛阶段目标

| 属性 | 值 |
|---|---|
| 状态 | In Progress / M1–M2 |
| 日期 | 2026-08-29 |
| 阶段代号 | Productization Convergence |
| 合同输入 | Platform v2 spec00～18（Accepted / In Progress）、AGENTS.md |
| 目标协议 | `insight.platform/v1` 与 `/v1`，不增加 `/v2` |
| 当前行为 | 不变；仍以 [`docs/current`](../../current/README.md) 为准 |
| 实施计划 | [`implementation-plan.md`](implementation-plan.md) |

## 1. 阶段决策

下一阶段停止扩展平台内核的横向边界，集中把已经完成的高保证执行内核变成可理解、可启动、可集成、
可观察的产品。目标不是复制 Agno 的 Provider、Toolkit 或 cookbook 数量，而是让使用者以接近轻量 Agent
框架的成本，获得本项目已经实现的 durable authority、exact binding、故障恢复、MCP、Artifact 和受限
Sandbox 能力。

产品定位固定为：

> **Insight Agent Platform 是面向关键业务 Agent 的高保证 durable execution backend。**

CLI、HTTP 示例和控制台都是 `/v1` 客户端或静态产品界面，不成为新的业务状态 authority，不在可信
Control/Orchestration Plane 内执行用户代码，也不绕过 Resource -> ResourceVersion -> Deployment -> Binding
生命周期。

## 2. 为什么现在进入产品化收敛

仓库已经完成 Platform v2 spec00～18 的仓库范围实现与验证，但产品入口仍有五个明显断层：

1. `docs/current` 和根 Quickstart 仍描述旧 DSL runtime，Platform v2 只是目标合同，用户面对两个“产品”；
2. Platform v2 有多个隔离进程和依赖，却没有一个面向开发者的启动、诊断和停止入口；
3. 没有低摩擦的 CLI/HTTP authoring path，用户必须手工拼接 Draft、Version、Deployment、Binding、Receipt
   等底层步骤；
4. 没有可视化 Run、Task、Event、Trace 和 Artifact 的最小控制台；
5. 验证证据以合同和基础设施门禁为中心，尚未由一组用户可运行的端到端场景证明产品闭环。

继续增加 ResourceKind、Worker、表、协议或资格门禁不会关闭这些断层。因此本阶段冻结新的架构扩张，
用用户旅程决定实现顺序。

## 3. 目标用户与核心旅程

### 3.1 目标用户

- **应用开发者**：使用 CLI/HTTP 创建、发布和运行 Agent，不需要阅读 Rust workspace；
- **平台工程师**：以本地 profile 评估能力，以 Kubernetes/GitOps 管理正式环境；
- **运行值班人员**：从控制台定位 Run、Task、依赖、重试、Artifact 和安全拒绝原因。

本阶段不以无代码 Agent Builder 用户或追求最多第三方工具数量的个人开发者为首要对象。

### 3.2 北极星旅程

在一台满足受支持前置条件的干净开发机上，使用者应当能够：

1. 运行 `insight doctor`，得到可操作的前置条件诊断；
2. 运行 `insight init` 生成显式、可审查的本地项目和非生产配置；
3. 运行 `insight dev` 启动同一套 `/v1` 合同的最小多进程平台；
4. 运行 `insight apply` 与 `insight run`，显式完成 Agent 发布、Deployment、Binding 和 Run；
5. 在终端接收流式结果，并在控制台查看同一个 Run 的状态、事件、Task、Trace 与 Artifact；
6. 停止并重启本地平台后读取相同 durable Run，不依赖进程内存恢复业务事实。

`insight init/dev` 可以编排显式 schema provision 和本地依赖，但服务进程本身不得隐式建表。CLI 必须
展示所创建的 Resource、Version、Deployment 和 Binding 标识；所谓“简单”不能变成隐藏的可变默认值。

## 4. 阶段目标与可测结果

### G1：消除首次使用摩擦

- macOS 与 Linux 首批支持环境具备 `doctor/init/dev/status/logs/stop` 一致入口；
- 在已安装文档列明前置依赖的机器上，从 clone 到无外部模型密钥的 deterministic first Run 不超过
  10 分钟，人工命令不超过 3 条；
- 重复 `insight dev` 不重新构建未变化的 workspace 或镜像；源码、lockfile、profile digest 未变化时复用
  已有产物；
- 启动失败必须指出具体进程、依赖、端口、schema 或凭据问题，不只返回聚合错误。

### G2：交付稳定的 CLI/HTTP 开发者入口

- authoritative OpenAPI/JSON Schema 与实际 `/v1` route 具备自动 drift 检查；
- `insight apply/run/watch/task/artifact/operation` 覆盖 Resource lifecycle、Run、Task、Artifact 与异步
  Operation 的首次使用路径；
- `apply` 只编排显式 `/v1` 请求并返回每一步 authority ID，不引入第二套 Agent DSL、不推断 Secret、
  不静默覆盖 active head；
- 为完整生命周期交付可复制的 curl/HTTP 示例和 closed request fixtures；
- CLI 保留平台 `problem` code、request ID、Receipt、retryability 与 CAS 冲突，不把错误压缩成普通字符串。

### G3：交付最小运行控制台

- 在 `web/console/` 交付静态 Web 控制台，只通过 public `/v1` 和受支持的事件流访问平台；
- 首批页面限定为 Agent/Deployment、Run timeline、Task inbox、Trace/diagnostics、Artifact；
- 支持人工审批或输入 Task，所有 mutation 使用版本/Receipt 合同并显示最终 authority 结果；
- 默认不展示 Secret、credential、原始敏感 prompt/tool body 或未脱敏远端错误；
- 本阶段不实现拖拽式 Agent Builder，也不创建 Console 专用业务数据库或后端 authority。

### G4：以十条黄金场景证明产品闭环

每条场景必须包含可复制命令、输入、期望结果、失败诊断和自动 smoke，不只是一段代码片段：

1. 无模型密钥的 deterministic Agent 与流式 Run；
2. exact Model binding 的流式对话；
3. Native 与 remote HTTP/gRPC Capability 调用；
4. remote MCP Tool 与 Resource；
5. Context 检索、citation 与数据集 generation；
6. approval/input Task 暂停与恢复；
7. timer/signal、进程重启和 durable resume；
8. typed Subagent child Run、配额和取消传播；
9. Artifact 上传、生成、读取与拒绝路径；
10. restricted WASI 与一个 Agno/LangGraph remote Capability 参考集成。

本地开发机无法诚实证明 gVisor 多节点隔离；对应场景只检查合同、manifest 和 preflight，真实 runsc 仍由
[`Platform v2 Production L4～L6`](../../qualifications/platform-v2-production-l4-l6.md) 资格门禁负责。

### G5：复用生态，而不是复制生态

- 定义普通 HTTP/gRPC 或 MCP remote Capability 模板，使 Agno、LangGraph 等框架运行在独立服务或受限
  Sandbox 后方；
- 框架不能作为 Platform Gateway、Scheduler、Worker 或 durable authority 的进程内插件；
- 首批只维护 2 个高质量参考适配，不以 Provider/Toolkit 数量作为阶段完成指标；
- 外部框架的可变状态、Secret 和网络访问必须服从 exact Deployment、Egress、Policy 与 Artifact 边界。

### G6：降低反馈时间与维护成本

- 普通 PR 的快速反馈 lane 目标不超过 10 分钟，workspace 全量验证目标不超过 30 分钟；
- 文档、CLI 或控制台单独变化不触发 production candidate 镜像构建与 cosign 签名；
- candidate 镜像只由手动发布、tag 或影响 runtime/deployment 的主干变更触发，同一 revision 的 Rust
  release binary 只编译一次并供镜像复用；
- BuildKit/Cargo 缓存键、产物 digest 和失效原因可观察，缓存 miss 不能静默退化为每次全量重建；
- 不新增超过 2,000 行的单文件；修改现有超大热点时按 authority/领域提取模块，不进行无验收收益的全仓重写。

时间预算是工程目标而非发布承诺；CI 必须记录实际 wall-clock 与 cache hit 数据后才能声明达成。

## 5. 阶段护栏

本阶段默认冻结以下扩张，只有黄金场景遇到 P0/P1 合同阻塞且完成受影响 spec cross-review 后才可例外：

- 新的 public API version、ResourceKind、JobKind、WorkClass、业务表或 current-state projection；
- 新的独立平台 role、消息系统、Sandbox backend 或 Model Artifact producer；
- managed MCP stdio、persistent Sandbox session、microVM、host/runc code execution；
- 为兼容旧 DSL 与 Platform v2 增加 dual write、fallback 或长期 compatibility layer；
- 为追平其他框架而批量增加 Provider、Toolkit、模板或抽象层。

## 6. 非目标

- 不在本阶段执行或宣称真实多节点 Kubernetes、runsc、容量、混沌、restore、24 小时 soak 或 production
  GitOps promotion；
- 不宣称 production-ready 或发布 CapacityProfile；
- 不提供与 Agno、LangGraph 等框架等量的 integrations/cookbook；
- Python SDK 已从产品化收敛阶段取消，不属于当前里程碑、黄金场景或退出门禁；未来如需重启，必须另立目标并基于稳定的 `/v1` 合同重新评审；
- 本阶段也不交付 JavaScript 或 Go SDK；它们不构成 CLI、HTTP、控制台或黄金场景的前置条件；
- 不把 Platform v2 改名为 `/v2`，不保留两个 public runtime；
- 不为了演示而使用内存 repository、mock authority 或服务启动时自动建表替代 durable 路径；
- 不在完成产品化退出门禁前修改 `docs/current`，使其描述尚不存在的产品行为。

## 7. 完成定义

只有同时满足以下条件，Productization Convergence 才能关闭：

1. 北极星旅程在 fresh supported environment 中可按文档复现，并保存机器可读报告；
2. 十条黄金场景均通过 public `/v1`，其中至少一条证明跨进程重启后的 durable resume；
3. CLI、HTTP fixtures、控制台和示例具备版本、测试、安装说明和安全负向用例；
4. 用户无需理解 Rust workspace 即可完成首次 Run、Task 操作和故障定位；
5. CI/image/signing 触发策略满足 G6，普通改动不再无条件重建并签名全部 candidate；
6. clean cut 审计确认 default build、根 README、`docs/current`、示例和发行物只指向新 `/v1` 产品；旧实现
   已移出默认构建/发行并归档，不存在双栈或 fallback；
7. specs、实现、当前文档和 conformance tests 无 P0/P1 冲突。

第 6 项只关闭仓库内产品入口的 clean cut。目标生产环境的 promotion/rollback 与 L4～L6 资格仍是独立
部署决策，不因本阶段完成而自动通过。
