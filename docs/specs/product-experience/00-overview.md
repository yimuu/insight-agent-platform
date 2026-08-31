# Agent 产品体验收敛规范索引

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-214 |
| 日期 | 2026-09-01 |
| 目标协议 | 保持 `insight.platform/v1` 与 `/v1` |
| 当前行为 | 不变；仍以 [`docs/current`](../../current/README.md) 为准 |
| 前置阶段 | [`productization`](../productization/00-goals.md) repository scope 已完成 |

本阶段把已经通过资格场景的平台内核收敛为普通开发者可使用的 Agent 产品。成功标准不是再增加
ResourceKind、Job、Worker、表或资格证据，而是让默认用户只需要理解 Agent、发布、Run 和结果。

## 1. 北极星旅程

安装预构建 `insight` 后，用户应能在不安装 Rust、Node.js、Kubernetes 或数据库客户端的前提下完成：

```text
insight init
insight dev
insight agent publish --file agent.yaml
insight agent run support-agent --input '...'
```

最后一个命令默认等待终态并直接输出结果。ResourceVersion、Deployment、Job、Receipt、ETag、cursor 和
内部 role 只在 `--verbose`、`--debug-authority` 或 Console 的“高级诊断”中出现。

## 2. 六份规范

| # | 规范 | 结果 |
|---:|---|---|
| 01 | [简化 Agent manifest](01-agent-authoring-manifest.md) | `agent.yaml` 编译为现有 exact Resource/Plan/Deployment 闭包 |
| 02 | [Agent CLI 旅程](02-agent-cli-journey.md) | `validate/publish/run/list/get/logs` 领域命令 |
| 03 | [Console authoring 与运行](03-console-authoring-and-run.md) | 从 Agent 列表到发布、运行和结果的浏览器旅程 |
| 04 | [渐进披露与内部概念隐藏](04-progressive-disclosure.md) | 默认摘要与高级 authority 诊断分层 |
| 05 | [预构建发行物](05-prebuilt-distribution.md) | CLI、Console 与镜像按 release 构建、签名和分发 |
| 06 | [轻量单节点开发模式](06-lightweight-development-profile.md) | 无源码编译、按功能启用、保持相同 `/v1` 契约 |

依赖顺序固定为 `01 -> 02/03 -> 04`，`05 -> 06` 为交付基础；03 不能自行发明与 01 不同的
Agent 编译语义。

## 3. 不变量

- 不新增 `/v2`、兼容层、双写或第二套 Agent runtime。
- 不新增业务表、current-state authority、Job/Task/Event/Receipt 种类或常驻服务角色。
- CLI 和 Console 只访问 public `/v1`；不得访问 PostgreSQL、内部 RPC 或 worker credential。
- 简化 manifest 是可确定编译的 authoring source，不是持久化 authority，也不直接执行脚本。
- Run admission 继续冻结 exact ResourceVersion 与 Deployment；简化只发生在调用方体验。
- Receipt、ETag、Operation 和 cursor 仍完整执行，只是由客户端在默认模式下代管。
- Python SDK 不属于本阶段。Node.js 只用于 Console 构建，不是用户安装或平台运行前置。
- 生产发布仍由 Kubernetes/GitOps 管理；本地 profile 不改变 L4～L6 `Not run` 状态。

## 4. 架构修订边界

本规范识别并由CR-207完成了三处ADR修订：

1. ADR-0004要求 `apply` 默认显示每个 Version、Deployment、Binding、Operation 和 Run ID；04改为默认隐藏、
   高级模式完整显示，但不改变 wire authority。
2. ADR-0005把 Console限定为最小运行/诊断界面；03将其扩展为 authoring + Run 产品入口，仍保持静态
   `/v1` 客户端边界。
3. ADR-0003只定义 `base/full`；06将开发体验 clean-cut 为默认 `starter` 加显式 feature closure。

03要求 Agent 和 Run 的安全分页列表读模型。它们必须由现有 Resource/Run authority直接查询并投影，不能新建
projection table。CR-207已按 17 -> 18 -> 00/cross-review 顺序修订 Platform v2 API 与资格矩阵。其余规范优先
复用当前路由，不新增公共 noun。

ADR-0003/0004/0005与Platform 17→18→00已由CR-207完成修订；cross-review确认没有新增business authority、表、
Job/Task/Event/Receipt种类或常驻role。本目录00～06进入Accepted并授权clean-cut实现。只有实现与适用仓库门禁通过后才能更新
`docs/current`或把状态推进为Implemented/Verified。

CR-209关闭实现前最后一个prompt authority缺口：`model_chat.spec.instructions`进入immutable Agent Revision的bounded
`author_instructions`，并在11/16的固定assembly序列中以`user` role、`trusted_instruction=false`投影。它不得进入platform safety、
Agent contract或Plan node instruction，也不能在Run时由active head、caller metadata或浏览器本地状态补取。

CR-210关闭`deterministic`模板的数据端口缺口：因为该模板没有转换节点，它只接受canonical digest相同的input/output schema，
并让Return消费该exact RunInput port。需要不同输出shape的Agent必须使用`model_chat`或高级Typed Plan。

CR-211冻结简化compiler的Interface contract与model requirement digest preimage，避免CLI/Console或恢复路径对opaque digest各自猜算法。

CR-212关闭产品summary的authoring name authority缺口：normalized `metadata.name`物化到现有Agent Resource document，创建后不可更名；
CLI lock只保存映射而不拥有name，Console/API不得用display name或Artifact内容猜测。

CR-213关闭产品summary的required feature authority缺口：compiler结果物化到现有Agent Resource document的closed sorted set，
草稿与发布态使用同一来源；CLI/Console/API不得从Plan Artifact、Deployment、lock或Event反推。

CR-214关闭Run输入默认的authority缺口：normalized input classification与default deadline seconds物化到同一Agent Resource/Revision，
CLI/Console在lock丢失、adopt或跨设备读取后仍可从exact服务端事实构造Run，不依赖隐藏默认或client-only profile。

## 5. 完成定义

本阶段只有同时满足下列条件才可关闭：

- fresh 用户只用北极星四条命令完成自定义 Agent 首次发布和 Run；
- 默认 CLI/Console 页面不要求输入 Version、Deployment、Job、Receipt 或 ETag；
- Console 可完成 Agent 创建/编辑/发布/运行/结果，不要求手工复制任何 ID；
- `starter` 冷启动不编译源码，warm start 达到 06 的时间与资源门禁；
- 同一 manifest 在 CLI 和 Console 编译出相同 canonical digest 与请求序列；
- advanced `/v1` lifecycle、崩溃恢复、租户隔离和 L1～L3 回归不退化；
- release artifact 可验证 checksum、签名、SBOM、provenance 与 exact image digest；
- `docs/current`、Quickstart、CLI help、Console 和场景证据在同一 commit clean-cut。

## 6. 明确不做

- Python/JavaScript SDK；
- 可视化任意 Plan 图编辑器；
- 插件市场或在线模板市场；
- 用自然语言在服务端生成未审计 Plan；
- 将单节点开发结果解释为 production capacity、HA 或 gVisor 资格；
- 为简化入口删除底层 durable authority 或弱化 fail-closed 行为。
