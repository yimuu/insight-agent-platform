# M0：Current-to-Target Cutover Matrix

| 属性 | 值 |
|---|---|
| 状态 | Passed / repository clean cut |
| 日期 | 2026-08-29 |
| 目标 | M5 以一次仓库 clean cut 取代双入口，不做 public compatibility layer |

本表记录当前事实和最终归属。`Target` 是计划目标而非当前承诺；每一行都必须以 real-process 或
contract evidence 关闭，不能只删除文档链接。

| 面 | 当前证据 | M5 target | 退出证据 / 防回流 |
|---|---|---|---|
| 公共 profile | 旧 current 使用 `insight.agent/v1`；target contract 是 `insight.platform/v1` | 只保留 `insight.platform/v1` 和 `/v1` | `check-cutover-residuals.sh` 扩展为检查 public/profile/package/config/example；无 `/v2`、fallback 或 dual write |
| 首次启动 | 根 README 使用 `cargo run` + `config/platform.quickstart.yaml` | `insight doctor -> init -> dev` 启动 target base profile | fresh supported environment journey，命令、时间与 role readiness 报告归档 |
| 作者入口 | `agents/`、DSL fixture 与旧 management routes | `insight apply/run/watch` + 原始 `/v1` HTTP fixture | CLI 输出 exact Resource/Version/Deployment/Binding/Run IDs；HTTP fixture 和 API conformance 一致 |
| 默认 binary | root `insight-agent-platform` binary 与旧 workspace default member | `insight` CLI + target role binary 的显式 profile closure | 默认 build/image 清单及 root README 不引用旧 runtime；old source 不被 target dependency 引用 |
| 运行 authority | 旧 SQLite/current runtime 与 Platform PostgreSQL/worker target 并存 | Platform PostgreSQL + separated workers 为唯一默认 authority | fresh PostgreSQL、real-process restart、Run/Job/Task/Receipt evidence；不以 migration/adapter 复制状态 |
| 配置 | `config/platform.quickstart.yaml` 是当前配置 | `deploy/dev/base`、`deploy/dev/full` 生成 digest 固定的 non-production config | CLI config schema/permissions/digest tests；运行时无隐式 DDL 和 mutable default |
| 文档 | `docs/current` 是唯一 current，产品化 docs 属于 specs | M5 后 `docs/current` 只描述通过 golden journey 的 target 产品 | docs link/residual checker；历史当前文档移入 `docs/archive`，不作为正向示例 |
| CI | `.github/workflows/ci.yml` 在所有 push/PR 执行 workspace-heavy jobs；candidate 仅 `workflow_dispatch` | path-aware quick/affected/full/candidate lanes；candidate 保持手动/tag/deployment trigger | 连续主干 wall-clock、触发矩阵和 failed-cache diagnostic；docs/UI/CLI-only 不触发 image/signing |
| 镜像与 GitOps | candidate 有 signed exact digest，environment closure 是 `built_not_promoted` | 同一 exact digest 被 promotion 使用，M5 不重建 | CandidateManifest/ReleaseBundle digest closure；promotion 与 L4～L6 是独立环境决定 |
| 外部框架 | Python SDK 与 Agno adapter 已取消 | 一个固定 LangGraph.js remote Capability reference service | reference process 仅走 typed HTTP，不能读取 Platform DB 或被链接到 Gateway/Scheduler |

## 不可接受的中间态

以下状态不满足本矩阵，即使看起来“兼容”或更容易通过局部测试：

- 新、旧 runtime 同时作为默认 Quickstart 或 release image；
- CLI 通过 SQL、internal RPC 或特权 header 伪造 Resource/Run；
- 为旧 DSL 读写新 Resource/Deployment/Run 加 dual-write 或 long-lived adapter；
- Console 有专用 BFF/数据库或持有 worker/database credential；
- 以单进程 mock 替代 target role closure；
- 以 L4～L6 尚未运行为由保留仓库内双栈。

## M5 删除前检查

M5 必须先针对上述每一行提交 checked-in report，并通过：

1. `bash scripts/check-cutover-residuals.sh` 的增强规则；
2. target default build、base/full journey、public OpenAPI 和 docs link checks；
3. `rg` 结果的人工审阅，确认历史文件只在 `docs/archive` 或明确的非默认 archival source 中；
4. release image / deployment inventory 只引用 target binaries；
5. GitOps qualification status 仍独立标记，不把未执行的环境证据改写成仓库完成证据。
