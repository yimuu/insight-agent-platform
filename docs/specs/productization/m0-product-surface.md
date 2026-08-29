# M0：产品面与进程 Inventory

| 属性 | 值 |
|---|---|
| 状态 | Baseline / verified from checked-in sources |
| 日期 | 2026-08-29 |
| 证据 | `contracts/platform-v1/openapi.yaml`、各 `crates/platform-*/Cargo.toml`、`deploy/helm/` |
| 当前行为 | 不变；本文不声称本地 profile 已经可运行 |

## 1. 公共产品面

`contracts/platform-v1/openapi.yaml` 是 target `/v1` 的唯一公共 HTTP 合同来源，已经包含如下产品
资源。M1～M4 的 CLI、HTTP 示例和 Console 必须只使用这些路由；不能用数据库、内部 gRPC 或另一个
管理 API 取得捷径。

| 面向用户的能力 | `/v1` 合同 | authority / 对应进程 |
|---|---|---|
| Resource、Draft、Version、Deployment | `/{resource_noun}` 及其 draft/version/deployment 子资源 | Gateway + PostgreSQL `resources/resource_versions/deployments` |
| Run、control、result、events、signal | `/runs` 及其子资源 | Gateway + Orchestration Worker + shared Run/Job authority |
| 人工 Task | `/tasks/{task_action}` | Gateway + shared Task authority |
| Artifact 与 Operation | `/artifacts*`、`/operations/{operation_id}` | Gateway/Artifact role + shared Artifact/Job authority |
| OAuth callback | `/mcp/oauth/callback` | Callback API；不是 Console 或 CLI 的私有回调 |

OpenAPI 当前仍标记 `implementing-not-current`。这个标记是 clean-cut 现状，不得被本阶段的示例或
Console 改写。M5 只有在 `docs/current`、默认发行物和 residual check 同时迁移后才能修改它。

## 2. 进程与隔离面

以下清单来自现有 workspace binary 与 Helm chart。它按 authority 边界分组，不是要求 `base` profile
一口气启动全部角色的列表。

| 平面 | 已有 binary / role | 首批产品化用途 |
|---|---|---|
| Public control | `platform-gateway`、`platform-callback-api` | Gateway 是 CLI/Console 的唯一业务入口；callback 只在 OAuth 场景启用 |
| Durable orchestration | `platform-orchestration-worker` | Run admission 后的 scheduler、node、Task、Subagent、恢复闭环 |
| Invocation lanes | `platform-model-worker`、`platform-capability-native-worker`、`platform-capability-remote-worker`、`platform-context-worker`、`platform-remote-context-worker` | 按黄金场景选择性启用；不能以 Gateway 内执行替代 |
| MCP | `platform-mcp-host`、`platform-mcp-resource-host`、`platform-mcp-discovery-worker`、`platform-mcp-subscription-worker`、`platform-mcp-cleanup-worker`、`platform-subscription-context-worker` | 仅 `full` profile 的 remote MCP/Context 场景 |
| Artifact | `platform-artifact-gateway`、`platform-artifact-data-worker`、`platform-artifact-maintenance` | Runtime Gateway 的 Artifact forwarder 与 Orchestration 的 Scheduler RPC 都在启动时强制连接前两者；因此它们以及真实 HTTPS S3/KMS-compatible dependency 属于 `base` closure。Maintenance 只在 `full` 的 Artifact lifecycle 场景启用；不在任一 profile 中伪造 object store。 |
| Egress/security | `platform-egress-broker`、`platform-security-authority` | remote Capability、Model、MCP 的网络和 Secret authority |
| Sandbox | `platform-sandbox-controller`、`platform-sandbox-attestor`、`platform-sandbox-executor`、`platform-sandbox-guest` | WASI 在专用 local/full 配置验证；gVisor 只做 preflight，真实 runsc 不在本地宣称通过 |

所有 role 必须继续使用自己的配置、连接池、permit 和 credential。不允许把多个 authority 合并进
`insight` CLI、Gateway 或 Docker Compose helper。

## 3. M1 profile 目标与未知项

| profile | 目标场景 | 必要依赖 | 现状与 M1 要求 |
|---|---|---|---|
| `base` | deterministic first Run、CLI/HTTP、Run event/read、重启恢复 | PostgreSQL 16、NATS、Management/Runtime Gateway、Artifact Gateway、Artifact Data Worker、最小 Orchestration/Native Capability/Registry Validation Worker role、显式 local OIDC、mTLS/local CA、真实 HTTPS S3/KMS-compatible test dependency | **已覆盖首条 P2 journey**：fresh authority 上的 Artifact -> Policy/Agent -> Run -> SSE/result 已通过，停止唯一 Orchestration Worker 后创建的 durable queued Run 可由同身份替代 Worker 恢复；NATS 已使用 fresh project CA、ServerAuth/ClientAuth 证书和 client certificate verification 收紧为 mTLS，且完整 base regression 通过。其余 M1 失败矩阵与 `full` profile 仍未完成。 |
| `full` | Model、remote Capability、MCP、Context、Artifact maintenance、WASI | `base` 加 Egress/Security、对应 worker、Artifact Maintenance 与其所需 lifecycle configuration | **实现中**。Context Native、Artifact Maintenance 与 Security Authority 已有 closed/digest-bound 配置、持久化动态端口，并已进入只对 full 生效的 release build/受监督 launch 规格；base 不额外构建这些二进制。Security Authority 另有独立 Egress service principal、ServerAuth/ClientAuth mTLS 资料且 fresh readiness 探针通过。三者尚未在一次 full journey 中共同启动，Egress Broker、MCP、Model、remote worker 与 WASI 闭包仍缺失。每个增量依赖只能由所需场景启用。 |
| `qualification` | gVisor preflight 与生产结构检查 | Kubernetes、`RuntimeClass=runsc`、restricted launcher RBAC、专用 node pool | 已有 static tooling；真实多节点 L4～L6 仍 Not run。 |

## 4. 当前启动与 bootstrap 事实

- 根 README 的 `PLATFORM_CONFIG=config/platform.quickstart.yaml cargo run` 启动的是当前旧 runtime，不可作为
  Platform `/v1` profile 的实现；
- Platform schema 由显式的 `platform-schema provision`（fresh target）或 CI 的
  `scripts/provision-platform-postgres.sh` 安装，并由 `platform-schema verify` 校验。运行时进程在启动时只验证
  schema，不能执行 DDL；重复 provision 会拒绝已有 authority，绝不尝试修补或覆盖它；
- `platform-bootstrap-operator` 需要明确的数据库 URL、installation operator ID、request ID 和三个 digest。
  M1 的 `init` 必须生成或要求这些 non-production 输入，不能以静态 privileged header 绕过 OIDC/principal binding；
- Gateway 需要 installed OIDC verifier、配置文件 digest、数据库连接和（runtime role）Artifact mTLS 资料。开发
  profile 必须生成 closed local config 及其 digest，不能以“development”分支关闭验证；
- PostgreSQL 与 NATS 已被现有 CI 当作真实服务依赖，`docker-compose.postgres.yml` 仅覆盖 PostgreSQL，不能
  误称为可运行 Platform profile。

## 5. M0 结论

M1 从 `base` profile 开始，目标是**最小的真实多进程 closure**，而不是启动现有 19 个 worker，也不是
复用旧单进程 runtime。profile 实现完成前，任何文档只能称其为 target，不能替换 `docs/current` Quickstart。
