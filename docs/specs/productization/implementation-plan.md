# 产品化收敛实施计划

| 属性 | 值 |
|---|---|
| 状态 | In Progress / M1–M4 |
| 日期 | 2026-08-29 |
| 目标 | 执行 [`00-goals.md`](00-goals.md) 的 Productization Convergence |
| 合同基线 | Platform v2 spec00～18（Accepted/In Progress；CR-203 Agent publication identity revision） |
| 当前行为 | 不变；每个批次完成并取得 conformance evidence 后才可更新 `docs/current` |

## 1. 实施原则

实施顺序固定为“可启动 -> 可调用 -> 可观察 -> 场景闭环 -> clean cut”，不按现有 crate 或组件逐个包装。
每个 milestone 必须产生一个用户可运行的增量，并保持 control plane、durable orchestration plane 与
untrusted execution plane 分离。

任何实现反馈若要求改变 Platform v2 observable contract，先停止该批次，按 upstream -> downstream 修订
受影响 spec 并完成 00～18 cross-review；CLI、HTTP 示例和控制台不得自行创造第二套语义。

## 2. 交付物布局

以下为本阶段预期产品面，不代表当前已存在：

| 路径/产物 | 职责 |
|---|---|
| `crates/insight-cli/` / `insight` | 本地项目、依赖、进程、诊断与显式 provision 编排 |
| `deploy/dev/` | digest 固定的本地 base/full profiles 与依赖清单 |
| `web/console/` | 只访问 public `/v1` 的静态运行控制台 |
| `examples/productization/` | CLI/HTTP 首次使用路径、十条黄金场景及固定输入 |
| `tests/productization/` | fresh environment、journey、restart 与安全 smoke |
| `docs/current/` | clean cut 后的 Quickstart、CLI/HTTP、Console、场景和运维文档 |

控制台作为静态产物由现有 Gateway/Ingress 产品面承载，不新增 Console 业务服务或数据库。若静态托管需要
独立 Web server，它只能托管不可变文件，不能拥有业务权限或状态。

## 3. Milestone M0：基线、决策与扩张冻结

M0 的已审查输入保存在：

- [产品面与进程 inventory](m0-product-surface.md)；
- [current-to-target cutover matrix](m0-cutover-matrix.md)；
- [CI 与候选构建基线](m0-ci-baseline.md)；
- [`examples/productization/scenarios.json`](../../../examples/productization/scenarios.json)；
- ADR-0003～0006（CLI/profile、HTTP authoring、Console、local identity）。

### 3.1 工作项

1. 生成产品面 inventory：public `/v1` route、required process role、依赖、schema provision、开发凭据和
   每条黄金场景的能力映射；
2. 建立 current-vs-target cutover matrix，逐项列出根 README、`docs/current`、旧 binary、旧 schema、示例、
   CI 和发行物的最终归属；
3. 记录首次 Run 与现有 CI/candidate pipeline 的 wall-clock、Cargo/BuildKit cache hit、镜像层和签名等待基线；
4. 为 CLI packaging、CLI/HTTP authoring UX、Web 技术栈、静态托管和本地身份 profile 写短 ADR；
5. 将十条黄金场景固化为 machine-readable manifest，包含所需 profile、能力、外部依赖、Secret、预期终态、
   restart point 和禁止 skip 的门禁；
6. 在贡献检查中加入本阶段护栏：未经 cross-review 不允许新增 public version、kind、work class、表或 role。

### 3.2 退出门禁

- inventory 覆盖全部北极星请求经过的 Gateway、worker、broker、repository 与 Artifact/Sandbox 边界；
- 每条黄金场景都有唯一 owner、依赖、fixture 和自动化层级；
- CI 基线报告可区分编译、镜像、push、cosign、attestation 和排队时间；
- ADR 与 cutover matrix 通过 owner review；没有为了产品面修改业务合同。

## 4. Milestone M1：`insight` CLI 与可复用本地平台

状态：**In Progress**。当前实现仅覆盖 base profile 的 `doctor`、`init`、`token`、`dev`、`status`、`logs`、`stop`，
以及 fresh PostgreSQL provision/bootstrap、真实 HTTPS S3/KMS fixture 和七个独立 Platform role。role
监听端口由本地 profile 分配，未变更源码时复用已构建的 release binaries。`stop` 停止 Platform roles 但保留
LocalStack Community dependency process，因为该固定版本不能在容器重建后恢复本 profile 的 S3/KMS authority；
依赖容器销毁明确不属于本地 durable restart。`full` profile 和本里程碑的其余
退出门禁仍未完成；不得据此标记 M1 或 spec 00–18 为
Verified。

2026-08-29 macOS P2 journey 已在 fresh PostgreSQL/LocalStack 上只通过 public CLI 完成 Artifact upload、Policy/Agent
publication、deterministic Run、durable SSE 与 Inline result。随后测试停止唯一 Orchestration Worker，在 Worker 缺席时
创建第二个 Run 并观察 PostgreSQL authority 保持 `queued`，再以同一配置和 workload identity 启动替代 Worker；第二个
Run 恢复为 `succeeded`。同一 fresh P2 journey 随后停止全部 Platform roles，保留固定依赖 authority process，重新执行
schema verify 与 exact development bootstrap replay，再启动七个 roles；旧 HumanTask Run 仍可按 ID 读取为 `succeeded`，
profile/build state 字节与 build-state mtime 均未变化，证明没有 release rebuild 或 identity/port/key rotation。该证据覆盖
应用角色重启，不宣称 LocalStack/KMS 基础设施销毁恢复或 production restore。

该 base 证据已由 [`run-productization-base-journey.sh`](../../../scripts/run-productization-base-journey.sh) 固化为单一可复制
入口：它执行 release CLI build、`doctor -> init -> dev -> status`、真实 P2 journey 与 `stop`，默认关闭精确 Compose
dependency project 但不删除卷，并保留 fresh project 供审计。显式 `--report-directory` 只允许 clean Git worktree，设置
fresh-profile evidence 环境并运行 manifest-aware partial checker；脚本不会把缺失 Console 的 incomplete report 升级为 Passed。

2026-08-29 fresh macOS P1 探针已证明 `doctor` 通过、`init` 可创建尚不存在的 project root，且真实 provision 后
Artifact Data/Gateway、Native Capability、Management/Runtime Gateway、Orchestration 与 Registry Validation 七个独立
role 全部 ready；`stop` 正常收束并保留 PostgreSQL/LocalStack 卷。探针同时发现并闭合 Agent public authoring P0：CR-203
将 typed Plan clean-cut 升级为 v5，以 Draft 已知的 `interface_contract_digest` 取代 publish 前不可知的 server-generated
Interface Revision ID；Deployment 与 materialization 分别重验 exact Interface/Plan 同 Agent、同 publish batch 和合同 digest。
真实 PostgreSQL 已覆盖 Resource lifecycle、Run kernel、Context、Model Turn、Capability Invocation 与独立 Orchestration
Coordinator，并包含 owner/batch/digest 漂移 fail-closed 断言。该闭环只解除首次 Run 的合同阻塞；public first Run 本身和
restart recovery 后续已由上述 P2 journey 补齐，但其余 M1/M2 门禁仍未完成。

已补齐的前置闭环：`platform-registry-validation-worker` 以独立 `registry_validation` pool 和 tenant-scoped
`ServiceIdentity` claim Job；成功路径在同一 PostgreSQL transaction 写入不可变验证摘要、Resource、Job、Event、
Outbox 和 Receipt，且保留 Job 原始 payload 供 public Operation 投影使用。它不会通过直接 CLI 数据库写入、Gateway
内联验证或伪造 `ValidationSummary` 绕过 authority。`insight apply` 已提供 public authoring/lifecycle 入口；仍缺后续
M1 门禁，故不得宣称 M1 或 spec 00–18 已 Verified。

### 4.1 工作项

1. 新建 Rust CLI，首批命令固定为：
   - `insight doctor`：检查容器运行时、端口、磁盘、架构、凭据、runsc 可用性和版本；
   - `insight init`：生成 closed local config、project ID、非生产身份和 profile pin；
   - `insight dev`：显式 provision 后启动 base/full 多进程 profile；
   - `insight status/logs/stop`：按 role 显示 readiness、依赖和退出原因；
2. `deploy/dev/base` 覆盖 deterministic first Run 的完整启动 closure：Management/Runtime Gateway、Artifact
   Gateway、Artifact Data Worker、HTTPS S3/KMS-compatible test dependency、最小 Orchestration/Native Capability、
   Registry Validation Worker role 与基础 authority。`full` 再按场景增加 Model、MCP、Context、Artifact Maintenance、Egress/Security 与 WASI
   所需现有 role。profile 不能用单进程 mock 替代 durable authority；
3. schema 安装作为可见的一次性 CLI step 调用现有 schema tool，服务进程继续保持零 DDL；
4. 使用 source/lockfile/profile digest 决定构建，复用 Cargo target、OCI layer 或已发布 dev image；未变化时
   `insight dev` 不执行 release rebuild；
5. readiness 汇总必须保留每个 role 的错误，输出下一条可执行修复命令；
6. 增加 interrupt、重复启动、端口占用、部分依赖失败、进程退出和重启后的幂等测试。

### 4.2 退出门禁

- fresh macOS 与 Linux runner 通过 `doctor -> init -> dev -> first Run -> stop`；
- deterministic first Run 无需外部 API key，总用时和人工命令数满足 G1；
- 第二次启动在源码和配置未变时不编译 release workspace、不构建 candidate 镜像；
- 停止一个 orchestration worker 后 Run 可由 durable authority 恢复；
- `doctor` 对缺少 runsc 明确标为 production qualification unavailable，不伪装成 gVisor 已验证。

## 5. Milestone M2：CLI/HTTP authoring 与首次 Run

状态：**In Progress**。CLI 已增加 bounded native public HTTP client、
`insight operation wait <job_id>` 和 closed [`insight apply`](m2-cli-apply.md) 首条 Resource lifecycle。客户端只连接
本地 profile 生成的 loopback Management Gateway，使用短期 OIDC token，禁止 redirect/proxy，限制 request/response
body 与 timeout，并重验 authority ID、tenant、ETag、trace、cache-control 及 closed `ApiProblem`。`apply` 已执行
create -> validate/wait -> read validated Draft -> publish -> create Deployment -> activate，并从 publish authority 响应
填入 self Version binding；它不接受调用方伪造尚未创建的 Version ID。CLI 已为 canonical manifest 建立 bounded closed
intent/result journal，在每个 mutation 前持久化 Receipt/If-Match，并通过 response-loss fixture 证明精确重放与完成后
零网络恢复。首次 Run 已由 fresh P2 journey 覆盖；完整失败矩阵和其余 M2 退出门禁仍未完成，因此不得标记 M2 或 spec 00–18 为
Verified。

[`insight run`](m2-cli-run.md) 已增加 create/get/pause/resume/cancel/result/watch 命令面，并严格区分 Runtime Gateway
与 Management Gateway。create 使用 canonical request Receipt，control 使用 current ETag + Receipt，result 对 Inline
content digest 或 Artifact digest/classification 做重验；watch 按 opaque Last-Event-ID 重连 durable SSE，逐条校验、
输出和 flush，直到 Run authority terminal。control 已在 mutation 前持久化 exact Receipt/If-Match，并由 response-loss
fixture 证明跨调用精确重放。命令级 fixture 已覆盖 control 的 409/412/429/503 closed Problem、
result-not-ready、SSE cursor invalid/expired/backpressure 以及 failed terminal watch，保留 problem code、retryability、
retry-after 与 CAS headers；parser 同时拒绝截断、重复和 oversized page。真实 first Run 与 Orchestration Worker restart
已由 P2 journey 覆盖；bounded watch timeout 也已证明不产生 mutation 或伪终态。Run 命令面的既定负向矩阵已闭合，
M2 仍受 Task、Artifact 与完整 HTTP fixture 门禁约束。

[`insight task`](m2-cli-task.md) 已增加 get/submit-input/approve/reject/cancel，只通过 Runtime Gateway 读取和提交
Interaction/Approval Task。mutation 使用 current ETag、deterministic Receipt 和 bounded closed intent/result journal；
response-loss fixture 已证明未决提交跨 CLI 调用精确重放，完成后只读 authority state。fresh P2 journey 已证明
waiting Task -> submit-input -> Run resume，并在终态后重放同一 CLI journal 得到同一 authority Task；旧 ETag 搭配新
Receipt 的 raw `/v1` mutation 被 closed 409 `invalid_state_transition` 拒绝。该子旅程已提取为独立 fixture，并生成
`approval-task-resume` 的 machine-readable incomplete report；仅因 Console 入口仍未运行而不升级为 Passed。命令级
fixture 又覆盖 409/412/429/503、错误 assignee 的 403、expired submit-input，以及 approve/reject/cancel 的成功与冲突
矩阵。Task 命令面的既定负向矩阵已闭合，M2 仍受 Artifact 与完整 HTTP fixture 门禁约束。

[`insight artifact`](m2-cli-artifact.md) 已增加 upload/get/read：upload 以本地计算的 exact size/digest prepare，使用不
携带 OIDC 的独立 no-proxy/no-redirect HTTPS client PUT，再 complete、等待 ArtifactVerify Operation 并重验 Ready
content；read 按 exact ArtifactRef 校验长度、media type、SHA-256 和 content ETag，最后 no-clobber 原子落盘。
upload 已使用 bounded `0600` crash-safe journal 固定 canonical request、prepare/complete Receipt、If-Match、同一
Artifact/Operation/Grant identity 与单调 effect phase。loopback response-loss fixture 已证明 complete 已接受但响应丢失时，
第二次命令只重放同一 complete、不会再次 PUT 或创建第二个 Artifact，完成后再次调用只读同一 Ready authority；临近过期
target 只能通过同一 prepare Receipt 刷新同一 generation。完整负向矩阵尚未完成；fresh 真实 S3/KMS upload 已由
deterministic first Run P2 journey 覆盖。

### 5.1 工作项

1. 以 authoritative Platform `/v1` OpenAPI/JSON Schema 校验实际 route、request、response、Problem 与样例，
   生成过程可重复且检查 drift；
2. 扩展 CLI，提供 `insight apply`、`run create/watch/control/result`、`task resolve`、
   `artifact upload/read` 与 `operation wait`；
3. `insight apply` 接受 closed、版本化的 `/v1` request manifest，显式执行 Draft -> validate -> publish ->
   Deployment -> activate 并逐步返回 authority ID。manifest 不是新的 Agent DSL，也不能携带自由 shell/URL/Secret；
4. 实现 OIDC/dev auth、Problem、Receipt、cursor、SSE reconnect、Operation wait 和 effect-aware retry；
5. 提供与 CLI 同路径的 curl/HTTP lifecycle 示例，固定 headers、Receipt、If-Match、请求正文和预期 Problem；
6. 日志默认不记录 token、Secret、prompt 或 Artifact 内容，并增加 CLI 与 OpenAPI drift 测试。

### 5.2 退出门禁

- 北极星路径通过 CLI 完成并只使用 public `/v1`，相同生命周期具备可复制的原始 HTTP fixture；
- CLI/HTTP contract tests 覆盖正常、409/412 CAS、429 backpressure、Operation terminal、SSE reconnect、Task replay、
  Artifact digest mismatch 与 closed Problem；
- 同一 Receipt 重放返回相同 authority 结果，不会创建第二个 Run/Task/Artifact；
- CLI 断开、平台 worker 重启后仍可按 Run ID 恢复观察。

## 6. Milestone M3：最小运行控制台

状态：**In Progress**。首批 [`web/console/`](../../../web/console/) React/Vite 静态客户端已经实现
readiness、Agent/Deployment、Run timeline/control/result、Task resolve、Artifact metadata/download 与
Operation safe projection。它只访问 public `/v1`，OIDC token 仅存在浏览器内存，mutation 携带 ETag/Receipt，
SSE 使用 opaque cursor，DOM 投影执行 closed sensitive-field redaction。10 个 Node P0 test、严格 TypeScript build
和 lint 已通过；真实浏览器 journey、restart/replay、敏感 fixture 与静态部署门禁仍未完成，详见
[`m3-console.md`](m3-console.md)，故不得标记 M3 或 spec00～18 为 Verified。

### 6.1 工作项

1. 实现登录/tenant context 与平台 readiness 页面，不在浏览器持久化长期 Secret；
2. Agent/Deployment 页面展示 immutable version、active binding 和 readiness，不提供绕过生命周期的快捷写入；
3. Run 页面展示状态、node/timeline、Event、重试/等待、Subagent 链、结果和 redacted diagnostic；
4. Task inbox 支持 approval/input 提交，显示 Receipt、最终 Task version 和冲突；
5. Artifact 页面只展示允许的 metadata、grant 状态和受控下载；Trace 页面遵守低敏投影；
6. 使用 SSE 增量更新，断线后按 cursor/replay contract 恢复；浏览器不能直连内部 RPC 或数据库；
7. 增加 keyboard/accessibility、空状态、错误状态、慢依赖和敏感字段负向测试。

### 6.2 退出门禁

- 使用者可在控制台完成“发现失败 Run -> 定位等待 Task -> 提交 -> 查看终态”闭环；
- 页面刷新或 Gateway 重启不丢失 authority state；
- checked fixture 中的 Secret、credential、原始受限正文和内部错误不会出现在 DOM、日志或 telemetry；
- 控制台没有 Console 专用业务表、写代理或内部 RPC 凭据。

## 7. Milestone M4：十条黄金场景与生态复用

状态：**In Progress**。已定义 closed
[`scenario-report/v1`](../../../examples/productization/scenario-report.schema.json) 与 manifest-aware
[`report checker`](../../../scripts/check-productization-scenario-reports.py)。严格门禁要求同一 exact Git revision 的十份
fresh-profile 报告全部 Passed，并逐项重验 entrypoint、assertion 与 failure probe；缺失、skip、`not_run` 或未知字段
均失败。现有 deterministic P2 journey 可产出明确标注剩余 HTTP/Console/故障探针的 `incomplete` 报告，详见
[`m4-golden-scenarios.md`](m4-golden-scenarios.md)；同一 fresh authority 的独立 Human Task fixture 也会产出第二份
`approval-task-resume` incomplete report。这不是 M4 完成证据，其余八条 fixture/report 与 full profile
仍未交付。

### 7.1 工作项

1. 按 goals G4 顺序交付 scenario manifest、示例、自动 smoke、故障注入点和用户文档；
2. 每条场景必须通过 CLI 和控制台可观察，并保留原始 HTTP fixture 证明两者未改变合同；
3. 为 Agno 与 LangGraph 各交付一个 remote Capability reference service：
   - 作为独立进程使用 typed HTTP/gRPC 或 MCP 合同；
   - 接收 bounded input，返回 Inline 或通过平台 Artifact port 交付大结果；
   - 网络、Secret、timeout、quota、retry/UnknownOutcome 由 Platform Deployment/Policy 冻结；
4. 对 durable resume、Subagent cancel、MCP remote error、Artifact mismatch 和 WASI limit 执行真实失败路径；
5. 建立 scenario compatibility table，记录受支持版本和最后通过的 commit/digest，不写“支持全部生态”。

### 7.2 退出门禁

- 十条场景全部在 fresh base/full profile 上取得 machine-readable passed report，禁止 required scenario skip；
- 至少一条场景在外部 effect 已发出、worker 在 commit 前退出后按 effect-aware 合同收敛；
- 两个框架适配均不链接进 Gateway/Scheduler/Worker，不获得数据库凭据；
- 故障场景能通过 CLI/控制台给出用户可操作诊断，而不是要求阅读数据库或 Rust backtrace。

## 8. Milestone M5：CI 收敛与仓库 clean cut

状态：**In Progress**。普通 CI 已实现 quick、CLI/Console affected、workspace full、MCP interop 与 dependency
policy 的 closed path classifier；未知路径 fail closed，手动/weekly run 强制全部普通 lane，并由稳定 summary 汇总。
production candidate 仍仅由独立手动 workflow 触发，普通 CI 不包含 image push、cosign 或 attestation。Dockerfile
保持单次 workspace Cargo build graph。尚缺连续主干 wall-clock/cache evidence、十条场景前置与 repository clean cut，
因此不得标记 M5 完成。

### 8.1 CI 与供应链

1. 将验证分为 path-aware quick、affected component、workspace full、candidate release 四类 lane；
2. docs/CLI/console-only 变更不触发 runtime image build、registry push、cosign 或 provenance；
3. candidate workflow 只由显式发布、tag 或影响 runtime/deployment 的主干变更触发；
4. 使用一次 `cargo build --locked --release --workspace --bins` 或等价构建图产出所需 binary，镜像阶段只复制
   已验证产物，不为每个 binary 启动独立 Cargo build；
5. 在 BuildKit cache mount 与 CI cache 中复用 registry、git、target 和 dependency layers；记录 miss reason；
6. dev image 不要求 keyless signing。production candidate 的 cosign/provenance 仍 fail closed，但设置明确 timeout、
   OIDC/网络诊断和可重试边界，不允许无限等待；
7. 同一 commit 的 image、SBOM、provenance、signature 与 GitOps candidate 使用同一 exact digest，后续 promotion
   不重建镜像。

### 8.2 Clean cut

1. 全量运行 current-vs-target matrix，确认 root README、默认 binary、示例、config、CI、image 和部署只指向
   `insight.platform/v1`；
2. 更新 `docs/current` 为已通过十条场景的产品行为，并将旧 DSL/runtime 文档、计划与历史资格移入 archive；
3. 从 default-members、默认 image 和发布拓扑删除旧实现；如仍保留历史源码，只能是非默认、无目标代码依赖的
   archive，不得形成可运行双栈；
4. 运行 residual checker，禁止 `insight.agent/v1`、旧 schema、旧 parser 或 compatibility fallback 回流；
5. 生成仓库 clean-cut 报告，明确 production L4～L6 与 GitOps promotion 仍未自动执行。

### 8.3 退出门禁

- G6 的 CI wall-clock 与触发条件由连续主干 run 数据证明；
- 普通 PR 不再无条件构建、推送和签名全部镜像；
- 根 Quickstart、CLI/HTTP、控制台与十条场景只走新 `/v1`；
- 旧、新 runtime 不同时出现在 default build、发行镜像或 current documentation；
- workspace checks、public contract checks、fresh PostgreSQL、关键 L2/L3、journey suite 和 residual checker 全部通过。

## 9. 依赖与关键路径

| 前置 | 阻塞的工作 | 处理方式 |
|---|---|---|
| Platform OpenAPI 与实现 drift | CLI、Console | M0 建立生成/校验门禁，先修 owner contract 或实现 |
| 本地身份和 Secret profile | CLI、Console | ADR 固定 non-production identity；禁止 production default fallback |
| 最小 role closure 不清楚 | `insight dev` | 从黄金场景反推 required role，不合并 authority |
| Artifact/S3/KMS 本地依赖过重 | base profile | Runtime Gateway 和 Orchestration 的现有启动闭包要求 Artifact；base 使用显式 digest-pinned、真实 HTTPS-compatible local dependency，不能用 mock 或将失败隐藏为可运行 first Run |
| gVisor 在 macOS/普通 CI 不可用 | Sandbox 场景 | 本地验证 WASI 和 runsc preflight；真实 gVisor 留给 L4～L6 |
| 旧 current 与新 `/v1` 名称冲突 | README/发行 | M5 一次 clean cut；M1～M4 不提前声称 current |

关键路径为 M0 -> M1 -> M2 -> M4 -> M5。M3 可在 M2 的稳定 OpenAPI 和 auth contract 完成后并行推进，
但不得绕过 public HTTP contract 单独发明 Console API。

## 10. 验证矩阵

| 层级 | 证明内容 | 必须环境 |
|---|---|---|
| P0 | CLI/Console unit、schema generation、redaction | 普通 CI |
| P1 | public `/v1` contract、Receipt/CAS/SSE/Problem | 真实进程 + fresh PostgreSQL |
| P2 | base/full profile journey 与 restart recovery | 容器化本地/CI runner |
| P3 | 十条黄金场景、remote framework、Artifact/WASI 负向 | 专用 integration runner |
| P4 | repository clean cut、default build/image/docs residual | release candidate workflow |

P0～P4 是产品化门禁，不替代 Platform v2 production L4～L6。任何报告必须分别记录两组状态。

## 11. Commit 与评审策略

- 每个 milestone 按“合同/ADR -> 最小实现 -> conformance -> 文档”形成多个单一目的 commit；
- 每个可运行行为闭包通过相称检查后立即提交，不让 CLI、Console 和 clean cut 混成一个巨型 commit；
- OpenAPI/schema 与 checked fixtures 同 commit，禁止手工维护漂移副本；
- 不提交 known failing、只支持 mock 或需要人工数据库修补的中间状态；
- M5 的删除/归档必须单独提交并保留 residual report，不能混入功能实现。

## 12. 启动顺序

owner 接受本计划后，第一实现批固定为 M0，不直接开始写控制台：

1. `/v1` product surface 与 required-role inventory；
2. current-vs-target cutover matrix；
3. 十条 scenario manifest；
4. CI/candidate wall-clock baseline；
5. CLI、HTTP authoring、Web、本地身份四项 ADR。

M0 退出门禁通过后才能开始 `insight` CLI。这样首次用户旅程、构建优化和最终 clean cut 使用同一组可测
目标，不再形成第三套临时入口。
