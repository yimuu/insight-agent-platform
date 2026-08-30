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

同一入口现已加入独立 Timer/Signal restart 子旅程：Run 以 Plan v5 先后进入 TimerWait 与 SignalWait，测试在确认第二个
waiting projection version 后停止 Worker，通过认证 `/v1/runs/{run_id}/signals/release` 两次提交同一 Receipt，再启动 exact
replacement Worker 并观察 terminal result。该 fresh P2 执行同时发现并闭合两个真实状态机缺口：最后一项 active work
进入 Timer/Signal durable wake 时必须原子把 Run 置 `waiting`，Wake owner 消费必须清除 `waiting_reason`、恢复 `running` 并写 Run/Node/Job Event；Signal
Receipt replay 必须读取 immutable Receipt 中冻结的原始 Job version/generation，不能读取消费后归零的当前 wake 字段。

2026-08-29 fresh macOS P1 探针已证明 `doctor` 通过、`init` 可创建尚不存在的 project root，且真实 provision 后
Artifact Data/Gateway、Native Capability、Management/Runtime Gateway、Orchestration 与 Registry Validation 七个独立
role 全部 ready；`stop` 正常收束并保留 PostgreSQL/LocalStack 卷。探针同时发现并闭合 Agent public authoring P0：CR-203
将 typed Plan clean-cut 升级为 v5，以 Draft 已知的 `interface_contract_digest` 取代 publish 前不可知的 server-generated
Interface Revision ID；Deployment 与 materialization 分别重验 exact Interface/Plan 同 Agent、同 publish batch 和合同 digest。
真实 PostgreSQL 已覆盖 Resource lifecycle、Run kernel、Context、Model Turn、Capability Invocation 与独立 Orchestration
Coordinator，并包含 owner/batch/digest 漂移 fail-closed 断言。该闭环只解除首次 Run 的合同阻塞；public first Run 本身和
restart recovery 后续已由上述 P2 journey 补齐，但其余 M1/M2 门禁仍未完成。

同日后续 fresh base regression 将 NATS 从仅有 JetStream 的开发依赖收紧为 project-local CA 签发的双向 TLS：
`init` 分别生成 ServerAuth 与带 workload SPIFFE URI 的 ClientAuth 证书，Compose 只挂载该 fresh project 的证书路径并
启用 client certificate verification，CLI 启停流程显式传递这些路径。七个 Platform role、P2 deterministic first Run
与收束流程在该边界下再次通过；这只证明 base dependency transport closure，不代表 `full` profile 或 M1 已完成。

`full` 的增量闭包已开始从 CLI 主文件抽入独立模块：首批生成 Context Native 与 Artifact Maintenance 的 closed、
digest-bound 配置及持久化动态端口，并复用 exact Artifact provider catalog。Context Native、Artifact Maintenance 与
Security Authority 现已加入 profile-aware release build 集合和受监督进程启动规格；base 集合不包含这些二进制，避免
无关 rebuild。生成器、launch-spec、CLI 全量单元测试和 Clippy 已通过；尚未在一次 `full` journey 中启动这三个角色，
其余 full roles 也未接通，因此 `insight dev --profile full` 继续 fail closed。

Security/Egress 前置身份随后已按独立 authority owner 扩展：development bootstrap 现在接受 bounded service-principal
集合，新建 profile 使用 schema v2 同时创建 Registry Validator 与独立 Egress Broker `ServiceIdentity`，旧 schema v1
仍可 exact replay。CLI identity schema v3 生成 Egress principal、Security Authority ServerAuth 证书及带 exact Egress
Broker SPIFFE URI 的 ClientAuth 证书，Security Authority 配置只包含 PostgreSQL/RPC 边界而不包含 Secret provider。
Egress Broker 现在另有独立 ServerAuth 证书、持久化动态 RPC/observability 端口，以及权限收紧的 exact 32-byte
MCP remote-task/subscription state key；旧的已持久化 port closure 使用固定 additive defaults 重放，不旋转已有 identity。
LocalStack closure 增加 Secrets Manager，fresh profile 创建并持久化 exact readiness Secret ARN；Egress 的 AWS provider
config 冻结同一 KMS key、Secret namespace、HTTPS endpoint 和 canonical config digest。未安装 exact Deployment 的 Model、
Capability HTTP/gRPC、MCP/OAuth 与 remote Context lane 使用显式空 catalog 作为 deny-all closure，任何请求仍在 dispatch 前
失败，而 Broker 不再被迫使用虚构 endpoint 才能启动。2026-08-29 fresh PostgreSQL/LocalStack 探针已同时启动七个 base
roles、Security Authority 与 Egress Broker；双向 TLS readiness 为 `ready`，dependency metrics 对 KMS 与 Secret 各记录一次
success、零 failure。实际 remote endpoint catalog、对应 worker、一次受监督 `full` journey 与 WASI 仍未完成，因此仍不构成
full/M1 完成证据。

随后 Model Worker 与 Remote Context Worker 也进入同一 additive closure：新建 profile 为两者分别签发 Egress RPC
允许的 exact workload URI SAN ClientAuth 证书，配置冻结 WorkerManifest、adapter/contract digest、Egress endpoint、
PostgreSQL预算与动态 observability port；Model 还冻结两个现有 provider adapter 及 project-local NATS mTLS live-delta
通道。fresh 探针按 Security -> Egress -> consumer 顺序启动后，Model 与 Remote Context `/readyz` 均为 `ready`，Model
NATS dependency metrics 为 success=1/failure=0。探针没有伪造 Model/Context Job，因此 Egress operation counter 保持 0；
这只证明进程配置、PostgreSQL schema、NATS transport 与 Egress TLS channel closure，不证明黄金场景的 provider dispatch。

MCP Host、MCP Resource Host 与 Remote Capability Worker 已加入同一 full-only closure。CLI 为两个 Host 分配独立
ServerAuth 证书，并为其 Egress 调用签发 exact MCP Host workload URI 的独立 ClientAuth 证书；Remote Capability 使用
exact Capability Worker workload URI 的 ClientAuth 证书同时连接 Egress 与 MCP Host。三个进程的 closed config、动态
RPC/observability port、PostgreSQL pool 和配置 digest 均由 profile 固定，Remote Capability 的 HTTP/gRPC/MCP builtin
codec digest 直接复用 capability-worker 的 authority 函数，不在 CLI 手工复制常量。fresh PostgreSQL/LocalStack 探针按
Security -> Egress -> MCP -> Capability 顺序启动后，两个 MCP Host 与 Remote Capability 均 ready；移除 MCP Host 后，
Remote Capability 在发布 readiness 前立即以 `MCP Host is unavailable` 失败，恢复 exact mTLS Host 后才重新 ready。
该探针证明启动依赖和身份边界闭合，但空 endpoint catalog 仍是 deny-all：尚未证明 remote Capability/MCP 实际 dispatch，
MCP discovery/subscription worker、一次受监督 `full` journey 与 WASI 也仍未完成。

MCP 后台闭包随后加入 Discovery Worker、Subscription Worker、OAuth Cleanup Worker 与 Subscription Context Worker。
四个进程各有 closed/digest-bound config、持久化 observability port 和独立 ClientAuth 证书：Discovery 使用 exact
`mcp-discovery-worker` workload URI 连接 Egress 与 Artifact Data，Subscription 使用 exact
`mcp-subscription-worker` URI 连接 Egress，Cleanup 使用专用 `mcp-cleanup-worker` URI 且只能调用 PKCE cleanup RPC，Subscription Context
使用 exact `context-worker` URI 连接 MCP Resource Host。2026-08-30 fresh PostgreSQL/NATS/LocalStack 探针先启动 base、
Security、Egress 与 Resource Host，再启动四个后台进程；四个 `/readyz` 均为 `ready`，其 PostgreSQL dependency counter
均为 success>0/failure=0。独立 mTLS method-role fixture 同时证明通用 MCP Host 不能调用 cleanup、Cleanup 身份不能调用
其他 MCP RPC；第二个 fresh profile 以证书 SAN `spiffe://insight.platform/workload/mcp-cleanup-worker` 启动 Cleanup 并 ready。
由于探针没有伪造 Discovery、Subscription 或 Cleanup Job，Egress operation counter 保持 0；
这不是 remote Streamable HTTP、OAuth callback 或 subscription lifecycle 的场景证据。

Callback API 随后进入同一 full-only closure：CLI 生成独立 32-byte OAuth state key、受 exact directory containment
约束的 closed config、持久化动态 HTTP port，以及 SAN 为
`spiffe://insight.platform/workload/mcp-callback-api` 的专用 Egress ClientAuth 证书。Egress Broker 的 authorization-code
exchange 不再接受通用 MCP Host 身份；真实 mTLS method-role fixture 证明 MCP Host/Cleanup 均被拒绝，只有 Callback
身份能越过 role gate。2026-08-30 fresh PostgreSQL/NATS/LocalStack 探针按 base -> Security -> Egress -> Callback 启动，
Callback `/readyz` 为 `ready`，process-ready 为 1，PostgreSQL dependency success=3/failure=0，证书 SAN 与专用 workload
identity 完全一致。探针没有创建 OAuth authorization authority 或向外部 token endpoint 发送 code，所以 Egress OAuth
operation counter 为 0；这只关闭 Callback 的配置、密钥、身份、PostgreSQL 与 mTLS 启动闭包，不宣称 OAuth lifecycle
黄金场景完成。Sandbox/WASI、实际 endpoint fixture 和一次由 `insight dev --profile full` 监督的整体 journey 仍未完成。

Sandbox/WASI 的 macOS 前置不再把 Linux Pod 事实伪造为本地证据。node attestor 的内部 observed-process registry 现以
closed variant 区分 production `linux_procfs` 与显式 non-production `local_unix`：前者仍绑定 node/pod UID、unified
cgroup、boot ID、PID namespace 与 start ticks；后者只绑定 kernel-authenticated UDS peer credentials、macOS/Linux
真实 process start identity、真实 system boot identity 和 project-local instance UID。两种 variant 都以完整观察值
生成 executor instance binding digest，并在 verify/absence 时重新观察，从而拒绝 PID reuse。Controller 默认仍只接受
private node CIDR；只有配置显式打开 local loopback 后才接受精确 `127.0.0.1/32` 或 `::1/128`。production Helm 明确固定
`linux_procfs` 且关闭 loopback，Sandbox deployment checker、Attestor 全目标测试和 Controller 测试均通过。该前置只
使 honest local composition 成为可能。

2026-08-30 随后的 fresh `insight dev --profile full` 探针已把 Attestor、Controller 与 restricted-WASI Executor
接入 CLI 的 closed/digest-bound 配置、project-local CA/mTLS、持久化动态端口、单次 release build closure 和受监督
启动顺序；与 base/full 其余角色合计 24 个进程持续报告 `ready`。Attestor registry 的最新记录为真实
`local_unix` variant，包含 kernel-authenticated UID/GID/PID、macOS boot identity、process start identity 与
project-local instance UID，没有伪造 Pod/cgroup 字段；Controller PostgreSQL dependency observations 为
success=71/failure=0，Executor NATS observations 为 success=2/failure=0。探针同时暴露并修复了三个真实重启/传输问题：
继承 setgid 目录 group 的 stale UDS socket 恢复、显式 local loopback route 与 production private-node route 的隔离，
以及 Executor authority interceptor 必须在 workload URI authorization 后安装 required internal trace context。
production Helm 仍固定 `linux_procfs`、`allow_loopback_advertised_route=false` 与 `allow_loopback_routes=false`。
本证据只关闭 full 进程 composition 与空队列 claim/readiness；尚未提交一个真实 WASI Job，也未证明 WASI limit 负向场景、
remote fixture dispatch 或 OAuth lifecycle，因此仍不得标记 M1/M4 或 spec 00–18 为 Verified。

2026-08-30 exact revision `00310f9ff5162c2c2aa259dd8565b133a32568ca` 的 fresh GitHub Linux run
`33283147235` / job `99181622944` 又从不存在的 project path 完成完整 `full` journey：24 个角色全部 ready，
在 ready 后安全刷新短期本地 token，随后由真实 Runtime Gateway 与 headless Chrome 完成三条既有 base journey，
最终清理 exact Platform/Compose closure。核心 step 约 19 分 32 秒。该结果把“整体 journey 尚未运行”关闭为
Passed composition evidence，但没有产生七条 full-only report；精确事实与边界见
[`full-journey-evidence.md`](full-journey-evidence.md)。

2026-08-30 exact revision `5a12a3deb8658e1dd496313b3f5bab9e352d5efe` 的 fresh full Linux run
`33290248516` / job `99200524415` 在 24 角色 ready 后进一步执行真实 Artifact lifecycle/rejection fixture：Ready、typed
link、受控下载、digest mismatch 与 wrong-tenant read 均由 CLI、raw `/v1` 和真实 Console 观察。该 run 生成并重验五份
Passed report；核心步骤约 20 分 48 秒。剩余 Model、remote Capability、MCP、Context、WASI/framework 五条仍未交付，
精确摘要与 digest 见 [`full-journey-evidence.md`](full-journey-evidence.md)。

2026-08-30 full 配置生成器的 dependency audit 进一步闭合了执行面边界：`insight` 不再为读取配置常量而链接
Capability Worker、Egress RPC、Sandbox RPC 或 Wasmtime adapter。跨进程必须一致的 workload identity、内置 JSON
codec 描述摘要和 WASI ABI runtime version 由无执行能力的 `platform-contracts` 统一拥有，各独立 Worker/RPC/Executor
消费并重新验证；crate-boundary 全图检查、CLI/Contracts 163 项 unit test、四个消费 crate 38 项测试和严格 Clippy 已通过。
这证明 CLI 只是进程配置与监督入口，不能执行用户代码；它仍不是上述真实 WASI Job 的完成证据。

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
零网络恢复。命令级 fixture 已覆盖 create 409/429、validate 412、Validation Operation failed/cancelled/timed-out/
reconciliation-required 与无副作用 timeout，并保留 closed Problem retry metadata 和安全 terminal detail。首次 Run
已由 fresh P2 journey 覆盖。checked `curl+jq` fixture 已执行同一七步 `/v1` lifecycle、exact Receipt replay 与
changed-body 409 Problem，普通 CI mock authority 测试通过；exact revision
`939cd9e9d766ce17b242627daba7697fa3687799` 的 fresh P2 也已将该 entrypoint 记录为 passed。CR-205将authoring surface扩为
13类closed noun：九类deployable closure逐一检查exact authority ID/digest注入与cross-kind fail-closed，四类
definition-only只执行validate/publish并以`null` Deployment ID报告；Apply/Operation子命令合同因此闭合；
其余 M2/M4 fresh scenario 门禁仍未完成，因此不得标记 M2 或 spec 00–18 为 Verified。

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
target 只能通过同一 prepare Receipt 刷新同一 generation。fresh 真实 S3/KMS upload 已由 deterministic first Run P2
journey 覆盖。自签 HTTPS fixture 又证明 no-redirect/no-proxy/no-token、非 200、TLS fail-closed，并覆盖 expired target、
409/412/429/503、非 Ready authority state 与 truncated/oversized/digest-mismatch download。Artifact 命令面的既定
负向矩阵已闭合，M2 仍受完整 HTTP fixture 与跨命令场景报告门禁约束。

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
和 lint 已通过；开发期 stateful fixture 上的真实浏览器 journey 已覆盖 Run/SSE -> Task mutation -> terminal Run、
409/412/429、cursor reconnect、DOM/console 脱敏、刷新清理内存态 token 和基础键盘语义。该证据不包含真实
Gateway/PostgreSQL 或 Gateway restart；正式部署、telemetry、慢依赖和 accessibility audit 仍未完成，详见
[`m3-console.md`](m3-console.md)，故不得标记 M3 或 spec00～18 为 Verified。

同一 stateful fixture 浏览器 journey 现由 `browser:fixture:qualify` 自行监督 fixture、同源代理和 headless
Chrome，并闭合检查 readiness 无凭据、全部 `/v1` 请求有凭据、Task mutation 的 exact ETag/Receipt 以及日志不含
原始 token。受影响 Console 的 CI 固定使用 GitHub `ubuntu-24.04` runner 中预装的 Chrome 执行该命令；它消除了
人工浏览器证据漂移。Git revision `d6dca5c180f9027441284b29b2c2684b3fd0c795` 的远端 job
`99158633401` 已以 `request_count=15` 和六项 closed journey check 取得 Passed；这不扩大下述真实 authority
证据边界。

Console 的 fresh authority runner 已加入 base journey 的显式 `--console-browser` 模式：它从用户选定的全局 Node
旁解析 Corepack，构建静态 bundle，以仅转发 `/readyz`/`/v1` 的 loopback 同源代理连接真实 Runtime Gateway，并在
独立 Human Task Run 上由 headless Chromium 完成 SSE Task 发现、typed mutation、terminal result、刷新清空内存态
凭据与 authority ID 重读。脚本对 stateful fixture 已通过；2026-08-30 fresh PostgreSQL/Gateway 尝试因本机 OrbStack
Docker API 无响应而在 `doctor` 阶段终止，故真实项仍为 Not run。该失败暴露并闭合了 `doctor` 外部命令可无限等待的
缺口：所有命令探针现在 5 秒 fail closed，真实无响应 daemon 在约 6 秒内返回 `docker_engine=failed` 的可操作 JSON，
不再挂住 CLI。M3 的真实 Gateway、Gateway restart、telemetry、慢依赖、accessibility 与正式部署门禁仍未完成。

2026-08-30 Git revision `591baf00c9b5bac04826f84b58ee96032aa2749b` 已在 GitHub `ubuntu-24.04` 的显式手动
run `33279353000` / job `99171748184` 完成首个 fresh PostgreSQL + 真实 Gateway + headless Chrome journey。
`approval-task-resume` 的 CLI、raw HTTP、Console、三项 assertion 与两项 failure probe 均 Passed，成为第一条完整
黄金场景报告；另外两份 base 报告继续诚实保持 Incomplete。核心步骤约 14 分 1 秒，仍超过 G1 的 10 分钟 cold
目标，因此 M1/M3/M4 继续 In Progress。完整摘要、artifact digest 与边界见
[`base-journey-evidence.md`](base-journey-evidence.md)。

2026-08-30 exact revision `e03b6cc123f5f1ada2c96a47f167956adde7a095` 的 fresh Linux run
`33284301192` / job `99184695618` 已让同一真实 Console session 按 exact ID 读取 deterministic 与 Timer/Signal
两个 Run，并分别核验 `succeeded` authority 与预期 Inline result；受控 PostgreSQL 探针还以只更改 lease-token
digest 的方式证明 exact continuation 拒绝 stale Job fence，且没有改变 Job version/token，随后由 replacement Worker
在 lease 到期后恢复。因此三份 base scenario report 均完整 Passed。核心步骤约 9 分 2 秒，使用了受控 Cargo cache；
因尚无独立 first Run commit timestamp 和
无缓存机器证据，不据此宣称 G1 cold clone 门禁 Passed。精确报告摘要与 digest 见
[`base-journey-evidence.md`](base-journey-evidence.md)。

2026-08-30 exact revision `5a12a3deb8658e1dd496313b3f5bab9e352d5efe` 的 fresh Linux run
`33289764921` / job `99199229740` 又把 `subagent-quota-and-cancel` 纳入同一 base closure。场景证明 exact child
Deployment 与独立 durable child Run、root descendant reservation、cascade-and-wait 取消，以及取消后的迟到 Timer
不能覆盖 first-winner；请求完整 500 后代硬上限会立即提交 `budget_exhausted`，且不产生部分 child link 或 quota
reservation。四份 base report 均完整 Passed，核心步骤约 10 分 15 秒。该结果仍不覆盖 full-only 场景或 G1 cold
clone 门禁，精确报告摘要与 digest 见 [`base-journey-evidence.md`](base-journey-evidence.md)。

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
`approval-task-resume` report；启用真实浏览器时该报告已可完整 Passed。Timer/Signal restart fixture 会产出第三份
`timer-signal-restart-recovery` report，Subagent fixture 会产出第四份 `subagent-quota-and-cancel` report，full profile
的 Artifact fixture 会产出第五份 `artifact-lifecycle-and-rejection` report。这不是 M4 完成证据；其余五条
fixture/report 与 remote full-profile workload 场景仍未交付。

远端 fresh base run `33289764921` 已使 `approval-task-resume`、`deterministic-first-run`、
`subagent-quota-and-cancel` 与 `timer-signal-restart-recovery` 四份报告完整 Passed。严格 M4 checker 仍会因另外六条
未全部 Passed 而失败。随后 full run `33290248516` 又使 `artifact-lifecycle-and-rejection` Passed，因此当前共有五份
Passed report，严格门禁仍因另外五条未交付而失败。精确报告摘要见
[`base-journey-evidence.md`](base-journey-evidence.md) 与 [`full-journey-evidence.md`](full-journey-evidence.md)。

2026-08-30 本地 fresh full journey 已实现并通过 `context-retrieval-and-citation`：新增定义 noun 的 public lifecycle、
Dataset Operation -> immutable Generation发现、exact Context binding、Native Context执行、citation投影、CLI/raw `/v1`与
真实 Console，以及 build/citation拒绝路径均在同一 fresh authority 闭合。该运行同时修复了 registry validator permission
映射、Dataset Worker RPC trace、Context query schema digest、Context durable quota fixture与 continuation attempt语义。
由于本段记录的是 working-tree验证，exact-revision report与 digest 尚待 clean commit 后生成；在此之前严格 M4 仍按五份
Passed report计算，不能提前关闭 Context 场景。

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
| full场景依赖的内部定义无authoring surface | Model/Capability/Context/WASI场景 | CR-205扩展closed domain noun；四类definition-only只publish Version，Model Provider走exact Deployment；禁止fixture预写数据库 |
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
