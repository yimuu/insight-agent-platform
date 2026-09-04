# Insight Agent Platform 整体代码库 Review 报告

- 日期：2026-09-02
- 分支：`main`
- 范围：仓库架构、Rust 实现、PostgreSQL 持久化、Sandbox/OpenSandbox、交付与供应链、测试、文档
- 基线：开始 review 时工作区干净；本报告与第 3 节所列修改作为同一 coherent review change 提交

## 1. 结论

仓库的常规工程质量门禁是健康的：54 个 workspace package、约 700,836 行 Rust，format、全 workspace
测试、strict Clippy、doc tests、RustSec、cargo-deny 与依赖边界检查均通过。PostgreSQL fence、terminal
transaction 和保守 orphan cleanup 等关键正向边界也实现得较扎实。

但当前仍**不能声明 production-ready**。本次 review 没有发现 P0，最初确认 8 项 P1 和 7 项 P2；随后 CR-217
关闭了 P1-07、P1-08 以及 P1-06 中已复现的 CRD/NetworkPolicy/workload/RBAC/admission 假通过路径。剩余 P1 主要集中在：

1. Sandbox cancel/timeout 与 runner boot rollover 无法可靠收敛；
2. 不可信 Package 与 runner authority 的进程、UID 和可写状态边界不足；
3. OpenSandbox readiness 仍可能在 create/controller/runner/delete 路径失效时假 Ready。

这些问题跨越 Accepted 合同、状态机、权限和部署拓扑。依照仓库的 contract workflow，本次没有直接重写
Sandbox 运行时语义，而是落地了边界清晰、无需架构扩权的供应链、CI、依赖安全、日志脱敏和文档修复。
L4～L6 仍为 `Not run`，与 reviewed contract 的当前声明一致。

严重度定义：

- P0：已确认的直接数据破坏、凭据泄漏或普遍远程执行；
- P1：可破坏业务状态收敛、隔离或发行/资格 authority，发布前必须关闭；
- P2：可靠性、可复现性、最小权限、合同一致性或长期维护风险。

## 2. Review 方法与覆盖

合同基线：

- `docs/specs/platform-v2/00-overview.md`
- `docs/specs/platform-v2/03-consistency-events-and-recovery.md`
- `docs/specs/platform-v2/07-scheduler-workers-and-concurrency.md`
- `docs/specs/platform-v2/10-capability-invocation.md`
- `docs/specs/platform-v2/14-sandbox-execution-plane.md`
- `docs/specs/platform-v2/18-deployment-observability-and-qualification.md`
- `docs/adr/0001-platform-v2-postgres-baseline.md`
- `docs/adr/0007-opensandbox-execution-provider.md`
- `docs/specs/platform-v2/cross-review.md`

代码检查覆盖 claim/lease/fence、cancel/timeout、状态机、runner 进程控制、OpenSandbox client、Helm
NetworkPolicy/RBAC、安全日志、GitHub Actions、release/candidate producer、L4 preflight、依赖与测试 skip 路径。

本地未执行真实多节点 Kubernetes、真实 registry/GitOps promotion、容量/混沌/soak/restore，也未启动外部
RustFS/S3 fixture；因此报告不会把静态检查或单节点测试描述为 L4～L6 证据。

## 3. 本次已落地的优化

### F-01 普通 CI 供应链与 required lane 改为 fail closed

- `.github/workflows/ci.yml` 的外部 Actions 全部固定到 40 字符 commit；
- PostgreSQL/NATS service 改为复用 development profile 的 exact image digest；
- 新增 `scripts/check-required-ci-results.py`，selected lane 必须为 `success`，未选择 lane 必须为 `skipped`；
- runtime 选中时 CLI 独立 lane 按既有互斥合同保持 `skipped`；
- 新增 5 个正/负向单元测试，并让静态 checker 冻结 checkout、参数与结果闭包。

这关闭了原 required job 只拒绝 `failure/cancelled`、却接受 selected lane 意外 `skipped` 的假绿路径。

### F-02 Production candidate 强制 main-only 身份

- `.github/workflows/platform-production-candidate.yml` 在任何 build/push 前拒绝非 `refs/heads/main`；
- 4 个 Cosign image/blob verification identity 全部精确绑定 main；
- `scripts/check-platform-candidate-pipeline.py` 精确校验 4 个证书 identity，不能以宽泛 `@refs/` 代替。

### F-03 清除两项 RustSec unsound dependency 警告

- `event-listener 5.4.1 -> 5.4.2`；
- `aws-sdk-s3 1.140.0 -> 1.144.0`，Smithy runtime/client 同步到兼容版本，`lru 0.16.4 -> 0.18.3`；
- 重新生成并验证 checked-in dependency feature/tree baseline。

更新后 `cargo audit` 和 `cargo deny check` 均通过，没有 advisory ignore。

### F-04 加固生产 tracing 脱敏门禁

- `scripts/check-platform-observability-redaction.py` 不再在文件首个 `#[cfg(test)]` 处截断，避免遗漏后续生产代码；
- 新增通用 `Error/Debug` tracing field 拒绝规则；
- 移除 Gateway transport、orchestration claim、safety scan 与 plan repository 的 raw error logging，只保留固定消息和安全布尔分类；
- 新增 post-`cfg(test)`、高基数字段、safe classification 三个回归测试。

### F-05 修正文档当前态

`README.md` 删除已被 CR-216 clean-cut 淘汰的 runsc L4 门禁说法，改为当前 containerd/runc + CNI exact
closure，并继续明确 L4～L6 为 `Not run`。

### F-06 CR-217 关闭本地资格假通过与发布顺序

- production soak 固定至少 86,400 秒，Evidence 时间窗口必须覆盖 profile；每个 gate 至少有一个专属 artifact digest，
  artifact 闭包无别名、无悬空项，CLI 不再把结构校验措辞成真实 production 通过；
- live preflight 对 BatchSandbox CRD 规范化 spec、Platform workload namespace、完整 NetworkPolicy、Sandbox
  ServiceAccount/RBAC 与三组 fail-closed AdmissionPolicy/Binding 取证并 fail closed；
- Callback/Cleanup/Dataset pool 映射回既有 `mcp_host/context_worker`，全部 production chart 形成 16-role、25-pool 闭包；
- Product Release 改为 candidate build/sign → offline starter qualification → 含资格 evidence 的 final bundle 重签 → final OCI tag/GitHub Release，静态 checker
  冻结真实 job dependencies；
- 定向 Rust qualification、17 项 topology/workload、Helm closure/lint、product release 与生成合同测试通过。

## 4. P1 跟踪

### P1-01 Sandbox cancel/timeout durable intent 没有生产消费者

证据：

- `crates/platform-invocations/src/execution.rs:2054-2094` 仅把 detached Sandbox Invocation 置为
  `Cancelling`，注释要求 owning backend controller 消费 committed Event；
- `crates/platform-postgres/src/capability_execution_repository.rs:1880-1950` 不修改 owning Job；
- `crates/platform-postgres/src/opensandbox_repository.rs:93-106` 仍会 claim ready Job，并在 deadline 到期后永久排除；
- `crates/platform-sandbox/src/dispatcher.rs:227-329` 没有 cancel/timeout 分支；
- `crates/platform-sandbox-dispatcher/src/lib.rs:340-369` 的 cancellation 只终止本地 drive loop。

影响：cancel 后 workload 仍可能被 create/activate；跨过 deadline 后 Job、Invocation 和 quota 可能永久悬挂。

建议：先在 owning spec 明确 pre-start、post-start、provider unavailable 和 unknown-outcome 收敛，再实现由 Sandbox
authority 事务消费 durable intent 的路径，并增加 expired-deadline reconciliation scan。必须覆盖 pre-claim cancel、Started
cancel、timeout、commit-window kill 和 provider unavailable。

### P1-02 runner boot rollover 无法进入合同要求的 UnknownOutcome

状态（2026-09-04）：**代码路径已修复，真实OpenSandbox runner/Pod rollover L3证据待补**。CR-218 revision 1已让
`ActivationAuthorized`与`Started`在任何activate/read-result之前比较boot，并以current Job fence持久化绑定original/observed
boot、request、sandbox与state-frame的摘要；随后只可进入`UnknownOutcome + cleanup`。定向L1覆盖两个Dispatcher分支的
zero-activate/zero-wait、同观察幂等、第二个不同观察与篡改fail closed；现有Kind PostgreSQL上的定向L2证明Job与Invocation原子进入
`ReconciliationRequired`。在真实BatchSandbox Pod重建证据通过前，本项不声明资格关闭。

原始审查证据：

- `crates/platform-sandbox/src/dispatcher.rs:252-285` 在 `ActivationAuthorized` 后看到新 `Armed` boot，仍以旧 boot
  构造 activation frame；
- `crates/platform-sandbox/src/opensandbox.rs:903-925` 将不同 boot 直接判为 `RunnerBootChanged`；
- 同文件 `:929-988` 在解释 `UnknownPriorActivation` 前先拒绝不同 boot ID。

影响：Pod/runner 重建后可能在 409、invalid frame 和重试之间循环，不能按 Accepted spec 10/14 进入
`UnknownOutcome + cleanup`。

建议：增加 fenced boot-rollover observation command；任何 activate 前先比较 boot，不同 boot 只能原子进入
UnknownOutcome，不能重新激活。覆盖保留 latch 与丢失 emptyDir 两种重启路径。

### P1-03 不可信 Package 没有与 runner authority 形成完整进程/文件边界

证据：

- `crates/platform-sandbox-runner/src/lib.rs:494-590` 只 kill/wait 直接 child，没有 process group、subreaper、pidfd
  或 per-execution cgroup；
- 同文件 `:502-517` 仅设置按 real UID 计数的 `RLIMIT_NPROC`；
- `deploy/helm/insight-platform-sandbox/files/batchsandbox-template.yaml:22-49` 让 runner 和 Package 同处一个容器、
  同 UID/GID 65532，并共享 `/run/insight-sandbox` 可写 emptyDir；
- latch/result 是普通 owner-writable 文件：`crates/platform-sandbox-runner/src/lib.rs:104-190`；写 result 失败在
  `:342-350` 被静默返回。

影响：恶意 Package 可 daemonize 后让父进程返回合法 JSON，runner 发布 `Succeeded` 后后代仍运行/出网；timeout 也只杀
直接 child。Package 还可 unlink/replace/fill runner state 或向同 UID runner 发信号，破坏 activation/result authority。

建议：Package 与 runner 分 UID/容器/权限域；每次 execution 使用独立 cgroup/process group，terminal 前 kill-all +
quiescence；以 `pids.max` 为权威，RLIMIT 仅作纵深防御；状态文件固定 owner/mode、`O_NOFOLLOW` 和 fd-based bounded read。
加入 daemonize、fork bomb、unlink/replace/fill/signal 负向资格测试。

### P1-04 OpenSandbox Server 位于 activation bearer 路径，与合同信任边界不一致

证据：

- `crates/platform-opensandbox-client/src/lib.rs:237-246,454-470` 通过 Server 的
  `/sandboxes/{id}/proxy/18080/...` 发送完整 activation token 和 input；
- `deploy/helm/insight-platform-sandbox/templates/networkpolicies.yaml:34-45,83-89,164-171` 的实际网络身份是
  Dispatcher -> Server -> runner，而不是 Dispatcher -> runner；
- 每个 candidate 共享 physical-attempt token digest，activation frame 不绑定 selected sandbox identity：
  `crates/platform-sandbox/src/opensandbox.rs:2376-2386,2737-2740`。

影响：Server 实际可读取并转发 activation bearer。若 Server 有缺陷或被攻陷，它可能对其他 candidate 重组 frame，从而破坏
“Dispatcher 唯一 activator / Package 最多启动一次”的合同假设。

建议：优先改为 authenticated Dispatcher -> runner 直连；若必须代理，则采用 candidate-specific token，把 sandbox identity
纳入签名/MAC frame，并通过 contract cross-review 正式声明 Server 的 activation relay 信任与负向测试。

### P1-05 Dispatcher readiness 是 list-only，可能假 Ready

证据：

- ADR-0007 与 spec 14 要求 authenticated create/list/delete、runner protocol、NetworkPolicy 和 exact digest closure；
- `crates/platform-opensandbox-client/src/lib.rs:146-163` 明确只做 metadata list、不创建 sandbox；
- `crates/platform-sandbox-dispatcher/src/main.rs:212-215,339-354` 的启动门禁和持续 `/readyz` 都复用该探针。

影响：Server list 正常但 controller、admission、template、runner、proxy、delete 或 NetworkPolicy 已坏时仍 Ready。

建议：实现低频、有界 inert canary：create -> runner protocol -> delete -> absence，并校验安装 digest；成功结果可短 TTL
缓存，失败必须立即撤 Ready。

### P1-06 L4 live preflight 可为漂移安全闭包生成“通过”证据

状态：**CR-217 已关闭。** preflight 现采集并验证 SA/RBAC/VAP/Binding，live CRD 使用规范化 observed digest，
NetworkPolicy 和标记 namespace 内的所有 Deployment/DaemonSet 进入 exact inventory；对应扩权、漂移、allow-all 和无 role
负向测试均已加入。正式 L4 仍需在真实多节点环境运行。

修复前证据：

- `scripts/check-platform-production-workloads.py:303-318` 只确认 default-deny 存在，未拒绝额外 allow-all policy；
- `scripts/preflight-platform-production-qualification.sh:46-63` 未采集 SA/RBAC/VAP/Binding；
- `scripts/check-platform-production-topology.py:82-111,194` 不校验 live CRD schema，却输出 hard-coded CRD digest。

修复前影响：过宽网络、缺失 admission/RBAC 或漂移 CRD 可被归档为 L4 topology passed，破坏 qualification authority。

落实：已采集并验证 closed SA/RBAC/VAP/Binding，拒绝未登记 workload 与过宽 policy，并对 live CRD 规范化后与 reviewed
digest 比较；对应负向测试已加入。

### P1-07 ComponentRole、默认 Helm 与 live candidate closure 不闭合

状态：**CR-217 已关闭。** dataset pool 映射 `context_worker`，Callback/Cleanup 映射 `mcp_host` 并补 candidate config、
resource/HPA；静态闭包覆盖全部 chart，当前为 16 roles / 25 isolated pools。

修复前证据：

- Rust/Candidate authority 只有 16 个 role：`crates/platform-contracts/src/qualification.rs:937-976`、
  `contracts/platform-v1/schemas/candidate-manifest.schema.json:15-32`；
- 默认 dataset pool 使用未知 `context_dataset_worker`：
  `deploy/helm/insight-platform-context-worker/templates/dataset-pool.yaml:27-33`；
- Callback API 与 MCP cleanup workload 没有 component-role/config-digest annotation，且不在静态 chart closure；
- `scripts/check-platform-production-workloads.py:160-169` 跳过无 role 标签 workload，却拒绝未知 role。

修复前影响：默认 render 不能通过真实 preflight；同时 Callback/Cleanup 可逃逸 candidate image/config/readiness/capacity 校验。

落实：没有扩展 role 合同；dataset、Callback/Cleanup 已映射到 owning reviewed role，全部 production workload 带 role、
candidate config digest 并进入同一个 render closure。

### P1-08 Product Release 在资格验证前已经公开不可变发行物

状态：**CR-217 已关闭已复现的 publish-before-qualification DAG。** signed candidate 先作为 CI artifact 供 offline
exact-cache qualification 使用；只有 qualification 成功后 publish job 才创建正式 OCI tag 与 GitHub Release。

修复前证据：

- `.github/workflows/product-release.yml:317-399` 的 `publish` 只依赖 CLI/images，并创建不可覆盖 GitHub Release；
- starter qualification 位于 `:401-435`，反向依赖 `publish`；
- qualification 结果没有进入 `scripts/build-product-release.py:21-29` 的 ReleaseBundle metadata。

修复前影响：一个无法完成 first Run 的版本可能已经拥有公开 tag、镜像、签名、attestation 和不可变 Release authority。

落实：CLI/image 先形成 commit-scoped candidate；candidate ReleaseBundle 先签名并作为 CI artifact 输入 offline qualification；成功后
重建并签名纳入 performance evidence digest 的 final ReleaseBundle，正式 image tag 与 GitHub Release 只存在于依赖 qualification 的 publish job。

## 5. 未解决 P2

### P2-01 Sandbox restricted DB role 缺少可复核的 grant/negative matrix

Helm 只接收 existing Secret URL；仓库和 CI 没有 provision Sandbox Dispatcher 最小权限角色。Sandbox L2/L3 使用共享
`PLATFORM_TEST_DATABASE_URL`，不能证明 Dispatcher 只能访问 Sandbox Job repository，也不能证明其他角色无法读取 activation token
或改写 Job。建议增加幂等 grant、`current_user` startup preflight，以及跨领域 SELECT/UPDATE/DDL 拒绝测试。

### P2-02 非 Orchestration claim 没有 tenant-first WDRR

Orchestration 使用 persisted WDRR，但 Sandbox `opensandbox_repository.rs:93-106` 和 Capability claim 仍采用全局
priority/time 排序后 LIMIT。持续高优先级 tenant 可让其他 tenant 饥饿。建议让所有 WorkClass 复用 persisted tenant WDRR 或等价
事务算法，并增加 fixed-seed 双租户 progress-bound property test。

### P2-03 关键 process-L3 测试在普通 required CI 中静默返回成功

`phase3_context.rs:4654-4662`、`phase3_model_turn.rs:3488-3497,3801-3817`、
`phase4_mcp_oauth.rs:1815-1825` 在缺 binary/TLS/S3 fixture 环境时直接 `return`。建议设独立 required process-L3 lane，缺
任何 env 直接失败；也可显式 `#[ignore]`，但 dedicated `--ignored` lane 必须执行并拒绝 skip sentinel。

### P2-04 Release archive 和 runtime layer 不完全可复现

`.github/workflows/product-release.yml:72-77,124-137` 使用普通 `tar -czf`，mtime、uid/gid、顺序和 gzip header 不规范；
Dockerfile 又从 mutable apt repository 安装未固定 `ca-certificates`。建议统一 SOURCE_DATE_EPOCH、排序、固定 metadata/gzip mtime，
并固定 CA bundle 内容；改变源文件 mtime 后两次 archive/image digest 必须相同。

### P2-05 Accepted contract 的 current-state 与类型 authority 漂移

- spec 03 的 `JobState` 仍含 `UnknownOutcome`，却漏 `Cancelling/ReconciliationRequired`；Rust owner type 与生成 schema 相反；
- Sandbox common constants/types 在 contracts 与 execution crate 平行定义；
- ADR-0001/spec 18 仍出现 schema v7，而实际 schema-contract 是 v8，并把物理版本/计数写进行为规范。

建议先修 owning contract/cross-review，明确 physical unknown outcome 到业务 `ReconciliationRequired` 的映射，再让 execution crate
复用/re-export contracts 的公共类型；物理 migration evidence 移出行为 spec。

### P2-06 旧 root runtime 不是当前部署 authority，但仍是受支持的可运行兼容面

正面上，它不在默认 CLI、candidate image 或 active Helm 中；但 root package、隐式 HTTP binary、public compatibility gates 仍在
workspace/CI 中。需要明确选择：迁入 archive/test-only 并取消可运行兼容面，或由 Accepted contract 定义其非生产生命周期与退出条件。

### P2-07 redaction gate 仍是文本门禁，不覆盖所有 sink

本次已修复 post-`cfg(test)` 截断和已确认的 raw tracing/error 输出；但 checker 仍未语法感知地覆盖 `eprintln!`、imported macro、
`event!`、span 和 `#[instrument]`。建议后续使用 Rust AST/syn 检查所有生产 logging sink，并让 runtime error 只暴露 stable
code/class/retryable/operation，不格式化通用 `Error/Debug`。

## 6. 确认良好的关键边界

- OpenSandbox create authorization 的 `Applied | Replayed` 在 PostgreSQL current fence 下完成，replay 不重复调用 provider；
- terminal commit 按 quota -> Invocation -> Job 锁序重验 fence，并在一个 transaction 中写 output、settle quota、更新
  Job/Invocation/Event；
- orphan cleanup 以 metadata tenant/job 做 point-read，corrupt/ambiguous/unavailable 时保守 retain；
- baseline schema 没有新增 Sandbox 专用业务 aggregate/table，shared Job 仍是 durable business authority；
- 普通 image/Helm composition 没有把旧 root server 当作第二生产 authority。

## 7. 验证结果

| 门禁 | 结果 |
|---|---|
| `cargo fmt --all -- --check` | 通过 |
| `cargo test --locked --workspace --all-targets --all-features` | 通过；1909 tests，2 ignored |
| ignored inventory | 两项均为外部 RustFS/S3 contract fixture |
| `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` | 通过 |
| `cargo test --locked --workspace --doc --all-features` | 通过；2 doctests |
| `cargo audit` | 通过；0 advisory |
| `cargo deny check` | 通过；advisories/bans/licenses/sources 均通过 |
| `bash scripts/check-crate-boundaries.sh` | 通过；54 workspace / 506 resolved packages |
| Productization/candidate/topology Python batch | 53 passed，1 conditional skip（本机 OpenSSL 无 Ed25519） |
| CI/candidate/redaction static checker | 通过 |
| Workflow YAML parse / `git diff --check` | 通过 |
| L4～L6 external qualification | **Not run** |

两项 ignored Rust test：

- `rustfs_preserves_object_identity_across_restart`
- `rustfs_supports_the_closed_file_service_s3_contract`

## 8. 建议修复顺序

1. 发起 Sandbox contract change review：cancel/timeout、boot rollover、runner isolation、Server relay 信任边界；
2. 实现 P1-01～P1-05，并补真实恶意 Package/runner/recovery fixtures；
3. 修复 L4 inventory、RBAC/admission/CRD digest 与完整 ComponentRole render closure；
4. 调整 Product Release DAG，资格通过前不产生发布态副作用；随后做 canonical archive/image；
5. 建立 Sandbox restricted DB role 和 process-L3 required lane；
6. 修复 JobState/schema current-state 文档与重复类型 authority；
7. 完成真实多节点 L4、容量/混沌/soak/restore L5、signed promotion/rollback L6 后，才重新评估
   production-ready 声明。

当前建议：允许继续开发和 L1～L3 验证；在上述 P1 关闭前，阻止 production candidate promotion 与公开 production-ready 声明。
