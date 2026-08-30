# M4 黄金场景证据合同

状态：**In Progress**。本文件定义 M4 的 machine-readable evidence，不表示十条场景已经通过。

## 1. 权威输入与输出

- [`examples/productization/scenarios.json`](../../../examples/productization/scenarios.json) 是十条 required scenario
  的 checked manifest；
- [`scenario-report.schema.json`](../../../examples/productization/scenario-report.schema.json) 是单次 fresh-profile
  journey 的 closed report schema；
- 每条报告文件名必须为 `<scenario-id>.json`，并由
  [`check-productization-scenario-reports.py`](../../../scripts/check-productization-scenario-reports.py) 对 manifest
做 exact ID、顺序、profile、automation layer 和 source revision 复核。
校验器默认要求报告的 `source_revision` 等于当前 Git `HEAD`；资格流水线也可用
`--source-revision <40-char-commit>` 显式固定同一候选 revision。

报告不能只记录测试进程 exit code。每个 manifest entrypoint、assertion 和 failure probe 都必须逐项给出
`passed | failed | not_run` 与 bounded evidence。只有全部 required check 为 `passed` 时，报告顶层 `status`
才能为 `passed`；`not_run` 必须使报告保持 `incomplete`。

## 2. 两种校验模式

开发中的单条/部分报告可以执行：

```console
python3 scripts/check-productization-scenario-reports.py \
  target/productization-reports --allow-incomplete
```

M4 gate 不使用该开关：

```console
python3 scripts/check-productization-scenario-reports.py \
  target/productization-reports
```

严格模式要求十个文件全部存在且全部 Passed；任何 required scenario 缺失、skip、`not_run`、未知字段、
working-tree revision 或 manifest drift 都失败。

## 3. 当前证据

`deterministic_first_run.rs` 的真实 P2 journey 已覆盖 CLI、checked curl 七步 Resource lifecycle、独立 raw public HTTP、
terminal Run、durable SSE、exact binding、Orchestration Worker replacement、Artifact S3/KMS I/O、invalid Receipt conflict、Gateway unavailable diagnostic
与角色重启。Human Task 子旅程已提取到独立 [`approval_task_resume.rs`](../../../tests/productization/approval_task_resume.rs)，
覆盖 waiting Task、first-winner、exact CLI journal replay、stale ETag/new Receipt fence、durable SSE resume 与 terminal result，
并从同一明确 fresh base authority 生成 `approval-task-resume` report。独立
[`timer_signal_restart_recovery.rs`](../../../tests/productization/timer_signal_restart_recovery.rs) 又覆盖 TimerWait 到期、
SignalWait exact key、Worker 缺席时两次相同 Receipt 的 204 replay、durable ready continuation、替代 Worker 恢复与 terminal
result，生成第三份 `timer-signal-restart-recovery` report。受控 PostgreSQL 探针会领取 exact durable continuation，
只替换 lease-token digest，证明 `RepositoryError::StaleFence` 且 Job version/token 不变，再由替代 Worker 在真实 lease
到期后恢复。Subagent 子旅程又覆盖 exact child Deployment、独立 durable child Run、root descendant reservation、
cascade-and-wait 取消、取消后的迟到 Timer first-winner，以及请求完整 500 后代硬上限时无部分 child link/quota
reservation 的 `budget_exhausted` 终态。远端 fresh Linux journey 已为四条 base 场景补齐真实 Console，四份报告现均
完整 Passed。其余六条场景尚未产生 Passed 报告。

这一区分防止把一个覆盖多项行为的集成测试误报为十条黄金场景，或用普通单元测试替代 fresh base/full
profile evidence。

Git revision `5a12a3deb8658e1dd496313b3f5bab9e352d5efe` 的 fresh Linux run `33289764921` 已使
`approval-task-resume`、`deterministic-first-run`、`subagent-quota-and-cancel` 和
`timer-signal-restart-recovery` 成为四份完整 Passed report：CLI、raw `/v1`、真实 Console、全部 assertion 与
failure probe 均 Passed。下载 artifact 后的 canonical report 摘要与 SHA-256 见
[`base-journey-evidence.md`](base-journey-evidence.md)。严格 M4 gate 仍因其余六条没有全部
Passed 而失败。

同一 revision 的 fresh full Linux run `33290248516` 又使 `artifact-lifecycle-and-rejection` 完整 Passed：CLI、raw
`/v1` 与真实 Console 共同观察 Ready Artifact、typed link 和受控下载，并执行 digest mismatch 与 wrong-tenant read
负向路径。五份 full report 下载后重验通过；严格 M4 gate 仍因 Model、remote Capability、MCP、Context、WASI/framework
五条场景缺失而失败。精确摘要见 [`full-journey-evidence.md`](full-journey-evidence.md)。

Context 场景的 Dataset 前置闭包现已补为独立 `platform-context-dataset-worker`：它只按冻结的 qualified Worker manifest、
Implementation adapter contract 与 installed adapter 三元摘要领取 `ContextDatasetBuild`；不能使用完整 Context Deployment
closure digest 作为进程能力身份，因为该摘要还包含 tenant 动态 Revision/Policy IDs。该进程使用独立 PostgreSQL
pool/Deployment/ServiceAccount/NetworkPolicy 和
队列指标，并仅通过带专用 workload identity 的 mTLS 调用 Artifact Data Worker。fresh PostgreSQL 16 fixture 已覆盖
未安装 digest 零 claim、运行中 Worker 丢失后的新 physical attempt、Artifact verification durable wait 的同-attempt
恢复、完整 generation 发布、重建以及失败验证不替换 active generation。该证据尚未通过 public `/v1`、CLI 与真实
Console 生成 `context-retrieval-and-citation` report，因此该场景仍保持缺失，不能升级为 Passed。

CR-205审计进一步确认，五条缺失场景共享一个authoring前置：Capability/Context Implementation、Model Provider与Sandbox
Runtime/Package已有领域合同但不在原八类Management noun中。实现必须先经新增closed noun发布这些exact定义；场景fixture不得
以SQL insert、Worker进程配置或占位ID替代Resource/Version/Deployment authority。

CR-206继续审计Context场景发现，build Operation虽返回预留Dataset ID，成功后却没有公开生成的Generation ID，导致既有exact
generation read route不可达。场景必须等待typed `context_dataset_generation` Operation result，再以其中`dgen + digest`读取并
校验同一immutable Version；禁止从PostgreSQL或mutable active head旁路发现。该typed result及fresh PostgreSQL正负夹具现已
交付，Context public journey可直接消费；这仍不是该场景的Passed report。

2026-08-30 exact revision `63fa4132ba274460d94ef03a44a21cec9f5017fc` 的本地 fresh `full` profile 已补齐
`context-retrieval-and-citation` 的完整实现闭包：public Management API
发布 Context Interface/Implementation、创建并激活 exact Context Deployment、触发 Dataset build Operation，并只从其
typed result发现 immutable Generation；CLI 创建并观察引用该 Generation 的 Run，真实 Console按 exact Run ID展示内容与
`observation_only` citation。失败探针覆盖过期 build admission 与 exact-only citation policy拒绝。场景启动前由受控
fresh-profile fixture建立 Context tenant/deployment 三条 durable quota authority；它不预写 Resource/Version/Deployment，
也不替代 public authoring path。六份 canonical report 的 source revision、closed checks 与 digest 已由 checker重验，
Context报告 SHA-256 为 `c16c68cacfeecafae03cfd9bba3c899ea09807e7994048537fb10c12eccec067`。当前已有六份
Passed report；严格 M4 仍因 Model、remote Capability、MCP、WASI/framework 四条缺失而失败。精确摘要见
[`full-journey-evidence.md`](full-journey-evidence.md)。

下一批 remote fixture 的首个安装前置已经闭合：Model endpoint安装合同新增默认关闭的
`development_loopback` 与 bounded `trusted_root_pem`。只有 endpoint host 精确为 `localhost` 且显式安装非空 CA bundle
时，DNS pinning才允许 loopback；普通 endpoint仍执行 public-destination拒绝，生产默认没有任何放宽。该实现只提供
本地受控 HTTPS fixture 的最后一跳，不创建 Provider/Deployment、不绕过 Egress exact catalog，也尚未生成
`exact-model-streaming-chat` report。

可从仓库根目录用下列单一入口复现当前 base journey；不带 `--report-directory` 时只运行测试，不写资格报告：

```console
scripts/run-productization-base-journey.sh \
  --report-directory target/productization-reports
```

runner 会保留其 fresh project 路径以便检查日志和 journal；默认只停止 exact Platform/Compose process，不删除持久卷。
为避免旧 Worker 通过固定本地 PostgreSQL 端口跨 profile 抢占 Job，runner 在任何构建或启动前检查本仓库 release
Platform process；发现孤儿或另一活动 profile 时 fail closed，并要求先从 owner project 执行 `insight stop`。
