# 可观测性与告警处置

告警清单由
[`PrometheusRule`](../../deploy/helm/insight-platform-observability/templates/prometheusrule.yaml)
定义。本文件保留每个告警的稳定标题和人工处置边界，并由部署合同检查逐项校验。

所有告警遵循以下规则：

- 只使用 `component_role`、`dependency`、`resource`、`operation` 等有界标签定位问题；不得记录 tenant/resource/request
  ID、request body、prompt/tool argument、endpoint、database name、subject、带查询参数的 URL、object key、provider
  identity、原始错误文本、credential 或 token；
- 先关联 readiness、流量、容量、依赖健康和最近发布，再决定恢复动作；扩缩容、限额变更和回滚必须走已审核的
  GitOps/CapacityProfile 路径；
- 不直接修改业务行、Job/Outbox 状态、lease、epoch 或 fence，不手工重放非幂等外部操作，也不绕过隔离与 admission；
- 修复依赖或 owner 后，让持有 fence 的 owner transaction 完成恢复，并保留失败记录作为审计证据。

## InsightPlatformTelemetryMissing

先检查 Prometheus、ServiceMonitor、namespace/Pod selector、NetworkPolicy 和目标发现，再与预期 workload role
清单比较。全量缺失属于观测链路故障，在区分发现失败与网络不可达之前不要重启业务依赖。

## InsightPlatformWorkloadNotReady

按 `component_role` 检查 rollout、启动错误和对应 PostgreSQL、NATS、S3/KMS、Secret、Egress、Artifact 或 Sandbox
依赖。单个 tenant/provider 请求失败不等同于组件 readiness 失效。

## InsightPlatformOperationalCapacityExhausted

按 role/resource 关联延迟、拒绝和依赖健康；`sandbox-controller/artifact_response` 还需检查 Artifact 响应流与 Data
Worker。先保护 critical-control 容量并收紧业务 admission；持续不足需要重新资格化 CapacityProfile。

## InsightPlatformHttpFailureRatioHigh

确认请求量达到有效样本，再关联 readiness、依赖延迟和发布事件。若故障随候选版本出现则通过 GitOps 回滚，
否则恢复对应依赖。

## InsightPlatformHttpLatencyHigh

先确认有效请求量、饱和点和下游延迟。只按已资格化 CapacityProfile 调整副本或 permits，不得绕过队列、
admission、硬限制或隔离。

## InsightPlatformCriticalControlPermitsExhausted

确认 `scheduler-recovery` Ready，比较业务与 critical-control permits，并检查恢复扫描和 PostgreSQL 专用连接池。
不得把业务 permits 借给恢复任务；应移除阻塞依赖或走 GitOps 扩容。

## InsightPlatformRecoveryFailureRatioHigh

比较恢复扫描尝试/失败、PostgreSQL 可用性、过期 lease 和发布事件，区分全局故障与单个扫描族故障。
保留 fencing 和 owner transaction，不得直接改行修复状态。

## InsightPlatformDurableJobLagHigh

比较 due 数量、最老延迟、业务 permits、claim 结果和 PostgreSQL 延迟。持续增长通常表示 admission 超出容量；
数量较少但年龄增长通常表示 claim 停滞。不得删除 Job 或修改优先级。

## InsightPlatformExpiredLeaseRecoveryLagHigh

关联过期 lease、critical-control permits、恢复结果、Worker 丢失、数据库时间和 fence 错误。不得清空 lease；
由 owner recovery transaction 结算。

## InsightPlatformModelDurableJobLagHigh

关联 Model due 数量/年龄、业务 permits、PostgreSQL、provider、NATS 和发布事件。修复阻塞依赖或走 GitOps 扩容，
不得直接修改 Job 优先级或状态。

## InsightPlatformModelExpiredLeaseRecoveryLagHigh

检查 Worker 重启、PostgreSQL 时间、critical-control permits 和 provider cancellation/recovery。保留 ModelTurn、
quota 与 Job fence，由 Model owner 恢复丢失 attempt。

## InsightPlatformCapabilityDurableJobLagHigh

按固定 role 区分 Native 与 Remote，关联 permits 和 PostgreSQL；Remote 还需检查 Egress 与 MCP Host，且不得加入
endpoint/codec 高基数标签。不得移动 WorkClass 或直接改 Job。

## InsightPlatformCapabilityExpiredLeaseRecoveryLagHigh

检查 Native/Remote Worker 重启、critical-control permits、PostgreSQL、fence 和外部依赖。保留 Invocation、quota
与 Job fence；不得手工重放非幂等 Remote effect。

## InsightPlatformSandboxDurableJobLagHigh

关联 Sandbox due 数量/年龄、Controller readiness、executor admission、Artifact response 容量和 PostgreSQL。
不得绕过 admission、改变 owner，或退回 Controller/宿主机执行。

## InsightPlatformSandboxExpiredLeaseRecoveryLagHigh

检查 executor 丢失、process-generation attestation、数据库时间、Controller fence 和恢复容量。只有证明旧 process
generation 已消失后，Controller 才能允许新的物理 attempt。

## InsightPlatformArtifactDurableJobLagHigh

按 role 区分 Data Worker scan/rescan 与 Maintenance delete/blob-cleanup，关联 PostgreSQL、S3/KMS 和本地容量。
不得改变 Job role、kind、priority 或 state。

## InsightPlatformArtifactExpiredLeaseRecoveryLagHigh

检查 Worker 重启、PostgreSQL 时间、object store/KMS 和 Artifact owner fence。保留 Artifact/Blob generation 与
Job lease，由 owner 重新验证存储证据并结算。

## InsightPlatformContextDurableJobLagHigh

按 role 区分 Native query、Remote query 和 subscription refresh，关联 PostgreSQL、permits 与对应 adapter。
不得移动 role 或改变 JobKind。

## InsightPlatformContextExpiredLeaseRecoveryLagHigh

检查 Worker 重启、PostgreSQL 时间、quota 和 Job fence。Remote/subscription 必须保留冻结的请求与执行 identity，
不得手工重放外部 I/O。

## InsightPlatformMcpDiscoveryDurableJobLagHigh

关联 discovery due 数量/年龄、专用 permit、PostgreSQL、Egress 和 Artifact Data Worker。普通 MCP Host 不得 claim
该 lane，也不得绕过持久 Artifact verification wake。

## InsightPlatformMcpDiscoveryExpiredLeaseRecoveryLagHigh

检查 discovery-worker 重启、数据库时间、heartbeat/fence 和 staged Artifact verification。保留 operation、lease、
Artifact/Blob generation 与 transport evidence，由 owner 从持久证据恢复。

## InsightPlatformMcpSubscriptionDurableJobLagHigh

关联 subscription due 数量/年龄、专用 permit、PostgreSQL 和 Egress，检查 reconcile 是否反复置为 ready 却未被
claim。普通 MCP Host 不得 claim 此 lane，也不得改 JobKind/state。

## InsightPlatformMcpSubscriptionExpiredLeaseRecoveryLagHigh

检查 subscription-worker 重启、数据库时间、heartbeat/fence 和 Egress stream termination。保留 session/event
generation 与 lease，由有界恢复扫描重建 session，并通过 owner transaction 触发完整 Context reconcile。

## InsightPlatformDurableObservationFailureRatioHigh

按固定 role 检查 PostgreSQL transport 与连接池。Gauge 会保留最后一次成功快照，告警期间的平坦值不能证明 backlog
仍是最新；应恢复有界只读 sampler，不得用 payload scan 或高基数标签替代。

## InsightPlatformDependencyFailureRatioHigh

按固定 role/dependency 关联 readiness、容量、延迟、发布与 provider telemetry。PostgreSQL 需保持业务/恢复连接池
分离；NATS、S3、KMS、Secret、Egress 需区分组件级传输故障与单次业务拒绝。

## InsightPlatformDueOutboxLagHigh

关联 due 数量/年龄、publisher readiness、NATS、PostgreSQL 和发布事件。不得手工删除、发布或推进 Outbox 行；
修复 publisher/transport 后由 fenced owner 保持顺序和重放语义。

## InsightPlatformExpiredOutboxClaimLagHigh

检查 publisher 丢失、数据库时间、claim fence 和 critical-control 容量。不得直接清空 `claim_owner`、增加 epoch
或改 `next_publish_at`，必须由 Outbox owner reclaim。

## InsightPlatformOutboxDeadEventsPresent

用固定队列和安全的 failure-code 聚合定位首次出现时间，不导出 Event payload 或 identity。保留 dead record，
走 owning domain reconciliation；遇到未知 event kind 时升级处理，不得绕过 owner contract 重放。
