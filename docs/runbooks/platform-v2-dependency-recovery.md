# Platform v2 依赖故障、恢复与轮换运行手册

状态：Ready to execute / execution evidence pending

本手册覆盖 `backup_restore`、`artifact_s3_kms_faults`、`event_outbox_recovery`、
`rolling_fault_recovery` 与 Secret/KMS rotation 场景。执行结果只有在 production-equivalent runner 上绑定 exact
CandidateManifest、CapacityProfile 和拓扑 digest 后才是 L4～L6 证据。

## 1. 安全边界与停止条件

- 只在隔离 staging/qualification account 执行破坏性注入；production 演练必须使用获批的 provider-native rehearsal。
- 执行前冻结写入窗口、恢复点和回滚负责人。任何目标 account、region、cluster、database 或 bucket 无法唯一确认时立即停止。
- 不删除 backup、object version、KMS key version、Secret version、Git ref 或历史 qualification artifact。
- 不把 NATS stream、metric、object-store listing 或 Kubernetes Pod 状态当作业务 current-state authority。
- 发现跨租户可见性、Secret 泄漏、无法解释的数据丢失、RPO/RTO 超限或恢复后写入仍使用旧 fence 时立即失败，
  关闭新 admission，并保留现场证据。

## 2. 执行前记录

在新建且不可覆盖的 evidence root 中记录以下非敏感事实：

1. QualificationProfile、CandidateManifest 与 CapacityProfile 的 canonical digest；
2. Git commit、镜像 digest、schema version、Kubernetes context digest、region 和开始时间；
3. PostgreSQL backup/PITR capability、S3 versioning/encryption/lifecycle、KMS/Secret versioning与审计均已启用的
   provider attestation digest；
4. 基准业务不变量：按 tenant 统计的 Run/Job/Task/Artifact/Blob/Link 数量、terminal/current-state摘要 digest、
   最新 committed Event/Outbox位置和抽样 content digest；
5. 本次故障注入的 owner、observer、abort authority 和批准记录 digest。

记录只保存摘要和 content-addressed artifact link。数据库 DSN、access token、证书私钥、Secret value、bucket内部路径、
对象 key 和 tenant payload不得进入证据。

## 3. PostgreSQL PITR 与 authority 恢复

1. 等待已选恢复点之前的事务提交，记录数据库时间、WAL/LSN摘要和 Artifact consistency watermark。
2. 在独立恢复实例执行加密 backup/PITR；禁止覆盖源实例。
3. 用 startup schema contract 验证唯一 baseline/schema version，并执行 repository transaction/CAS/tenant负向套件。
4. 将恢复环境的所有非 terminal lease视为过期候选；由正常 safety scan/recovery fence重新 claim，禁止直接改写
   Run、NodeExecution、Invocation 或 Job 为成功/失败。
5. 重放 committed outbox；重复投递必须由 Receipt/first-winner语义去重，未提交 wake hint不得制造业务事实。
6. 对照执行前摘要验证 tenant ownership、Run frozen bindings、Job/Task/Receipt current state、Event history与Artifact links。
7. 用新 fence完成一组恢复中的工作，并证明旧 worker/fence 的 heartbeat、commit和callback全部被拒绝。

通过条件：恢复点满足 CapacityProfile 的 RPO/RTO；不存在第二 current-state authority、跨租户引用、静默丢失的已提交事实、
旧 fence 提交或无法解释的 Event/aggregate分歧。

## 4. Artifact S3/KMS 一致性恢复

1. 在恢复点冻结 Gateway新写入，只允许已提交 cleanup/reconciliation进入有界 drain。
2. 在独立 prefix/account 恢复目标时间点的 object versions，不复用源环境可写 identity。
3. 以 PostgreSQL Blob identity和exact generation/version为权威核对对象；listing只能帮助发现 orphan，不能创建业务引用。
4. 对每种状态验证：staging对象不会被当作 Ready；Ready Blob的size/digest/generation匹配；删除中的Link/Blob遵守
   retention/hold；orphan由Maintenance有界回收。
5. 注入 S3 timeout、迟到成功、错误 generation、KMS deny/unavailable和损坏 ciphertext；Gateway/Data Worker/Maintenance
   必须保持各自权限边界，未知结果进入reconcile而不是伪造安全重试。
6. 恢复KMS/Secret reference解析能力后抽样解密并重新计算plaintext content digest；证据只保存匹配结果和摘要。

通过条件：PostgreSQL引用与可读object version一致，损坏/错误generation fail closed，quota/retention不被绕过，
Data Worker以外的role不能取得明文或越权写入，orphan backlog在profile时限内收敛。

## 5. NATS 丢失与重建

1. 记录数据库 committed Outbox/Event水位后停止或清空 qualification NATS stream；不得改动数据库业务行。
2. 保持消息不可用期间执行有界 safety scan，验证API与critical-control reserve仍满足profile要求。
3. 恢复NATS配置，从 committed outbox重新投递；同时注入duplicate与out-of-order wake hint。
4. 验证Job/Task/Invocation最终收敛、Receipt只产生一个逻辑winner，consumer ack不改变数据库authority。

通过条件：完全丢失或重复 wake hint 均不丢业务事实、不重复副作用、不产生第二状态投影，recovery/outbox lag在SLO内回落。

## 6. Secret 与 KMS rotation/revocation

1. 创建新 provider-owned key/Secret version，保留旧版本用于已冻结 Deployment/Run 的显式策略窗口；不得原地改写引用内容。
2. 发布引用新 SecretBinding/rotation policy 的 immutable Deployment，并只让未来 admission切换到新active binding。
3. 证明已有 Run继续使用其冻结闭包，未来 Run使用新闭包；日志、trace、metric、Event、DB和API回读均不出现Secret value。
4. 对旧version执行获批revoke场景：新解析必须fail closed，已取得的短期credential按policy到期，未知外部副作用进入人工处置。
5. 完成审计后由provider lifecycle回收旧version；资格流程不得自行销毁key material。

## 7. Rolling fault matrix

按 Component role 逐一执行 Pod kill、graceful drain、watch断开和node loss；一次只改变一个已记录变量。随后分别注入
PostgreSQL连接耗尽、NATS不可用、S3/KMS/Secret/Egress延迟或拒绝。每个场景都验证：

- 新claim停止或按WorkClass隔舱降级，critical-control不被business/Sandbox容量占满；
- 已有lease terminal、handoff或自然过期，不在失去fence后继续提交；
- readiness只反映该组件必需authority，不因单tenant/provider故障使全平台失活；
- 恢复后由数据库事实驱动收敛，无人工SQL状态修补。

## 8. 证据与验收

每个场景保存开始/结束时间、工具版本、批准记录、输入digest、注入动作摘要、SLI窗口、断言结果和原始报告content digest。
至少形成以下gate结果：

- `backup_restore`
- `artifact_s3_kms_faults`
- `event_outbox_recovery`
- `rolling_fault_recovery`

任一子场景失败时对应gate必须为`failed`，不能以skip或部分成功合并为`passed`。修复后必须使用新evidence root完整重跑，
不得覆盖失败报告。
