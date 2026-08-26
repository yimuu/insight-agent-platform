# Platform v2 GitOps clean cut、监视与回滚运行手册

状态：Ready to execute / production approval pending

本手册覆盖 `signed_supply_chain`、`upgrade_rollback_rehearsal` 与 `gitops_rollout_rollback`，并规定 `/v1` clean
replacement 的人工审批边界。GitOps repository、registry、CI artifact store和Kubernetes rollout history是发布authority；
业务数据库不创建 Release/Gate/Candidate 状态。

候选制品由`.github/workflows/platform-production-candidate.yml`产生：所有action与GitOps environment输入固定commit SHA，后者与application
Helm/Docker closure共同形成deployment config digest；runtime与sandbox guest均以exact digest
签名并附SPDX SBOM和provenance；CandidateManifest与包含migration、测试报告、SBOM和各签名bundle摘要的release-bundle index分别签名。
运行人员必须验证该实际CI run来自受保护environment和目标Git commit；仅审查workflow源码不等于供应链门禁通过。

## 1. 发布输入

promotion请求必须只引用不可变输入：

- exact Git commit与所有component image digest；
- 签名SBOM、provenance、migration artifact digest；
- QualificationProfile、CandidateManifest、已资格CapacityProfile和QualificationEvidenceManifest digest；
- production topology、HardLimitProfile、Helm/Kustomize render digest；
- 目标environment GitOps ref及上一已资格闭包digest；
- change owner、approver、observer、rollback authority和窗口。

任何mutable tag、占位digest、签名/attestation缺失、evidence gate失败、profile/candidate不匹配或审批人缺失都停止promotion。

## 2. 预检

1. 在全新的evidence root运行 `scripts/preflight-platform-production-qualification.sh`，确认多节点、独立node pool、
   exact `runsc` RuntimeClass、ValidatingAdmissionPolicy和版本偏差均合格。
2. 验证镜像签名、SBOM和provenance的subject均为CandidateManifest中的exact digest，builder/source/material闭包符合组织policy。
3. 运行 `platform-qualification validate-release-evidence`并传入只读artifact root，确认26个required gate恰有一个通过结果；命令必须逐一读取
   `artifact_links[].name`对应的普通文件并重算长度和SHA-256，禁止只验证manifest内自洽引用或接受符号链接。
4. 验证生产数据库部署状态。首次clean cut只允许经确认未发布的candidate schema使用唯一baseline；baseline一旦发布，
   后续只允许immutable forward migration，禁止重放或替换`0001`。
5. 确认backup/PITR、Artifact versioning、KMS/Secret恢复、流量切换和上一闭包回滚路径在当前窗口可用。

## 3. Rehearsal

在production-equivalent staging使用与production相同的digest闭包执行：

1. 从空环境应用唯一baseline并启动全部role；执行startup manifest/readiness与minimal `/v1` smoke。
2. 从上一已资格闭包执行schema step、滚动drain/handoff、新Pod readiness和流量切换。
3. 在新闭包创建并推进Run，证明冻结bindings不随active Deployment或GitOps rollout改变。
4. 将GitOps ref回退到上一闭包；若数据库存在不可逆forward migration，只能按获批runbook前滚兼容应用，禁止旧binary直接读取
   不兼容schema或回滚数据库历史。
5. 再次前滚到candidate并验证Receipt replay、SSE cursor、Task、Artifact和MCP路径。

rehearsal任何一步需要dual write、旧wire fallback、手工业务SQL修补或Installation Release row时，门禁失败并停止clean cut。

## 4. Clean replacement

1. 冻结旧系统新admission，等待已接受工作按其合同terminal或达到获批停机边界；记录剩余工作摘要。
2. 执行最后backup与恢复点确认。若旧数据不属于已审核迁移范围，不导入新baseline；clean cut不构造兼容层。
3. 合并经人工批准的GitOps exact-digest变更，由controller执行schema step和各role rollout。
4. 新Pod必须先通过startup manifest与readiness；按Management/Runtime API、Scheduler、Workers、Artifact、MCP、Sandbox顺序
   验证依赖与隔舱，再开放`/v1`流量。
5. 验证路由只有目标`/v1`与`insight.platform/v1`，无`/v2`、dual write、legacy fallback或旧协议桥接。
6. 保存GitOps commit、controller reconciliation、Kubernetes rollout和流量切换记录的content digest。

## 5. 监视窗口

至少覆盖CapacityProfile规定的rollout observation window，并持续检查：

- API availability/latency/error、Run admission、queue age、lease loss、outbox/recovery lag；
- Model、Capability、Context、MCP、Sandbox、Artifact各lane saturation；
- PostgreSQL/NATS/S3/KMS/Secret/Egress依赖健康；
- cross-tenant拒绝、Secret/log redaction、旧fence与Receipt replay负向探针；
- Artifact orphan/delete backlog和无界memory/connection增长。

指标必须使用低基数维度；告警/证据不得包含tenant payload、prompt、tool argument、URL query、object key或Secret。

## 6. 回滚决策

出现以下任一情况立即关闭新admission并由rollback authority裁决：startup manifest漂移、P0/P1安全问题、跨租户访问、
Secret泄漏、schema不一致、数据完整性未知、error budget超限、critical-control不可用或无法在RTO内恢复。

回滚只允许：

1. GitOps ref回到上一已资格的exact应用/config/profile闭包；
2. 保持已发布forward migration，并部署与其明确兼容的上一闭包或修复闭包；
3. 必要时恢复到独立实例并按依赖恢复手册验证，再由人工决定流量切换。

禁止逆向修改已发布baseline、删除生产数据、恢复mutable tag、启用旧wire fallback或通过业务表伪造release current state。
若不存在已证明schema-compatible的回滚闭包，保持流量关闭并前滚修复；不能把“旧Pod已启动”当作安全回滚证明。

## 7. 证据与归档

`signed_supply_chain`、`upgrade_rollback_rehearsal`和`gitops_rollout_rollback`分别保存独立报告。promotion完成后：

1. 重新计算最终Candidate/Capacity/QualificationEvidence digest并确认与GitOps ref一致；
2. 记录人工approval、promotion时间、observation结果及上一/当前闭包digest；
3. 更新 `docs/current` 为实际运行合同；
4. 将00～18状态推进到Verified，再在current文档与证据交叉检查后归档规范和通过报告。

手册存在、静态Helm检查或GitOps PR创建都不构成gate通过；只有目标环境实际执行并保留可解析证据才允许标记`passed`。
