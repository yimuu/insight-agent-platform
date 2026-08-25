# Platform v2 Production L4～L6资格运行手册

状态：Pending / requires production-equivalent runner

仓库已提供`.github/workflows/platform-production-candidate.yml`，要求以exact commit读取GitOps environment closure并生成exact runtime/guest image、签名SBOM/provenance、
CandidateManifest、baseline与测试报告的signed传递闭包。只有受保护CI environment的实际运行产物及后续目标环境验证可作为本手册输入；
workflow存在或本地生成器通过不改变本页Pending状态。

本手册执行18的外部资格门禁，不改变产品合同，也不把静态Helm检查或development集群冒充为L4证据。

## 前置条件

- always-on、多节点Kubernetes runner；
- 独立且不重叠的WASI与gVisor节点池，execution node均有NodeRestriction保护的attestor label；
- `RuntimeClass/runsc`的handler与scheduling selector均为exact `runsc`闭包；
- v1 `ValidatingAdmissionPolicy`；
- exact CandidateManifest，包含全部首版component image digest并引用checked-in production QualificationProfile；
- 独立staging PostgreSQL、NATS、versioned S3/KMS、Secret Manager与workload identity；
- 新的、不会覆盖历史证据的输出目录。

单节点、`crun`/`runc`替代、重叠执行节点、缺失attestor label、mutable image、占位digest或缺失CandidateManifest
都必须在测试部署前失败。

## 拓扑预检

```bash
export PLATFORM_CANDIDATE_MANIFEST=/secure/ci-artifacts/candidate-manifest.json
export PLATFORM_CAPACITY_PROFILE=/secure/ci-artifacts/capacity-profile.json
export PLATFORM_QUALIFICATION_OUTPUT_DIR=/secure/ci-artifacts/platform-v2-qualification-$CI_RUN_ID

bash scripts/preflight-platform-production-qualification.sh
```

预检先用`platform-qualification validate-production-candidate`验证Candidate/Profile闭包，再采集Kubernetes version、
Ready node、runsc RuntimeClass和admission API。输出的`topology.json`只包含版本、计数、architecture、exact selector与
canonical topology digest；它不保存credential、Secret、Pod环境、外部URL或对象key。失败运行保留其raw preflight输出，
不得在同一路径覆盖重跑。

## 后续门禁

预检通过后，runner仍须依序完成：

1. 部署exact digest闭包并验证startup manifest、mTLS、NetworkPolicy、PDB/HPA与role-separated DB/storage pool；
2. 执行gVisor Launcher RBAC逐verb/resource负向矩阵和guest admission绕过矩阵；
3. 执行真实WASI/runsc ABI、limit、escape、cleanup、process-kill、watch-restart与node-loss；
4. 执行MCP、Artifact S3/KMS、Model、lane saturation、rolling fault recovery与backup/restore；
5. 按qualification profile执行不少于86,400秒的持续soak，冻结实测CapacityProfile；
6. 验证签名image/SBOM/provenance，完成GitOps rollout/rollback rehearsal与人工promotion；
7. 为每个required gate生成content-addressed artifact，并构造QualificationEvidenceManifest；
8. 运行：

```bash
cargo run --locked -p insight-platform-contracts --bin platform-qualification -- \
  validate-release-evidence \
  contracts/platform-v1/qualification/production-release-profile.json \
  "$PLATFORM_CAPACITY_PROFILE" \
  "$PLATFORM_CANDIDATE_MANIFEST" \
  "$PLATFORM_QUALIFICATION_OUTPUT_DIR/qualification-evidence.json"
```

只有该命令通过、GitOps environment repository收到同一exact digest且人工批准promotion后，才可归档通过报告并更新
implementation plan。任何missing/failed gate、profile/candidate漂移或evidence digest无法解析都保持Pending。

依赖故障、PITR/Artifact一致性恢复、NATS重建与Secret/KMS轮换按照
[`platform-v2-dependency-recovery.md`](../runbooks/platform-v2-dependency-recovery.md)执行；signed supply chain、
upgrade/rollback rehearsal、clean replacement、监视和GitOps回滚按照
[`platform-v2-clean-cut.md`](../runbooks/platform-v2-clean-cut.md)执行。两个手册都只定义操作与停止边界，不能替代目标环境中的
实际执行证据。

Launcher RBAC矩阵的执行入口为：

```bash
python3 scripts/qualify-platform-gvisor-rbac.py \
  --subject system:serviceaccount:platform-sandbox-exec:sandbox-insight-platform-sandbox-executor-gvisor \
  --namespace platform-sandbox-guests \
  --output "$PLATFORM_QUALIFICATION_OUTPUT_DIR/gvisor-launcher-rbac.json"
```

它明确验证允许的`create/get/watch/patch/delete pods`与`get pods/status`，并逐项拒绝list/update、log、
exec、attach、port-forward、ephemeralcontainers、Secret、ConfigMap、ServiceAccount、RBAC、Node和RuntimeClass。
任一额外权限或必要权限缺失都写入失败报告并返回非零。

guest admission绕过矩阵必须以Launcher已创建且已通过准入的真实guest Pod为源。探针删除服务器字段、恢复唯一
`insight.platform/await-fenced-start` scheduling gate，并先以Launcher身份执行一次必须成功的server-side dry-run；随后逐项
验证runtime、直接node绑定、Pod namespace、ServiceAccount、mutable image、privilege、resource、Secret/ConfigMap环境注入、
hostPath、mount、annotation、token audience和ephemeral container绕过均被拒绝：

```bash
python3 scripts/qualify-platform-gvisor-admission.py \
  --subject system:serviceaccount:platform-sandbox-exec:sandbox-insight-platform-sandbox-executor-gvisor \
  --namespace platform-sandbox-guests \
  --source-pod "$ADMITTED_GVISOR_GUEST_POD" \
  --output "$PLATFORM_QUALIFICATION_OUTPUT_DIR/gvisor-guest-admission.json"
```

源Pod必须是本次exact candidate运行产生的`insight-gv-<digest>`对象。正向探针失败、任一绕过被接受、源对象不存在或
输出路径已存在都写入失败证据并返回非零；不得用静态Helm渲染或策略语法检查替代这项L4证据。
