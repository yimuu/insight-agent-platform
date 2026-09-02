# Platform v2 Production L4～L6资格运行手册

状态：Not run / optional deployment release gate after CR-217

仓库内CR-216的L1、真实PostgreSQL L2与单节点真实OpenSandbox/Kubernetes/containerd-runc L3已经通过；这些证据不等于
production topology、容量、HA、恢复或promotion资格。本手册只供未来部署方在需要production-ready声明时执行，未产生exact
environment证据前必须保持Not run。

仓库提供`.github/workflows/platform-production-candidate.yml`，以exact commit读取GitOps environment closure并生成签名
CandidateManifest、SBOM、provenance与测试报告传递闭包。workflow存在、本地生成器通过或开发L3通过，都不会把本页状态改为passed。

## 前置条件

- production-equivalent多节点Kubernetes，节点使用受支持且固定版本的containerd/runc；
- Established `batchsandboxes.sandbox.opensandbox.io` CRD；
- source-pinned OpenSandbox Server、BatchSandbox Controller与execd official image digest，以及exact Platform Dispatcher/runner digest；
- OpenSandbox Server与Dispatcher只通过internal ClusterIP暴露，集群中不存在Sandbox public Ingress；
- operator-owned Direct与Disabled NetworkPolicy/CNI闭包，Direct拒绝internal/metadata范围，Disabled为零workload egress；
- OpenSandbox Server、Controller和Dispatcher各自使用least-privilege ServiceAccount；workload Pod不挂载ServiceAccount token、
  Platform credential、host network、host path或Docker/CRI socket；
- exact CandidateManifest与checked-in production QualificationProfile；
- 独立staging PostgreSQL、NATS、versioned S3/KMS、Secret Manager与workload identity；
- 新的、不会覆盖历史证据的输出目录。

单节点开发集群、mutable image、占位digest、缺失CRD、public ingress、host runtime socket、共享Platform credential或缺失
CandidateManifest都必须在测试部署前失败。

## 拓扑预检

```bash
export PLATFORM_CANDIDATE_MANIFEST=/secure/ci-artifacts/candidate-manifest.json
export PLATFORM_CAPACITY_PROFILE=/secure/ci-artifacts/capacity-profile.json
export PLATFORM_QUALIFICATION_OUTPUT_DIR=/secure/ci-artifacts/platform-v2-qualification-$CI_RUN_ID

bash scripts/preflight-platform-production-qualification.sh
```

预检先用`platform-qualification validate-production-candidate`和`validate-production-capacity`验证Candidate/Profile闭包，再采集
Kubernetes版本、Ready node、BatchSandbox CRD、Namespace、Deployment/DaemonSet、Service、Ingress、NetworkPolicy、PDB、HPA、
ServiceAccount、Role/RoleBinding、ClusterRole/ClusterRoleBinding及ValidatingAdmissionPolicy/Binding inventory。
`scripts/check-platform-production-topology.py`与`scripts/check-platform-production-workloads.py`把live inventory与exact digest、配置、
identity、安全和容量闭包比较。输出只保存版本、计数、closed identity和canonical digest，不保存credential、Secret、Pod环境、
外部URL或对象key；失败运行不得在同一路径覆盖重跑。

## L4 topology、identity与故障门禁

部署exact candidate后至少证明：

1. Sandbox物理路径只有Dispatcher -> internal OpenSandbox Server -> Kubernetes API -> BatchSandbox Controller -> containerd/runc；
2. Server、Controller、Dispatcher、runner与execd均使用Candidate中固定digest，readiness和rollout generation完整；
3. Server/Controller的Kubernetes API RBAC逐verb/resource负向矩阵拒绝Secret、Platform业务API、exec/attach/port-forward、Node修改和
   RuntimeClass修改；workload身份不能创建或修改BatchSandbox；
4. admission拒绝mutable image、privilege、hostPath、host network/PID/IPC、runtime socket、ServiceAccount token、Platform credential、
   wrong runner/config/profile digest和非Controller来源的workload Pod；
5. 真实并发create、response loss、Server/Controller restart、Dispatcher kill/reclaim、runner-start uncertainty、boot rollover、cancel、
   timeout、TTL/delete/absence与orphan cleanup均保持同一shared Job fence和单次Package activation；
6. Direct只允许已声明的外部网络且拒绝cluster/internal/metadata范围；Disabled无法出站；两者均无public ingress；
7. rolling drain、Pod/Node/DB/NATS/S3/KMS/Egress fault injection最终恢复或产生closed fail-safe结果，不伪造业务终态。

L4必须使用production-equivalent多节点拓扑；仓库的单节点L3不能替代这组证据。

## L5 capacity与soak门禁

- 按qualification profile执行mixed load和lane saturation，证明Sandbox饱和不消耗API、Orchestration、其他worker或critical-control容量；
- 验证queue/lease/cleanup/orphan lag、连接、内存、CPU、ephemeral storage和对象数量有界；
- 持续soak不少于86,400秒，基于实际结果冻结CapacityProfile、SLO与error-budget判断；
- 保存production Prometheus scrape、alert delivery和故障恢复窗口的content-addressed evidence。

未运行、提前停止、使用合成容量或缺失完整时间窗均为Not run/failed，不得推断通过。

## L6 supply chain、restore与promotion门禁

1. 验证全部image、SBOM、provenance、chart/CRD/config digest与CandidateManifest闭包；
2. 完成upgrade/rollback rehearsal、PostgreSQL PITR、Artifact一致性恢复、NATS重建和Secret/KMS轮换；
3. 为每个required gate生成至少一个不被其他gate复用的专属content-addressed summary artifact，并构造无digest别名、无未引用项的
   QualificationEvidenceManifest；共享原始观测可以作为额外输入，但不能成为某个gate唯一的证据；
4. 运行：

```bash
cargo run --locked -p insight-platform-contracts --bin platform-qualification -- \
  validate-release-evidence \
  contracts/platform-v1/qualification/production-release-profile.json \
  "$PLATFORM_CAPACITY_PROFILE" \
  "$PLATFORM_CANDIDATE_MANIFEST" \
  "$PLATFORM_QUALIFICATION_OUTPUT_DIR/qualification-evidence.json" \
  "$PLATFORM_QUALIFICATION_OUTPUT_DIR/artifacts"
```

`artifact_links[].name`必须逐一解析为artifact root下同名普通文件；validator流式重算byte length和SHA-256，拒绝缺失、符号链接、
长度或digest漂移，并核对manifest时间跨度至少86,400秒。命令成功只表示manifest结构和声明结果闭合；运行人员还必须验证Evidence
来自受保护CI producer、artifact store与组织批准的签名identity。只有这些验证通过、GitOps environment repository收到同一exact digest、
rollout/rollback observation完整且人工批准promotion后，才可归档通过报告并声明该environment production-ready。

依赖故障和恢复按照[`platform-v2-dependency-recovery.md`](../runbooks/platform-v2-dependency-recovery.md)执行；signed supply chain、
upgrade/rollback、监视和GitOps切换按照[`platform-v2-clean-cut.md`](../runbooks/platform-v2-clean-cut.md)执行。手册与静态检查不能替代目标
环境实际证据。CR-217当前结论是：L4 Not run、L5 Not run、L6 Not run，系统不声明production-ready。
