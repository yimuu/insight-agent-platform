# CR-220 本机 L4 mechanics 资格报告

| 属性 | 值 |
|---|---|
| 日期 | 2026-09-04 |
| 实现基线 | `8164064b21f8970371dfc99aaec83ba6042045fd` |
| 合同基线 | `0dc1091f`，CR-220 revision 1 |
| 结论 | 本机 L4 mechanics passed（`production:false`）；正式 production-equivalent L4、L5、L6 仍为 `Not run` |

## 闭包结论

本轮在单台 macOS/OrbStack 宿主上的三节点 Kind 中完成本机可验证的 L4 拓扑、身份、网络、漂移与故障恢复矩阵。
证据绑定实现基线、不可变镜像 ID和deployment config digest；12项动态检查全部通过，测试结束后三个节点Ready，28个
Platform namespace Deployment的50/50 replicas Ready，50个Pod无非Ready项，Sandbox workload namespace无BatchSandbox/Pod残留。

该结论只表示本机mechanics闭合。三个Kind节点共享一个物理宿主，LocalStack替代真实云服务，因此不能证明独立故障域、
production HA、强多租户隔离、真实云workload identity、容量/SLO、24小时soak、backup/restore、签名供应链或GitOps promotion。

## CR-220 revision 1 修复

首次滚动部署暴露出readiness与orphan cleanup竞态：旧Dispatcher会把新Dispatcher创建、没有shared Job row的synthetic
readiness candidate判断为`DeleteMissingOwner`并删除，导致新Pod无法Ready。修复包括：

- candidate metadata增加closed `purpose = job | readiness`，normal create固定为job，full readiness固定为readiness；
- HTTP metadata要求exact 12字段，缺失/unknown purpose拒绝整页作为删除依据；
- orphan sweep在repository point-read前跳过readiness，PostgreSQL repository防御性拒绝readiness orphan裁决；
- BatchSandbox和Pod admission都要求purpose只能为job/readiness；
- 定向测试证明job/readiness constructor、wire codec、full readiness cleanup，以及orphan sweep对readiness零repository/零delete。

升级时旧、新Dispatcher实际发生滚动重叠，新版本startup full probe成功并完成rollout。动态恢复probe存在期间，strict orphan
decoder对其非完整operator metadata按合同fail closed且未删除；probe删除后再等待一个scan周期没有新增错误。最终cleanup backlog为0，
且无readiness candidate残留。

## 环境与候选闭包

| 项目 | 观测值 |
|---|---|
| Kubernetes server / kubectl | `v1.35.8` / `v1.35.8` |
| Ready schedulable nodes | 3 |
| 节点架构 / runtime | `arm64` / `containerd://2.3.4` |
| provider / physical store | `opensandbox_kubernetes` / `batchsandbox_crd` |
| public ingress | false |
| BatchSandbox CRD digest | `sha256:176f3ccba68f75fc8311d34a49551b78e9743659a28d794ecb7f24605675d1af` |
| Platform runtime image（含Dispatcher与runner） | `sha256:0122ed3bff39293ffa8fa2da0380edc2112b17830bb61b45c17db7048cb50e65` |
| Deployment config digest | `sha256:dd2682e488336710ae0b8a349a93ec48a07520986c2989f1c1fdc4f3f73057ab` |
| Workload closure | 16 closed roles、42 NetworkPolicy |
| Topology / workload digest | `sha256:ab55efaece7a3b7665dad9c96159c3818dc88c7dda18c8a6f9a339c6b2ea584d` / `sha256:e4973faf8a7b87f741165a60005633e617e3d7e71e5dc9d8e47126e0a3c13333` |

## 仓库定向验证

- 受影响的`insight-platform-sandbox`、`insight-platform-opensandbox-client`、`insight-platform-postgres` all-targets check通过；
- 4个exact unit tests通过：job/readiness constructor、closed purpose codec、full readiness create/list/Armed/delete/absence、
  orphan sweep对readiness零repository/零delete；
- 三个受影响package的all-targets strict Clippy通过；
- `scripts/check-platform-sandbox-deployment.sh`通过，Helm admission已锁定purpose closure；
- 按用户要求没有重复运行无关的完整Rust test suite。

## 本机 L4 动态矩阵

| Gate | 结果 | 观测 |
|---|---|---|
| inventory | pass | exact topology and 16-role workload closure accepted |
| network-policy | pass | cross-node allow/deny/allow transitions enforced |
| rbac | pass | Server、Controller、workload ServiceAccount满足最小权限 |
| admission | pass | 禁止的Secret与调用方自行创建的Pod fail closed |
| mtls | pass | 无client certificate拒绝，批准证书完成TLS |
| config-drift | pass | 语义漂移保持Unready，exact config恢复 |
| image-drift | pass | unknown digest保持Unready，批准digest恢复 |
| rolling-restart | pass | Runtime API滚动期间至少一个Ready endpoint |
| pod-fault | pass | 删除一个Egress Pod后服务存活并恢复 |
| node-fault | pass | worker转为NotReady，服务存活，节点与workload自动恢复 |
| opensandbox-recovery | pass | physical object跨Server/Controller restart存活并正常删除 |
| final-readiness | pass | 全部Platform Deployment和Pod恢复Ready |

## 取证与清理说明

原始本机证据位于`/private/tmp/insight-l4-cr220-8164064b/evidence`，未作为production artifact提交：

| 文件 | SHA-256 |
|---|---|
| `summary.json` | `2211226281f3667bb0793feec5d8a74681f743898b330434dc0e482cb3f78455` |
| `topology.json` | `9b77115cfefde611b91e1f9a0293ed6933393f1f447cf689303e2a98e65a6017` |
| `workloads.json` | `8be5bbbff8ee3d70b61a70bb173b3cfd9803e0f200e646e046157522a0ceb385` |
| `checks.json` | `069059b44a6064ee1ad499476033bb0609d44436bc84f0129d3d8425a99badf1` |

运行前发现上轮故障注入遗留两条synthetic cleanup Job。精确删除了唯一含已废弃
`runner_boot_rollover_digest`、任何当前版本均不可解析的一条；另一条由旧Dispatcher通过正常cleanup fence完成，随后重新升级当前版本。
没有清空数据库或删除集群。动态矩阵的NetworkPolicy、mTLS、配置/镜像漂移、Pod/Node和OpenSandbox故障均已由脚本恢复；
Kind集群保留供后续检查。

## 最终判定

本机可验证的L4 mechanics完成，报告与证据均明确`production:false`。正式production-equivalent L4继续`Not run`，
L5 capacity/24小时soak与L6 restore/supply-chain/GitOps promotion也继续`Not run`；不得据此声明production-ready。
