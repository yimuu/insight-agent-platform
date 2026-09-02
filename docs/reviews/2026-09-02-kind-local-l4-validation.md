# Platform v2 Kind 本地验证与修复报告

- 执行时间：2026-09-02～2026-09-03
- 最终代码基线：`fd3e636d00e8cee5c750b04df2bd89e24ca4bf39`
- 集群：`insight-l4-local`
- 最终 preflight 证据：`target/local-kind-l4-complete-fd3e636d-final-preflight/`（gitignored，仅限本机诊断）
- 最终结论：**完整 Platform workload/preflight 与部分 L4 机制在本机通过；正式 L4～L6 仍为 Not run**

## 1. 执行摘要

本轮把验证从“仅 Sandbox 三个角色”推进到了完整 Platform：三节点 Kind 集群中，CandidateManifest 登记的
16 个 `ComponentRole`、25 个隔离 workload pool 全部部署，47 个角色副本全部 Ready；production topology/workload
preflight 在最终 live inventory 上通过。验证还真实覆盖了 containerd、BatchSandbox CRD、准入、跨节点
NetworkPolicy、OpenSandbox 最小物理生命周期，以及 Egress 副本删除后的 Service endpoint 保留和副本恢复。

验证过程中复现并修复了三类仓库问题：Registry NetworkPolicy 渲染无效、若干 Platform 内部调用的
NetworkPolicy selector/namespace/port 不闭合，以及 full profile 对部分配置集合越界并遗漏 Management API cursor key。
修复已分别提交为：

- `b71a9889` `fix: render valid registry network policy selectors`
- `fd66cd2d` `fix: scope full-profile process launches`
- `fd3e636d` `fix: close platform network policy routes`

Kind 的三个节点仍共享同一台 macOS/OrbStack 宿主；本轮也没有生产 Prometheus、真实外部 Provider、gVisor
强隔离、24 小时 soak、restore 或 GitOps promotion。因此本报告只证明本机可验证的拓扑、部署和故障机制，不能作为
production L4、L5 或 L6 的 passed evidence。

## 2. 环境与前置条件

| 项目 | 实测结果 | 判定 |
|---|---|---|
| Kind | `v0.33.0` | 通过 |
| Kubernetes client / server | `v1.35.8` / `v1.35.8` | 通过，零 minor skew |
| Ready schedulable node | 3 | 通过，要求至少 2 |
| 节点 runtime | 三节点均为 `containerd://2.3.4` | 通过 |
| 执行架构 | Linux `arm64` | 通过 |
| BatchSandbox CRD | `Established=True`，live digest 精确匹配 | 通过 |
| Metrics API | Metrics Server Ready | 只支持 HPA/preflight 机械验证 |
| Prometheus | 仅安装 Operator CRD，没有生产 Prometheus 实例 | 未验证真实 scrape/alert |
| 物理故障域 | 三节点共享 OrbStack kernel `7.0.14-orbstack-...` | 不是独立主机/区域故障域 |

使用独立 kubeconfig `/private/tmp/insight-kind-l4.kubeconfig` 和 kubectl
`/opt/homebrew/opt/kubernetes-cli@1.35/bin/kubectl`，没有修改默认 Kubernetes context。

## 3. 完整 Platform 部署结果

最终 live inventory 与本地 CandidateManifest/CapacityProfile 的结构闭包一致：

| 指标 | 最终结果 |
|---|---|
| `ComponentRole` | 16/16 |
| 隔离 workload pool | 25/25 |
| 角色期望/Ready 副本 | 47/47 |
| NetworkPolicy | 40 |
| 平台 runtime image | `sha256:c7aeb3c8010fcfa6f5e6f0ddace7622a02dde9de8572eb6d04ca695c30e8c40f` |
| deployment config digest | `sha256:fd0d83ac05be51cd3d995de271286c14135386bbd1a2b17206233851d13336bf` |
| Helm release | 14 个，最终均为 `deployed` |
| 非 Ready Pod | 0 |

聚合角色中，`context_worker` 为 4 个 pool / 8 个 Ready 副本，`mcp_host` 为 6 个 pool / 12 个 Ready
副本，`egress_secret_broker` 为 Broker 与 Security Authority 两个 pool / 4 个 Ready 副本；其余普通角色各 2
个副本，Sandbox Dispatcher、OpenSandbox Server、Controller 各 1 个副本。

本机 host-process full profile 也使用实际二进制启动验证，21 个进程全部 Ready。该路径暴露出部分 profile 仍构造
全部 role launch、从缺失 digest map 下标取值而 panic，以及 Management API 未收到 run-event cursor key 的问题；
`fd66cd2d` 将 launch 限定为实际存在的 role，并补齐 cursor key path/digest。

## 4. NetworkPolicy 问题与修复

完整部署最初有 Orchestration、Context Dataset、Remote Context、Remote Capability 等 Pod 反复启动失败。根因不是
应用协议，而是 chart 中多条内部路由与实际 namespace、workload label 或监听端口不一致：

- Artifact Data Worker 仍允许旧的 `scheduler`/`public-gateway` selector，遗漏 Context Dataset caller；
- Orchestration 与 Context Dataset 指向不存在的 `platform-artifact` namespace 或错误 component label；
- Orchestration/MCP Discovery 到 Artifact 使用 `8443`，实际为 `9443`；
- Egress Broker 仍允许旧的泛化 Capability selector，且遗漏 Remote Context caller；
- Registry Validation Worker 把 label 直接放在 selector 根部，渲染对象不符合 Kubernetes selector schema，并且只取第一个 PostgreSQL CIDR。

`b71a9889` 和 `fd3e636d` 修正上述 chart。静态 closure checker 也增加了 peer schema、内部目标、端口以及反向
ingress 的闭包校验，最终输出：`16 roles, 25 isolated pools`。修正后的失败 Pod 重建后全部 Ready。

## 5. 本地 Service DNAT 临时覆盖

本地依赖使用 LocalStack Service：`10.96.165.138:443 -> Pod 10.244.1.12:4566`。Egress chart 按生产 HTTPS
Provider 合同放行目的端口 `443`；Kind 的 `kube-network-policies` 在这条 Service 路径上按 DNAT 后的 Pod
targetPort 判定，导致一个新建 Egress Pod 的 Secret Provider readiness 超时。

诊断结果为：DNS 与 kube-dns `:53` 正常；策略修正前，Service `:443` TLS 握手超时，Pod `:4566` 也超时；在
本地 live NetworkPolicy 额外放行 `10.244.1.12/32`、`10.96.165.138/32` 的 TCP `4566` 后，使用
`localhost.localstack.cloud` SNI 的 TLS 1.3 握手立即成功，新建 Egress Pod 随后在原 worker 上 `1/1 Ready`。

这项变更只存在于临时 Kind release，不写入生产 chart：生产 Provider endpoint 应使用 CandidateManifest 审核后的真实
HTTPS 路由；本地 LocalStack 的 Service targetPort 不能成为生产默认值。Helm 再次升级 `l4-security` 会覆盖这项
live patch，后续若重建本地环境应在本地 values/render 中显式重放。

Sandbox 到 Kubernetes API 也有同类 CNI 差异：本地 release 同时放行 Service VIP `10.96.0.1:443` 和 DNAT 后
control-plane endpoint `192.168.97.3:6443`。覆盖后 authenticated BatchSandbox list 返回 HTTP 200。

## 6. 故障注入结果

在 Egress 两个副本分别位于 `worker` 与 `worker2`、Service 有两个 endpoint 时，删除 `worker` 上的 Ready Pod：

1. Service 保留 `worker2` 上的健康 endpoint，依赖 Egress 的 Remote Context/Capability 等 workload 保持 Ready；
2. 首个替代 Pod 因上一节的 LocalStack targetPort 策略差异而 fail closed，没有带病进入 Service；
3. 应用本地端口覆盖并重新创建后，替代 Pod 在 `worker` 上恢复，restart count 为 0；
4. Egress Service 最终重新拥有 `10.244.1.49` 与 `10.244.2.56` 两个 Ready endpoint。

这证明单副本删除期间的 endpoint 收敛和基本可用性，但不是独立主机故障、node-loss、连接排空、长时间 chaos 或
完整 rolling-fault recovery gate。

## 7. PostgreSQL 连接上限发现

完整副本数首次启动时，本地 PostgreSQL 默认 `max_connections=100` 被平台各隔离 pool 的连接池耗尽，出现
`sorry, too many clients already`，并引发多角色 readiness 失败。本轮将本地测试实例提高到 400 并重启同一 Pod；
最终当前连接数为 124，所有 workload 恢复。

这是本地依赖容量不足，不是把 400 固化为生产建议。它说明 CapacityProfile 不能只声明应用副本数，还必须用 L5
mixed-load/soak 实测冻结数据库连接预算、pool 隔舱和 headroom。

## 8. 正式 preflight 结果

最终使用已构建的 `target/debug/platform-qualification` 执行
`scripts/preflight-platform-production-qualification.sh`，没有通过 `cargo run` 重编译或重复执行 Rust test。

| 证据 | 摘要/结果 |
|---|---|
| Candidate closure | `sha256:b10ddba2a22bcc1a9b264f90562bf6500038c6fad1849a3388b4f10829b16693` |
| Capacity input closure | `sha256:1d807af5961a0748c23ee3c144cdf20e55931c934fcbeec51a0dfd87ea3ab571`；仅结构有效，尚未获得容量资格 |
| Topology | `sha256:ab55efaece7a3b7665dad9c96159c3818dc88c7dda18c8a6f9a339c6b2ea584d` |
| Workloads | `sha256:a0bdc181e2b76a5142b090297474fb0e0e13ee4c12b76497d5d0db2995eccd10` |
| BatchSandbox CRD | `sha256:176f3ccba68f75fc8311d34a49551b78e9743659a28d794ecb7f24605675d1af` |
| topology/workload preflight | 通过 |

本地输入是为此次现场构造的 exact live candidate 和副本闭包，不是由受保护 CI、签名 registry、批准的
CapacityProfile 与真实生产环境共同产生的 release authority。

## 9. 最终检查

| 检查 | 结果 |
|---|---|
| `ruby scripts/check-platform-component-workload-closure.rb` | 通过：16 roles / 25 pools |
| 6 个相关 Helm chart（含 Registry）lint | 通过：0 failed |
| `cargo fmt --check` | 通过 |
| 完整 Platform live readiness | 通过：全部 Deployment 达到期望副本，非 Ready Pod 为 0 |
| Egress 故障注入与恢复 | 通过本地机制验证 |
| Rust tests | 按要求未重复执行 |
| 临时诊断资源 | `egress-policy-diag` 已删除，无测试 Pod 残留 |

## 10. 分层判定与下一步

| 层级 | 本地已获得证据 | 正式资格状态 |
|---|---|---|
| L4 Topology / isolation / recovery | 三节点、containerd、CRD、准入、跨节点策略、完整 live inventory、一个 Egress 副本故障恢复 | **Not run**：同宿主节点、无 gVisor/生产 Prometheus/真实 Provider，动态 mTLS/RBAC/租户隔离与完整 fault matrix 未完成 |
| L5 Capacity | Metrics API 可用；发现并修正本地 PostgreSQL 连接上限 | **Not run**：无批准的 CapacityProfile、mixed load、饱和验证和至少 86,400 秒 soak |
| L6 Release | 仓库内 fail-closed validator 已存在 | **Not run**：无受保护 CI 签名供应链、backup/restore、upgrade/rollback 和 GitOps promotion 证据 |

下一步不应继续把 Kind 结果包装成 production passed。若要推进正式资格，应创建 production-equivalent 的独立节点环境，
接入真实 Prometheus、gVisor 和适用 Provider，执行完整 L4 动态隔离/故障矩阵；随后按批准的 CapacityProfile 运行至少
24 小时 L5 soak，最后由受保护 CI/GitOps 完成 L6 签名、restore、rollback 和人工 promotion。

当前 Kind 集群与平台 workload 保持运行，便于继续检查；只删除了本轮临时诊断 Pod。
