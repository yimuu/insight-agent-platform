# Platform v2 Kind 本地 L4 跟进验证报告

- 日期：2026-09-02
- 仓库提交：`e6d17e9b`（验证开始时工作树 clean）
- 集群：`insight-l4-local`
- 证据目录：`target/local-kind-l4-l6-e6d17e9b/`（gitignored，仅限本机诊断）
- 结论：**Sandbox L4 拓扑/隔离的本地实测通过；Platform-wide L4～L6 仍为 Not run**

## 1. 结论

安装 Kind 后，本机已不再受 OrbStack 单节点、`docker://` runtime 和旧 kubectl 的前置条件阻塞。独立三节点
Kind 集群通过仓库 production topology checker，生成 topology digest
`sha256:ab55efaece7a3b7665dad9c96159c3818dc88c7dda18c8a6f9a339c6b2ea584d`。

本轮还实测了跨节点 NetworkPolicy、live CRD digest、RBAC/ValidatingAdmissionPolicy、真实 OpenSandbox
Server/Controller/Dispatcher 就绪，以及 `Server -> Kubernetes API -> BatchSandbox Controller -> Pod ->
containerd` 的最小物理生命周期。

这仍不是 Platform-wide production L4 通过：正式 preflight 在 topology 阶段之后，正确拒绝了未部署的 13 个
Platform ComponentRole、与 CapacityProfile 不一致的单副本/HPA，以及与合成 CandidateManifest 不一致的 Dispatcher
镜像。L5 24 小时 soak、容量结论、真实故障域和 L6 发布/恢复证据均未执行。

## 2. 环境与前置条件

| 项目 | 实测结果 | 判定 |
|---|---|---|
| Kind | `v0.33.0` | 通过 |
| Kubernetes node image | `kindest/node:v1.35.8@sha256:07b2536e...9323c0` | 固定摘要 |
| kubectl client / API Server | `v1.35.8` / `v1.35.8` | 通过，零 minor skew |
| Ready schedulable node | 3 | 通过，要求至少 2 |
| 节点 runtime | 三节点均为 `containerd://2.3.4` | 通过 |
| 执行架构 | Linux `arm64` | 通过 |
| ValidatingAdmissionPolicy API | `admissionregistration.k8s.io/v1` | 通过 |
| 宿主故障域 | 三节点共享 OrbStack kernel `7.0.14-orbstack-...` | 仅本地逻辑/容器实测，不是独立主机 |

使用独立 kubeconfig `/private/tmp/insight-kind-l4.kubeconfig`，没有修改 OrbStack 默认 context，也没有持久修改
shell PATH。

## 3. NetworkPolicy 实测

在 `insight-netpol-probe` namespace 中把 client 与 HTTP server 调度到两个不同 worker：

1. 安装策略前，client 请求 server 返回 `ok`；
2. 应用只选择 server 的 ingress default-deny 后，请求第一次重试即失败；
3. 增加只允许 `access=allowed` client 的规则后，请求第一次重试即恢复并返回 `ok`。

这证明当前 Kind CNI 对跨节点流量真实执行 NetworkPolicy，不只是 API 接受了 NetworkPolicy 对象。

## 4. CRD、控制面与物理生命周期

从当前 Sandbox Helm chart 安装三项 source-pinned CRD，均为 `Established=True`：

- `batchsandboxes.sandbox.opensandbox.io`；
- `pools.sandbox.opensandbox.io`；
- `sandboxsnapshots.sandbox.opensandbox.io`。

仓库检查器对 live BatchSandbox CRD 完整规范化 spec 计算出的摘要为
`sha256:176f3ccba68f75fc8311d34a49551b78e9743659a28d794ecb7f24605675d1af`，与 reviewed expected
digest 完全一致。

复用此前 CR-216 L3 已固定的本机 OCI 镜像，离线导入三个 Kind 节点，没有重新运行 Rust test：

| 组件 | live image digest | 最终状态 |
|---|---|---|
| Sandbox Dispatcher | `sha256:c7aeb3c8...8c40f` | `1/1 Ready` |
| OpenSandbox Server | `sha256:ae8dfbb2...ab778` | `1/1 Ready` |
| BatchSandbox Controller | `sha256:a9a5f73c...3532a` | `1/1 Ready` |
| OpenSandbox execd | `sha256:0d8f44cf...c2684` | 在冒烟 Pod 中由 containerd 启动 |
| Sandbox package | `sha256:18e9d07f...056da` | 在冒烟 Pod 中由 containerd 启动 |

本地 Postgres 16.14 使用仓库 `platform-schema provision` 初始化并验证 schema contract 8；这是部署初始化命令，
不是 Rust test。Helm release `insight-sandbox` 最终为 revision 3、`STATUS=deployed`。

通过 OpenSandbox HTTP API 发起最小 create 后，观察到：

- Server 创建 `BatchSandbox/da62f51f-...`；
- Controller 创建 `Pod/da62f51f-...-0`；
- Pod 调度到 worker，execd init 与 package 镜像均报告 exact digest 已在节点本地并成功启动；
- 测试使用故意为空的 Runner config，只用于物理链路冒烟，因此主容器最终 `Error`，不作为 Runner 协议成功证据；
- 60 秒 TTL 自动删除 BatchSandbox 与 Pod，workload namespace 最终零残留。

完整 Runner activation/result/restart/orphan 语义已有
`docs/qualifications/cr-216-opensandbox-l1-l3.md` 的 L3 证据，本轮按要求没有重复执行相同 Rust 测试。

## 5. Kind 专用临时网络覆盖

Sandbox chart 默认只放行 Kubernetes Service VIP `10.96.0.1:443`。Kind 的 kube-network-policies 在本机路径按
DNAT 后的 control-plane endpoint `192.168.97.3:6443` 评估，导致 Server 的 BatchSandbox list 请求超时，Dispatcher
初次无法 Ready。

本轮没有修改仓库默认值，而是对临时 release 同时放行：

- `10.96.0.1/32:443`；
- `192.168.97.3/32:6443`。

覆盖后，从 Server Pod 发起的 authenticated BatchSandbox list 返回 HTTP 200，Dispatcher 重建后为 `1/1 Ready`。
生产安装必须从目标集群发现并审核实际 API endpoint/CNI 语义，不能复制本机 endpoint。

## 6. Live 准入负向测试

| 操作 | 结果 |
|---|---|
| 在 workload namespace 创建 Secret | 被 `opensandbox-inactive-surfaces` policy 拒绝 |
| 普通用户在 workload namespace 创建 Pod | 被 `opensandbox-pods` policy 拒绝，仅 Controller identity 可创建 |
| 负向测试残留 | Secret 0、Pod 0 |

当前 release 包含 3 个 `ValidatingAdmissionPolicy` 和 3 个 `Deny` binding；Sandbox 两个 namespace 共 8 个
NetworkPolicy，控制面 Service 均为 internal `ClusterIP`，无 public Ingress。

## 7. 正式 preflight 结果

执行 `scripts/preflight-platform-production-qualification.sh`：

1. 合成 CandidateManifest 结构闭包通过；
2. CapacityProfile 输入闭包通过，但工具明确说明不因此获得资格；
3. live topology checker 通过并写出 `topology.json`；
4. production workloads checker fail closed。

`topology.json` 的关键 observed 值：

| 字段 | 值 |
|---|---|
| `ready_schedulable_node_count` | 3 |
| `container_runtime_versions` | `["containerd://2.3.4"]` |
| `batchsandbox_crd_digest` | `sha256:176f3ccb...d1af` |
| `provider` / `physical_store` | `opensandbox_kubernetes` / `batchsandbox_crd` |
| `public_ingress` | `false` |

workload 阶段拒绝原因：

- 本机只部署 Sandbox 3 个角色，缺少其余 13 个 Platform ComponentRole；
- 三个 Sandbox role 的单副本/HPA 与合成 CapacityProfile 不一致；
- Dispatcher live image 与合成 CandidateManifest 中的占位 digest 不一致；
- 本集群没有 Metrics API，HPA CPU target 为 unknown，不能生成容量或 soak 结论。

因此本轮只把“本机是否能真实覆盖多节点 Kubernetes/Sandbox 拓扑逻辑”从失败推进为通过，没有把 Platform-wide
L4、L5 或 L6 标为 passed。

## 8. 最终分层判定

| 层级 | 本地结果 | 正式资格状态 |
|---|---|---|
| Sandbox L4 topology / NetworkPolicy / admission / physical lifecycle | 通过本地实测；Runner 业务帧沿用既有 L3 证据 | 非 production L4 |
| Platform-wide L4 workload / isolation / recovery | preflight 正确拒绝未部署角色和容量漂移 | Not run |
| L5 capacity / 24h soak | Metrics API、完整角色、生产负载与 24h 窗口均未提供 | Not run |
| L6 supply chain / restore / promotion | 未执行受保护 CI、签名、restore、GitOps promotion | Not run |

Kind 解决了单机上的节点数量、containerd、CRD、准入和跨节点策略验证问题；它不能制造独立宿主、网络、电源、存储
和区域故障域。真实 production L4～L6 仍需按 reviewed qualification profile 在目标环境生成不可伪造证据。
