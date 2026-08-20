# ADR-0002：gVisor 使用 admission-locked Kubernetes single-Job Pod

| 属性 | 值 |
|---|---|
| 状态 | Accepted |
| 日期 | 2026-08-21 |
| 影响规范 | 01、03、04、07、14、18、cross-review、implementation-plan |

## 背景

CR-166 要求首版 gVisor 为每个 shared Job 创建全新 sandbox，同时禁止 plain runc fallback、privileged、hostPath、
device、host PID/network、runtime socket、metadata 和通用 Kubernetes API。实现审计发现，把 `runsc create/start/wait`
嵌套在普通 Kubernetes Executor Pod 内无法同时满足这些约束：direct runsc 需要 node namespace/cgroup/runtime 权限，
而现有 node-local UDS process attestor 又要求 hostPath。把清单渲染出来并不能证明该拓扑可启动或可隔离。

## 决策

首版 gVisor Executor 是独立的、credential-free 业务请求适配器和受限 Pod Launcher。它不在自身进程中执行用户代码，
而是通过专用 ServiceAccount 在单一 execution namespace 中创建一个 admission-locked Pod：

- Pod 必须使用按发布清单固定的 `RuntimeClass=runsc`，并调度到 NodeRestriction 保护的 gVisor node pool；
- 每个 fenced Job attempt 使用全新 Pod UID、filesystem、process/network namespace，terminal/lease loss 后删除；
- Launcher RBAC只允许该namespace中的`create/get/watch/delete` Pod与`get` Pod status/log，不允许Secret、ConfigMap、
  ServiceAccount、RBAC、Node、RuntimeClass、exec、attach、port-forward或任意其他资源；
- fail-closed ValidatingAdmissionPolicy把owner、镜像digest、runtimeClass、ServiceAccount、security context、volume、probe、
  resource、network和label/annotation closure固定到发布清单；Launcher不能通过构造Pod获得更大权限；
- guest Pod不automount token，不使用hostPath/device/host namespace/privileged，不访问metadata、Kubernetes API、Docker或
  runtime socket；package/input/output只通过Artifact Data Worker的一次性、fenced grant，network默认deny；
- `runsc`由节点容器运行时安装并按digest/版本生成node qualification evidence。不存在runc fallback；RuntimeClass或node
  evidence漂移时admission/claim/readiness fail closed；
- Kubernetes Pod只是physical attempt；shared Job仍是lease/retry/current physical-work authority。Pod status/log是需复核的
  外部evidence，不成为业务current-state authority。

WASI Executor继续使用node-local process attestation且不获得Kubernetes API。gVisor Launcher使用独立mTLS workload identity、
ServiceAccount、queue、permit、NetworkPolicy和process generation；它的Kubernetes权限不能被其他Executor复用。

## 否决方案

- Executor Pod内嵌套direct runsc：需要与既有安全禁令冲突的node/cgroup/runtime权限；
- privileged DinD、host runtime socket或hostPath cgroup：扩大host compromise面；
- Controller/API直接创建guest Pod：混合durable authority与不受信执行平面；
- plain runc/OCI fallback：违反首版backend闭集；
- 以fake adapter或只检查Helm字段声明gVisor完成：没有真实runtime evidence。

## 结果与资格

L3必须覆盖Pod request canonicalization、fence、watch/restart、log/result bounds和delete uncertainty；L4必须使用真实
Kubernetes admission/RBAC/NetworkPolicy与真实runsc RuntimeClass执行escape/cleanup/process-kill/node-loss测试；L5/L6再冻结
容量、soak和rollout/rollback evidence。未通过这些门禁前不得声明gVisor production-qualified。
