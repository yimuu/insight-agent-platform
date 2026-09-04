# CR-219 Sandbox control recovery L1～L3资格报告

| 属性 | 值 |
|---|---|
| 日期 | 2026-09-04 |
| 实现基线 | `a5aceabb`；crash-window门禁`58a84199` |
| 合同 | CR-219 revision 1；spec 03/10/14/18、cross-review、ADR-0007 |
| 结论 | L1～L3 passed；L4～L6 Not run |

## 闭包结论

Sandbox-backed Capability的cancel/timeout现在由既有shared Job承载唯一durable control intent。显式control事务同时推进
Invocation与Job到`Cancelling`；Dispatcher用reserved critical-control容量执行有界数据库扫描。终态事务按
quota → Invocation → Job锁序一次提交Job、Invocation、四维quota settlement、Event/Outbox及cleanup intent，且不调用provider。
pre-claim timeout保持attempt 0、无physical evidence和零provider；started control保留原physical identity，late worker fence写入被拒绝。

## L1 contract/unit

- `insight-platform-contracts`：102/102通过，Job状态图与生成`states.json`/manifest一致；
- `insight-platform-invocations`：15/15通过，只有Cancelled/TimedOut允许pre-claim logical attempt 0；
- `insight-platform-sandbox`：13/13通过，覆盖intent摘要、第二intent拒绝、pre-claim零physical、control terminal及late fence拒绝；
- `insight-platform-sandbox-dispatcher`：2/2通过；control scan在普通claim前使用独立critical-control permit；
- OpenSandbox client定向回归通过：首次DELETE 204与重放DELETE 404都必须得到strict、bounded absence observation；
- `check-platform-contracts`、`check-platform-v1-contracts.py`、format/diff门禁通过；受影响六包all-target/all-feature strict Clippy通过。

## L2 fresh PostgreSQL

在Kind PostgreSQL中创建专用空库`insight_cr219_l2`，`platform-schema provision`得到schema contract 8、migration digest
`sha256:ba5080ceb152d56d54684f8345f0809b947bb880b660a904995b293d9b9d7612`、inventory digest
`sha256:6e09d0a4ff73e880bebe8cf221c7a4ea064130522d67efcf4f3695af37cc9067`与23张表。

定向`opensandbox_shared_job_is_fenced_atomic_and_recoverable`为1/1通过，覆盖：

- public `control_capability_invocation`把started Job与Invocation及摘要绑定intent原子推进到`Cancelling`；
- 旧lease/provisioning replay返回`StaleFence`，不能覆盖control；
- bounded scan提交Cancelled，attempt保持1，四维quota各结算一次并释放reservation；
- database-time deadline在pre-claim物化timeout，提交TimedOut、attempt 0、无physical/cleanup effect；
- Job/Invocation终态、terminal digest、Event/Outbox和quota处于同一事务可见边界。

## L3 real Kind/OpenSandbox

环境为单台macOS/OrbStack宿主上的三节点Kind，Kubernetes v1.35.8、containerd 2.3.4、真实BatchSandbox CRD、官方
OpenSandbox Server/Controller/execd固定镜像，以及Package镜像
`insight-agent-platform@sha256:18e9d07f90c6d7791c9bafe23b4471652c67bd8f06a84e2f116b2a14a50056da`。
运行时合同摘要为`sha256:36b0ab993663980370353a3bf14bf6a8dc777ee707913588411fd223cbc76bc9`。

cancel故障链路：

1. 将集群内Dispatcher缩容为0，由当前源码的测试Dispatcher创建并激活真实30秒Package，观察到runner `Started`；
2. 通过真实Capability control入口写入cancel intent，确认Job/Invocation均`Cancelling`且BatchSandbox仍存在；
3. 对测试进程发出SIGABRT，形成intent commit之后、terminal commit之前的硬崩溃窗口；
4. 将OpenSandbox Server缩容为0，由全新进程执行control scan；定向测试1/1通过，Job/Invocation进入Cancelled、attempt保持1、
   四维quota释放且terminal Event拥有同事务Outbox，cleanup仍required；
5. 恢复Server后执行cleanup。首次资格尝试发现官方404 code为`KUBERNETES::SANDBOX_NOT_FOUND`，严格client此前只接受零body；
   修复为按vendor合同解析bounded、closed ErrorResponse后，DELETE重放与absence proof 1/1通过；
6. 独立started deadline场景在8.32秒内1/1通过，数据库时间物化timeout并提交TimedOut，随后删除真实候选并取得absence proof。

结束时`opensandbox-controller`、`opensandbox-server`、`sandbox-dispatcher`均为1/1 Ready；
`platform-sandbox-workloads`中BatchSandbox与Pod均为零。测试使用的数据库与端口转发在取证后删除/关闭。

## 边界

本证据关闭CR-219代码与L1～L3门禁。它不是production-equivalent L4：三个Kind节点共享同一物理宿主，不含独立故障域、gVisor、
生产Prometheus、容量/饱和、24小时soak、backup/restore、升级/回滚、受保护CI签名或GitOps promotion。因此正式L4、L5、L6继续
`Not run`；必须在当前HEAD重建并部署精确镜像、重新生成candidate/capacity/environment闭包并完成本机L4 mechanics后，才可更新本地
非生产验证报告。
