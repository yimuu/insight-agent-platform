# CR-218 Sandbox boot rollover L1～L3 资格报告

| 属性 | 值 |
|---|---|
| 日期 | 2026-09-04 |
| 实现基线 | `df53f9da`及本报告同批L3门禁 |
| 合同 | CR-218 revision 1；spec 03/10/14、cross-review、ADR-0007 |
| 结论 | L1～L3 passed；L4～L6 Not run |

## 闭包结论

`ActivationAuthorized/PotentiallyStarted`或`Started`的shared Sandbox Job观察到不同runner boot时，Dispatcher现在先以
current Job fence持久化domain-separated rollover evidence，再以同一摘要提交`UnknownOutcome`并进入
`ReconciliationRequired + cleanup`。它不会向新boot发送旧activation frame，也不会等待或读取旧boot的结果，不创建新的token、
candidate、sandbox或physical attempt。

## L1 contract/unit

- `cargo test --locked -p insight-platform-sandbox --lib`：12/12通过；
- 两个Dispatcher回归分别从`ActivationAuthorized`与`Started`进入不同boot，均证明observation一次、terminal一次、
  `activate_calls=0`，Started分支也不进入旧result wait；
- domain回归证明相同observation幂等、第二个不同observation拒绝、evidence digest篡改fail closed；
- runner state在任何外部激活前重验sandbox/request identity；
- `cargo clippy --locked -p insight-platform-sandbox --all-targets --all-features -- -D warnings`通过。

## L2 fresh PostgreSQL

在Kind PostgreSQL中创建专用临时空库，执行`platform-schema provision`后得到schema contract 8、migration digest
`sha256:ba5080ceb152d56d54684f8345f0809b947bb880b660a904995b293d9b9d7612`、inventory digest
`sha256:6e09d0a4ff73e880bebe8cf221c7a4ea064130522d67efcf4f3695af37cc9067`与23张表。

随后只运行
`opensandbox_boot_rollover_is_durable_unknown_outcome`：1/1通过。该测试证明rollover observation在同一Job fence下持久化，
相同观察重放保持同一摘要，terminal transaction把Job与Invocation原子推进到`ReconciliationRequired`。测试后专用数据库已删除。

## L3 real OpenSandbox/Kubernetes

环境为三节点Kind Kubernetes v1.35.8、containerd 2.3.4、官方OpenSandbox Server/Controller/execd精确摘要和真实
BatchSandbox CRD。`opensandbox_kubernetes_l3_runner_boot_changes_after_workload_pod_recreation`执行如下故障：

1. 创建使用精确Package镜像的BatchSandbox，等待runner `Armed`；
2. 激活`90s`长任务并等待同一boot进入`Started`；
3. 删除该Sandbox的workload Pod；
4. 等待Controller创建不同Pod UID，并通过真实lifecycle API读取到不同runner boot；
5. 证明新runner因Pod级`emptyDir`丢失回到`Armed`，同时sandbox/request identity保持不变；
6. 删除BatchSandbox并确认最终BatchSandbox与workload Pod均为零，恢复Dispatcher为1个Ready副本。

定向L3为1/1通过；`platform-opensandbox-client`该测试target strict Clippy通过。第一次fixture探针未缩容Dispatcher时，
候选被生产orphan sweeper按合同删除；正式运行按既有L3脚本隔离规则临时将Dispatcher缩容为0并在退出时恢复，说明业务孤儿清理和
provider-only资格夹具的authority边界生效。

## 边界

本证据关闭CR-218的代码与L1～L3门禁。它仍运行在单台macOS/OrbStack宿主上的Kind节点，不包含独立物理故障域、生产Prometheus、
gVisor、容量/饱和/24小时soak、backup/restore、升级/回滚、受保护CI签名或GitOps promotion，因此不能提升L4、L5或L6状态。
