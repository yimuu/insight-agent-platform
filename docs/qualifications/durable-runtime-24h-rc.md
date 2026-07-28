# Durable Runtime 24 小时 RC 资格验收

创建日期：2026-07-28

状态：Pending / requires always-on runner

## 目标

为已经完成 implementation 与 capacity qualification 的 Durable Runtime 50 active Run 能力补齐
release-candidate 级 24 小时稳定性证据。本验收不引入新产品语义，也不阻塞既有
`capacity-qualified` 结论；在通过前不得把现有结果描述为 `release-candidate-qualified`。

已完成基础：

- [归档设计规范](../archive/specs/2026-07-26-durable-runtime-50-active-runs-optimization.md)；
- [容量资格报告](../../bench/reports/2026-07-26-durable-runtime-50-active-runs-optimized.md)；
- v3 Gate A/B/C 与 Gate D 2 小时已经通过。

此前两次 24 小时尝试均因 macOS `Clamshell Sleep` 导致 OrbStack 控制面失效，属于 benchmark
基础设施失败，不形成资格结论。下一次必须使用 always-on Kubernetes runner；若使用 macOS，
必须全程接电、保持开盖并记录宿主机 sleep 证据，但仍优先选择 always-on runner。

## 固定工作负载与故障

- 持续时间：不中断的 24 小时；
- 到达率：10 arrivals/s；
- in-flight 上限：50；
- 20 分钟时强制断开并恢复 PostgreSQL listener；
- 45 分钟时在存在 pending claim 的窗口重启 runtime，验证 lease/fence 收敛；
- 使用通过 2 小时 Gate D 的同一 v3 镜像、C1 profile、schema contract 和 workload。

权威执行入口和宿主机要求见
[`bench/k8s/README.md`](../../bench/k8s/README.md)。正式运行必须使用新的结果目录，不得覆盖此前
两次无效尝试的原始证据。

## 通过门槛

- terminal success 不低于 99.9%，其余失败全部可解释且不存在数据丢失；
- deadlock、PostgreSQL conflict、OOM kill 和非预期 Pod restart 均为 0；
- listener 故障与 claimed-task runtime restart 均实际发生并正确恢复；
- 最后 30 分钟 runtime RSS、数据库连接数和 queue oldest age 无持续上升；
- 抽样 Run 的 event、projection、terminal snapshot 与 public event 顺序通过一致性校验；
- 完整保存环境、镜像 digest、配置、k6、数据库、Pod、故障和一致性证据。

任何宿主机休眠、控制面中断、故障注入缺失、运行不足 24 小时或证据缺失都必须 fail closed，不能
用 2 小时结果外推，也不能标记为通过。

## 完成动作

1. 保存新的 immutable raw evidence 和独立 RC 报告；
2. 更新容量报告，明确 24 小时结果和适用边界；
3. 只有正式通过后才允许使用 `release-candidate-qualified` 表述；
4. 将本文件标记最终结果并移入 `docs/archive/qualifications`；
5. 更新 [`docs/README.md`](../README.md) 和归档索引。
