# Fresh-checkout 北极星旅程证据

状态：**Passed（repository scope）**。

## 证据合同

G1 不能再用整个 job 时长或场景测试主体时长间接推断。base qualification 在 checkout 前记录 UTC epoch；
轻量 qualifier 在真实 base roles ready 后，通过现有 `insight` CLI 与 public `/v1` 完成 Artifact、Policy、Agent、
Deployment、Binding 与 deterministic Run；只在 Run 达到 `succeeded` 且 Inline result 已重验后写入一次性 marker。
runner 立即生成 `insight.productization.north-star-report/v1`，然后继续执行不计入首次使用时延的完整
restart、Receipt、raw HTTP、Console 与场景测试。报告由
[`check-productization-north-star-report.py`](../../../scripts/check-productization-north-star-report.py) fail closed 校验：

- exact 40 字节 source revision、`insight.platform/v1`、base profile；
- fresh checkout 与 fresh project 均为 true；
- checkout start 到首个 Run result 的时间戳差值与 `elapsed_to_first_run_ms` 完全一致且不超过 600,000ms；
- 文档路径至多三条人工命令且不需要外部模型 key；
- `doctor`、`init`、`dev`、`first_run` 四项均有 Passed evidence；
- Run authority ID、`succeeded` 状态与 result verification 均闭合。

JSON Schema 位于
[`examples/productization/north-star-report.schema.json`](../../../examples/productization/north-star-report.schema.json)。
工作树本地运行、测试进程 exit code、缓存命中推断或后来完成的其他场景都不能替代该报告。

## Exact qualification

GitHub Actions [`33334106135`](https://github.com/yimuu/insight-agent-platform/actions/runs/33334106135) / job
[`99317793534`](https://github.com/yimuu/insight-agent-platform/actions/runs/33334106135/job/99317793534) 在 fresh
`ubuntu-24.04` checkout 上验证 exact revision
`aac4b74d37712b3739b2cc34fc4555da645d8d95`。下载的
`productization-base-north-star-aac4b74d37712b3739b2cc34fc4555da645d8d95` artifact 经仓库 checker 独立重验：

- `elapsed_to_first_run_ms = 509569`（8 分 29.569 秒，门槛 600000ms）；
- 两条 documented manual commands，外部模型 key 不需要；
- Run `run_01a0546a-f1b2-7e43-b1ed-6cc32e5f8135` 为 `succeeded`，Inline result 已验证；
- `doctor/init/dev/first_run` 全部 Passed；
- 报告 SHA-256：`dce94a774b86ca90f6fd78227a15688b8c5ffb47e3df1e1d91893c0f7aa580fc`。

因此 G1 的 repository-scope 北极星门禁关闭。该结果不覆盖真实多节点 Kubernetes、容量、混沌、restore、
24 小时 soak 或 production GitOps promotion，不能将 Platform spec00～18 升级为 Verified。
