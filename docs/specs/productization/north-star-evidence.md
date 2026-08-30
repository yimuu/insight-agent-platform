# Fresh-checkout 北极星旅程证据

状态：**Pending exact GitHub base qualification**。

## 证据合同

G1 不能再用整个 job 时长或场景测试主体时长间接推断。base qualification 在 checkout 前记录 UTC epoch；
`deterministic_first_run.rs` 只在首个 public Run 达到 `succeeded` 且 Inline result 已重验后写入一次性 marker。
runner 随后生成 `insight.productization.north-star-report/v1`，并由
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

## 待关闭项

提交包含该合同的 exact revision 后，手动运行 `Productization base journey` 的 `base` profile，下载
`productization-base-north-star-<revision>` artifact，以 checker 重验并记录 SHA-256。只有报告为 Passed 才恢复
Productization Convergence 的 repository-scope Completed 状态。
