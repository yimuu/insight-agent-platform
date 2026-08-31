# ADR-0004：CLI/HTTP authoring 保持 `/v1` 一致

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-207 |
| 日期 | 2026-08-31 |
| 影响阶段 | Productization、Agent Product Experience |

## 决策

`contracts/platform-v1/openapi.yaml` 和 owner JSON Schema 是唯一 authoring wire contract。`insight apply --advanced`
读取一个版本化、closed 的 project request manifest，并将其逐项映射到公开 `/v1` 请求：create/update Draft、
validate、publish、create Deployment、activate、create Run。它保留完整Resource、Version、Deployment、Binding、
Operation与Run ID输出，作为automation/authority入口。

默认作者入口是`insight agent validate|publish|run|list|get|logs|result`。closed `agent.yaml`只支持评审过的
`deterministic`与`model_chat`模板，由无网络shared compiler确定性生成同一public lifecycle请求；它不是第二runtime、
第二DSL或服务端authority。默认输出只显示Agent、发布状态、Run与结果；`--verbose`显示运营摘要，只有
`--debug-authority`显示底层ID、ETag、Receipt与cursor。隐藏仅发生在客户端展示层，不允许跳过任何CAS、Receipt、
validation、deployment或Run frozen binding。

manifest 是请求编排文件，不是另一套 Agent DSL：

- 不定义 Plan、Node、Provider、工具或调度语义；这些内容必须是对应 `/v1` schema 的 request body；
- 不接受自由 URL、shell、数据库连接、internal RPC、Secret value 或“跳过 validation/activate”的隐式选项；
- mutation 必须显式携带/生成 Receipt、If-Match 和 trace；CLI 重试遵守 `problem` retryability 与 effect-aware
  约束；
- 同一生命周期提供 checked curl fixture，CLI/Console 均不得拥有 HTTP 之外的写通道。

本阶段不提供任何语言 SDK。语言客户端在 public contract、CLI 和 scenario suite 稳定后，以独立 ADR 评估。

## 后果

初次使用以Agent产品词汇完成旅程；完整Resource lifecycle仍可审计并通过advanced入口取得。CLI可以代管active-head
切换所需的Receipt/ETag/Operation/cursor，但不能吞掉CAS/Receipt冲突、伪造恢复状态或用新随机值重做不确定effect。
