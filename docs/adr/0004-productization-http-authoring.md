# ADR-0004：CLI/HTTP authoring 保持 `/v1` 一致

| 属性 | 值 |
|---|---|
| 状态 | Accepted |
| 日期 | 2026-08-29 |
| 影响阶段 | Productization M2、M4 |

## 决策

`contracts/platform-v1/openapi.yaml` 和 owner JSON Schema 是唯一 authoring wire contract。`insight apply`
读取一个版本化、closed 的 project request manifest，并将其逐项映射到公开 `/v1` 请求：create/update Draft、
validate、publish、create Deployment、activate、create Run。它必须显示每一步的 Resource、Version、Deployment、
Binding、Operation 和 Run ID。

manifest 是请求编排文件，不是另一套 Agent DSL：

- 不定义 Plan、Node、Provider、工具或调度语义；这些内容必须是对应 `/v1` schema 的 request body；
- 不接受自由 URL、shell、数据库连接、internal RPC、Secret value 或“跳过 validation/activate”的隐式选项；
- mutation 必须显式携带/生成 Receipt、If-Match 和 trace；CLI 重试遵守 `problem` retryability 与 effect-aware
  约束；
- 同一生命周期提供 checked curl fixture，CLI/Console 均不得拥有 HTTP 之外的写通道。

本阶段不提供任何语言 SDK。语言客户端在 public contract、CLI 和 scenario suite 稳定后，以独立 ADR 评估。

## 后果

初次使用会保留 Resource lifecycle 的可见性与审计性，但减少用户手写 header、cursor 与 polling 的负担。
CLI 不能以“方便”为由隐藏 active-head 切换或吞掉 CAS/Receipt 冲突。
