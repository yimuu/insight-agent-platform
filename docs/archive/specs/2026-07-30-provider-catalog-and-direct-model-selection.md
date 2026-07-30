# Provider Catalog 与直接模型选择优化规范

> **归档状态：已实施并验证。** 当前使用合同见
> [DSL v1 指南](../../current/dsl.md)与[部署与运维](../../current/operations.md)。

日期：2026-07-30

状态：Implemented / verified（2026-07-30）

目标阶段：1.0 之前

变更类型：Provider Boundary / Agent DSL / Configuration / Clean Cutover

影响范围：Provider Catalog、平台配置、Agent DSL、模型解析与冻结、结构化输出、Quickstart、测试和文档

## 1. 决策摘要

本规范删除“Agent 引用模型别名、`models.yaml` 再把别名映射到 Provider”的间接层。Agent 的 `llm`
步骤直接声明 Provider 路由和 Provider 侧模型 ID：

```yaml
- type: llm
  id: answer
  model:
    provider: dashscope-cn
    id: qwen3.6-flash
  parameters:
    temperature: 0.3
    enable_thinking: false
  response: string
```

其中：

- `type: llm` 保持不变；LLM 调用不是 Agent，不能改名为 `type: agent`；
- `provider` 是平台已知的 Provider 路由 ID，不是传输协议名；
- `id` 是原样发送给 Provider 的模型 ID，不是平台别名或展示名称；
- 不提供 `model: dashscope-cn/qwen3.6-flash` 字符串简写，因为合法模型 ID 自身可能包含 `/`；
- `general_chat`、`vision_chat` 等模型别名从公共配置中删除；
- `config/models.yaml` 及 `platform.yaml` 中必需的 `models.config` 一并删除；
- 已启用 Agent 中出现的 `model` 引用就是部署实际使用的模型集合，不再维护第二份 `enabled` 或模型列表；
- Provider Catalog 由平台维护，负责 Provider 身份、路由、适配器、凭据约定和客观模型元数据；
- `stream`、工具、`temperature`、`enable_thinking` 等属于调用配置，不写入 Catalog；
- `response` 是输出合同；结构化输出始终依赖 Prompt 约束和本地解析/Schema 校验，Provider 原生 JSON
  模式只作为内部优化，不再要求用户声明 `json_object_output`；
- 中国站与国际站等不同端点是不同的 Provider 路由，例如 `dashscope-cn` 和
  `dashscope-intl`。选择 `provider` 即选择端点，不做隐式跨区域故障转移。

这是一次 pre-1.0 clean cutover。实现、仓库 Agent、fixtures、Quickstart 和文档必须在同一变更中
迁移；不保留模型别名解析、旧 `models.yaml` 加载或双格式 Agent 解析。

## 2. 当前问题

当前配置把四类不同职责压在一个用户维护的模型条目中：

```yaml
models:
  general_chat:
    type: open_ai_chat
    base_url: https://dashscope.aliyuncs.com/compatible-mode/v1
    api_key_env: OPENAI_API_KEY
    model: qwen3.6-flash
    capabilities: [json_object_output]
    connect_timeout: 5s
    request_timeout: 2m
```

这里同时混入了：

1. 业务别名：`general_chat`；
2. Provider 连接信息：`base_url`、`api_key_env`；
3. 传输适配器：`open_ai_chat`；
4. 模型能力和调用行为：`json_object_output`、timeout。

由此产生以下问题：

- “兼容 OpenAI API”被误当成“Provider 是 OpenAI”，DashScope 因而使用了误导性的
  `OPENAI_API_KEY`；
- 每个模型重复 Provider endpoint、鉴权和 timeout；
- Agent 和 `models.yaml` 分别维护引用与列表，重命名别名会造成无业务价值的联动；
- Action-only 部署也必须携带一份无实际调用的模型配置；
- 用户需要人工声明 Provider/Adapter 本应知道的低层能力；
- `json_object_output` 被当成结构化输出的前置条件，使公共输出合同依赖传输优化；
- 中国站、国际站、多账户和私有网关没有明确、可冻结的路由身份；
- 模型元数据和每次调用选择混在一起，容易把 `enable_thinking: false` 之类的业务选择错误固化为
  Provider 能力。

## 3. 目标

1. Agent 作者能够在一个位置明确看到实际 Provider 路由和模型 ID；
2. 已知 Provider 的 endpoint、推荐凭据环境变量和 adapter 由平台统一维护；
3. Provider、模型客观事实、LLM 调用选择和运行时输出策略形成清晰分层；
4. 结构化输出不依赖用户声明 `json_object_output`；
5. 没有 LLM 步骤的部署不需要模型配置或模型凭据；
6. 中国站、国际站、单端点 Provider 和自定义网关使用同一解析模型；
7. 支持平台内置模型、用户自定义 Provider/模型和可选治理策略，但不恢复全局模型别名表；
8. Deployment Revision 能冻结完整且不含秘密值的模型解析结果；
9. 配置错误在启动/编译阶段失败，而不是到第一次真实调用时才发现；
10. 公共 DSL 命名保留 `llm` 与 `agent` 的语义边界。

## 4. 非目标

- 不设计跨 Provider 或跨区域自动故障转移；
- 不让运行时通过付费探测自动猜测模型能力；
- 不把价格、展示名称、营销标签或完整上游模型市场作为最小 Catalog 的必需内容；
- 不允许 Agent 文件直接覆盖 Provider endpoint、鉴权 header 或 adapter；
- 不把秘密值写入 Agent、Catalog、Plan、Deployment Revision、日志或错误；
- 不保证 Prompt 一定能使模型生成合法 JSON；平台通过解析、Schema 校验和现有重试/失败语义处理；
- 不在本规范中实现 Provider 插件市场或远程 Catalog 服务；
- 不引入 Agent 级模型别名、默认模型或隐式模型选择；
- 不把 `type: llm` 改名为 `type: agent`。

## 5. 参考设计与取舍

### 5.1 Dify

Dify 将 Provider 和模型实体分离：Provider 负责统一鉴权和支持模型列表，模型实体描述模型类型、
feature、属性和参数规则；同时区分预定义模型和用户自定义模型。参考：

- [Dify Model Specs](https://docs.dify.ai/en/develop-plugin/features-and-specs/plugin-types/model-designing-rules)
- [Dify Tongyi Provider 定义](https://github.com/langgenius/dify-official-plugins/blob/main/models/tongyi/provider/tongyi.yaml)
- [Dify OpenAI-compatible Provider 定义](https://github.com/langgenius/dify-official-plugins/blob/main/models/openai_api_compatible/provider/openai_api_compatible.yaml)

本平台吸收“Provider 拥有鉴权和预定义模型知识”“自定义兼容端点显式配置”这两个边界，但不复制
Dify 面向 UI/市场的展示、价格和表单元数据。

### 5.2 OpenClaw

OpenClaw 使用 Provider/模型组成的模型引用；官方 Provider 插件发布自己的 Catalog，显式
`models.providers` 主要用于自定义 Provider、代理、base URL 或覆盖。参考：

- [OpenClaw Model Providers](https://docs.openclaw.ai/concepts/model-providers)
- [OpenClaw Qwen Provider](https://docs.openclaw.ai/providers/qwen)

本平台吸收“Provider 路由是一等身份”“已知兼容信息归 Provider Catalog”“自定义路由显式声明”
三个边界。但公共 YAML 使用结构化的 `provider`/`id`，不使用容易与模型 ID 中 `/` 冲突的组合字符串。

### 5.3 DashScope 凭据与结构化输出

阿里云 Model Studio 官方文档使用 `DASHSCOPE_API_KEY`，并明确 endpoint 会因区域和协议而不同；
DashScope OpenAI-compatible API 也支持 `response_format.type=json_object`。参考：

- [获取和配置 DashScope API Key](https://www.alibabacloud.com/help/en/model-studio/get-api-key)
- [Qwen 结构化输出](https://help.aliyun.com/en/model-studio/qwen-structured-output)

因此内置 DashScope 路由的默认凭据名采用 `DASHSCOPE_API_KEY`。原生 `json_object` 支持只作为
adapter 可选择的传输优化，不能成为 Agent 输出合同。

## 6. 配置分层

目标架构包含四层，后层不能反向污染前层：

| 层 | 所有者 | 内容 | 不应包含 |
|---|---|---|---|
| Provider 连接 | Provider Catalog / 运维扩展 | 路由 ID、adapter、endpoint、鉴权约定 | temperature、thinking 开关、业务别名 |
| 模型 Profile | Provider Catalog / 运维扩展 | Provider 模型 ID、输入模态、硬限制、客观支持事实 | 本次调用是否启用 thinking/stream |
| LLM 调用 | Agent `llm` 步骤 | model selector、messages、tools、stream、parameters | endpoint、API key、adapter |
| 输出执行策略 | Runtime | Prompt 约束、原生输出模式选择、解析、Schema 校验 | 用户伪造的 Provider capability |

### 6.1 “支持”与“启用”

Catalog 可以记录一个模型“支持 thinking”，但不能写：

```yaml
enable_thinking:
  default: false
```

`enable_thinking: false` 是某次调用的选择，应位于 Agent 的 `parameters`。同理：

- `tools: supported` 可以是模型事实，工具列表和 `tool_choice` 是调用选择；
- `streaming: supported` 可以是模型事实，`stream: true` 是调用选择；
- `temperature` 的上游合法范围可以是模型约束，`temperature: 0.3` 是调用选择；
- `context_tokens` 是模型硬限制，某次请求的 token budget 是调用选择。

## 7. Provider Catalog

### 7.1 定位与权威

Provider Catalog 是随平台版本发布的只读资源，不是每个部署必填的用户配置。初期采用仓库内置、
随二进制发布的版本化 manifest；未来可改为 Provider 插件，但必须保持本规范的解析结果和冻结合同。

Catalog 对以下事实具有权威：

- canonical Provider route ID；
- adapter/协议实现；
- canonical endpoint；
- 默认凭据环境变量名和鉴权方式；
- 内置模型 ID；
- 运行时安全或编译期校验所需的最小客观模型元数据；
- adapter 已验证的兼容差异。

Catalog 更新是平台发布的一部分，必须有版本或内容 digest。它不是秘密存储，也不能读取秘密值。

### 7.2 最简 Catalog

只接入一个 Provider、只支持文本模型时，最简形态如下：

```yaml
catalog_version: 1

providers:
  dashscope-cn:
    adapter: openai_chat
    endpoint: https://dashscope.aliyuncs.com/compatible-mode/v1
    credential:
      type: bearer
      env: DASHSCOPE_API_KEY
    models:
      qwen3.6-flash: {}
```

这已经足以替代当前模型条目中的 `type`、`base_url`、`api_key_env` 和 `model`。Catalog 不需要
为每个模型重复 endpoint，也不需要 `enabled`、默认 temperature 或 thinking 开关。

### 7.3 多端点 Provider

区域端点不是一个 Provider 下由运行时猜测的数组，而是独立、稳定的路由：

```yaml
providers:
  dashscope-cn:
    adapter: openai_chat
    endpoint: https://dashscope.aliyuncs.com/compatible-mode/v1
    credential:
      type: bearer
      env: DASHSCOPE_API_KEY

  dashscope-intl:
    adapter: openai_chat
    endpoint: https://dashscope-intl.aliyuncs.com/compatible-mode/v1
    credential:
      type: bearer
      env: DASHSCOPE_API_KEY
```

单端点 Provider 只定义一个路由，例如 `deepseek`；不需要 `default`、`region` 或长度为一的
`endpoints` 包装层。

选择 `dashscope-cn` 后，Deployment Revision 必须冻结中国站 route 和 endpoint identity。
运行时不得因超时或限流静默切换到 `dashscope-intl`。未来若需要 failover，必须单独定义路由策略、
数据驻留、凭据、计费、幂等和可观测性合同。

### 7.4 最小模型 Profile

模型 Profile 只保存编译或安全执行确实需要的客观事实：

```yaml
providers:
  dashscope-cn:
    # connection fields omitted
    models:
      qwen3.6-flash:
        input: [text]
      qwen-vl-plus:
        input: [text, image]
```

以下字段不是最小 Catalog 的必需项：

- 展示名称、描述、图标和价格；
- `enabled`；
- `temperature.default`；
- `enable_thinking.default`；
- `streaming: true`、`tools: true` 等已由 adapter 普遍保证且不参与静态判定的重复事实；
- `structured_output.prompt_fallback`；
- 用户手写的 `json_object_output`。

只有当某项差异确实参与 fail-closed 校验或 request shaping 时，才允许加入 Profile。例如模型明确
不接受图片、thinking 使用特殊参数名，或上游存在硬 context/output limit。未知或未经验证的营销
能力不能写成 `supported`。

### 7.5 内置模型的解析策略

内置 Provider 路由采用 Catalog allowlist：

- Provider 不存在：启动失败；
- Provider 存在但模型 ID 不在该路由的 Catalog：启动失败；
- 模型存在但 Agent 使用了 Catalog 明确不支持的输入模态或调用特性：启动失败；
- Catalog 没有参与某项校验的元数据：不得擅自推断支持，也不得为填表而伪造默认值。

这使拼写错误和过时模型在部署前暴露。用户需要使用 Catalog 尚未包含的新模型时，可以先通过第
11 节的显式扩展注册；不允许靠运行时付费探测把错误配置变成非确定状态。

## 8. Agent DSL

### 8.1 Canonical model selector

`llm.model` 的唯一公共形态是：

```yaml
model:
  provider: dashscope-cn
  id: qwen3.6-flash
```

约束：

- `provider` 必须匹配 canonical Provider route ID；
- `id` 是非空、不透明的 Provider 模型 ID，可以包含 `/`、`.`、`-` 等 Provider 合法字符；
- `provider` 和 `id` 都是必填字段；
- `name`、`model_name`、`alias`、`endpoint`、`api_key_env` 等未知字段拒绝；
- 不提供 scalar、`provider/model`、别名或隐式默认模型的替代语法；
- 同一 Agent 的不同 `llm` 步骤可以直接选择不同模型。

例如文本规划和视觉理解承担不同业务角色时，分别在步骤上声明真实模型：

```yaml
- type: llm
  id: plan
  model:
    provider: dashscope-cn
    id: qwen3.6-flash
  messages: [...]
  response: Plan

- type: llm
  id: inspect_image
  model:
    provider: dashscope-cn
    id: qwen-vl-plus
  messages: [...]
  response: Finding
```

不再创建 `general_chat`、`vision_chat` 之类的间接角色别名。业务角色由步骤 ID、Prompt、输入和
输出类型表达，模型字段只表达执行模型。

### 8.2 为什么保留 `type: llm`

顶层：

```yaml
kind: agent
```

表示整份文档定义一个 Agent。步骤中的：

```yaml
type: llm
```

表示一次模型调用。两者不是同一抽象。`type: agent` 应保留给未来“调用另一个 Agent / 子 Agent”
的步骤，不能用来替代普通 LLM leaf。

### 8.3 调用参数

调用选择继续属于 `llm` 步骤：

```yaml
- type: llm
  id: answer
  model:
    provider: dashscope-cn
    id: qwen3.6-flash
  stream: true
  tools: [current_time]
  tool_choice: auto
  parameters:
    temperature: 0.3
    enable_thinking: false
    max_tokens: 4096
  response: Answer
```

参数必须由 adapter 和已知模型约束验证，然后在 Plan 中冻结。Catalog 可以提供合法范围和字段映射，
但不能通过“默认值”替 Agent 作者作业务选择。未写参数时由明确的 runtime/provider adapter 默认
语义处理，不能从任意 Catalog model row 注入隐藏业务行为。

## 9. 模型集合与部署解析

### 9.1 模型集合的来源

用户不再维护全局模型列表。部署实际使用的模型集合按以下方式计算：

1. 读取平台配置中启用的 Agent；
2. 编译每个 Agent；
3. 遍历所有可达 `llm` 步骤；
4. 收集去重后的 `{provider, id}`；
5. 通过内置 Catalog 和显式用户扩展解析；
6. 只对被引用的 Provider 检查凭据；
7. 校验模型 Profile、输入模态、参数和 adapter；
8. 将解析结果冻结进 Deployment Revision/Plan identity。

因此 Action-only Quickstart 的集合为空，不需要 dummy model，也不要求 `DASHSCOPE_API_KEY`。

### 9.2 Deployment Revision 冻结内容

每个模型引用至少冻结：

- Provider route ID；
- Provider model ID；
- adapter ID 及其兼容版本；
- endpoint identity；
- Catalog version/digest；
- 生效的非秘密 Provider 设置；
- 参与编译或 request shaping 的模型 Profile/digest；
- 编译后的调用参数；
- 输出执行策略版本。

秘密值和秘密摘要不得冻结。可以记录使用的 secret reference，例如环境变量名
`DASHSCOPE_API_KEY`，但不能记录其值。

同名 Agent 在 Catalog、Provider extension、endpoint、adapter 或模型 Profile 变化后必须形成新的
Deployment Revision；已经 admission 的 durable Run 继续使用其冻结身份，不能随进程当前配置漂移。

### 9.3 启动失败边界

以下问题在 production readiness 前失败：

- Provider route 未注册；
- 内置或扩展模型未注册；
- 引用模型所需的凭据不存在；
- Agent 消息包含图片，但模型 Profile 明确不接受图片；
- 参数类型、范围或 adapter 映射不合法；
- 自定义 Provider 定义不完整；
- 同一 ID 同时由内置 Catalog 和用户扩展定义；
- Catalog/extension digest 无法稳定计算。

未被任何启用 Agent 引用的 Provider 不得因缺少凭据阻止启动。

## 10. 结构化输出

### 10.1 唯一公共合同

Agent 的：

```yaml
response: Answer
```

是唯一公共输出合同。若 `Answer` 是结构化类型，编译器生成 JSON Schema，运行时执行：

1. 将返回 JSON 的要求和 Schema 约束加入 Prompt；
2. 若 Catalog/adapter 已验证 Provider 原生 `json_schema`，优先使用该模式；
3. 否则若已验证原生 `json_object`，可使用该模式；
4. 否则只使用 Prompt 约束；
5. 对最终文本执行本地 JSON 解析；
6. 对解析结果执行本地 Schema 校验；
7. 失败时按现有模型错误、重试和 terminal 语义处理。

无论是否使用原生模式，第 5、6 步都不能省略。Provider 成功响应不等于业务输出有效。

### 10.2 删除的公共能力

普通用户配置不再出现：

```yaml
capabilities: [json_object_output]
```

也不提供：

```yaml
structured_output:
  prompt_fallback: true
```

原因是：

- Prompt fallback 是平台正确性策略，不是部署者逐模型选择；
- 原生 JSON mode 是 adapter 优化，不是业务输出类型；
- 用户不能仅凭写一个 capability 字符串改变 Provider 的真实能力；
- 同一 `response` 合同应在支持不同原生模式的 Provider 上保持一致。

内部实现可以保留比 `json_object_output` 更精确的 adapter capability/profile，但它不属于 Agent 或
普通 Provider 配置，并且不能作为结构化 `response` 的唯一准入条件。

## 11. 用户扩展与治理

### 11.1 为什么仍允许用户配置模型

删除 `models.yaml` 不等于禁止用户使用新模型。两类来源并存：

- 平台内置 Catalog：常用官方 Provider 和已验证模型；
- 平台配置中的可选 Provider extension：私有网关、OpenAI-compatible 服务、独立账户或 Catalog
  尚未包含的模型。

Agent 引用仍是“实际使用列表”；extension 是连接和能力事实，不是另一个业务别名注册表。

### 11.2 继承内置路由

使用不同账户、私有凭据名或补充模型时，定义新的 route ID：

```yaml
providers:
  dashscope-cn-team-a:
    extends: dashscope-cn
    credential:
      env: TEAM_A_DASHSCOPE_API_KEY
    models:
      qwen-new-model:
        input: [text]
```

Agent 直接引用：

```yaml
model:
  provider: dashscope-cn-team-a
  id: qwen-new-model
```

规则：

- extension ID 不得覆盖内置 Provider route ID；
- `extends` 继承 adapter、endpoint 和内置模型 Profile；
- extension 可以替换 secret reference、补充模型，或在显式允许时覆盖非秘密 transport 设置；
- extension 不得把父路由明确不支持的能力伪装为支持；
- 凭据环境变量名由部署者配置，值仍只来自 secret store/process environment。

### 11.3 自定义兼容 Provider

私有或第三方 OpenAI-compatible endpoint 显式声明：

```yaml
providers:
  company-llm:
    type: openai_compatible
    endpoint: https://llm.company.internal/v1
    credential:
      type: bearer
      env: COMPANY_LLM_API_KEY
    models:
      internal-chat-v1:
        input: [text, image]
```

Agent 使用同一 selector：

```yaml
model:
  provider: company-llm
  id: internal-chat-v1
```

自定义 Provider 的模型清单是必需的，并采用 fail-closed 解析。因为平台无法替操作者证明自定义
endpoint 的真实行为，扩展字段必须明确标记为 operator assertion，并进入 Deployment Revision
digest。实现不得把一次成功探测提升为长期能力证明。

### 11.4 治理策略独立

“模型存在”和“组织允许使用”是两件事。若部署需要 allow/deny 治理，应使用独立策略，而不是给
模型条目增加 `enabled`：

```yaml
model_policy:
  allow:
    - provider: dashscope-cn
      id: qwen3.6-flash
    - provider: dashscope-cn
      id: qwen-vl-plus
```

`model_policy` 是可选部署治理边界；没有该字段时，Catalog/extension 中可解析的模型均可被 Agent
引用。策略只收窄可用集合，不能注册模型、修改能力或设置默认模型。

## 12. Platform 配置目标形态

使用纯内置 Provider 时，`platform.yaml` 不包含任何模型块：

```yaml
version: 1

agents:
  directory: ../agents
  enabled:
    - action_demo
    - researcher

actions:
  enabled:
    - current_time
```

运行环境只需为实际引用的 Provider 提供 secret：

```text
DASHSCOPE_API_KEY=...
```

以下旧配置删除：

```yaml
models:
  config: models.yaml
```

`agents.enabled` 和 `actions.enabled` 不受“模型条目不需要 enabled”的结论影响：它们决定部署装载
哪些可执行资源；被启用 Agent 内部的模型引用才决定模型集合。

只有存在自定义 Provider route 或治理要求时，才增加第 11 节的 `providers` 或 `model_policy`。

## 13. 迁移

### 13.1 映射

当前条目：

```yaml
models:
  general_chat:
    type: open_ai_chat
    base_url: https://dashscope.aliyuncs.com/compatible-mode/v1
    api_key_env: OPENAI_API_KEY
    model: qwen3.6-flash
    capabilities: [json_object_output]

  vision_chat:
    type: open_ai_chat
    base_url: https://dashscope.aliyuncs.com/compatible-mode/v1
    api_key_env: OPENAI_API_KEY
    model: qwen-vl-plus
    capabilities: [json_object_output, vision]
```

迁移为内置 Catalog 中的：

| 旧别名 | 新 Provider route | 新模型 ID |
|---|---|---|
| `general_chat` | `dashscope-cn` | `qwen3.6-flash` |
| `vision_chat` | `dashscope-cn` | `qwen-vl-plus` |

Agent 中：

```yaml
model: general_chat
```

改为：

```yaml
model:
  provider: dashscope-cn
  id: qwen3.6-flash
```

环境变量：

```text
OPENAI_API_KEY
```

改为：

```text
DASHSCOPE_API_KEY
```

只有原变量确实专用于 DashScope 时才做值迁移；实现和文档不得把真正的 OpenAI credential
重命名或复用为 DashScope credential。

### 13.2 Clean cutover 清单

1. 增加版本化 Provider Catalog 和 DashScope route/model Profile；
2. 将 Agent raw AST、JSON Schema、compiler、Plan 和 verifier 改为结构化 selector；
3. 迁移全部 checked-in Agent、fixtures、tests 和文档；
4. 从平台配置删除 `models.config`；
5. 删除 `config/models.yaml` 和模型别名 registry loader；
6. 删除普通配置的 `api_key_env`、`base_url`、`capabilities` 和模型级 timeout；
7. 将 DashScope secret reference 切换为 `DASHSCOPE_API_KEY`；
8. 删除 structured response 对用户 `json_object_output` 声明的硬依赖；
9. 让启动过程从已启用 Agent 推导被引用模型和所需 secret；
10. 删除 Action-only Quickstart/测试中的 dummy model；
11. 冻结 Catalog、Provider route、model Profile 和 output policy identity；
12. 同步 `docs/current`，完成 conformance 后将本规范归档。

不实现：

- `model: general_chat` 兼容解析；
- `model: dashscope-cn/qwen3.6-flash` 简写；
- 旧、新 `models.yaml` 双读；
- 缺少 Provider 时默认使用 DashScope；
- 缺少模型时从第一个 Catalog 模型中自动选择。

### 13.3 API 版本策略

仓库仍处于 `0.1.x` / pre-1.0 阶段，本次与仓库 Agent 同步 clean-cut 当前
`insight.agent/v1` authoring surface，不维护 scalar `model` 的跨版本兼容层。若该变更开始实施前
项目已经对外承诺稳定的 Agent v1，必须停止 clean cutover，改为单独设计 `insight.agent/v2` 和明确
迁移窗口；不能在已有稳定承诺下静默改写 v1。

## 14. 安全、确定性与可观测性

### 14.1 安全

- Catalog 和 Provider extension 只保存 secret reference，不保存 secret；
- 错误只报告缺失的变量名，不回显变量值、Authorization header 或 URL query secret；
- Agent 文件不能改变 endpoint 或鉴权；
- 自定义 endpoint 在 production 下必须满足现有 HTTPS/网络出口策略；
- provider/model ID 在日志中可以出现，但 Prompt、响应正文和凭据继续遵循现有非干扰规则。

### 14.2 确定性

- 启动不通过网络发现模型列表；
- 启动不通过真实模型调用探测能力；
- Catalog 和 extension 解析结果必须可 canonicalize 和 digest；
- 同一个 Agent source、Catalog digest、Provider extension digest 和 compiler 版本产生相同模型
  Deployment identity；
- 滚动模型别名如上游 `latest` 可以被引用，但生产文档应优先推荐上游提供的固定版本 ID；平台不得
  假装滚动 ID 自身可复现。

### 14.3 可观测性

日志和 metrics 至少暴露非秘密字段：

- Provider route ID；
- Provider model ID；
- adapter ID；
- Deployment Revision；
- Catalog digest；
- 是否使用 native structured-output mode；
- structured-output 失败阶段：provider、parse 或 schema validation。

不得将 `provider/model` 拼接字符串作为内部唯一 identity；结构化字段必须保留，拼接形式只允许作为
UI/日志展示。

## 15. 实施边界

预计需要修改但本规范不直接实现的区域：

| 区域 | 目标变化 |
|---|---|
| `crates/dsl` | `llm.model` 从 scalar alias 改为 `{provider, id}` |
| `crates/resources` | alias registry 改为 Catalog/extension 解析和冻结 identity |
| `src/config.rs` | 删除必需 `models.config`，增加可选 `providers` / `model_policy` |
| `src/resources/config.rs` | 删除 `models.yaml` loader，装载 Catalog 和 extensions |
| runtime model adapter | Provider route 解析、调用参数校验、结构化输出策略 |
| `config/` | 删除 `models.yaml`，简化 `platform.yaml` |
| `agents/` | 将所有模型别名迁移为结构化 selector |
| Quickstart / Helm / env docs | 改用 Provider 官方 secret reference，Action-only 不要求模型 |
| tests / fixtures | 正向、负向、冻结、secret 和输出 conformance |
| `docs/current` | 只在实现和验收完成后更新当前合同 |

实现不得顺手引入默认 Agent、子 Agent 调用、自动模型路由或故障转移。

## 16. 验收标准

### 16.1 配置与 DSL

- 纯内置 Provider 部署在没有 `models.yaml`、没有 `models.config` 时可启动；
- Action-only 部署在没有任何模型 credential 时可启动并运行；
- `model: general_chat` 被 schema/parser 拒绝；
- `model: dashscope-cn/qwen3.6-flash` 被 schema/parser 拒绝；
- `{provider: dashscope-cn, id: qwen3.6-flash}` 编译成功；
- 模型 ID 包含 `/` 时仍能无歧义编译和调用；
- 未知 Provider、未知模型、未知 selector 字段均 fail closed；
- 同一 Agent 可在不同步骤选择不同 Provider/模型；
- `type: llm` 保持有效，`type: agent` 不被当作 LLM 同义词。

### 16.2 Provider 与凭据

- DashScope 内置路由默认读取 `DASHSCOPE_API_KEY`；
- 不再把 `OPENAI_API_KEY` 当作 DashScope 默认凭据；
- 只为已启用 Agent 实际引用的 Provider 检查 secret；
- `dashscope-cn` 和 `dashscope-intl` 解析为不同 endpoint identity；
- 请求失败时不自动跨 route；
- 单端点 Provider 不要求 endpoint selector；
- 自定义 Provider 和继承路由进入独立 digest，不能覆盖内置 route。

### 16.3 模型与调用分层

- Catalog 中不存在 `enabled` 和调用级 thinking 默认值；
- `enable_thinking`、temperature、stream 和 tools 从 Agent/Plan 进入请求；
- 模型 Profile 只包含被 compiler/runtime 消费的客观字段；
- 明确不支持图片的模型在包含图片的 Agent 上编译失败；
- Provider extension 的 operator assertions 可追溯并进入 revision identity。

### 16.4 结构化输出

- 结构化 `response` 在没有公共 `json_object_output` capability 时可以编译；
- native `json_schema`、native `json_object` 和 prompt-only 三条路径共享本地解析和 Schema 校验；
- Provider 返回 HTTP success 但 JSON 无效或 Schema 不匹配时，Run 不得提交无效业务结果；
- native mode 不可用时使用平台 Prompt 策略，而不是要求用户修改模型配置；
- 日志能区分 provider failure、JSON parse failure 和 schema validation failure。

### 16.5 回归与文档

- 所有 checked-in Agent 和 LLM fixtures 已迁移；
- Quickstart 不包含 dummy model；
- compiler、Plan verifier、runtime、durable replay 和 Deployment Revision 测试通过；
- secret non-interference 测试覆盖 Provider route 与 extension；
- `docs/current`、根 README、配置样例和部署文档与实现一致；
- 完成后将本规范状态改为 Implemented 并移入 `docs/archive/specs`。

## 17. 最终不变量

1. Agent 直接选择真实 Provider route 和真实模型 ID，不选择平台模型别名；
2. Provider 身份不同于 API 兼容协议；
3. endpoint、adapter 和默认 credential reference 归 Provider route；
4. 模型 Profile 只表达客观事实，不表达本次调用选择；
5. thinking、stream、tools、temperature 和 token budget 归 `llm` 调用；
6. `response` 归 Agent 输出合同，native JSON mode 只是内部优化；
7. 没有 LLM 引用就没有模型配置和模型凭据要求；
8. 多区域通过不同 Provider route 显式选择，不隐式切换；
9. 用户可通过显式 Provider extension 注册自定义模型，但不能恢复 alias registry；
10. 治理 allow/deny 与模型注册分离，不使用 `enabled` 混合两种职责；
11. Deployment Revision 冻结非秘密的完整解析身份；
12. `type: llm` 表示模型调用，`kind: agent` 表示整份 Agent，二者不可混用。

## 18. 实施结果

本规范已完成 clean cutover：

- 内置、版本化 Provider Catalog 与可选 `providers` / `model_policy` 已落地；
- Agent、fixtures、Helm 与配置样例已迁移到严格的 `{provider, id}` selector；
- 旧 `models.yaml`、`models.config`、模型别名和用户 `json_object_output` 配置已删除；
- prompt-only、native `json_object`、native `json_schema` 共用本地 JSON 解析和 Schema 校验；
- Provider route、模型 ID、adapter、endpoint identity、Catalog/extension digest 和非秘密策略已进入
  Deployment Revision，secret 值保持非干扰；
- Action-only 启动、Provider 扩展、区域身份、图片能力、结构化输出、durable replay、Helm 和
  checked-in Agent 均由专项与 workspace 回归覆盖。

完成时通过 `cargo check`、严格 Clippy、workspace 全量测试、doc tests、公共 API baseline、
cutover residual、crate boundary、Helm lint 与格式门禁。
