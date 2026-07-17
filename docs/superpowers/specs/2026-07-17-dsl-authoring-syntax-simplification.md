# DSL 作者语法精简规范

| 属性 | 值 |
|---|---|
| 状态 | Implemented |
| 变更类型 | Breaking |
| 日期 | 2026-07-17 |
| 最后更新 | 2026-07-18 |

## 1. 范围

本文是 [DSL 作者层 LLM、Action 与消息模型重设计规范](./2026-07-17-dsl-authoring-surface-redesign.md) 的精简增量规范，只调整以下作者可见语法：

- 消息及多模态 content part；
- 运行时值引用与文本插值；
- 自然 YAML 对象、数组和常量；
- 平台内建 `Message` 与 Agent 自定义类型；
- LLM 响应类型。

若本文与上一版规范在上述语法上冲突，以本文为准。控制流、作用域边界、Action 合同、运行时调度、预算、安全和错误码不在本文重复定义。

## 2. 核心规则

1. 作者消息使用 `{role, content}`，`content` 是有序 content part 列表。
2. content part 使用严格的单键对象，只保留 `{text: ...}` 和 `{image_url: ...}`。
3. 完整运行时值使用 `$name` 或 `$name.field` 引用。
4. 作者文本内部使用 `{{ name }}` 插值。
5. YAML mapping、sequence 和 scalar 直接表示对象、数组和常量。
6. 平台统一提供 `Message`，Agent 不得重复定义消息 Schema。
7. Agent 自定义类型使用 PascalCase；数组使用 `Type[]`。
8. LLM 的 `response` 直接声明结果类型。

## 3. 类型声明

普通 Agent 的顶层类型面统一为 `types`、`inputs` 和 `output`：

```yaml
types:
  HealthResult:
    fields:
      answer: string

inputs:
  question: string

output: HealthResult
```

作者不再在类型与响应合同位置书写 `schema_dialect`、`input.schema`、`output.data_schema` 或 JSON Schema 对象；普通业务 payload 中同名 mapping key 不属于类型语法。

### 3.1 输入

简单输入直接写类型：

```yaml
inputs:
  report_text: string
  question: string
```

需要默认值或可选性时使用完整形式：

```yaml
inputs:
  messages:
    type: Message[]
    default: []

  image_url:
    type: string
    optional: true
```

`image_url` 的运行时表示就是 `string`，不定义 `ImageUrl` 名义类型。URL、非空值和模型 Vision 能力在 `image_url` content part 的使用位置校验。

缺省值规则：

- `default` 与 `optional: true` 互斥；
- 有 `default` 时，缺省字段在进入工作流前被物化为该默认值，字段的静态类型仍为 `T`；
- `optional: true` 时，缺省字段在进入工作流前被物化为 null，字段的静态类型为 `T | null`；
- 规范化发生在验证、持久化和执行之前，并且必须幂等，因此 `$image_url` 等引用始终读取一个已存在的值。
- `default` 是纯静态数据，不执行运行时引用或文本插值；完整匹配 `$name`/`$name.field` 的字符串必须拒绝，旧 value wrapper 在任意嵌套层级同样必须拒绝。

本规范只在顶层 `inputs` 开放 `default` 与 `optional`；自定义对象字段仍为必填，避免把未定义值带入字段投影。

### 3.2 自定义对象与数组

对象使用 `fields` 定义，默认封闭：

```yaml
types:
  Indicator:
    fields:
      name: string
      value: string
      abnormal: boolean

  SynthesisResult:
    fields:
      answer: string
      indicators: Indicator[]
      warnings: string[]
```

数组不使用 `type: array` 和 `items`。数组元素是对象时，先定义对象，再使用 `ObjectType[]`：

```yaml
inputs:
  indicators:
    type: Indicator[]
    default: []
```

只有数组本身具有独立业务语义或约束时才声明数组别名：

```yaml
types:
  IndicatorSet:
    type: Indicator[]
    min_items: 1
    max_items: 100
```

本文不新增 enum、union、map、递归类型或 nullable 简写；这些能力沿用既有合同或另行设计。

### 3.3 命名

| 类别 | 规则 | 示例 |
|---|---|---|
| 基础类型 | 小写 | `string`、`number`、`integer`、`boolean` |
| 平台及自定义类型 | PascalCase | `Message`、`Indicator`、`SynthesisResult` |
| 数组 | `Type[]` | `Message[]`、`Indicator[]` |
| 输入、字段、步骤 ID | snake_case | `report_text`、`health_advice` |
| Step 判别值与 content key | snake_case | `llm`、`text`、`image_url` |

`type` 用于声明值类型，或判别包含多个字段的复杂 Step。只有一个载荷字段的 content part 直接用字段名判别，不重复书写 `type`：

```yaml
type: Message[]       # 输入值类型
type: llm             # Step 变体

- text: hello         # ContentPart 变体
- image_url: $image_url
```

Parallel branch 与 Switch 的业务输出也使用同一个类型表达式：

```yaml
type: parallel
branches:
  technical:
    output: Perspective

type: switch
output: SynthesisInput
```

根 `output`、branch `output`、Switch `output` 与 LLM `response` 都接收 `Type` 或 `Type[]`；作者层彻底删除 `data_schema` 和 `output_schema`。

## 4. 运行时引用与文本插值

### 4.1 `$` 传递完整值

`$name` 引用当前词法作用域中的输入或前序步骤；`$name.field` 读取字段：

```yaml
query: $question
payload: $search_result
answer: $synthesize.answer
image_url: $image_url
```

步骤名直接代表该步骤的业务输出，不再书写 `steps.<id>.output`。`$` 引用必须占据完整 YAML scalar，且保留字符串、数字、布尔、对象、数组或 null 的原始类型，不做隐式 JSON 字符串化。

完整匹配 `$name` 或 `$name.field` 的 scalar 是保留语法，始终按运行时引用解释，不作为同名字面量。

未定义、歧义、跨作用域或类型不匹配的引用必须在编译期失败。同一可见作用域不得用隐式优先级解决重名。

### 4.2 `{{ }}` 只构造文本

作者书写的文本或 Markdown 可以插值：

```yaml
answer: |-
  {{ abnormal_indicators }}

  {{ comprehensive_interpretation }}

  {{ health_advice }}
```

规则：

- 整个字段就是一个运行时值时使用 `$value`；
- 值只是文本的一部分时使用 `{{ value }}`；
- `{{ }}` 的最终结果始终是字符串；
- 插值值必须静态为 string；number、boolean、object、array 和 null 不得被隐式字符串化；
- 编译器从模板 AST 自动解析可见变量，只注入实际引用的值；
- 运行时字符串不得被再次当作模板执行。

因此不再由作者书写 `template` 和 `bindings`。

## 5. 通用 Message 与 content part

### 5.1 平台内建类型

`Message` 是平台统一维护的封闭、role-correlated 类型。所有需要历史对话的 Agent 只声明：

```yaml
inputs:
  messages:
    type: Message[]
    default: []
```

Agent 不得复制 Message、role 或 content part 的 `$defs`。平台统一负责结构校验、动态消息角色策略、消息预算、图片校验和 Provider 映射。调用方提供的历史消息不得注入高权限 system/developer/tool 指令。

### 5.2 消息写法

```yaml
messages:
  - role: system
    content:
      - text: system

  - $messages

  - role: user
    content:
      - text: health_advice
      - image_url: $image_url
```

每个 content part 必须是恰好包含一个字段的封闭对象；不允许额外字段，也不允许 `type` 判别字段。

在 `text` 中：

- 精确匹配已声明 Prompt ID 的字符串引用该 Markdown prompt；
- `$value` 直接使用运行时字符串；
- 其他作者字符串是内联文本，可包含 `{{ }}`。

已声明 Prompt ID 在该位置是保留名称，YAML 引号不改变其含义。

在 `image_url` 中：

- 可以使用固定 URL 字符串；
- 可以使用静态类型为 `string` 或可空 `string` 的 `$value`；
- 可空引用为 null 时省略整个 part；空字符串或非法 URL 必须失败。

作者 YAML 与运行时 `Message[]` 使用相同的单键 part 结构。区别是：运行时传入的 `text` 永远作为普通文本，不解析 Prompt ID，不执行 `{{ }}`，也不会触发二次模板渲染。

`llm.messages` 中的 `$messages` 若静态类型为 `Message[]`，在当前位置自动展开零条或多条消息。该规则只适用于消息列表，不引入通用 `spread`、`concat` 或任意数组 flatten。

## 6. LLM 响应

`response` 直接声明步骤业务输出类型：

```yaml
response: string
response: SynthesisResult
response: Indicator[]
```

每个 LLM Step 必须显式声明 `response`，不提供隐式默认值。

语义：

- `string` 以及最终解析为 `string` 的命名别名使用文本响应；命名别名上的长度、模式和枚举约束不得丢失；
- 对象、数组或其他结构类型使用严格 JSON 响应；
- 编译器根据声明类型生成 JSON Schema，并将其作为结构化输出合同传给模型 Provider；
- `json_schema_output` 直接向 Provider 发送严格 JSON Schema，适用于任意结构化根类型；
- `json_object_output` 只适用于顶层对象，适配器必须注入平台生成的 JSON/Schema 指令，Provider 返回后仍以本地 Schema 校验为最终合同；
- 两种能力同时存在时固定选择 `json_schema_output`，编译计划记录具体选择，运行时不得重新选型；缺少可用能力时在编译期失败；
- Provider 返回值仍须在本地按同一份 Schema 校验，不能只信任上游；
- 步骤输出就是声明的业务类型，不再包装为 `output.data`。

示例：

```yaml
- id: synthesize
  type: llm
  response: SynthesisResult

result:
  return: $synthesize
```

## 7. 自然 YAML 值

mapping、sequence 和 scalar 直接构造 JSON 值：

```yaml
result:
  return:
    mode: initial
    answer: |-
      {{ abnormal_indicators }}

      {{ comprehensive_interpretation }}

      {{ health_advice }}
```

这里：

- `mode` 是普通业务字段；
- `initial` 是字符串常量；
- `return` 的 mapping 自然构造对象；
- `answer` 是作者文本模板。

不再需要 `object`、`array`、`literal`、`template`、`bindings` 或 `{from: ...}` 包装。

为避免旧表达式被静默解释成普通业务对象，精确匹配旧 `{from: ...}`、`{literal: ...}`、`{object: ...}`、`{array: ...}` 或 `{template: ...}` 形状的 mapping 必须直接拒绝，而不是作为兼容别名或自然对象接受。

## 8. JSON Schema 边界

普通 Agent DSL 禁止作者在类型与响应合同位置书写 `$schema`、`$defs` 和 `$ref`。平台内建类型以及 `types` 中的命名类型由编译器转换成规范化 JSON Schema；这些关键字可存在于编译后 IR、Provider 请求、高级资源合同，或作为 Action 等自然业务 payload 的普通字段名。

例如：

```yaml
response:
  format: json
  schema: {$ref: "#/$defs/SynthesisResult"}
```

改为：

```yaml
response: SynthesisResult
```

## 9. 删除的作者语法

| 删除 | 替代 |
|---|---|
| `{from: inputs.question}` | `$question` |
| `{from: steps.answer.output.data}` | `$answer` 或 `$answer.field` |
| `{literal: initial}` | `initial` |
| `{object: {...}}` | 自然 YAML mapping |
| `{array: [...]}` | 自然 YAML sequence |
| `template.text + bindings` | 作者文本中的 `{{ name }}` |
| bare content prompt atom | `{text: prompt_id}` |
| content part 的 `type` 判别字段 | `{text: ...}` 或 `{image_url: ...}` |
| `{image: {from: inputs.image_url}}` | `{image_url: $image_url}` |
| 每个 Agent 的 Message `$defs` | 平台类型 `Message[]` |
| `format: json + schema + $ref` | `response: Type` |
| `input.schema`、`output.data_schema`、`output_schema` | `inputs`、`output: Type` |
| `spread`、`concat` | `Message[]` 在消息列表中自动展开 |

删除项不得作为兼容别名继续存在；否则同一语义会重新出现多套写法。

## 10. 验收标准

1. 仓库 Agent 不再包含 `$schema`、`$defs`、`$ref`、`literal`、`object`、`array`、`template.bindings` 或 `{from: ...}` 作者表达式。
2. 所有对话 Agent 使用同一个平台 `Message[]` 类型。
3. content part 只使用严格单键 `{text: ...}` 与 `{image_url: ...}`，不出现 `type` 判别字段。
4. `$` 引用保持原始类型；`{{ }}` 仅在文本位置工作。
5. `Message[]` 只在 `llm.messages` 中自动原位展开一层。
6. LLM 的文本、对象和数组响应都通过 `response: Type` 声明并得到静态步骤输出类型。
7. 未定义引用、跨作用域引用、错误类型、动态高权限消息和对象隐式文本化均在 Provider 调用前失败。
