# DSL v3 指南

状态：Current

适用版本：`insight.agent/v3`

DSL v3 使用自然 YAML 表达类型、数据依赖和结构化控制流。平台在启动或发布 revision 时完成解析、
类型检查、链接与 lowering；编译失败的 Agent 不会进入运行时。

## 最小结构

```yaml
api_version: insight.agent/v3
kind: agent

metadata:
  id: action_demo
  name: Action Demo

types:
  TextMetrics:
    fields:
      characters: integer
      words: integer
      lines: integer

inputs:
  text: string

output: TextMetrics

workflow:
  steps:
    - type: action
      id: analyze_text
      call: example.text_metrics
      inputs:
        text: $text
      response: TextMetrics

    - return: $analyze_text
```

`metadata` 可以省略；`types`、`prompts` 和 `errors` 按 Agent 需要声明。`workflow.steps` 是作者层的
顺序入口，最终必须以类型正确的 `return` 或 `raise` 闭合所有可达路径。

## 类型与输入

内建基础类型包括 `string`、`integer`、`number`、`boolean`、`any` 和 `Message`。数组写成
`Type[]`，对象类型使用 `fields`：

```yaml
types:
  Approval:
    fields:
      decision:
        type: string
        enum: [approved, rejected]
      comment:
        type: string
        min_length: 1

inputs:
  messages:
    type: Message[]
    default: []
  image_url:
    type: string
    optional: true
```

字段可以使用 `enum`、`min_length`、`max_length`、`pattern`、`min_items` 等约束。作者不需要手写
JSON Schema；compiler 会生成并执行输入、节点响应和最终输出校验。

## 值与表达式

- `$name` 引用输入或已闭合 step 的值；
- `$object.field` 读取结构化字段；
- 普通 YAML scalar、mapping 和 sequence 保持字面值；
- 文本中的 `{{ name }}` 执行模板插值；
- 条件使用受限表达式，例如 `size(messages) > 0`。

裸字符串不会被隐式当成变量。跨分支使用值时必须通过 `yield` 闭合分支结果。

## 叶节点

`action` 调用注册的本地 Action：

```yaml
- type: action
  id: current_time
  call: current_time
  inputs:
    timezone: Asia/Shanghai
  response: CurrentTime
```

`llm` 使用注册的模型和标准 `Message` 列表：

```yaml
- type: llm
  id: answer
  model: general_chat
  stream: true
  publish: true
  messages:
    - role: system
      content:
        - text: system
    - $messages
    - role: user
      content:
        - text: "Question: {{ question }}"
        - image_url: $image_url
  parameters: {temperature: 0.2}
  response: Answer
```

`stream` 控制 Provider 请求模式，`publish` 控制 provisional 内容是否进入 Attached 响应流；二者
互相独立，最终结果始终按 `response` 类型验证。Prompt 可以用 `inline` 或相对 Agent 文件的 `file`
声明，message 中的文本槽引用 Prompt 名称。

## 控制流

- `if / elif / else` 选择执行路径；
- `match` 只选择值；
- `parallel` 配合 `all_success` 或 `all_settled` 表达 fork/join；
- `map`、`loop`、`agent_loop` 表达动态重复；
- `call` 调用固定 revision/interface 的子流程；
- `try / catch / finally` 表达错误边界；
- `human_task`、signal wait 和 timer wait 表达可恢复的长流程；
- `yield` 闭合局部结构结果，`return` 与 `raise` 终结工作流。

`human_task` 是独立的持久化工作项，不是 signal 的别名。候选人、候选组、响应类型和 claim lease
都必须显式声明。

## 示例与规范

- [`agents/`](../../agents)：随仓库运行的完整 Agent；
- [`tests/fixtures/v3/`](../../tests/fixtures/v3)：正向和负向 compiler fixtures；
- [DSL v3 规范](specifications/2026-07-18-dsl-v3-durable-graph-execution-design.md)：完整作者语义与 lowering 合同。
