# DSL v1 指南

状态：Current

适用版本：`insight.agent/v1`

DSL v1 使用自然 YAML 表达类型、数据依赖和结构化控制流。平台在启动或发布 revision 时完成解析、
类型检查、链接与 lowering；编译失败的 Agent 不会进入运行时。

## 最小结构

```yaml
api_version: insight.agent/v1
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

Deployment persistence policy 是可选的严格顶层字段：

```yaml
execution:
  persistence_mode: terminal_only
```

可选值只有 `full` 和 `terminal_only`；未知值或 `execution` 下的未知字段会在 parse 阶段失败。
未声明时当前平台默认使用 `full`。该字段不改变 Canonical Plan 控制流，而是在 publication 时进入
不可变 Deployment Revision identity；Run 请求不能覆盖它。

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

LLM 节点可以通过部署时冻结的白名单调用注册 Action：

```yaml
- type: llm
  id: answer
  model: general_chat
  messages:
    - role: user
      content:
        - text: request
  tools: [current_time, text_metrics, integer_calculator, text_replace]
  tool_choice: auto
  tool_limits:
    max_rounds: 8
    max_calls: 16
  response: string
```

`tool_choice` 支持 `auto`、`required` 或白名单中的一个工具名。模型只能产生调用意图；运行时仍会
校验工具名、参数 JSON Schema、调用轮数和总次数，再执行对应 Action 并把类型化结果续接给模型。
模型输出的 `response.function_call_arguments.*` 只表示调用意图；只有
`workflow.tool.started/progress/completed/failed` 表示平台真实执行 Action。前端必须按
`call_id` 合并两者，同名工具的并行调用不能按名称合并。

工具活动采用 LLM `publish` 与 Action 冻结 `public_policy` 双重授权。第三方 Action 默认完全私有；
Action 注册方可以独立授权调用元数据、参数、进度和最终结果：

```rust
ToolPublicPolicy {
    call: true,
    arguments: ToolPublicArguments::Private,
    progress_schema: Some(json!({
        "type": "object",
        "required": ["completed", "total"],
        "properties": {
            "completed": {"type": "integer", "minimum": 0, "maximum": 1000},
            "total": {"type": "integer", "minimum": 1, "maximum": 1000}
        },
        "additionalProperties": false
    })),
    result_schema: Some(/* 完整、闭合的公开结果 Schema */),
}
```

这些授权互不推导：

- `arguments: all` 才允许标准 function-call argument 帧，但不会自动公开进度或结果；
- `progress_schema` 描述一次 Action 主动发布的安全进度值；值必须通过冻结 Schema，且只以
  `output_text/output_json` 发送；
- `result_schema` 描述完整公开结果，不是对私有结果任意裁剪；
- `call: true, result: none` 的成功调用仍发送 `completed`，但 `content: []`，含义是“结果未
  授权公开”；
- LLM `publish: false` 时不发送 provisional 工具事件，但模型 continuation 和最终工作流结果不受
  影响。

Action 可以通过当前 `ActionContext::publish_progress` 发布进度。它是有界、非阻塞、best-effort 的
live-only 观测：可能因频率限制、队列满、订阅者断开或调用已经结束而返回 `Dropped`，不会进入
durable history、Conversation message、GET Run 或 terminal snapshot，也不能成为 Action 成败或
retry 的依据。非法进度返回不包含 payload 的错误，原始值不会进入公共事件或默认日志。

内置 [`tool_assistant`](../../agents/tool_assistant/agent.yaml) 的四个工具采用不同结果策略：
`current_time`、`text_metrics`、`integer_calculator` 公开闭合的安全结果，`text_replace` 只公开
成功状态并保持调用者文本私有。另一个
[`progress_tool_assistant`](../../agents/progress_tool_assistant/agent.yaml) 使用
`progress_counter` 演示两个 Schema-valid progress 事件和公开最终结果。

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

上述 human task、外部 signal wait、长 timer、durable child subflow 和依赖 durable effect fence 的
provider 与 terminal-only v1 不兼容，publication 会静态拒绝。`allow_volatile_waits` 默认关闭；当前
版本显式打开时也只允许接受进程退出即丢失语义的有界短 timer，external signal 和 human task
仍会被拒绝，不能把任何 volatile wait 描述为可恢复等待。

## 示例与相关文档

- [`agents/`](../../agents)：随仓库运行的完整 Agent；
- [`tests/fixtures/dsl/`](../../tests/fixtures/dsl)：正向和负向 compiler fixtures；
- [架构概览](architecture.md)：Canonical Plan、持久化执行与恢复边界。
