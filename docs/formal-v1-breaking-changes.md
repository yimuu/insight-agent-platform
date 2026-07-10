# 正式 V1 破坏性变更

仓库原有的 `/v1`、Agent YAML、事件和历史结构都是未发布的原型，不是稳定合同。本次重写不提供兼容层，而是把第一个稳定合同直接命名为 HTTP/DSL/Event `V1`。这样可以避免为了兼容一个从未正式发布的原型而永久保留 `v2` 和双运行时分支。

迁移原则：重新编译 Agent、切换客户端合同、使用全新的历史数据库。旧客户端和旧 YAML 会明确失败，不会被静默解释成新语义。

## 变更总表

| 原型 | 正式 V1 | 为什么改 | 最小迁移示例 |
|---|---|---|---|
| `steps: [...]` 隐式顺序，混合 `goto/end` | `entry` + `nodes` DAG | 顺序、跳转和依赖都是图语义；启动期可统一检查缺边、环、不可达节点和前驱引用 | `steps: [{id: answer,...}]` → `entry: answer; nodes: {answer: {...}}` |
| `prompt` / `text` step | `core.template` | 模板渲染与模型调用、内容发布是不同职责 | `type: text; prompt: "{{ input.x }}"` → `type: core.template; config.value: "{{ input.x }}"` |
| 通用 `llm` step 和 Agent 顶层 model | 命名模型资源 + `core.chat` | chat、embedding、speech、rerank 需要不同合同；不应塞进一个可选字段集合 | `type: llm` → `type: core.chat; config.model: general_chat` |
| `tool` 与 `code` 两套调用边界 | `core.action` + `ActionRegistry` | 两者本质上都是严格 JSON 能力调用；统一 Schema、取消和错误语义 | `type: code; handler: example.text_metrics` → `type: core.action; config.action: example.text_metrics` |
| 运行时编译 condition/CEL | `core.condition` 在启动时预编译 | 平台自有工作流的表达式错误不应延迟到用户请求 | `cases[].goto` → `config.cases[].next`，上下文从 `steps.x` 改为 `nodes.x` |
| 最后一个 step 隐式成为结果 | 必需的终端 `core.output` | 最终显示内容、格式和结构化数据必须确定且可验证 | 添加 `result: {type: core.output, config: {content: {template: ...}, format: markdown}}` |
| 节点 `stream: true/false` | 公共 envelope 的 `emit: content/none` | 供应商是否使用流传输与内容是否公开给客户端是两件事 | `stream: true` → `emit: content` |
| Agent `public/default_public/exposure` | 平台 `auth.mode` + `agents.enabled` | 工作流元数据不能决定部署安全；鉴权和启用策略必须分离 | 删除 `public`，在平台配置中显式写 `auth.mode` 和 `agents.enabled` |
| SSE 连接拥有所有 Run | attached 与 detached 两种创建接口 | 交互式断开取消和后台执行是两种明确意图 | `/runs/stream` 创建 attached；`/runs` 创建 detached 并返回 202 |
| 传输消费时才写历史 | 独立 `EventHub` + bounded journal | 持久化不能依赖某个 SSE 消费者是否在线 | 客户端用 `/events?after_seq=N` 从 journal 补发 |
| 数字 API/event code | 稳定字符串码 | 字符串自描述，避免维护未文档化的数字区间 | `"code":0` → `"code":"OK"`；错误如 `RUN_NOT_FOUND` |
| 原型 SQLite/PostgreSQL 表 | 全新 formal V1 `runs/run_events/node_outputs` | 新生命周期、attachment、序号和终态事务约束无法安全套用旧表 | 删除本地旧 SQLite 文件或重建开发 PostgreSQL volume 后启动 |

## DSL 迁移

原型：

```yaml
id: demo
model: {provider: provider_a, type: llm, model: model_a}
input:
  schema: {type: object}
steps:
  - id: answer
    type: llm
    prompt: "{{ input.question }}"
    stream: true
```

正式 V1：

```yaml
version: 1
id: demo
name: Demo
input:
  schema: {type: object}
entry: answer
nodes:
  answer:
    type: core.chat
    next: result
    emit: content
    config:
      model: general_chat
      messages:
        - role: user
          content: "{{ input.question }}"
  result:
    type: core.output
    config:
      content: {template: "{{ nodes.answer.output.text }}"}
      format: markdown
```

Prompt 文件继续相对 Agent 目录声明，但消息通过 `{template_ref: name}` 引用。所有跨节点模板和 CEL 引用使用 `nodes.<node_id>.output`；编译器只允许引用所有到达路径上都已完成的前驱。

## HTTP 与事件迁移

正式端点只有：

```text
GET    /health
GET    /v1/agents
GET    /v1/agents/{agent_id}
POST   /v1/agents/{agent_id}/runs/stream
POST   /v1/agents/{agent_id}/runs
GET    /v1/runs/{run_id}
GET    /v1/runs/{run_id}/events?after_seq=<u64>
DELETE /v1/runs/{run_id}
```

原型的 Run 列表/过滤端点和 `X-Caller-Service/X-Tenant-Id/X-User-Id` 元数据不属于正式 V1。原因是执行合同不应提前绑定多租户管理面；后续查询/管理 API 可以在独立授权与分页合同下增加。`X-Request-Id` 保留用于关联请求。

请求体仍是 Agent 输入对象本身，不使用 `{input: ...}` 包装。attached POST 在构造 SSE 前完成 JSON 与 Schema 校验，并返回 `X-Run-Id`；detached POST 返回 202。`DELETE` 是唯一显式取消接口且幂等。

事件增加 `schema_version: 1`、`agent_version` 和可选 `node_id`；`step.*` 攺为 `node.*`。客户端应持久化最后成功处理的 `seq`，断线后请求 `after_seq`。keepalive 注释不是协议事件，不消耗序号。

## 配置与安全迁移

- 平台 YAML 和模型 YAML 都必须写 `version: 1`，每层拒绝未知字段。
- `PLATFORM_CONFIG` 一旦设置，目标文件必须存在；相对路径以平台文件目录为基准。
- `auth.mode` 必须明确为 `disabled` 或 `bearer_env`。
- `agents.enabled` 默认空，不再从 Agent YAML 推断公开范围。
- YAML 只保存环境变量名；Bearer token、模型 API key 和 PostgreSQL URL 从环境变量读取，`Debug` 输出脱敏。
- 模型别名取代 provider/type/model 三元组，Agent 不再关心供应商组织方式。

医学示例也不是兼容合同。正式示例使用单个 `image_url` 展示一个有 `vision` 能力的多模态消息；原型的 `images` 数组调用方需要选择一个图片 URL，或通过自定义节点/后续多图内容类型扩展。这个限制只属于当前示例输入，不是核心运行时的医学规则。

## 历史重置

不执行原型表的就地升级，也不读取原始输入。迁移开发环境：

```bash
rm -f data/run_history.sqlite3
docker compose -f docker-compose.postgres.yml down -v
docker compose -f docker-compose.postgres.yml up -d
```

正式 migration 只位于 `migrations/formal_v1/{sqlite,postgres}`。生产环境如需保留原型审计数据，应先导出到独立只读存储；不要把旧表复制进正式 V1 数据库。
