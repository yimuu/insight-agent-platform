# Spec 04：渐进披露与内部概念隐藏

| 属性 | 值 |
|---|---|
| 状态 | Implemented / CR-215；L1～L3 passed，L4～L6 Not run |
| 日期 | 2026-09-01 |
| 适用面 | CLI、Console、Quickstart、产品DTO与错误信息 |

## 1. 目标

底层exact authority继续存在且可审计，但默认用户只理解：

```text
Agent -> Publish -> Run -> Task/Result
```

隐藏只改变展示和客户端代管，不改变安全、幂等、CAS、恢复或Run冻结语义。

## 2. 概念分层

| 层级 | 默认概念 | 进入方式 |
|---|---|---|
| Product | Agent、发布状态、Run、Task、结果 | 默认CLI/Console/Quickstart |
| Operational | environment、feature readiness、retryability、request/trace ID | `--verbose`或Console诊断摘要 |
| Authority | ResourceVersion、Deployment、Job/Operation、Receipt、ETag、cursor、digest | `--debug-authority`或高级诊断 |
| Internal | lease token、fence、worker generation、DB row、RPC credential | 永不进入public客户端 |

`Operation`可以在等待时间较长或需要人工恢复时以“后台操作”呈现，但默认文案不能要求用户理解它等同shared Job。

## 3. 默认产品DTO

CLI和Console共享下列逻辑摘要；具体序列化类型必须closed并带`schema_version`：

### AgentSummary

- `name`
- `agent_id`
- `state`: `draft/validating/publishing/ready/blocked`
- `environment`
- `updated_at`
- `required_features`：只来自current Agent Resource/Revision冻结的closed set

### RunSummary

- `run_id`
- `agent_name`和`agent_id`
- `state`: public Run state的安全投影
- `started_at`、`terminal_at`
- `waiting_task_count`
- `result_available`

### UserProblem

- `code`
- `message`
- `retryable`
- `suggested_action`
- `request_id`或`trace_id`（可选）

summary DTO不得包含Version/Deployment/Job/Receipt/ETag/cursor、worker、lease、fence、数据库或endpoint。list page的
`next_cursor`是有界分页protocol metadata，由客户端代管且默认text/DOM不渲染，不属于summary字段。`agent_id/run_id`
保留是因为它们是用户可引用的产品身份，不属于需要隐藏的物理实现。

input classification与default deadline不进入列表summary；创建Run前客户端通过exact Agent Resource读取这两个immutable authoring事实。
本地lock可缓存但不能覆盖authority，缺失或漂移时必须fail closed并重新读取。

## 4. 客户端代管

- Receipt：客户端稳定生成、0600 journal保存、冲突时转换为产品错误；
- ETag：每次mutation前读取或复用刚返回值，412/409时引导重新加载；
- Operation：客户端轮询/恢复并显示四阶段产品进度；
- cursor：SSE客户端内部保存和重连，只在诊断中显示；
- Version/Deployment：publish编译器创建并验证，默认仅输出`release ready`；
- Artifact：Agent authoring和Plan artifact自动上传/验证，普通文件功能仍显式展示Artifact。

任何被代管事实丢失或不确定时必须fail closed并提供恢复动作，不得用新随机值假装尚未发生过。

## 5. 错误翻译

翻译层是一对一安全映射，不吞掉服务端Problem。示例：

| Authority Problem | 默认消息 | 操作 |
|---|---|---|
| `etag_mismatch` | Agent已被其他会话修改 | 重新加载并比较 |
| `idempotency_conflict` | 同一发布请求的内容发生变化 | 保留journal并检查manifest |
| `deployment_not_ready` | Agent依赖尚未就绪 | 查看缺失feature/binding |
| `operation_pending` | 发布仍在进行 | 继续等待或稍后恢复 |
| `capacity_exhausted` | 当前执行容量已满 | 按retryability等待 |

未知Problem code不能显示为“未知成功”或自动重试；默认给出安全通用错误并在诊断中保留原code。

## 6. 文档分层

`docs/current` clean-cut后按以下顺序组织：

1. 5分钟Quickstart：只出现`init/dev/agent publish/agent run`；
2. Agent manifest与Console；
3. Tasks、Artifacts和运行控制；
4. Platform automation：Resource lifecycle、Receipt/CAS、Operation；
5. Architecture/operations/qualification。

README首屏不得列出完整数据库aggregate或25个role作为首次使用前置。架构事实保留在Architecture章节，不能删除或改写。

## 7. 遥测与支持

- 产品错误和诊断使用低基数code；不把Agent名、输入、instructions、provider body作为metric label；
- support bundle由用户显式生成，默认不含token、Secret、input/result正文；
- `--debug-authority`不会自动开启Rust backtrace、数据库日志或credential输出；
- UI/CLI隐藏层必须有测试证明不会影响服务端permission projection。

## 8. 验收

- 新用户按Quickstart完成旅程时默认输出中零出现Version/Deployment/Job/Receipt/ETag/cursor；
- advanced用户可从同一Run/Agent取得足够authority ID用于支持排障；
- text/json/Console三种默认投影字段一致且closed；
- 所有现有Problem code都有稳定产品映射或明确unknown fallback；
- 模拟Receipt journal丢失、CAS冲突、Operation不确定和SSE断线均不会重复effect；
- 安全测试证明“隐藏”不等于越权读取或弱化tenant隔离；
- ADR-0004完成修订：默认隐藏、advanced保留，不再要求首次旅程打印全部authority ID。
