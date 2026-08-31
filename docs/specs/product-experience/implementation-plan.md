# Agent 产品体验实施计划

| 属性 | 值 |
|---|---|
| 状态 | Implementing / CR-214 |
| 日期 | 2026-09-01 |
| 输入 | product-experience 00～06、Platform 17/18、ADR-0003～0005 |
| 不变量 | `/v1` clean cut、无新业务authority/表/常驻role、L4～L6 Not run |

本计划只把Accepted合同拆成可验证、单一职责的实现批次。每批通过相称门禁后独立提交；失败或发现P0/P1合同冲突时，
先修上游spec与cross-review，不用兼容层、fallback或测试fixture伪造完成。

## Phase 0：合同与cross-review

状态：**Complete**。

- 修订ADR-0003/0004/0005；
- 在Platform 17冻结Agent/Run bounded list、产品summary与cursor envelope；
- 在Platform 18冻结compiler、API、CLI、Console、distribution与starter资格矩阵；
- product-experience 00～06恢复Accepted。
- CR-208关闭Artifact ID物化环；CR-209冻结`model_chat`作者指令在Agent Revision中的落点与untrusted assembly角色。
- CR-210冻结`deterministic`的identity input/output schema与exact RunInput terminal port。
- CR-211冻结Interface contract与`primary_model` requirement的canonical v1 preimage。
- CR-212把normalized authoring name物化到现有Agent Resource authority并冻结创建后不可更名语义。
- CR-213把compiler required feature set物化到现有Agent Resource authority，关闭草稿态summary来源缺口。
- CR-214把normalized input classification与default deadline物化到现有Agent Resource authority，关闭adopt/lock丢失后的Run默认来源缺口。

完成证据：文档链接与状态一致、`git diff --check`、CR-214无P0/P1。

## Phase 1：shared Agent authoring compiler

交付：

- 无网络/数据库/执行依赖的Rust owner compiler；
- strict YAML 1.2 JSON-compatible parser、project-root/symlink-safe reference loader与closed nominal manifest；
- `deterministic`和`model_chat`生成canonical Agent resource intent、Typed Plan v5、schema/manifest digest、binding intent、
  required features和ordered lifecycle plan；
- `model_chat.instructions`物化到Agent Revision，并以`AgentInstruction`/`user`/untrusted block进入canonical model assembly；
- `deterministic`拒绝不同input/output schema，禁止隐式coercion或不可执行terminal plan；
- 上传后以exact Artifact authority materialize并重验真正的`ResourceDocument::Agent`，compiler不预留或猜测ID；
- repository-level跨语言fixture corpus与TypeScript conformance adapter。

门禁：positive/negative corpus、canonical map/order/OS independence、path/Secret/URL/shell rejection、Rust/TypeScript逐字节一致、
workspace fmt/strict Clippy与crate-boundary checker。

## Phase 2：Agent/Run safe list projection

交付：

- owner DTO、OpenAPI/JSON Schema/generated registry；
- HMAC/AEAD list cursor codec，绑定purpose/tenant/principal/filter/page/snapshot/keyset/expiry；
- PostgreSQL direct-authority queries与Management/Runtime Gateway handlers；
- product Problem mapping所需的`cursor_invalid|cursor_expired`稳定投影。

门禁：unit schema/codec、fresh PostgreSQL tenant/CAS/concurrency/keyset、HTTP wrong-role/auth/filter/cursor负向、OpenAPI生成漂移、
无新migration/table/Event projection。

## Phase 3：`insight agent` CLI

交付：

- `validate/publish/list/get/adopt/run/logs/result`与`text|json`、`--verbose`、`--debug-authority`分层；
- project `insight.lock`和0600 publication/Run cursor journal；
- publish四阶段恢复、Receipt/ETag代管、unchanged replay、SSE重连与Ctrl-C resume；
- 默认help只突出`init/dev/agent`，`apply/run/task/artifact/operation`移入advanced help但不删除。

门禁：每个HTTP crash window、并发publish、journal丢失/损坏/权限、terminal success/failure、字段泄漏矩阵与public-only dependency检查。

## Phase 4：Console authoring与Run

交付：

- 默认Agents/Runs/Tasks/Settings信息架构；
- Agent分页、表单/YAML import/export、shared fixture compile、四阶段publish与CAS diff；
- schema驱动Run输入、durable SSE timeline/result、inline Task与显式Artifact download；
- 折叠Advanced diagnostics，token/manifest/input/result不进入browser persistence。

门禁：TypeScript/lint/build、headless browser空tenant北极星旅程、reload/crash/CAS并发、storage/DOM canary、keyboard/mobile/
high-contrast/ARIA，以及既有Task/Artifact安全回归。

## Phase 5：预构建发行物

交付：

- 四target CLI archive、runtime/guest/Console immutable image与manifest list；
- checksum、SPDX/CycloneDX SBOM、provenance、signature及canonical ReleaseBundle；
- `version`、`update check/apply`的exact platform/release验证与原子替换；
- PR/main/tag分层workflow、single build reuse、bounded signing/push与machine-readable performance report。

门禁：archive内容、offline verification、wrong arch/tag/digest/partial push/cache poisoning/sign timeout负向，四runner
`version/doctor`；未实际push/sign/跨架构运行的项必须报告Not run。

## Phase 6：starter与feature profile

交付：

- `insight dev [--features ...] [--offline|--from-source]`及closed canonical feature registry；
- starter与model/remote-capability/context/mcp/wasi/all exact role/image/config/identity/readiness closure；
- `start/stop/reset`语义、drift/preflight/doctor、feature_not_enabled零I/O；
- cold/warm/idle machine report与single-node/non-production/L4～L6 Not run声明。

门禁：feature permutation digest、unknown/duplicate/conflict零I/O、starter/single/all readiness负向、stop/start与process/dependency
recovery、deterministic Agent在starter/all结果与authority一致。资源预算只在指定runner实际测量后标passed。

## Phase 7：总回归与clean cut

交付：

- L1～L3适用workspace门禁、Console与release/profile checks；
- 北极星fresh journey与machine-readable evidence；
- README、`docs/current`、CLI help和Console同一commit clean cut；
- product-experience状态按真实证据推进，保留L4～L6 Not run。

禁止用route/object count、mock-only旅程、checked-in dist、静态Compose或本机Docker结果替代所声明层级证据。
