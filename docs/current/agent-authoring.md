# 共享作者入口与评测

CLI 和 Console 使用 [纯 Rust compiler](../../crates/authoring/platform-agent-compiler)，浏览器通过同核心 WASM 调用解析、校验和编译。作者文件、完整 Plan 源、schema、exact bindings 和编译 policy 输入共同形成一个有版本的 source bundle。Registry 从 exact Artifact 字节独立重编译，比较完整 ResourceDocument、Plan bytes 和编译证据；客户端编译成功不授予发布权限。

发布 Agent 时，Plan revision 绑定编译后的 Plan Artifact 及其内容摘要。原始 source bundle 是另一个 Artifact，引用保存在
Agent 定义的 `authoring_package` 中，供检查与源码恢复使用；两者不能互换。
接口或 Plan 内容相同仍可发布新版本，每个版本独立冻结作者源与校验证据。部署、运行和源码恢复按 exact Version ID 寻址，
内容摘要用于校验，不能替代版本身份。

发布的重试身份与源包的内容身份分别管理。CLI 使用私有持久记录，Console 在当前浏览器会话保留恢复记录；
同一次请求的重试复用原 Receipt 与并发条件，后续新的发布意图使用新的命令身份。验证和激活前的查询不能吞掉他人的并发修改。
完成记录或 Receipt 的历史成功响应不能证明当前已激活状态，客户端报告成功前仍检查当前 Agent 与 exact Deployment。
Console 不提供丢弃未完成恢复记录后隐式重发的入口。

CLI 和 Console 在查询任何作者依赖前，先把同一组有界源文件交给共享 Rust 源预检，校验清单、schema 和本地可判定的 Plan 结构。预检只返回编辑诊断，不构成编译或发布证据；当前权限、exact 部署、功能闭包和 policy 仍在解析后由完整编译及 Registry 终裁。解析期间文件变化不会混入已捕获的这次编译。

Console 新建对话 Agent 使用适合多行输入与长回答的有界文本字段；输入字节上限与会话提交边界一致。字段上限是结果校验条件，实际生成长度还取决于模型输出 Token 与上下文预算。调整新建默认值不会改变已发布接口或恢复的作者源；旧 Agent 的字段限制需要修改作者源并重新发布。

Context 观察结果与引用的时间使用既有 `UtcTimestamp` 格式，在计算内容摘要前统一为 UTC 六位微秒。
系统或提供方的纳秒精度不会改变输出 Schema；读取拒绝非规范时间，摘要核验仍覆盖实际输出字节。

## 完整计划与依赖

`full_plan` 引用唯一 [RuntimePlan](../../crates/definitions/platform-plan/src/lib.rs) 的完整源。内部标量、数组和对象端口使用冻结的 ClosedValueSchema；公开输入输出及人工响应使用对象 schema。模型、Capability、Context、Skill、Child Agent、控制流和等待沿用各自 typed 节点及领域合同。完整节点源可在 CLI 导入，也可在 Console 的完整源与节点编辑器中编辑；布局不进入 Plan 的语义身份。

编译输出同时包含[不可变来源映射](../../crates/authoring/platform-agent-compiler/src/source_map.rs)，绑定作者文件、Plan 与 compiler semantic identity；文件、JSON pointer 和实际解析器行列用于定位节点、端口和表达式。CLI 发布缓存及 exact source 恢复会导出 `source-map.json`。映射可以从同一冻结作者包重建，不拥有运行状态。诊断只返回受限错误与来源位置，不附带源文件正文。

[节点编辑描述](../../contracts/platform-v1/agent-node-editor.v1.json)从 owning Rust 节点生成，模板中的未绑定引用和零预算是待填写草稿。表达式编辑通过同一 Rust/WASM `TypedExpressionProgram::build` 重建摘要与栈深度，使用与完整编译相同的表达式限额；生成模板与重建成功均不取代完整编译、Registry 验证或发布授权。

作者用 `agent dependencies <kind>` 查询当前可读部署，并可通过 manifest 父目录下 `.insight/agent-binding-selections.json` 表达 Active 或 Exact 选择。在线编译调用只读 resolve API，Active 在该次解析转为 exact 引用。`.insight/agent-exact-bindings.json` 保存已经解析的逻辑 binding；Context 的 snapshot identity 在发布时由服务端分配。查询返回的兼容性判断与当前调用授权分开显示，发布和运行仍重新检查权限。

## 恢复已发布作者包

```bash
insight agent restore-source --path ./project \
  --agent agt_... --version arev_... --output ./restored
insight agent validate --path ./project/restored \
  --file ./project/restored/path/from/restore-report/agent.json
```

使用实际 exact Version ID 和恢复响应中的 manifest 路径。恢复会先读取当前授权下的 Version、作者 Artifact 和 Plan metadata，校验 exact 引用、完整源和重编译结果，再原子创建新目录。目标已存在、目录竞争、非法路径、未知字段、内容篡改或授权拒绝都会失败。源文件与 private profile/binding 文件不会覆盖已有目录。

默认离线 `validate` 使用恢复包 manifest 父目录的冻结 profile；存在但无效的 profile 会直接报错。发布使用当前服务端 profile，并再次经过 Registry 独立验证。恢复文件不包含服务端 credential，也不取代资源、部署或权限 authority。

## 框架集成的恢复范围

[compiled-framework 示例](../../examples/productization/compiled-framework) 使用显式、数据化的静态导出，经同一 compiler 生成 Plan。原导出仍保留为作者源，Registry 能重新 lowering。导出必须显式提供 typed ports、节点、控制关系和 exact dependencies；它不解析任意 Python/JavaScript 程序，也不接受隐式 reducer、动态 graph 或外部 checkpointer 作为平台状态。

这类编译后的节点由平台 Run 保存进度。[LangGraph remote Capability 示例](../../examples/productization/langgraph-reference) 则在一次 Capability invocation 外部运行整个 graph；平台保存调用边界，框架内部节点的 checkpoint 与恢复属于该外部服务。

## 评测复用普通 Run

[评测请求、manifest 与报告 schema](../../contracts/platform-v1/schemas/evaluation-plan-request-v1.schema.json) 对应 [owning Rust types](../../crates/authoring/platform-agent-compiler/src/evaluation.rs)。manifest 固定 exact Subject/Evaluator Agent deployment、sample Artifact、预期值 schema、重复次数和 metric schema。trial 身份绑定完整 manifest digest、sample ID 与 repetition；重试不生成另一 trial。

```bash
insight agent evaluation-init --path ./project \
  --file ./evaluation-request.json --output ./evaluation-agent
insight agent evaluation-input --path ./project \
  --file ./evaluation-request.json --output ./evaluation-input.json
insight agent publish --path ./project --file ./project/evaluation-agent/agent.json
insight agent run echo-evaluation --path ./project --file ./project/evaluation-input.json
insight agent evaluation-report --path ./project \
  --file ./evaluation-request.json --run run_... --output ./evaluation-report.json
```

`evaluation-init` 生成普通父 Agent 源；`evaluation-input` 当前授权读取 exact JSON Artifact，核对 bytes、digest 和实际 schema 后写 private Run 输入。manifest、input 和 expected Artifact 必须保存 canonical JSON 原始字节；pretty JSON 等非 canonical 编码会在写入前明确拒绝，因此 sample Artifact digest 与普通 RunInput 的 canonical digest 保持一致。样本正文进入普通 Run input，不进入作者包。父 Plan 以有界 ChildAgent 与 AllSettled 组合调用 Subject 和 Evaluator，不增加评测 scheduler、Job kind 或状态表。生成器会拒绝超过 Plan/schema/profile 限额的组合。

Evaluator 的输入协议由生成的 `evaluator-input.schema.json` 定义，包含 trial metadata、实际 input/output 及可选 expected；输出须满足 manifest 的 metric schema。先按这个协议发布 Evaluator，再用其 exact deployment 构造最终请求。示例中的 IDs 是显式测试引用，需要替换成所在租户的已发布资源与 Artifact。

`evaluation-report` 从受权的 parent/Node→child 查询取得运行关系，再读取同一 Run 的实际输入输出和终态版本。它核对 exact deployment、样本与评分输入 digest、metric schema 和完整 trial 计数；已知失败即使没有输出值也保留为 Failed，未开始或证据不足保持 Missing。Inline 与 Artifact RunValue 均可作为证据。报告通过私有临时文件与原子禁止覆盖发布写入，并通过普通 Artifact upload 发布；重试复用相同报告与上传 journal。报告读取和上传始终使用现有内容授权、classification 和 Artifact 生命周期。

## 验证范围

[公开文档检索示例](../../examples/productization/document-review) 提供冻结项目文档的独立 HTTPS provider，复用 Remote Context
协议。返回的原文、URI 与行范围可单独核对，平台引用仍是 RemoteOpaque / ObservationOnly。该示例的本地检索和 TLS
测试不证明公开部署、真实模型回答或人工确认；完整流程需要环境中的真实 exact 依赖，并由用户审核实际回答后提交 Task 响应。

[可重现示例生成器](../../tools/rust/platform-contract-tooling/src/bin/product_authoring_examples.rs) 实际调用共享 compiler，并独立以 JSON Schema 验证合法文件及闭合字段、版本、身份和结果负例。[CLI HTTP tests](../../apps/insight-cli/src/evaluation_tests.rs) 覆盖 actual loopback HTTP 的内容读取、授权拒绝、失败无输出、错误 ancestry、digest 与分页；[恢复 tests](../../apps/insight-cli/src/agent_restore.rs) 覆盖原子目录写入与可重新编译性。这些是编译和产品协议证据；普通 Run 的执行、并发和恢复由其 PostgreSQL/runtime tests 验证，不能将 loopback fixture 描述为生产资格证明。

编译输入中的部署特性证据只来自 actual exact 候选解析，并进入完整 source bundle 身份。Capability 后端、Context 的 embedding/MCP 依赖和 ChildAgent 已验证 Plan 的传递依赖共同决定功能要求；Model 节点本身具有模型执行要求。额外、重复或无关证据明确拒绝。Registry 在原验证提交事务内按真实发布的部署闭包重推，客户端编译成功不构成已安装能力证明。`evaluation-init` 也先执行同一只读解析，再生成普通父 Agent 源。
