# ADR-0018：作者提示词模板与顺序模型工作流

状态：Accepted（设计与作者类型已交叉评审；实现和运行证据另行验证）。

作者通过输入字段和先前步骤的输出字段编写提示词，由共享 Rust compiler 静态解析并生成
既有 RuntimePlan。表达式不在浏览器或模型中求值，不运行脚本，不恢复旧 DSL 解释器。
作者文件的 owning types 在 compiler 的 workflow 模块；Agent manifest、源预检响应和 compiler
semantic identity 同步更新。公开输入输出仍由 ClosedJsonSchema 定义。

`model_chat` 可指定独立的输入 prompt；静态 `instructions` 不插值。顺序 workflow 使用独立
作者文件及共享模型 binding，步骤输出经各自 schema 校验后才可被后续步骤引用。模板通过现有
Compute/GetField/StringConcat/MakeObject 编译，模型调用和最终 Return 仍由现有执行器处理。
不新增数据库表、状态机、消息种类、运行服务或 RuntimePlan ABI。

插值只接受已声明、必填、有界字符串字段。未知路径、可选值、非字符串、自引用、前向引用、
重复步骤及不完整输出映射在离线源预检时拒绝。按字段 UTF-8 上界累加渲染预算，并遵守已有
表达式、Plan、RunValue 与模型上下文限制。模板不是任意表达式语言，不隐式转换类型。
嵌入的数据保持 user 输入角色，不提升为可信 AgentInstruction；模型返回仍须满足精确输出 schema。

原始作者文件、依赖和 profile 进入既有冻结源包。CLI/native/WASM 使用同一编译器，Registry
重编译完整作者包，source map 指向实际步骤与 prompt。改变作者语义升级 compiler semantic
identity，不增加旧版本读取回退。既有已编译执行状态由 PostgreSQL 保持，恢复及重新发布必须
匹配当前编译边界；不把新模板解释结果写回已运行的节点。

交叉评审须覆盖 owning types、schema 17、现有 RunValue/Agent/Artifact 身份、发布事务和权限、
来源映射、容量、恢复与验证。证据要求包括模板真实表达式执行、否定输入、编译源往返、native/WASM
一致性、CLI/Console 保留作者文件，以及 Text2SQL 的多个模型步骤数据传递。测试通过不代表生成 SQL
已经通过数据库解析、执行或外部模型端点验证。

交叉评审结论（2026-09-12，主代理与独立 review_stream、review_conversation）：接受上述
作者层实现，既有数据库 schema 17 和 RuntimePlan v6 不变。生成节点采用独立固定前缀，作者
步骤标识不得与生成命名空间混淆。模型 token/轮次预算逐节点应用，工作流上界按步骤数累加，
不伪称共享总预算；deadline 仍约束整次 Run。

首版输出映射限定顶层必填字符串字段，并精确覆盖公开输出字段。嵌套输入对象的每层字段必须
必填；叶子必须是有界字符串。按字符和 UTF-8 字节双界保守推导，另计 JSON 转义开销；生成后
继续完整 Plan/profile 校验。双花括号是保留分隔符，首版不提供字面分隔符转义；插入的数据
不会递归解释。复杂控制流与多模型选择继续使用 full_plan，本决定不宣称自动兼容旧 DSL。

Console 保留原作者文件和提示词字段，workflow 文件不交给 RuntimePlan 节点编辑器。CLI 与
Registry 沿既有模型绑定和 source bundle 链路接入。来源映射按作者步骤位置生成。上述评审
是设计接受，不是测试通过声明。
