# 运行控制台

[Console](../../apps/console) 是无 BFF、无数据库的静态 React 客户端，访问 Gateway 的 `/readyz` 与 public `/v1`，
生产静态资源由 Gateway/Ingress 同源托管。默认页面为 Agents、Runs、Tasks 和 Settings；关联对象提供低敏诊断信息。
业务状态、资格判断和正文读取授权由服务端拥有。

Agent 编辑器提供表单与 YAML 视图，可编辑引用的输入/输出 schema、完整 Plan JSON、静态框架 graph export 和 slot bindings。
编译与错误诊断来自与 CLI 共用的[纯 Rust 编译器](../../crates/authoring/platform-agent-compiler)，浏览器通过 WASM 调用；
TypeScript 只组织输入和投影字段。[节点编辑器](../../apps/console/src/agent/PlanEditor.tsx)使用 owning Rust 生成的 draft descriptor，
直接编辑同一份 source JSON 的节点、连接、exact port、分支顺序、Join、Loop 和表达式字段；新增模板只是未完成草稿。
表达式的栈深度与语义摘要由 Rust builder 重建，完整编译仍是最终检查。编译 source map 提供真实源文件、JSON pointer 与行列位置，可独立导出。
框架导出使用显式 Platform 节点，恢复粒度随这些节点确定；不执行导入的 Python/JavaScript 程序。
不支持的源格式或能力会阻止编译。

导入 source bundle、打开已有 draft 时，从完整作者包读回原始 manifest、schema、Plan/框架源码文本及 slot 输入。
也可按 Agent ID 与 exact published version ID 恢复：先核对 immutable version/ETag、当前受权 Artifact 的精确字节与摘要，
使用包内 frozen profile/bindings 重编译核对完整 ResourceDocument，再替换编辑器。任一步失败都会保留已有草稿。
恢复的原包和 source map 可独立导出；界面显示原编译 profile 摘要，并明确说明下次 Validate/Publish 使用当前租户 profile、可能产生新 Plan。
缺失引用文件或多余未处理文件会报错，不从执行产物猜测缺失源码。导入内容作为待编辑输入，发布和 bundle 导出都会使用当前租户
profile 重新编译。浏览器编译通过不授予发布或运行权限；Registry 仍独立验证。发布进度与刷新恢复依据 publication handle
及服务端 Resource、Artifact、Operation、Deployment 状态，具体协议见[发布实现](../../apps/console/src/agent/publication.ts)。

[依赖查询面板](../../apps/console/src/agent/AuthoringBindingsPanel.tsx)支持筛选、分页发现和 Active/Exact 选择解析。
界面分别显示 contract match 与 call authorized；未提供契约摘要时明确显示未检查。解析得到的 exact slot 输入仍须与完整源码一起编译。
这些查询不创建 Deployment、Job 或 Receipt。编译前对实际 Capability、Context 和 ChildAgent 候选再解析 exact 部署特性；界面不从节点名称猜测后端，Registry 在提交验证结果时重推同一证据。

Task inbox 可选择可响应任务或仅可查看的安全元数据，使用服务端按目的和当前权限过滤后的分页；空页有 next cursor 时仍可继续。详情与冻结表单分开读取，表单按主体、Task、generation、version
隔离。常规控件覆盖 required、枚举、嵌套对象和数组；其余合法 closed schema 使用有界 typed tree，包括本地 `$defs`、带标签分支、null、const 和完整枚举。
exact pinned nominal 只解析构建时随 owning registry 绑定的本地 schema，并验证名称、版本与摘要；未知引用明确禁止提交。
const/enum 中的 `$ref` 保持普通数据，不发起网络解析，不执行 HTML。复杂字段分页显示，提交仍受 Task 输入字节、深度和集合边界约束。
缺少可用表单时不会生成替代 JSON 表单。操作按钮只取服务端当前 `allowed_actions`；OAuth 无通用按钮，可查看权限不赋予表单读取或提交权限。
提交绑定冻结表单返回的当前 ETag、幂等键、分类和冻结 schema digest，服务端实例验证及当前资格检查仍是最终依据。

新 Run 的默认 schema、classification 与 deadline 来自所选 active exact 部署的已发布 Plan。提交携带同一 exact 条件；active 已变则提示冲突，用户显式刷新后再提交。当前编辑 draft 不改变这些默认值，同一提交意图重试保留完整请求和 Receipt。

Run 中的“Inspect frozen source”按其服务端冻结定义读取 exact 版本与受权 Artifact，通过共享 compiler 重建 source map。节点/端口/表达式位置描述冻结 Plan，不能作为执行进度；正文读取失败会清除局部结果。

Run timeline 持续跟随服务端有限 SSE 页，处理任意 UTF-8/帧切分，以 event_id 去重，只用最后完整事件的 opaque cursor 继续读取。
断流会重连，空页使用有界退避；每页必须提供真实 replay floor、high water 与截断信息。历史被保留策略截断时显示提示，
cursor 过期或低于 floor 等不可重试错误会停止跟随，不静默重置游标。刷新独立读取当前 Run，不填补缺失历史，事件终态不代替 Run authority。
Run signal 使用已有 public signal 端口，支持无 payload 或带 exact schema digest 的 Inline 输入。同一意图重试保持 Receipt，
Run/主体切换取消请求并清空输入，接受 signal 后独立刷新当前 Run。

Run values 先列出 metadata，再由用户选择读取单个值。正文必须重新通过服务端授权，并与所选 metadata 的身份和摘要相符；
明确请求的受权正文按文本完整显示，包含合法的 prompt/tool output 字段；Artifact reference 不隐式下载字节。
下载会再次读取当前 metadata/content，核对精确引用、长度、media type、ETag 与实际摘要。Run result 的新读取失败会清空旧正文。
token 只保存在内存，切换连接、租户、主体或选中对象会取消在途请求并清空相关正文与游标；
401/403 清除受影响内容。诊断展示使用脱敏投影，浏览器不持久化 credential。

本地真实浏览器测试使用透明同源代理，分别读取实际 runtime profile 的 Management 与 Runtime Gateway 地址。
作者、发布和依赖查询转到 Management；Run、Task 与 Artifact 操作转到 Runtime。两个地址仅允许 loopback HTTP origin，
代理不生成响应、不缓存正文，也不改变 Authorization、Receipt、ETag 或 SSE cursor；未知路径仍由真实 Gateway 拒绝。
此开发测试 transport 不增加 BFF 或业务权限；生产仍由部署配置完成同源路由。

Node.js 只用于构建静态 bundle 和运行浏览器测试，不是平台服务运行时：

```bash
cd apps/console
corepack pnpm install --frozen-lockfile
corepack pnpm test
corepack pnpm run lint
corepack pnpm run build
```

当前新增 UI 路径由真实 Chrome、真实生成的 WASM 和模拟 public API fixture 验证，包括全部 owning 节点草稿、字段编辑与表达式重建、
exact 恢复、查询、Task typed tree、signal、SSE、正文清除与页面无障碍检查。
真实 PostgreSQL 的[Task/Receipt 读取测试](../../crates/adapters/platform-postgres/tests/product_task_reads_pg.rs)另行验证分页扫描、资格、当前成员权限和正文门禁；
它直接种入符合 owning contract 的 Task 数据，不能据此推断 Run 创建或 Task mutation 已端到端通过。
作者依赖查询另由[资源生命周期测试](../../crates/adapters/platform-postgres/tests/phase1_z_resource_lifecycle.rs)中的
[查询助手](../../crates/adapters/platform-postgres/tests/product_authoring_query_fixture.rs)在真实 PostgreSQL 验证：复用实际发布并激活的 Agent，
以有类型的 Selection Policy 数据验证解析和权限；这不代替该 Policy 的发布准入或浏览器到 Gateway 的完整旅程。
测试入口与真实 Gateway journey 的区别见[Console README](../../apps/console/README.md)。
