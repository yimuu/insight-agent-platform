# 运行控制台

“模型配置”页面中的来源、密钥轮换、连接检测与默认模型操作见[模型配置](model-configuration.md)。

[Console](../../apps/console) 是 React 客户端，访问 Gateway 的 `/readyz` 与 public `/v1`，
静态资源由独立 Console transport 提供，公开 API 按路由透明转发至 Management 或 Runtime Gateway，保持同源。
页面包含智能体、对话、运行记录、待办任务、模型配置和设置；关联对象提供低敏诊断信息。
业务状态、资格判断和正文读取授权由服务端拥有。

Compose 和 Helm 的首次页面创建管理员账号，之后使用邮箱和密码登录。密码哈希和会话由独立
local-identity 服务保存在 PostgreSQL；Console transport 没有数据库凭据或签名私钥。
浏览器只持有八小时 HttpOnly、SameSite=Strict cookie，服务端为 Gateway 换取短期 OIDC 令牌，
令牌不交付 JavaScript。重启保留会话，退出登录撤销当前会话。Native 的显式 bearer 模式继续使用私有会话文件。
401 清除工作会话并取消在途请求，403 展示当前操作权限不足。

侧栏按工作空间和管理分组；列表优先展示内容、筛选和主要动作。切换页面保留当前编辑草稿。
工作流画布显示节点与控制流连接，点击节点编辑同一份源码；新工作流由共享 Rust 编译器生成可验证的起始结构。
原始 JSON、依赖摘要和编译位置收进高级区域，画布不会把节点位置当作运行进度。

Agent 创建采用四步表单：基本信息、模型与任务、输入输出、检查发布。字段表格编辑同一份 schema 源文档，
保留未编辑约束，复杂根结构留在高级 JSON 编辑器；新字段包含平台 closed schema 要求的显式上下界。
YAML 与 JSON 使用按需加载的 CodeMirror，提供高亮、行号、搜索及语法定位。YAML 与表单往返保留备注和未编辑字段，
无法解析的格式保留原稿并阻止转换。高级编辑仍支持完整 Plan JSON、静态框架 graph export 和 slot bindings。
编译与错误诊断来自与 CLI 共用的[纯 Rust 编译器](../../crates/authoring/platform-agent-compiler)，浏览器通过 WASM 调用；
TypeScript 只组织输入和投影字段。[节点编辑器](../../apps/console/src/features/agents/PlanEditor.tsx)使用 owning Rust 生成的 draft descriptor，
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
及服务端 Resource、Artifact、Operation、Deployment 状态，具体协议见[发布实现](../../apps/console/src/features/agents/publication.ts)。

[依赖查询面板](../../apps/console/src/features/agents/AuthoringBindingsPanel.tsx)支持筛选、分页发现和 Active/Exact 选择解析。
界面分别显示 contract match 与 call authorized；未提供契约摘要时明确显示未检查。解析得到的 exact slot 输入仍须与完整源码一起编译。
这些查询不创建 Deployment、Job 或 Receipt。编译前对实际 Capability、Context 和 ChildAgent 候选再解析 exact 部署特性；界面不从节点名称猜测后端，Registry 在提交验证结果时重推同一证据。

Task inbox 可选择可响应任务或仅可查看的安全元数据，使用服务端按目的和当前权限过滤后的分页；空页有 next cursor 时仍可继续。详情与冻结表单分开读取，表单按主体、Task、generation、version
隔离。常规控件覆盖 required、枚举、嵌套对象和数组；其余合法 closed schema 使用有界 typed tree，包括本地 `$defs`、带标签分支、null、const 和完整枚举。
exact pinned nominal 只解析构建时随 owning registry 绑定的本地 schema，并验证名称、版本与摘要；未知引用明确禁止提交。
const/enum 中的 `$ref` 保持普通数据，不发起网络解析，不执行 HTML。复杂字段分页显示，提交仍受 Task 输入字节、深度和集合边界约束。
缺少可用表单时不会生成替代 JSON 表单。操作按钮只取服务端当前 `allowed_actions`；OAuth 无通用按钮，可查看权限不赋予表单读取或提交权限。
提交绑定冻结表单返回的当前 ETag、幂等键、分类和冻结 schema digest，服务端实例验证及当前资格检查仍是最终依据。

“对话”页面把工作空间会话、消息和选中轮次的执行画布放在一起。会话与轮次由 PostgreSQL
保存，正文复用 RunValue；刷新通过 URL 中的会话 ID 重新读取，不依赖浏览器保存聊天正文。
会话共享工作空间权限，创建者字段仅用于审计。新会话固定已发布 Agent；部署变化后旧记录
仍可读取；页面发现新部署后提示使用新版本创建对话，服务端也会拒绝向旧绑定继续发送。当前聊天接口接收一个必填文本字段并输出 answer。
失败轮次可通过“查看失败原因”读取执行事件的安全摘要，并在回答位置显示；记录没有具体原因时不推测原因。

发送消息通过会话版本和幂等键提交，未结束轮次期间禁止重复发送；不确定失败保留同一提交意图。
成功历史按真实 User/Assistant 角色进入后续模型请求，失败轮次不会作为助手回答加入历史。
停止生成复用 Run 取消命令；切换会话或关闭页面只中止观察，不取消运行。

实时文字使用独立 SSE 端口，与有限 durable event 页分开。它可能缺少订阅前的文字或因断线
丢失片段，因此页面明确标记临时输出，完成后重新读取受权结果。不同模型轮次和执行尝试分别
显示，点击历史消息只切换右侧执行画布。撤权清除相应正文；事件画布不能代替最终 Run 状态。
画布按当前 Run 隔离，NodeExecution 和 ModelTurn 状态、耗时从各自数据库记录读取，旧事件缺少完成记录也不会被当作仍在执行。
选中模型调用可查看真实输入消息、输出和响应中的用量；编排节点展示实际关联的 RunValue，不把 Run 总结果复制给所有节点。
正文单独授权读取并受预览容量限制，系统指令、原始数据和诊断标识默认折叠。详情置于画布下方，避免压缩节点区。

新 Run 优先按已发布 schema 提供结构化输入表单，也可切换高级 JSON。默认 schema、classification 与 deadline 来自所选 active exact 部署的已发布 Plan。提交携带同一 exact 条件；active 已变则提示冲突，用户显式刷新后再提交。当前编辑 draft 不改变这些默认值，同一提交意图重试保留完整请求和 Receipt。

Run 中的“查看冻结源码”按其服务端冻结定义读取 exact 版本与受权 Artifact，通过共享 compiler 重建 source map。节点/端口/表达式位置描述冻结 Plan，不能作为执行进度；正文读取失败会清除局部结果。

Run timeline 持续跟随服务端有限 SSE 页，处理任意 UTF-8/帧切分，以 event_id 去重，只用最后完整事件的 opaque cursor 继续读取。
断流会重连，空页使用有界退避；每页必须提供真实 replay floor、high water 与截断信息。历史被保留策略截断时显示提示，
cursor 过期或低于 floor 等不可重试错误会停止跟随，不静默重置游标。刷新独立读取当前 Run，不填补缺失历史，事件终态不代替 Run authority。
Run signal 使用已有 public signal 端口，支持无 payload 或带 exact schema digest 的 Inline 输入。同一意图重试保持 Receipt，
Run/主体切换取消请求并清空输入，接受 signal 后独立刷新当前 Run。

已授权的最终答案在运行详情顶部展示，原始结果、事件数据及源码检查保留在高级诊断中。
Run values 先列出 metadata，再由用户选择读取单个值。正文必须重新通过服务端授权，并与所选 metadata 的身份和摘要相符；
明确请求的受权正文按文本完整显示，包含合法的 prompt/tool output 字段；Artifact reference 不隐式下载字节。
下载会再次读取当前 metadata/content，核对精确引用、长度、media type、ETag 与实际摘要。Run result 的新读取失败会清空旧正文。
token 只保存在内存，切换连接、租户、主体或选中对象会取消在途请求并清空相关正文与游标；
401/403 清除受影响内容。诊断展示使用脱敏投影，浏览器不持久化 credential。

Console 源码按 app / features / shared 组织；业务样式使用 CSS Modules，共享主题使用 CSS 变量。
手写页面、服务、脚本和测试统一为 TypeScript，生产服务编译到 `server-dist`，Native 与镜像消费生成的实际字节。
Console transport 使用 Node.js 提供静态 bundle 和透明同源转发，不持有业务状态或增加业务权限。
Compose、Helm 和 Native 都从共享安装配置取得 Management 与 Runtime Gateway 地址；Native 启动计划
传入冻结的 Console 配置文件，其上游限定为 loopback HTTP。转发保留 Authorization、Receipt、ETag 和 SSE cursor，不缓存私有响应。
各路由的归属由[共享路由实现](../../apps/console/server/gateway-server.ts)定义；未知公开路径由 Gateway 拒绝。

Console 的源代码构建和本地测试命令为：

```bash
cd apps/console
corepack pnpm install --frozen-lockfile
corepack pnpm test
corepack pnpm run lint
corepack pnpm run build
```

UI 回归使用真实 Chrome、真实生成的 WASM 和模拟 public API fixture，覆盖中文连接的 401/403 区分、模型向导、字段重名、窄屏布局、
YAML 高亮与备注保留，以及 owning 节点草稿、字段编辑与表达式重建、
exact 恢复、查询、Task typed tree、signal、SSE、正文清除与页面无障碍检查。
真实 PostgreSQL 的[Task/Receipt 读取测试](../../crates/adapters/platform-postgres/tests/product_task_reads_pg.rs)另行验证分页扫描、资格、当前成员权限和正文门禁；
它直接种入符合 owning contract 的 Task 数据，不能据此推断 Run 创建或 Task mutation 已端到端通过。
作者依赖查询另由[资源生命周期测试](../../crates/adapters/platform-postgres/tests/phase1_z_resource_lifecycle.rs)中的
[查询助手](../../crates/adapters/platform-postgres/tests/product_authoring_query_fixture.rs)在真实 PostgreSQL 验证：复用实际发布并激活的 Agent，
以有类型的 Selection Policy 数据验证解析和权限；这不代替该 Policy 的发布准入或浏览器到 Gateway 的完整旅程。
测试入口与真实 Gateway journey 的区别见[Console README](../../apps/console/README.md)。

本次持久会话、真实流式与服务重启后的 HTTP 验证见[本机开发记录](../qualifications/conversation-live-2026-09-12.json)。
浏览器自动化连接不可用，因此该记录不宣称自动点击或截图验收通过。
