# ADR-0011：Console TypeScript 构建与结构化编辑

状态：Accepted（设计决定；实现与运行证据单独验证）。

Console 保持静态 React 客户端及无状态同源 transport。手写服务、脚本、测试与页面统一使用
TypeScript。生产 transport 由 TypeScript 编译到 `server-dist`，NativeLaunchPlan 先更新实际
入口及产物核验，renderer、镜像和检查再消费此布局；不读取旧服务源文件作为回退。
生成的 JavaScript 属于构建产物，仍按真实字节冻结，已有安装不得热替换。

页面按 app、features、shared 分层，样式使用 CSS Modules 和共享设计变量。中文仅是展示层，
不改变协议、用户文本或发布文档字段。连接草稿与有效会话分离，健康检查不能授予身份；受保护
请求成功后才进入业务页面。凭据仍只在内存，到期或认证失效撤销本地会话及相关内容。

智能体采用分步表单、schema 字段表格和带高亮的高级源码视图。表格保留源 schema 中未编辑的
约束；无法无损映射的源码留在高级编辑器。Rust/WASM 编译器仍拥有语义，Registry 仍拥有发布
资格，切换视图不会自动提交。运行展示依赖当前授权结果，不从终态事件推断正文。

实施前已交叉核对现有 Console 文档、ADR-0010、NativeLaunchPlan、renderer 和 transport：

- 拥有域、身份、schema：不增加业务拥有域或公共字段；进程产物路径由部署 owning type 定义。
- 错误、安全：401 清理会话，403 保留明确的权限错误；地址与 token 只在显式提交后生效。
- 事务、事件：Receipt、ETag、发布恢复和 SSE cursor 沿用既有客户端，不增加写入路径。
- 容量：沿用有界配置、请求缓冲与源码限制，编辑器按需加载。
- 恢复：源码导入失败保持原稿；过时异步结果不得覆盖新修订；冻结安装保持原字节身份。
- 证据：实施后必须验证类型检查、transport、产物核验、源码往返及真实浏览器关键旅程。
  这里的静态核对不代表以上测试已经通过，也不代表独立人员评审。

数据库结构和后端业务语义未变。当前行为与测试范围见[运行控制台](../current/console.md)，
开发与验证命令见 [Console README](../../apps/console/README.md)。
