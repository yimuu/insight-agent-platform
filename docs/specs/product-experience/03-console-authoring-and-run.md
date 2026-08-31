# Spec 03：Console Agent authoring 与运行

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-215 |
| 日期 | 2026-09-01 |
| 前端 | 现有静态 React Console |
| authority | public `/v1` only；无BFF、无Console数据库 |

## 1. 目标

Console 从按ID检查authority的运维界面，收敛为默认可完成下列旅程的Agent产品入口：

```text
Agent列表 -> 新建/导入 -> 校验 -> 发布 -> 输入 -> 运行 -> 时间线/结果
```

用户不需要手工复制Agent、Deployment、Run、Task、Artifact或Operation ID。高级诊断仍可查看这些事实。

## 2. 信息架构

默认一级导航固定为：

1. `Agents`：列表、状态、创建、编辑、发布；
2. `Runs`：最近运行、过滤、状态、结果；
3. `Tasks`：当前用户可处理的人机任务；
4. `Settings`：Gateway、身份、project和feature readiness。

Artifact、Operation、Deployment、Receipt、Trace不再是默认一级导航，统一进入关联对象的“Advanced diagnostics”。

## 3. Agent 页面

编译新manifest前，Console读取bounded tenant-scoped `GET /v1/agent-authoring-profile`并把其exact bindings与limits交给shared
compiler adapter。missing、disabled、stale或unauthorized profile必须给出actionable error；browser storage与build-time默认都不是fallback
authority。

### 3.1 列表

列表只显示安全摘要：display name、authoring name、gate/ready状态、当前environment、最后发布时间和最近Run状态。
必须使用opaque cursor分页，默认页不超过50项；不能一次加载全tenant。

现有API没有安全Agent列表，因此需要在`/v1`内增加只读Resource authority投影。它只能查询现有Resource aggregate，
不得新建列表表、缓存current head或用Event重建current state。排序必须稳定，cursor不暴露数据库key。

### 3.2 新建与编辑

两种等价入口：

- 表单模式：name、display name、execution kind、instructions、model binding、schemas和limit；
- `agent.yaml`导入/导出。

表单生成Spec 01同一closed manifest。浏览器实现与CLI必须通过共同compile fixture；不能维护宽松的第二套字段。
编辑existing Agent先读取current ETag，但默认界面不显示；提交冲突时展示“服务器版本已变化”，允许重新加载或查看diff，
不得自动覆盖。

### 3.3 发布

发布页面显示面向用户的四步进度：

```text
Validating -> Publishing -> Activating -> Ready
```

底层Artifact、Version、Deployment和Operation进度进入折叠诊断。刷新浏览器后可通过publication handle和服务端authority
恢复进度；不能把仅存在于React state的状态当作发布事实。

## 4. Run 页面

- 从Agent详情点击`Run`进入，自动携带Agent ID和input schema；
- 表单依据closed input schema生成，另提供严格JSON模式；
- 创建后自动进入Run详情，使用durable SSE cursor更新；
- Run input classification与default deadline只从exact Agent Resource/Revision读取；reload、跨设备或adopt后不得用浏览器缓存或
  profile隐藏默认补值；
- terminal时在同页展示typed result或安全failure；
- waiting Task以内联卡片跳转，完成后回到原Run；
- pause/resume/cancel由Console生成Receipt并携带ETag，用户不输入；
- Artifact结果只在显式下载动作后读取，不把未授权正文渲染到DOM。

历史页面使用CR-207已评审的`GET /v1/runs` bounded safe projection，至少按Agent ID、state和created time过滤。它直接查询
Run authority，不能新建projection表或把Event当current state。opaque cursor只在分页envelope中由客户端代管，不进入默认DOM、标题或消息；
不得用localStorage伪造远端历史。

## 5. 浏览器安全

- OIDC token继续只保存在内存；reload要求重新认证或走标准OIDC流程；
- manifest草稿默认只在内存；显式下载由用户决定，不写localStorage/sessionStorage/URL；
- instruction、input、result和event均按classification与public projection渲染；禁止`dangerouslySetInnerHTML`；
- CSP、CSRF/CORS、frame、content type和cache header沿用public Gateway合同；
- error/diagnostic递归脱敏token、Secret、raw provider body、signed URL和内部endpoint；
- Console bundle没有worker identity、数据库URL、内部RPC descriptor或发布私钥。

## 6. 可访问性与状态

- 核心旅程满足WCAG 2.2 AA适用项，键盘可完成创建、发布、Run和Task；
- 状态不只靠颜色表达；异步进度使用`aria-live`且避免重复播报；
- schema错误定位到字段和YAML path；
- 断网、401、403、409、429、503和SSE恢复有不同操作建议；
- 所有时间展示本地格式，同时在高级信息保留UTC值。

## 7. 高级诊断

只有用户主动展开时显示：Resource/Version/Deployment/Operation/Run/Receipt/ETag/cursor、request/trace ID、digest和
安全Event JSON。诊断提供复制按钮，但默认页面、标题、按钮和成功消息不得使用这些概念。

高级模式不增加权限；服务端仍按当前principal投影字段。隐藏不是授权机制。

## 8. 验收

- headless Chrome从空tenant完成Agent创建、发布、Run和结果，全程不手工复制ID；
- YAML导入与表单导出在canonical manifest上等价，CLI/Console编译digest一致；
- reload覆盖发布四个阶段和Run SSE恢复，不重复mutation；
- 两个浏览器并发编辑得到明确CAS冲突，不发生last-write-wins；
- Agent/Run分页在上限、过滤、cursor重放、wrong tenant和删除/归档并发下稳定；
- token/Secret/provider body/authority detail的默认DOM与浏览器持久化零泄漏；
- mobile宽度、键盘、screen reader和高对比度关键旅程通过；
- 原有diagnostic、Task、Artifact安全负向测试不退化。
