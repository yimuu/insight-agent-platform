# ADR-0017：持久会话、实时调试与保留数据升级

状态：Accepted（设计裁决；实现与资格化状态以 current 文档和测试为准）
日期：2026-09-12

## 决策与评审

用户明确要求 PostgreSQL 持久会话、刷新后继续、真实流式输出，并在确认现有规则与保留数据
冲突后授权新增表和执行升级。会话、流式、安装三项独立评审已完成；本文接受修订后的边界。
首版限制和 DTO 由 owning Rust 类型及机器契约定义，本文不复制字段或参数清单。

提交前依赖交叉评审确认 Gateway 可直接消费 models domain 的实时文本 owning 类型与 subject 规则。
PostgreSQL 集成资格测试可仅以 dev 依赖组合 API、Gateway、Model Worker 与 axum，验证真实
PG/NATS/HTTP 链路及升级后的领取行为；这些依赖不得进入存储 crate 的 normal/build 图。
边界检查保留精确角色和依赖种类白名单，并以 normal/build 及混合依赖负例验证隔离。

会话为工作空间业务 aggregate。created_by 仅审计，读取、内容披露和执行沿用现有租户权限；
不宣称创建者私有。轮次引用 Run，正文复用 RunValue，不复制执行状态。末轮终态决定能否
发送下一轮；会话与轮次、Run admission 和 Receipt 在同一事务提交。并发通过会话版本和锁
裁决；不确定响应重放相同 Receipt，不重跑模型。

会话固定 exact Agent 部署。active head 改变后仍可读取历史，但拒绝继续并引导新会话。
首版只接收经过明确 chat-compatible schema 校验的文本输入与 answer 输出，不随意把任意
JSON 当作聊天内容。成功历史以 Run/Value 身份、摘要和固定高水位引用，服务端重新授权，
以明确 User/Assistant 角色组装；历史不能升级为系统指令。总历史与单轮请求均有界。
会话引用参与 Run retirement 判断，并由外键兜底；不能靠可手动释放的 history hold 保证保留。

实时文本是有损观察，不是持久状态。新增独立 bounded SSE，保留 durable events 分页语义。
Gateway 从 PG 验证 Run/ModelTurn/Job 当前关联、分类、权限、attempt/fence 和期限，并在空闲
期间复查。Worker 在丢弃前分配每 attempt 的 text sequence；包含非文本帧的 transport sequence
不能用于判断文本丢失。不同模型轮次/attempt 不混合；晚订阅、重连和背压明确标示缺口。
最终正文仍取已提交的受权结果。NATS 和 Console 不获得业务状态 authority。

## 修改 ADR-0009 的升级限制

本决策取代 ADR-0009 对任何保留数据升级的全面禁止；保留唯一当前 schema 和 serving 无 DDL
约束。独立部署升级命令可对精确验证的已知源版本执行有界、事务化的升级。不能自动清库，
不能忽略未知 schema 或把历史 reader 带入业务运行路径。

bootstrap input 与 installation identity 保持不变，因为 provider 身份、凭据和存储标签引用它们。
新增独立安装 release 记录，绑定原安装身份、精确源/目标 schema inventory 与二进制 package。
升级命令停止 serving、验证源状态、事务提交目标 DDL/版本并完整校验，再原子发布 release；
崩溃恢复只接受同一升级意图及精确已提交目标。未完成升级不得启动新版 serving。不得删除
prepared input 或改写私有身份来绕过 package 检查。新角色配置按精确升级差异更新。

同一 schema 的后续补丁由显式 package rollout 完成。ReleaseV1 始终表示 bootstrap 到当前
发布的绑定；版本化 rollout intent 单独绑定完整上一 release、其 canonical digest 与目标
release，且仅允许目标 package 改变。部署专属 PG receipt 对完整上一记录执行 CAS，目标
inventory 必须仍与当前定义精确相同，不执行 DDL 或业务变更。intent 和逐文件更新计划按目标
release digest 固定，未知文件改动拒绝；输出验证完成后才 CAS 替换最终 release。PG 已提交或
文件部分完成时，仅相同 intent 可重试。后续发布必须基于当前完成 release，不能任意覆盖。

## 证据要求

会话并发与重放、两轮真实上下文、授权变化、retirement、增量首段早于终态、旧 fence/丢包/
重连/慢消费、重启及升级中断恢复必须分别验证。现有 my-platform 只有在隔离测试通过后才能
执行明确升级；不得用浏览器文本动画、HTTP 200 或单纯编译通过证明完整功能。
