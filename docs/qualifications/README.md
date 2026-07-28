# 活动资格验收

本目录只跟踪已经定义正式门槛、但尚未取得有效完整证据的资格验收工作。资格文档描述待验证的能力
声明、固定工作负载、通过条件和证据要求，不是功能设计规范，也不改变
[`docs/current`](../current/README.md) 描述的当前运行合同。

验收取得正式结果后：

- 通过：更新对应报告和当前能力表述，然后移入 `docs/archive/qualifications`；
- 失败或取消：保留失败证据和结论，同样移入归档；
- 门槛发生设计性变化：先建立新的 `docs/specs` 规范，不能直接在资格文档中引入产品语义。

## 当前验收

| 验收 | 状态 | 目标 |
|---|---|---|
| [Durable Runtime 24 小时 RC](durable-runtime-24h-rc.md) | Pending / requires always-on runner | 补齐 50 active Run 能力的 release-candidate 级 24 小时稳定性证据 |
