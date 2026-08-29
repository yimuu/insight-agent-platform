# 活动资格验收

本目录跟踪已定义、但尚未取得目标环境完整证据的资格验收工作。资格文档描述待验证的能力
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
| [Platform v2 Production L4～L6](platform-v2-production-l4-l6.md) | Not run / optional deployment release gate | 部署方如需production-ready声明，执行真实runsc拓扑、容量、恢复与GitOps clean cut门禁 |

Platform v2当前仓库实现与四阶段exit gate的逐项状态见
[spec00～18完成度审计](platform-v2-completion-audit.md)。CR-201已关闭仓库实现范围；审计保留的L4～L6项目是未执行的环境资格，
不是spec00～18或implementation plan的剩余任务。

## Platform v2 机器门禁

Platform v2 production release使用checked-in
[`QualificationProfile`](../../contracts/platform-v1/qualification/production-release-profile.json)
声明18要求的完整L1～L6 gate集合。CI通过`platform-qualification`验证profile；production-equivalent
runner完成测试后，还必须用同一工具验证exact CandidateManifest与QualificationEvidenceManifest。

Evidence manifest只能引用content digest、媒体类型和长度，不保存Secret、URL、对象key或测试正文。每个required gate
必须恰有一个`passed`或`failed`结果，并且每个结果的evidence digest必须解析到manifest内的artifact link。最终release validator还必须接收
只读artifact root，读取每个link同名的普通文件并重算长度与SHA-256；缺失、symlink或内容漂移均fail closed。
缺失、skip、错误layer、不同profile/candidate digest或任一failed gate都不能通过release evidence门禁。

checked-in profile只是资格要求，不是通过报告、CapacityProfile或promotion授权；L4～L6未实际运行前，本目录对应环境资格保持Not run，
但不影响implementation plan与spec00～18的仓库范围关闭。

当前成功candidate已在GitOps环境仓库以`built_not_promoted`记录绑定exact制品，并由独立CI验证；该记录保持
`production_ready=false`、`clean_cut_completed=false`，不创建qualified release或`production/current`，因此不能作为clean cut证据。
