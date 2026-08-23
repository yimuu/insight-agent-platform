# Platform v2 运行手册

本目录保存 Platform v2 production qualification 与 clean cut 使用的操作手册。手册定义执行顺序、停止条件、
权威检查和证据边界；它们不是资格通过报告，也不改变 `docs/current` 描述的当前产品行为。

- [依赖故障、恢复与轮换](platform-v2-dependency-recovery.md)
- [GitOps clean cut、监视与回滚](platform-v2-clean-cut.md)

所有命令都必须在本次 CandidateManifest 指定的 production-equivalent 环境执行。云厂商或 GitOps 产品专用命令由
环境仓库维护，执行时以内容寻址附件纳入证据；不得把 credential、Secret value、内部 URL、对象 key 或完整日志
复制到 qualification manifest。
