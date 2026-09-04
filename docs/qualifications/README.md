# 开发阶段验证状态

当前项目处于开发阶段，生产级 L4～L6 不属于当前里程碑，也不阻塞功能开发。

- 仓库合同、单元测试、真实 PostgreSQL 和 OpenSandbox/Kubernetes 进程级 L1～L3 已验证；
- 2026-09-04 在单台 macOS 宿主的三节点 Kind 中完成本机 L4 mechanics，12/12 动态检查通过；
- Kind 集群和 `/private/tmp` 原始证据已按要求删除，该结果只保留为 Git 历史中的开发验证记录；
- 生产级多故障域、容量、24 小时 soak、restore 和 GitOps promotion 当前均不执行，状态保持 `Not run`；
- 项目不声明 production-ready、生产容量或 SLO。

日常门禁以格式、编译、Clippy、单元/集成测试和静态部署合同为主。部署机制发生重大变化时，可按需重建
Kind 环境执行本机检查。只有进入生产准备里程碑时，才恢复完整 L4～L6 资格运行。

机器可读的生产资格要求仍由
[`QualificationProfile`](../../contracts/platform-v1/qualification/production-release-profile.json) 保存；它是未来发布门禁，
不是当前通过报告。
