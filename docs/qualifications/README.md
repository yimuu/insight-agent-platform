# 开发阶段验证状态

当前项目处于开发阶段，生产级 L4～L6 不属于当前里程碑，也不阻塞功能开发。

- 仓库合同、单元测试、真实 PostgreSQL 和 OpenSandbox/Kubernetes 进程级 L1～L3 已验证；
- 2026-09-04 在单台 macOS 宿主的三节点 Kind 中完成本机 L4 mechanics，12/12 动态检查通过；
- Kind 集群和 `/private/tmp` 原始证据已按要求删除，该结果只保留为 Git 历史中的开发验证记录；
- 生产级多故障域、容量、24 小时 soak、restore 和 GitOps promotion 当前均不执行，状态保持 `Not run`；
- 项目不声明 production-ready、生产容量或 SLO。

日常门禁以格式、编译、Clippy、单元/集成测试和静态部署合同为主。部署机制发生重大变化时，可按需重建
Kind 环境执行本机检查。只有进入生产准备里程碑时，才恢复完整 L4～L6 资格运行。

产品化 10/10 是独立的精确版本发布门禁，不等同于生产级 L4～L6 声明。自动 Productization workflow
在 `all` closure 下创建一次性 Kind。普通 CI 的 `source` 模式从当前提交构建 Platform、Sandbox runner 和资格测试
Package；tag release 则必须在 `signed-release-candidate` 模式下载 `assemble-release` 产生的同一签名候选，验证 bundle、
commit-scoped OCI index、当前宿主 child manifest 与镜像签名，并用候选 CLI 的离线 release cache 启动候选 runtime。
独立的候选 Sandbox runner child 同时作为 Dispatcher 全链路 readiness 镜像；Sandbox Package 只作为资格测试载荷
构建在该 runner child 上。Console 浏览器 journey 直接服务候选 tar，且逐文件
核对它与候选 Console 镜像 `/console` 的内容一致。两种模式都安装仓库内 OpenSandbox chart、运行 fail-closed L3 qualifier，
再由同一提交的公共 CLI/HTTP/Console journey 消费该物理证据；release 模式不存在源码产物回退。
场景清单和单场景报告中的 `profile` 表示该场景的最小必需 feature closure；`all` 是实际执行的超集。只有十份
报告均为 Passed、版本完全一致并生成严格 aggregate 后，tag release 的 publish job 才能继续。starter 或单 feature
运行只产生显式 partial reports，不能生成 aggregate，也不能替代 Sandbox 物理证据。
这些 JSON 不是独立的第三方证明；证据信任根是固定版本的 Productization workflow 执行与最终发布的签名 attestation。

机器可读的生产资格要求仍由
[`QualificationProfile`](../../contracts/platform-v1/qualification/production-release-profile.json) 保存；它是未来发布门禁，
不是当前通过报告。
