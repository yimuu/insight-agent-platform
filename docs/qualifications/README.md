# 开发阶段验证状态

当前项目处于开发阶段，生产级 L4～L6 不属于当前里程碑，也不阻塞功能开发。

架构重整的本地开发验收属于当时修订的历史证据，不表示当前工作树已经通过。当前提交应运行自己的 CI 与
[productization journey](../../tools/qualification/run-productization-journey.sh)，源码旅程和签名发行候选分别验收。

[`v0.2.3`](https://github.com/yimuu/insight-agent-platform/releases/tag/v0.2.3) 已作为开发预览发布，对应提交
`26eeea4460f121d527d6a4d8870f5cbcb4c022e9`。[发布工作流](https://github.com/yimuu/insight-agent-platform/actions/runs/34247796183)
通过构建与签名、Linux x86_64 预构建 starter 性能、同一签名候选的 Kind／真实 OpenSandbox L3、`all` 配置下
公共 CLI／Gateway／Console 十场景及严格 10/10 汇总，seed、旅程退出和 Kind 清理均成功。
公开发行资产已完成受信 Ed25519 签名、全部校验和及三个镜像索引的匿名读取核验；下载后的原始报告经独立重算，
完整 ReleaseBundle 和校验和文件与发行原件逐字节一致。macOS ARM CLI 下载包也已实际执行版本检查；
这不是 macOS 或 ARM 的完整运行时资格。稳定版 latest 仍为 `v0.1.0`。

本次 starter 冷启动为 57.90 秒、热启动为 6.42 秒，空闲稳定 300 秒后按既定方法测得 CPU 为单核的 8.47%，均通过既定开发门禁。
十场景来自同一次 `all` 配置运行，不表示每种最小 feature closure 都已分别实跑；其中模型响应和审批提交来自验收夹具，
不能作为外部真实模型调用或人工确认的证据。[标签 CI](https://github.com/yimuu/insight-agent-platform/actions/runs/34247795880)
首次运行的 Console 在 20 秒内未取得浏览器 endpoint 文件，清理成功但根因未确认；同一提交、参数和预算的单次重跑
通过 Console、编译器资源验证和最终 CI 汇总。该通过记录不应表述为首次超时原因已修复。

提交 `62deeec6` 的 [starter+model 源码旅程](https://github.com/yimuu/insight-agent-platform/actions/runs/34170055780)
通过六个场景，公共 CLI、真实 Gateway 和 Console 及退出清理均完成；报告独立校验为 partial，不能替代完整 `all` 资格。
模型响应和审批输入来自验收夹具，不是外部真实模型调用或人工确认的证据。
同一修订的[完整源码旅程](https://github.com/yimuu/insight-agent-platform/actions/runs/34169971017)完成真实 OpenSandbox L3、公共 CLI／Gateway／Console 十个场景与严格 10/10 汇总，下载后的报告也通过独立重算。
该工作流最终在 `stop` 读取已退出进程身份时清理失败；功能报告通过不等于整个工作流通过。Linux 退出身份读取已修复并通过真实进程回归，后续完整签名候选的退出清理结果见上面的 `v0.2.3` 记录。
[首次 `0.2.0` 候选](https://github.com/yimuu/insight-agent-platform/actions/runs/34175910582)完成四平台 CLI 构建，但 runtime 镜像构建超过既有预算后取消；签名候选与发行均未生成。
[`0.2.1` 候选](https://github.com/yimuu/insight-agent-platform/actions/runs/34189643497)完成构建与签名，但 Kind 的 OCI descriptor 读取和开发环境空闲 CPU 门禁失败，未发布。
[`0.2.2` 候选](https://github.com/yimuu/insight-agent-platform/actions/runs/34232707897)通过构建、签名和预构建开发环境性能验证，Kind 在 Outbox 容器创建时失败，完整产品旅程与发布未执行。已用该候选的真实镜像复现 containerd 临时引用规范化后的查询失败，并在新隔离集群验证导入命名修复；`v0.2.3` 随后完成了完整签名候选资格验证。
源码验收不能替代签名候选、性能、匿名镜像读取与实际发行资产核验。

- 仓库合同、单元测试、真实 PostgreSQL 与进程恢复验证按当前提交重新运行，具体证据来自对应的 CI 或 owning qualification harness；
- 2026-09-04 的 OpenSandbox/Kubernetes L1～L3 与单台 macOS 宿主三节点 Kind L4 mechanics 属于当时 revision 的历史开发证据，不转授后续架构或 exact release 资格；
- Kind 集群和 `/private/tmp` 原始证据已按要求删除，该结果只保留为 Git 历史中的开发验证记录；
- 生产级多故障域、容量、24 小时 soak、restore 和 GitOps promotion 当前均不执行，状态保持 `Not run`；
- 项目不声明 production-ready、生产容量或 SLO。

日常门禁以格式、编译、Clippy、单元/集成测试和静态部署合同为主。部署机制发生重大变化时，可按需重建
Kind 环境执行本机检查。只有进入生产准备里程碑时，才恢复完整 L4～L6 资格运行。

产品化 10/10 是独立的精确版本发布门禁，不等同于生产级 L4～L6 声明。普通 push 和 pull request 只运行
Productization 静态合同、脚本测试和资格 harness 编译，不重复构建完整 Kind 产品旅程环境。
统一安装另有独立门禁：涉及运行时或 Console 的变更会构建实际镜像，运行共享证书生产器的 TLS 检查、
完整 Compose 安装及本地 Kind/Helm 的启动、验证和恢复检查。安装门禁不调用外部模型，也不代替
OpenSandbox、十场景产品旅程或人工审批证据；当前工作树新增的 CI 配置仍需由提交后的实际工作流验证。
每周定时或手动触发的
`source` workflow 在 `all` closure 下创建一次性 Kind，并从目标提交构建 Platform、Sandbox runner 和资格测试
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
