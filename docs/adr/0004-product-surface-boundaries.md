# ADR-0004：产品入口共享单一 authority 边界

| 属性 | 值 |
|---|---|
| 状态 | Accepted |
| 日期 | 2026-08-31（2026-09-04 汇总修订） |
| 取代 | ADR-0003、ADR-0005、ADR-0006 |
| 影响范围 | CLI、HTTP authoring、Console、local development identity profile |

## 背景

平台需要命令行、浏览器和原始 HTTP 三种产品入口，但不能因为使用体验而引入第二套业务
authority、DSL、身份旁路或可变发行路径。开发环境也必须经过真实公开合同与身份校验，同时明确保持 non-production。

## 决策

### CLI 与本地 supervisor

交付单一 `insight` binary。它是 public `/v1` 客户端和本地多进程 supervisor，不是
Gateway、Worker、repository 或 Sandbox；所有业务 mutation 只能经 public `/v1`，不直连
数据库、internal RPC，也不生成特权身份 header。

本地 profile 默认为 `starter`，并只允许机器合同中的 closed、additive features 及其
canonical `all` 并集。每个 feature 只追加已有 role、identity、config、dependency 和 exact image；
unknown、duplicate、非法组合或隐式移除必须在 pull、build、provision 和启动前 fail closed。

`starter` 包含首次真实 Run 所需的最小多进程闭包，包括 Runtime/Orchestration 强制依赖的
Artifact 路径。Sandbox feature 仍使用受审查的 OpenSandbox/Kubernetes 执行路径，
不允许 host execution、Docker provider 或向 Platform/Sandbox Pod 挂载 Docker/CRI socket。

默认运行路径消费签名的同版本 `ReleaseBundle`、exact binary 和 image digest。仅贡献者显式
`--from-source` 时才从 checkout 构建；离线模式只能复用已验证的 exact cache，绝不回退
mutable tag、隐式下载或源码构建。

`init` 只创建 project-local、gitignored 的 non-production 身份与配置。fresh schema provision 是
可见的 one-shot transaction；运行时 role 只验证 schema 且不拥有 DDL 权限。签名产物、
schema 和 profile digest 未变时可复用已验证本地资产。`reset` 必须先显示精确范围并
要求 project name 确认；普通启动失败不得删除 project-local authority。

### Authoring 与 advanced 自动化

[`contracts/platform-v1/openapi.yaml`](../../contracts/platform-v1/openapi.yaml) 与 owning schema/type 是唯一 authoring wire contract。closed
`agent.yaml` 由无网络 shared compiler 确定性生成同一 public lifecycle 请求；它不是第二
runtime、第二 DSL 或服务端 authority。

advanced `apply` manifest 只编排公开 Resource、Draft、Version、Deployment 与 Operation 请求，
不得定义 Plan/Node/Provider 语义，也不得接受自由 URL、shell、数据库连接、Secret value、
internal RPC 或跳过 validation/activation 的选项。CLI 可以管理 Receipt、ETag、trace、Operation 和
cursor，但不得吞掉冲突、伪造恢复状态或以新随机值重做不确定 effect。

### Console

Console 是不可变的静态单页应用，只访问同源 `/readyz` 和 public `/v1`。它不引入 SSR、
BFF、Console database、worker credential 或 internal RPC client，也不以 React state、浏览器存储
或 Event 重建 current state。

Console 使用与 CLI 同一 compiler conformance corpus、public bounded list 和 opaque SSE cursor。
所有 mutation 保留 Receipt/ETag 语义；默认页面隐藏 authority 细节只是展示投影，
不改变服务端 permission 或持久化事实。长期 Secret、token、credential 和正文不得进入
持久化浏览器状态。

### 本地身份

本地开发使用 project-local issuer material、digest-bound JWKS、短期 RS256 access token 和显式
tenant/principal bootstrap。token 必须通过 Gateway 现有 OIDC verifier 的 issuer、audience、tenant、
subject、principal kind、issued-at 与 expiry 校验；不设 unauthenticated mode、test identity header
或常驻认证旁路。

私钥、token cache、database password 与 generated config 只能位于 restrictive-permission、gitignored
project state，不得出现在 manifest、日志、报告、Git 或 image 中。诊断只显示非敏感 digest
与到期状态。生产 OIDC、Secret、KMS、workload identity 和 GitOps 绝不复用 local material；
environment class 与 config digest 必须阻止混用。

## 后果与权威来源

产品入口可以简化展示和编排，但 PostgreSQL 与现有 Platform role 仍持有唯一业务事实。
精确字段、命令、profile closure、限制、线上路由与当前验证状态以 owning contract/type、
migration、[`docs/current`](../current/README.md) 和可执行测试为准，不在本 ADR 维护平行 registry。

本 ADR 取代 ADR-0003、ADR-0005 和 ADR-0006；三者的过程性清单、里程碑状态与旧 profile 命名不再是当前规范，历史保留在 Git 中。
