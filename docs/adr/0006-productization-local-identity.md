# ADR-0006：本地开发身份使用显式受限 OIDC 闭环

| 属性 | 值 |
|---|---|
| 状态 | Accepted |
| 日期 | 2026-08-29 |
| 影响阶段 | Productization M1～M4 |

## 决策

`base`/`full` profile 使用项目级 local development OIDC issuer material、固定 JWKS digest、短期 RS256 access
token 和显式 bootstrap 的 tenant/principal binding。Gateway 已经安装静态、digest 固定的 JWKS，因此 local
profile 不另起一个常驻认证旁路 role；CLI 在 project-local state 中生成和轮换开发 issuer material。开发 token 的
audience、issuer、tenant、subject、principal kind、issue/expiry 必须继续由 Gateway 已有 verifier 验证；不得开启
unauthenticated mode、test-only identity header 或 InstallationOperator 作为普通用户。

`insight init` 只把 local issuer key material、token cache、database password 和 generated config 写入 project-local
gitignored state，并设置 restrictive permissions。它不得将 value 写进 request manifest、日志、scenario report、
Git 或 container image。`doctor` 必须显示 issuer/JWKS/config digest 和到期状态，但不显示 key/token value。

生产 OIDC、Secret、KMS、SPIFFE/mTLS 和 GitOps 不复用 local development material；profile name、environment class
和 config digest 必须阻止它们混用。

## 后果

本地 first Run 可真实经过 public authentication/principal binding，而不用牺牲生产边界。M1 必须补齐 profile config
生成、bootstrap 编排、短 token rotate 和泄漏负向测试；在此之前不能声称 local profile 已经实现。

## 当前实现边界

`insight init` 现生成 gitignored project-local RS256 private key、JWKS、development bootstrap config 和 15 分钟
developer token。`insight token` 每次生成新的 `jti` 并原子替换缓存 token，只向 stdout 输出 token；project state、
JWKS 与 bootstrap config 不含 private key 或 token。自动测试把生成的 token 交给现有 Gateway OIDC verifier，
证明 audience、issuer、tenant、principal kind 和 digest identity 仍走同一验证合同。

仓库同时提供 `platform-dev-bootstrap` 作为一次性的开发数据库引导工具。它只接受绝对路径的、严格解析且
digest 固定的 development JSON config，并在已 provision 和通过 schema verification 的 PostgreSQL authority
中创建 installation operator、development tenant 及最小 developer bindings。它拒绝 production environment
class、installation/developer principal 复用、非 canonical config 和未提供的显式环境变量；服务进程仍不执行
DDL，`insight` CLI 也不直接连接数据库。

bootstrap 工具尚未由 `insight dev` 编排，且 profile role closure、readiness、restart 和泄漏负向 smoke 仍未交付；
因此这不是已完成的 local profile，M1 仍保持 In Progress。
