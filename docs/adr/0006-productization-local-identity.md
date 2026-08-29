# ADR-0006：本地开发身份使用显式受限 OIDC 闭环

| 属性 | 值 |
|---|---|
| 状态 | Accepted |
| 日期 | 2026-08-29 |
| 影响阶段 | Productization M1～M4 |

## 决策

`base`/`full` profile 使用专门的 local development OIDC issuer、固定 JWKS digest、短期 RS256 access token 和
显式 bootstrap 的 tenant/principal binding。开发 token 的 audience、issuer、tenant、subject、principal kind、
issue/expiry 必须继续由 Gateway 已有 verifier 验证；不得开启 unauthenticated mode、test-only identity header 或
InstallationOperator 作为普通用户。

`insight init` 只把 local issuer key material、token cache、database password 和 generated config 写入 project-local
gitignored state，并设置 restrictive permissions。它不得将 value 写进 request manifest、日志、scenario report、
Git 或 container image。`doctor` 必须显示 issuer/JWKS/config digest 和到期状态，但不显示 key/token value。

生产 OIDC、Secret、KMS、SPIFFE/mTLS 和 GitOps 不复用 local development material；profile name、environment class
和 config digest 必须阻止它们混用。

## 后果

本地 first Run 可真实经过 public authentication/principal binding，而不用牺牲生产边界。M1 必须补齐 local issuer
container、bootstrap 顺序、短 token rotate 和泄漏负向测试；在此之前不能声称 local profile 已经实现。

## 当前实现边界

仓库提供 `platform-dev-bootstrap` 作为一次性的开发数据库引导工具。它只接受绝对路径的、严格解析且
digest 固定的 development JSON config，并在已 provision 和通过 schema verification 的 PostgreSQL authority
中创建 installation operator、development tenant 及最小 developer bindings。它拒绝 production environment
class、installation/developer principal 复用、非 canonical config 和未提供的显式环境变量；服务进程仍不执行
DDL，`insight` CLI 也不直接连接数据库。

该工具尚未由 `insight init`/`insight dev` 编排，也尚未提供 local issuer 或 token rotation；因此这不是已完成的
local profile，M1 仍保持 In Progress。
