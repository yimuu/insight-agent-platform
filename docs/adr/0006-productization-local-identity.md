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
