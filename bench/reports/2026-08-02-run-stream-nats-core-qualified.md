# Run Stream 可插拔总线与 NATS Core 资格报告

日期：2026-08-02

状态：**Qualified。实现、真实 NATS 安全/故障测试、双后端数据库 regression、30 分钟混合负载和
2 小时 soak 全部通过。**

对应规范已退出工作树，可从 Git 历史查看。

正式证据：

- [`in_memory` 数据库 regression](../results/run-stream-in-memory-db-final-20260802/report.json)；
- [`nats_core` 数据库 regression](../results/run-stream-nats-core-db-final-20260802/report.json)；
- [真实 NATS release profile](../results/run-stream-nats-core-release-final-20260802/report.json)；
- [2 小时最后 30 分钟泄漏分析](../results/run-stream-nats-core-release-final-20260802/soak-2h-leak-report.json)。

## 1. 资格结论

本轮证明了以下边界：

- 单 Runtime 默认 `in_memory`；跨 Runtime live fan-out 可显式选择 `nats_core`；
- PostgreSQL 继续承担 Run、checkpoint、terminal snapshot 与 snapshot hash 的 durable authority；
- Run Stream live body 不再使用 PostgreSQL listener 或 per-frame `pg_notify`；
- Core NATS 只提供 at-most-once live observation，不启用 JetStream、ack、replay 或 terminal 存储；
- 每个 Runtime 共享一个 NATS data connection，50 个 Attached Run 使用 50 个普通 subscription；
- NATS restart、slow client 或 subscriber 重建可以产生同步拒绝/drop/gap，但不会改变 durable Run
  终态；SSE 最终由 terminal snapshot 校准并立即 EOF；
- TLS、credentials、Account subject ACL、完整 SHA-256 Run subject key、bounded queue、control reserve、
  公平调度和闭合低基数 metrics 已完成；
- 公共 `run-stream/v1` wire 未升级版本，也没有增加 Go/TypeScript transport client。

该结论不把 NATS 提升为 durable queue，也不授权用增大 database pool 或 operation permit 代替独立容量
资格。`in_memory` 仍禁止 distributed topology。

## 2. 构建与环境身份

| 项目 | 值 |
|---|---|
| Git 基线 | `0594b4bb9675a9df203c1e7d7c8289ceec53e61e` |
| 资格镜像 | `insight-agent-platform:run-stream-nats-core-qualification` |
| 镜像 digest | `sha256:6984f8967aa2ed3ca4c16c55ea15198fcb2dd6f17ebdf3838719d6bcca5afdf1` |
| 镜像平台 | `linux/arm64` |
| Kubernetes | `v1.34.8+orb1`，`linux/arm64` |
| Rust | `rustc 1.94.1` / `cargo 1.94.1` |
| Core NATS | `nats:2.12.4-alpine` |
| NATS 管理工具 | `natsio/nats-box:0.18.0` |
| Rust client | `async-nats 0.50.0`，最小 Core/TLS feature graph |

资格镜像从该 Git commit 加当前工作树构建；因此可执行重现身份以镜像 digest 为准，commit 只表示基线。
release harness 另保存了 Cargo/NATS 版本、feature graph、server varz/connz 与原始 NDJSON。

## 3. 配置、部署与安全 Gate

Helm/static Gate 验证了默认 `single_runtime + in_memory`、显式 `distributed + nats_core`，并对以下配置
fail closed：

- `distributed + in_memory`；
- SQLite + distributed；
- NATS production profile 缺 TLS、credentials 或 Secret key；
- Secret/ConfigMap 引用或 server scheme 不一致；
- 旧 `postgres_notify`、旧字符串 backend、未知字段、queue group 或 `no_echo`。

真实安全测试使用临时 operator/account/user credentials、临时 CA/server certificate 与精确 subject ACL：

```text
publish allow:   insight.qualification.run_stream.v1.*
subscribe allow: insight.qualification.run_stream.v1.*
```

正确 CA/credential 通过；错误 CA、外部 operator credential、publisher/subscriber 越权和跨 namespace
subject 均被拒绝。凭据与 CA 只存在于私有临时目录和 Kubernetes Secret，退出时删除；正式报告确认
`raw_credentials_captured=false`。日志、metrics 和 subject 都不包含 payload 或 raw Run ID。

## 4. PostgreSQL regression：50 Attached Run

两个 profile 均使用真实 HTTP lifecycle、真实 SSE、PostgreSQL pool `max_connections=10` 和唯一临时
Kubernetes namespace。

| Gate | `in_memory` | `nats_core` | 门槛 |
|---|---:|---:|---:|
| Attached / terminal success | 50 / 50 | 50 / 50 | 100% |
| terminal 后 EOF | 50 | 50 | 50 |
| terminal 与 `GET Run` 一致 | 50 | 50 | 50 |
| canonical snapshot hash | 50 | 50 | 50 |
| peak active subscriptions | 50 | 50 | 50 |
| Runtime PostgreSQL connections | 10 | 10 | 不因 50 SSE 线性增加 |
| probe + Runtime connections | 11 | 11 | pool 10 + probe 1 |
| active PostgreSQL connections | 1 | 1 | 无长期占用 |
| legacy Run Stream listener | 0 | 0 | 0 |
| legacy Run Stream source matches | 0 | 0 | 0 |
| NATS data connections at peak | N/A | 1 | 每 Runtime 1 |
| 50 concurrent waiting `GET Run` p95 | 22.718ms | 25.936ms | ≤100ms |

每个并发 `GET Run` 都经过 Runtime 的有界 PostgreSQL pool，因此端到端 p95 是 pool-acquire p95 的保守
上界；两种后端均低于既有 100ms gate。profile 中 `pg_stat_statements` 各观察到 1 次 body-free
scheduler/public-event wake hint，它不按 frame 执行，也不承载 Run Stream body；Run Stream-specific
listener、query 和源码匹配均为 0。

## 5. 真实 NATS 协议与故障 Gate

真实 `nats-server` integration 覆盖：

- Runtime A publish、Runtime B subscribe，以及两个普通 subscriber 都收到同一消息；
- shared connection 上的 50 个 dynamic subscription；
- SUB 后 flush-before-admission barrier；
- self-echo，且不使用 queue group；
- NATS stop/restart、client reconnect、active subscription 恢复和断线期不 replay；
- producer/inbound bounded queue、global message/byte bound、control reserve、公平调度；
- oversize、malformed wire、wrong namespace/run/schema、out-of-order、late publisher、seal replay/conflict；
- slow subscriber、shutdown drain timeout、gap/seal/terminal barrier；
- TLS server verification、credential 与 subject ACL negative tests。

Core-only dependency Gate 检查 `async-nats` feature graph，不允许 JetStream、service、KV 或 object-store
feature。

## 6. Release workload 与 2 小时 soak

release profile 顺序执行 20 轮 × 50 Run 短 burst、30 分钟混合负载和 2 小时 soak；所有子 profile
均要求 50/50 seal 收敛才可通过。

| Profile | 结果 | Publication | Gap | Seal | 故障注入 |
|---|---:|---:|---:|---:|---|
| 20 × 50 burst | Passed | 每轮完成 | 每轮收敛 | 每轮 50/50 | 每轮 NATS restart、subscriber restart、slow client |
| 30 分钟 mixed | Passed | 353,600 | 52 | 50/50 | NATS restart、subscriber restart、slow client/overflow |
| 2 小时 soak | Passed | 358,520 | 50 | 50/50 | NATS restart、subscriber restart、slow client |

30 分钟档的外部 restart 窗口产生 50 次同步 `RunClosed`，符合 Core NATS at-most-once 合同；后续 gap
和 terminal seal 显式校准，durable 结果不受影响。2 小时档的 restart 短于 1 秒发布周期，因此没有
断线期 publication；subscriber 重建产生 50 个显式 gap。

2 小时档共保存 240 个 30 秒资源样本。自动分析取最后 30 分钟 60 个样本，并比较前后各 5 个中位数：

| 指标 | 窗口起点中位数 | 窗口终点中位数 | 判定 |
|---|---:|---:|---|
| RSS | 21,872,640 bytes | 21,872,640 bytes | 无增长 |
| publisher tasks | 52 | 52 | 无增长 |
| subscriber tasks | 52 | 52 | 无增长 |
| pending messages | 1 | 0 | 已排空 |
| pending bytes | 366 | 0 | 已排空 |
| final active subscriptions | 50 | 50 | 最后 5 个样本全部 50 |

NATS 活跃 client connection 在 workload 前后均为 0；workload 关闭时没有遗留 connection。报告最终
`passed=true`、`last_30m_leak_window_verified=true`、`tls_credentials_acl=true`。

## 7. 静态与测试验证

以下最终实现链已通过：

- `cargo test --workspace --locked`；
- `cargo test -p insight-runtime --lib --locked`（90/90）；
- `cargo check -p insight-runtime --all-targets --locked`；
- `cargo fmt --all -- --check`；
- `bash scripts/check-crate-boundaries.sh`（9 packages / 379 dependencies）；
- `bash scripts/check-public-api-baseline.sh`（5 audited bridges）；
- `bash bench/run-stream-nats-core/helm-static.sh`；
- `git diff --check`。

公共 `run-stream/v1` schema/baseline 未发生协议版本升级。Run Stream wire 已移入 runtime private transport；
PostgreSQL broker module、配置与 re-export 已删除，scheduler/public-event notification 回归测试保留并通过。

## 8. 运维结论

- 当前单 Runtime 保持默认 `in_memory`，不要求部署 NATS；
- 只有 Worker/SSE 可能跨 Runtime 时才选择 `nats_core`，并使用同一 namespace、TLS、credentials 与 ACL；
- NATS outage 时 liveness 保持，readiness 和新 Attached admission fail closed；durable Worker/terminal commit
  继续；
- dashboard 同时观察 ready、connected connection、subscription/task、分级 pending、reconnect/drop/gap、
  decode/slow-consumer 和 terminal barrier；
- 不通过增大 PostgreSQL pool 修复 NATS/slow-SSE 背压，也不把 Core NATS 当 durable authority。

上述边界已经同步到 `docs/current`，规范完成定义 1～18 全部关闭。
