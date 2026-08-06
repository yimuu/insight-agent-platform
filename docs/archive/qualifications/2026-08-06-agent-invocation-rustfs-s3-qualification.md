# AgentInvocation、Conversation 与 RustFS/S3 资格验收

日期：2026-08-06 CST

状态：Archived / Qualified

对应规范：[Agent 调用、Conversation 与 S3 文件合同规范](../specs/2026-08-05-agent-invocation-conversation-and-s3-files.md)

## 结论

AgentInvocation、Conversation、File Service、LLM 图片附件与 S3-only Artifact 合同已经完成实现和
真实 RustFS 资格验收。公开调用面固定为 `query/messages/files/inputs`；Conversation 历史由平台托管；
公开 File 只暴露 `file_id`；对象身份、预签名 capability、引用与 GC 均由平台持有。

官方 RustFS `1.0.0-beta.12` 上的闭合合同和同数据目录重启验证均以退出码 `0` 完成。测试覆盖完整
File Service 创建、直传、complete、读取、公开 tombstone 与 GC，不只验证底层 SDK 能否发出 S3 请求。

## 验收环境

| 项目 | 版本/事实 |
|---|---|
| 主机 | macOS 26.5.2（25F84），Apple ARM64 |
| Rust | `rustc 1.94.1 (e408947bf 2026-03-25)` |
| Cargo | `cargo 1.94.1 (29ea6fb6a 2026-03-24)` |
| RustFS | `1.0.0-beta.12`，commit `2e5cef513fb31e25940f1c77e208f28b302561bb` |
| RustFS 归档 | 官方 macOS ARM64 zip；SHA-256 `f5266eda245fa4dab5acf28bef7bbab6c1da7f3e9575ddc7db803894107e09f5`，与发行版 `SHA256SUMS` 一致 |
| S3 client | `aws-sdk-s3 1.140.0`；默认 HTTPS client；workspace rustls 仅启用 AWS-LC provider |
| 部署形态 | 只监听 `127.0.0.1:19000` 的隔离单盘 RustFS；专用 bucket 与临时数据目录 |

资格环境使用一次性测试凭据；本记录不保存凭据、预签名 URL、对象 key 或文件正文。

## 闭合 S3 与 File Service 证据

以下命令在真实 RustFS 上通过：

```bash
RUSTFS_CONTRACT_ENDPOINT='http://127.0.0.1:19000' \
RUSTFS_CONTRACT_PUBLIC_ENDPOINT='http://127.0.0.1:19000' \
RUSTFS_CONTRACT_BUCKET='insight-agent-platform-contract' \
RUSTFS_CONTRACT_ACCESS_KEY='...' \
RUSTFS_CONTRACT_SECRET_KEY='...' \
RUSTFS_CONTRACT_PRESIGN_TTL_SECONDS=1 \
RUSTFS_CONTRACT_CREATE_BUCKET=1 \
cargo test --locked --test rustfs_s3_contract \
  rustfs_supports_the_closed_file_service_s3_contract -- --ignored --nocapture
```

实际验证：

- `HeadBucket` readiness；
- 带冻结 content length、content type、SHA-256 checksum 与 `If-None-Match: *` 的预签名 PUT；
- 重复覆盖和错误 checksum 均被拒绝，失败上传不产生可读取对象；
- HEAD 返回大小、媒体类型、checksum、ETag 与 version identity；
- 完整 GET、bounded Range GET 和短期预签名 GET；
- 错误 ETag 条件删除失败，正确冻结 identity 删除成功；
- 1 秒 TTL 的预签名 PUT 到期后被拒绝；
- File metadata create、幂等 replay、预签名上传、complete/HEAD、principal-scoped GET、resolve、
  download capability、公开 DELETE/tombstone、durable GC claim/conditional delete/ack 和最终 `deleted` 状态。

## 重启持久性证据

同一个 RustFS 二进制、数据目录和 bucket 依次执行 `seed`，完整停止服务，重新启动，再执行 `verify`。
两个阶段均通过：

```bash
RUSTFS_CONTRACT_RESTART_PHASE=seed \
RUSTFS_CONTRACT_RESTART_STATE_FILE='/secure/tmp/rustfs-restart-state.json' \
cargo test --locked --test rustfs_s3_contract \
  rustfs_preserves_object_identity_across_restart -- --ignored --nocapture

# 停止 RustFS，并以同一数据目录重新启动

RUSTFS_CONTRACT_RESTART_PHASE=verify \
RUSTFS_CONTRACT_RESTART_STATE_FILE='/secure/tmp/rustfs-restart-state.json' \
cargo test --locked --test rustfs_s3_contract \
  rustfs_preserves_object_identity_across_restart -- --ignored --nocapture
```

verify 逐项比较 ETag、version、SHA-256、长度和完整内容，随后按冻结 identity 删除探针，并删除状态文件。

## 实现完成证据

最终源代码在禁用 incremental、启用完整 workspace targets 的独立构建目录中通过：

```bash
CARGO_TARGET_DIR=target/public-api-baseline \
CARGO_INCREMENTAL=0 \
cargo test --workspace --locked -j 4
```

命令退出码为 `0`；单元、集成、二进制冒烟和文档测试均无失败。默认测试运行中两项真实 RustFS
测试按设计显示为 `ignored`，其显式执行与重启验证结果记录在上文，不能由默认跳过替代。

| 合同 | 结果 | 主要证据 |
|---|---|---|
| 调用信封与 discovery | Passed | checked-in schema/samples、strict deserialization、默认值物化、保留输入名与旧 alias 拒绝测试 |
| Run 与 Conversation | Passed | 无会话历史、托管历史、幂等 replay、terminal/full 原子事务、summary、archive/privacy delete 测试 |
| File 生命周期 | Passed | SQLite/PostgreSQL repository contract、File API、complete、绑定、retention、GC fence 与真实 RustFS 测试 |
| 历史文件不可变绑定 | Passed | 公开 File tombstone 后 Conversation 历史仍通过冻结 binding 重放；显式重新附加被拒绝 |
| 多模态 | Passed | 多图片顺序、inline/presigned、provider capability、媒体欺骗、超限、像素炸弹和解码失败测试 |
| S3-only 产品合同 | Passed | 产品配置拒绝 local/shared filesystem；Helm 不再挂载 Artifact PVC；Run Artifact 与超限 Conversation 内容走统一 S3 client |
| 架构与安全 | Passed | API 不依赖 storage/SQL；storage 不依赖 HTTP client；AWS-LC-only rustls；crate boundary 与 cutover residual 门禁通过 |

## 剩余边界

- 该测试故意 `#[ignore]`，因为默认 hermetic CI 没有真实 RustFS；不得把默认跳过解释为 S3 资格通过。
- 每次 RustFS 版本、endpoint、TLS/proxy 或 S3 client 版本变化，都必须在独立 bucket 上重跑闭合合同和
  restart seed/verify。
- 本次单盘实例只证明协议和持久性合同，不构成容量、纠删码、高可用、备份或灾难恢复承诺。
- 应用运行身份仍只需要闭合 bucket 权限；`CreateBucket` 仅供一次性 qualification fixture 使用。
