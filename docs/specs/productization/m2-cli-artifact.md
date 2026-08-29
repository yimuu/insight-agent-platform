# M2 CLI Artifact lifecycle evidence

| 属性 | 值 |
|---|---|
| 状态 | Implemented initial lifecycle / M2 In Progress |
| public authority | Runtime Gateway `/v1/artifacts*` |
| 当前命令 | `artifact upload|get|read` |

## 1. 命令面

```text
insight artifact upload --file <file> --purpose <purpose> --classification <classification>
  [--media-type <type>] [--display-name <name>] [--timeout-seconds <1..3600>] [--path <project>]
insight artifact get <artifact_id> [--path <project>]
insight artifact read <artifact_id> --output <file> [--path <project>]
```

`upload` 对 regular file 进行有界流式 SHA-256，使用 exact size/digest 构造 closed prepare request；prepare 返回的 URL
和 completion proof 均为 secret-bearing，仅保存在项目本地 `.insight/artifact-upload/` 的 `0600` crash-safe journal，
不进入 stdout、report、日志或 Git。对象 PUT 使用独立
no-proxy/no-redirect HTTPS client，且不设置 Runtime OIDC Authorization；只接受 exact `200 OK`。随后 CLI 以 prepare
Artifact ETag 完成 complete-upload，等待同一 ArtifactVerify Operation terminal，并读取 Ready metadata 重验 purpose、
classification、length、media type 和 digest。

upload journal 在任何 mutation/effect 前固定 canonical prepare request、request digest、prepare/complete Receipt、If-Match
和 authority identity，并按 `prepared -> object_uploaded -> completed -> result` 单调持久化。prepare 响应丢失由同一
Receipt 恢复；complete 响应丢失只重放同一 complete，不再次执行已经成功的 object PUT；完成后再次运行只读取同一
Artifact authority。尚未 PUT 且 target 临近过期时，CLI 只允许用原 prepare Receipt 刷新同一
Artifact/Operation/Grant/generation，任一 identity 或 ETag 改变都 fail closed，绝不创建第二个 candidate 来隐藏恢复失败。

`get` 输出 closed `ArtifactViewV1`。`read` 先从同一 Runtime Gateway 读取 metadata，只允许 Ready 且具有 exact
`ArtifactRef` 的内容，再经 `/v1/artifacts/{artifact_id}/content` 流式下载。CLI 不读取 object locator、不获取 S3/KMS
凭据、不直连数据库或 Artifact internal RPC。

## 2. 完整性与文件安全

- metadata 校验 Artifact ID、state/content closure、classification、expected length、verified media type、version、
  body/header ETag 与时间顺序；
- content 要求 `attachment`、private no-store、strong ETag、bounded Content-Length 和 explicit Content-Type；
- 下载过程中增量计算 SHA-256；只有 length、media type、digest 与 `"<content_digest>"` ETag 全部匹配
  `ArtifactRef` 才可发布文件；
- 内容先写同目录 `0600` 临时文件并 `sync_all`，再用 no-clobber hard-link 原子发布；目标已存在时拒绝覆盖，失败
  时清理临时文件；
- machine-readable `insight.platform.artifact-download-report/v1` 只包含安全 metadata、输出路径和 trace ID。

## 3. 当前证据与剩余门禁

loopback HTTP fixture 覆盖 prepare -> isolated uploader -> complete -> Operation wait -> Ready metadata，并断言
Authorization、Receipt、If-Match、Location、trace、exact Operation target 和最终 content closure；安全负向断言最终 report
不含签名 URL、completion proof 或 token。另一 fixture 覆盖 metadata -> content、exact digest 和最终文件。命令 parser
覆盖 upload/get/read 的必需参数。response-loss fixture 在 Artifact authority 已接受 complete 后主动断开连接；第二次
调用复用同一 Receipt/If-Match 和 Artifact identity，object PUT 计数保持一次，成功后的第三次调用只读 Ready authority
并返回相同报告。

fresh deterministic first Run P2 journey 已通过真实 Artifact Gateway、Artifact Data Worker 与 HTTPS S3/KMS dependency
上传 scheduler 所需 typed Plan、authoring 与 qualification Artifact；对 typed Plan 又通过公开 CLI 显式等待同一
ArtifactVerify Operation、读取 Ready metadata、受控下载并逐字节校验 canonical 内容。重复下载到同一路径必须失败且
原文件保持不变，证明真实 public download 主路径与 no-clobber 边界。随后该 Artifact 被 scheduler 读取并完成 Agent
publication 与 Run。该证据覆盖真实 upload/download 主路径，但不替代下列故障矩阵。

以下仍未完成：

- 真实 HTTPS object PUT 的 redirect/proxy/token 泄露、非 200、TLS、expiry 和 digest mismatch 负向 fixture；
- 409/412/429、quarantined/rejected/deleted、truncated/oversized stream。

因此本文件只声明初始 Artifact lifecycle，不声明 M2 或 spec00～18 已完成。
