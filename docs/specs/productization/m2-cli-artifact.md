# M2 CLI Artifact read evidence

| 属性 | 值 |
|---|---|
| 状态 | Implemented read subset / M2 In Progress |
| public authority | Runtime Gateway `/v1/artifacts*` |
| 当前命令 | `artifact get|read` |

## 1. 命令面

```text
insight artifact get <artifact_id> [--path <project>]
insight artifact read <artifact_id> --output <file> [--path <project>]
```

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

loopback HTTP fixture 覆盖 metadata -> content，并断言 Runtime Authorization、closed envelope、exact digest 和最终文件；
命令 parser 覆盖 get/read 与必需 output。

以下仍未完成：

- `artifact upload` 的 prepare -> secret-bearing HTTPS PUT -> complete -> Operation wait -> Ready 闭环；
- upload target 绝不能携带 Runtime OIDC token，且需要 no-proxy/no-redirect、expiry、response-loss journal 与 digest
  mismatch 负向 fixture；
- 409/412/429、quarantined/rejected/deleted、truncated/oversized stream 和真实 Artifact Gateway + S3/KMS P1 journey。

因此本文件不声明 Artifact lifecycle、M2 或 spec00～18 已完成。
