# M0：CI 与候选构建基线

| 属性 | 值 |
|---|---|
| 状态 | Baseline / sampled, not a performance claim |
| 采样日期 | 2026-08-29 |
| 数据来源 | GitHub Actions run metadata 与 `.github/workflows/*.yml` |

## 1. 当前触发事实

- `ci.yml` 的 `push` 与 `pull_request` 没有 path filter；一次纯文档 push 也会触发 lint、workspace test、
  MCP interop 和 dependency policy 四个 job；
- `platform-production-candidate.yml` 只由 `workflow_dispatch` 触发，当前不会自动在普通 PR/push 构建、push、
  cosign 或 attest runtime image；
- candidate 使用 `type=gha,scope=platform-production-candidate` BuildKit cache，两个 image 串行 build；
- image subject sign/verify 以及 blob sign/verify 均已有 10 分钟 job timeout。此前“长时间等待”不能被解释为
  cosign 合同可删除；M5 应记录 OIDC、registry 和 cache diagnosis 后缩小触发面。

## 2. 可复核采样

| run | commit | 结论 | 总跨度 | 关键阶段 |
|---|---|---|---:|---|
| [33225314459](https://github.com/yimuu/insight-agent-platform/actions/runs/33225314459) | `cb9c2b98` | success | 19m42s | Lint 19m38s；Test 17m08s；MCP interop 3m29s；dependency policy 5m12s |
| [33183969085](https://github.com/yimuu/insight-agent-platform/actions/runs/33183969085) | `1efcbabc` | success | 3m56s | runtime image 21s；sandbox guest 65s；exact image cosign 6s；SBOM 19s；candidate qualification 49s |
| [33180077453](https://github.com/yimuu/insight-agent-platform/actions/runs/33180077453) | `acbdef0` | failure | 24m07s | runtime image 22m25s；image cosign 6s；GitHub attestation verify failed immediately after attestations |

样本不足以计算 p50 或宣布性能改善。它已经证明：缓存命中与 miss 的差异显著，且失败发生在 attestation
verify 而不是 `cosign sign --yes`。后续报告必须单独记录 queue、build runtime、cache hit/miss、registry push、
cosign、attestation 和 verification 时长。

## 3. M5 目标触发矩阵

| 变化范围 | target lane | 禁止触发 |
|---|---|---|
| `docs/**`、非运行控制台静态文本 | docs/links | workspace test、image build、registry push、cosign、attestation |
| `crates/insight-cli/**`、`examples/productization/**`、console | quick + relevant contract/journey smoke | candidate image/signing；除非同时修改 runtime/deployment |
| 一个 Platform crate | affected component + contract checker | 无关 interop/policy job；full 保留给合并前/主干 |
| dependency/contract/schema/worker | workspace full + required PostgreSQL/real-process | 无 |
| Dockerfile、`deploy/**`、candidate workflow、release/tag/manual dispatch | candidate release | 无；使用 exact GitOps closure |

这张矩阵在 M5 之前是 target。现有 CI 不应被误报为已 path-aware。

## 4. 后续测量格式

每个主干 run 保存一份只含非敏感数值的 JSON：

```json
{
  "schema_version": 1,
  "revision": "sha1:<40-lowercase-hex>",
  "lane": "candidate",
  "queue_ms": 0,
  "cargo": {"cache": "hit|partial|miss", "build_ms": 0},
  "images": {"cache": "hit|partial|miss", "runtime_build_ms": 0, "guest_build_ms": 0, "push_ms": 0},
  "supply_chain": {"cosign_ms": 0, "attestation_ms": 0, "verify_ms": 0},
  "outcome": "passed|failed|cancelled"
}
```

该数据是 CI artifact，不进入 Platform business database、Event 或 public API。
