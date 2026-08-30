# Productization repository clean-cut report

状态：**Passed（M0～M5 repository scope）**  
切换 revision：`b8dada4ec805ffc80777ea203b9f5d72dde20b6c`  
外部资格：**Not run（L4～L6）**

## 1. 产品入口闭包

| 检查面 | clean-cut 结果 |
|---|---|
| 公共合同 | `contracts/platform-v1/openapi.yaml` 为 `insight.platform/v1`、`/v1`、`current`；没有 `/v2` 或兼容入口 |
| 默认构建 | Cargo `default-members` 仅选择 `crates/insight-cli`，`cargo build --locked` 产出 `insight` |
| 默认镜像 | runtime image 一次 Cargo build graph 构建 `insight` 与 Platform role binaries；不再构建/复制旧 `insight-agent-platform` binary，默认 entrypoint 是 `platform-gateway` |
| 部署拓扑 | active `deploy/helm` 只保留 Platform role charts；旧单进程 chart 移至 `deploy/archive/helm` |
| 当前文档 | 根 README、`docs/README.md` 与 `docs/current` 只描述 `/v1`、CLI、Console 与独立 role；旧 DSL current snapshot 移至 `docs/archive/current-dsl-v1` |
| 示例 | 当前正向示例为 productization HTTP lifecycle、十场景 manifest 与 LangGraph.js reference；旧 DSL fixture 不在默认入口或发行镜像 |
| 外部框架 | 固定 `@langchain/langgraph` 1.4.13 的独立服务只走 exact Egress typed HTTP，不读取 Platform DB；Python SDK/Agno adapter 已取消 |

旧 Rust DSL/runtime 源码仍作为非默认历史实现和回归 fixture 保留在 workspace；`insight` CLI、Platform role 与
candidate image 不依赖它来提供新 `/v1` 行为。该保留不构成 public fallback、dual write 或双栈发行。

## 2. 已执行门禁

- `bash scripts/check-cutover-residuals.sh`：Passed；
- `python3 scripts/check-platform-candidate-pipeline.py`：Passed；
- `python3 scripts/check-productization-ci.py`：Passed；
- `bash scripts/check-public-api-baseline.sh`：Passed；
- `bash scripts/check-crate-boundaries.sh`：Passed（58 workspace packages / 586 resolved packages）；
- `cargo check --locked`：Passed，默认只检查 `insight-cli`；
- `cargo test --locked -p insight-cli`：64 Passed；
- `python3 scripts/tests/test_productization_http_lifecycle.py`：2 Passed；
- `python3 scripts/tests/test_platform_candidate_pipeline.py`：4 Passed；
- clean-cut 前最后一个完整功能 revision `a70a9f99f58b8fd9fecb4c309f910aa99434b122` 的 full-profile
  fresh journey：10/10 report Passed，严格 checker `complete_gate=true`，详见
  [`full-journey-evidence.md`](full-journey-evidence.md)。

clean-cut 只改变默认入口、发行清单、合同状态与文档，不改变十场景业务合同。最终候选仍需在 clean-cut 后的 exact
revision 复跑 full journey，报告按自己的 `source_revision` 保存，不通过修改历史报告伪造新 revision。

## 3. CI 与供应链

普通 CI 使用 closed path classifier：docs、Console 与 CLI 变化只进入各自 affected lane；未知路径 fail closed 到
workspace full。普通 CI 不构建、推送、签名或 attest candidate image。candidate 只能手动触发，要求 exact GitOps
environment commit，以共享 BuildKit cache 构建两个 image，并为 cosign/signature/attestation 设置有界 timeout。

已有样本证明 workspace full 低于 30 分钟、cached candidate 低于 10 分钟；10/30 分钟是持续运行 SLO，后续主干
继续采样，不把尚未发生的未来 run 当作 repository 功能缺口。

## 4. 明确不关闭的门禁

本报告不宣称 production-ready，也不关闭真实多节点 Kubernetes、`RuntimeClass=runsc`、容量、混沌、restore、24 小时
soak 或 production GitOps promotion。它们继续由 `docs/qualifications/platform-v2-production-l4-l6.md` 跟踪为
**Not run**；因此 Platform spec00～18 继续保持 Accepted/In Progress，而不是 Verified。
