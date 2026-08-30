# 部署与运维

本地环境使用 `insight init/dev/status/logs/stop` 管理 fresh project。`base` 和 `full` 都运行与生产相同的 public
`/v1` 合同与独立 role；本地 loopback/OIDC/CA 放宽均显式标记 non-production，默认 fail closed。

生产镜像由手动 candidate workflow 构建一次 workspace binary graph，产出 runtime 与 sandbox-guest exact digest、
SBOM、provenance、signature、CandidateManifest 和 ReleaseBundle。签名步骤有明确 timeout；普通 PR 不构建、推送或
签名 candidate 镜像。GitOps environment repository 只提供 exact deployment closure，promotion 不重建镜像。

Helm role chart 位于 [`deploy/helm`](../../deploy/helm/)，每个 role 固定 command、ServiceAccount、NetworkPolicy、
workload identity 与 digest。旧单进程 chart 已归档到 [`deploy/archive/helm`](../../deploy/archive/helm/)。

真实多节点 Kubernetes、`RuntimeClass=runsc`、capacity、chaos、restore 与 soak 属于
[`platform-v2-production-l4-l6.md`](../qualifications/platform-v2-production-l4-l6.md)，当前仍为 Not run。
