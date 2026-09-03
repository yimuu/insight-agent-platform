# Kind 本地 Platform 验证

此目录提供可重复的三节点 Kind 环境，用于验证 Platform v2 的本机 L4 机制。它会部署完整的 16 个
`ComponentRole`、OpenSandbox、PostgreSQL、NATS、LocalStack、Metrics Server 及所需 CRD，然后执行拓扑、
workload、NetworkPolicy、mTLS、RBAC、准入、配置/镜像漂移、Pod/Node 故障和 OpenSandbox 恢复检查。

这不是 production-equivalent 环境：三个节点共享一台宿主机，依赖项是本地单副本，未提供真实 Prometheus、
真实云 Provider、gVisor、容量 soak、备份恢复或 GitOps 供应链。因此脚本输出固定标记 `production: false`，不能将
结果用于把正式 L4～L6 状态改为 Passed。

LocalStack 路径在 live Deployment 上使用固定的 `test` AWS 凭据，因为本地 Kind 没有生产 workload identity。
该覆盖只由引导脚本在 Helm 创建资源后注入，不进入生产 chart，也不代表生产凭据或身份配置。

## 前置条件

- Docker、Kind、Helm、Ruby、Python 3、`jq`、OpenSSL 和与 server 相差不超过一个 minor 的 `kubectl`；
- 已构建的 `target/debug/insight` 与 `target/release/platform-schema`；
- 本地精确镜像 `insight-agent-platform:cr216-l3-runtime-v2` 和
  `insight-agent-platform:cr216-l3-package`；
- 首次下载镜像和上游 manifest 前，在当前 shell 开启需要的网络代理。例如本机配置提供 `proxy_on` 时，先执行
  `proxy_on`，再运行下面的脚本。脚本不固化个人代理地址。

## 从零重建并验证

所有输出都放在一个独立目录，且脚本拒绝复用已有集群或输出目录：

```bash
export INSIGHT_KIND_CLUSTER_NAME=insight-platform-local
export INSIGHT_KIND_OUTPUT_DIR=/private/tmp/insight-platform-local
export INSIGHT_KIND_KUBECONFIG="$INSIGHT_KIND_OUTPUT_DIR/kubeconfig"
export INSIGHT_KIND_KUBECTL=/opt/homebrew/opt/kubernetes-cli@1.35/bin/kubectl

scripts/bootstrap-platform-kind-local.sh
scripts/verify-platform-kind-l4.sh
```

若已有一次 `insight dev --features all` 生成的完整运行时，可以通过
`INSIGHT_KIND_SEED_PROJECT=/path/to/seed-project` 复用其本地测试 CA、客户端证书和状态密钥。否则引导脚本会在输出
目录中创建并停止一个 seed profile；它只用于生成本地输入，不构成资格证据。

引导结果写入 `$INSIGHT_KIND_OUTPUT_DIR/environment.json`，动态证据写入
`$INSIGHT_KIND_OUTPUT_DIR/evidence/summary.json`。原始 inventory 与预期失败输出也保留在 evidence 目录中。

## 清理

验证脚本会恢复每项故障注入并删除探针。完成后可删除整个一次性集群：

```bash
kind delete cluster --name "$INSIGHT_KIND_CLUSTER_NAME"
```
