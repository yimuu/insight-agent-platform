# CR-216 OpenSandbox L1～L3资格报告

状态：Passed / repository qualification

日期：2026-09-02

范围：OpenSandbox clean-cut实现、shared Job authority、Dispatcher/runner/provider lifecycle、真实PostgreSQL事务与单节点真实
Kubernetes/containerd-runc集成。该报告不是production CapacityProfile、HA、强多租户隔离、restore或promotion证据。

## 资格闭包

当前唯一Sandbox物理路径为：

`Sandbox Dispatcher -> internal OpenSandbox Server -> Kubernetes API -> BatchSandbox Controller -> containerd/runc`

Platform没有修改OpenSandbox源码。shared Job仍是attempt、lease、fence、cancel、terminal、selected candidate和cleanup intent的唯一业务
authority；OpenSandbox、Controller、execd和runner只拥有physical lifecycle。Dispatcher是唯一provider caller，并在terminal transaction
重新验证current Job fence。workload内部网络、API、数据库或消息副作用不属于Platform幂等承诺。

## L1 contract、runner与配置

以下测试作为一个closed contract批次实际运行并通过：

- `insight-platform-sandbox`：10/10；
- `insight-platform-opensandbox-client`：3/3 unit；
- `insight-platform-sandbox-runner`：3/3；
- `insight-platform-sandbox-dispatcher` library/binary：4/4；
- `insight-platform-contracts`：118/118。

共20项Sandbox target测试覆盖closed Runtime/Profile/Request/Candidate/Activation/Result合同、canonical digest、bounded input/output、
fixed-path atomic result、one-shot activation、restart uncertainty、database-time create authorization hard limit、unknown field和非法配置。
四个target的all-target/all-feature strict Clippy、workspace locked all-target/all-feature check与Sandbox Helm/static boundary均通过。

## L2真实PostgreSQL

环境使用PostgreSQL 16.14，在全新数据库中原子安装schema contract 8：

- migration set：`sha256:ba5080ceb152d56d54684f8345f0809b947bb880b660a904995b293d9b9d7612`；
- 物理表数量：23；
- qualification target：`crates/platform-postgres/tests/phase3_opensandbox.rs`，2/2。

fixture覆盖concurrent claim单winner、同ordinal create authorization的`Applied | Replayed`、lease expiry continuation rollover且attempt和
physical identity不变、old-fence stale result零写入、terminal replay/first-winner、Cancelled/TimedOut、四维quota settle/release、
selected/late/stale-attempt/missing-owner orphan decision，以及terminal后的cleanup claim与absence replay。

## L3真实OpenSandbox/Kubernetes

`scripts/qualify-platform-sandbox-l3.sh`在fresh kind v0.31集群中以Kubernetes v1.35、containerd 2.2和runc执行完整单次通过。
网络实现使用Calico 3.32.1。OpenSandbox source固定为commit
`c39b814f36ded4c61d5ac6f9332ee4dfbab86c00`，official image固定为：

- OpenSandbox Server：`sha256:ae8dfbb277f40a39ff01ef35e5e1c10675acfe0fa9db15259b8f323e5efab778`；
- BatchSandbox Controller：`sha256:a9a5f73c1785ebd955336ffa313973a35c1a1b662cb7afc4ea82d92021b3532a`；
- execd：`sha256:0d8f44cf4194732719aa79999d4b120c98bdab02bc61e9ad13f75f83af4c2684`。

fixed test Package image为
`docker.io/library/insight-agent-platform@sha256:18e9d07f90c6d7791c9bafe23b4471652c67bd8f06a84e2f116b2a14a50056da`；
Dispatcher runtime contract digest为
`sha256:36b0ab993663980370353a3bf14bf6a8dc777ee707913588411fd223cbc76bc9`。

同一完整L3运行证明：

- wrong-source CNI和wrong API credential在业务处理前拒绝；
- Direct可访问声明的外部目标，Disabled对同一目标无法出站；Helm/static gate另行验证Direct的internal/metadata deny列表；
- concurrent create、create response loss后的metadata discovery与candidate first-winner；
- OpenSandbox Server和Controller restart后同一persistent candidate恢复；
- Dispatcher强杀/lease reclaim保持同一attempt、token、candidate、boot和Package execution，不创建replacement；
- TTL removal、explicit delete/absence、orphan cleanup与最终零BatchSandbox residue；
- Dispatcher、Server和Controller全部Ready，且无public ingress、host network/runtime socket或Platform credential。

## 全量回归

在L1～L3后实际通过：

- `cargo test --locked --workspace --all-targets --all-features`；
- `cargo test --locked --workspace --doc --all-features`；
- `cargo check --locked --workspace --all-targets --all-features`；
- `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings`；
- Platform v1 contract/schema/generated contract、candidate pipeline、CLI/profile、component/workload、deployment、observability、
  redaction、security、MCP、Artifact、Capability、Context、Model和public API static gates；
- clean-cut residual scan与production workload正负fixture。

## 未运行与声明边界

- L4 production topology/identity/fault：Not run；
- L5 capacity/mixed-load/86,400秒soak/SLO：Not run；
- L6 restore/supply-chain rollout/rollback/人工GitOps promotion：Not run。

因此CR-216仓库实现和L1～L3已完成，但不声明production-ready。未来environment资格必须按
[`platform-v2-production-l4-l6.md`](platform-v2-production-l4-l6.md)生成exact evidence。
