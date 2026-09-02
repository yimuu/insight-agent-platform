# Platform v2 L4～L6 本地逻辑验证报告

- 日期：2026-09-02
- 被测提交：`fdc4311a68106e5569238d9cdf1a42bec19f2134`
- 验证类型：本地真实 preflight + adversarial logical validation
- 生产资格结论：**Failed / L4～L6 仍为 Not run**
- 原始本地证据：`target/local-l4-l6-validation-fdc4311a/`（gitignored，不是可发布资格证据）
- 修复复验：**CR-217 已关闭 LV-P1-01～06 的已复现绕过；Sandbox LV-P1-07 未改，L4～L6 仍为 Not run**

## 1. 结论

本地验证确认：多节点、工作负载和 release evidence 的数据结构可以在单机覆盖。下文保留修复前的可执行
false-positive 证据；CR-217 随后对这些输入完成 fail-closed 修复，但这不等于真实 L4～L6 已运行。

最严重的问题是 `platform-qualification validate-release-evidence` 接受了以下 adversarial evidence，并输出
`production release evidence valid and passed`：

- `started_at` 到 `completed_at` 只有 1 秒，而 checked-in production profile 要求 86,400 秒；
- 26 个 L1～L6 gate 全部引用同一个 `capacity-profile` JSON；
- `signed_supply_chain`、`backup_restore`、`gitops_rollout_rollback` 等 gate 没有各自的可辨识证据；
- topology digest 只是任意格式正确的 SHA-256 字符串。

因此修复前的 release evidence validator 只能证明 JSON、digest 和文件完整性闭合，不能证明 L4～L6 已实际完成。

## 2. 本机真实环境结果

| 项目 | 本机结果 | production preflight 要求 | 结论 |
|---|---|---|---|
| Kubernetes | OrbStack `v1.35.6+orb1` | production-equivalent Kubernetes | 可连接 |
| kubectl | `v1.33.9` | 与 server 相差不超过 1 minor | 失败：相差 2 minor |
| Ready node | 1 | 至少 2 | 失败 |
| Kubernetes runtime | `docker://29.4.0` | `containerd://...` | 失败 |
| BatchSandbox CRD | 不存在 | Established exact CRD | 失败 |
| CPU / memory | 18 CPU / 16 GiB | 未用于 production capacity 声明 | 仅开发环境 |

使用合成但结构有效的 CandidateManifest/CapacityProfile 调用
`scripts/preflight-platform-production-qualification.sh` 后，脚本在读取 BatchSandbox CRD 时 fail closed。它没有生成
`topology.json` 或伪造本机 L4 通过结果。这一部分行为正确。

## 3. 修复前执行结果

| 验证 | 结果 |
|---|---|
| production profile 原文件校验 | 通过，digest `sha256:7fe0962f...` |
| `requires_multi_node=false` | 正确拒绝 |
| 删除 `sustained_soak` gate | 正确拒绝 |
| `minimum_soak_seconds=1` | **错误接受** |
| topology/workload Python tests | 11/11 通过，但没有覆盖下述 adversarial case |
| CRD schema 漂移 | **错误接受，topology digest 不变** |
| 额外 allow-all NetworkPolicy | **错误接受，workloads digest 不变** |
| 未标记 role 的 privileged Deployment | **错误接受，workloads digest 不变** |
| 一秒钟、单 artifact、26 gate 全 passed evidence | **错误接受** |
| 任一 gate 标为 failed | 正确拒绝 |
| artifact 内容追加一个字节 | 正确拒绝 |
| artifact 使用符号链接 | 正确拒绝 |
| ComponentRole 静态 checker | 命令返回通过，但 closure 不完整 |
| Product Release 静态 checker | 命令返回通过，但 publish DAG 顺序错误 |

## 4. 发现

### LV-P1-01 Evidence validator 不执行 soak 时长合同

`QualificationEvidenceManifest::validate_against` 只检查 `completed_at >= started_at`，没有检查两者差值是否达到
`QualificationProfile.minimum_soak_seconds`。`QualificationProfile::validate_for_production_release` 也只要求时长大于零，
因此把 production profile 改成 1 秒仍然有效。

影响：没有执行 24 小时 soak 的 evidence 可以成为 production release authority。

建议：production profile 明确要求 `minimum_soak_seconds >= 86_400`；release evidence validation 使用安全的 UTC duration
计算并拒绝短于 profile 的窗口；增加 86,399/86,400 秒边界测试和时钟逆序测试。

### LV-P1-02 Gate evidence 没有 gate-specific closure

26 个 gate 可以全部引用同一个 capacity JSON digest。validator 只验证每个 digest 能解析到普通文件，不验证 artifact role、
media type、producer、tool identity、topology/candidate binding，也不要求不同 gate 具有独立 evidence。

影响：只要构造一份格式合法的文件，就能把 restore、故障注入、隔离、签名供应链和 GitOps rollback 全部标成 passed。

建议：先由 reviewed qualification contract 定义 gate-specific typed evidence；至少为每个 gate 定义 evidence kind/schema、
exact candidate/topology/seed/time-window binding 和 producer identity。不要仅通过 artifact 文件名或互不重复的 digest 修补语义。

### LV-P1-03 L4 topology digest 不绑定 live CRD

修改合成 inventory 内的 CRD OpenAPI schema 后，`check-platform-production-topology.py` 仍返回成功且 digest 不变。
脚本输出的是 hard-coded CRD digest，不是 live normalized CRD 的 digest。

影响：CRD schema、subresource、conversion 或 validation 漂移可以被记录成 exact topology evidence。

建议：规范化 live CRD 的 owning fields 后计算 digest，并与 reviewed CRD render/candidate closure 比较。

### LV-P1-04 Workload checker 接受额外 allow-all policy 和未跟踪特权 workload

在已有 default-deny 的 namespace 中加入 `ingress: [{}], egress: [{}]` 的 allow-all NetworkPolicy，validator 仍通过且 digest
不变。加入没有 `insight.platform/component-role` 的 privileged、mutable-image Deployment，同样被完全忽略。

影响：安全闭包可以在不改变资格 digest 的情况下被绕过。

建议：以 CandidateManifest/Helm render 为 authority 建立完整 workload 与 NetworkPolicy inventory；拒绝未登记 workload、
未登记 policy 和任何扩大 selector/peer/port 的 live drift。preflight 还必须纳入 ServiceAccount、RBAC、
ValidatingAdmissionPolicy/Binding。

### LV-P1-05 ComponentRole 静态 closure checker 自身不闭合

`scripts/check-platform-component-workload-closure.rb`：

- CHARTS 不包含 `insight-platform-callback-api`；
- CHARTS 不包含 `insight-platform-mcp-cleanup-worker`；
- EXPECTED_COUNTS 接受 CandidateManifest 未注册的 `context_dataset_worker`；
- 最终信息硬编码为 `16 roles`，不是实际 authority 计数。

因此命令虽然输出 `passed`，其结果不能证明 CandidateManifest 的完整 ComponentRole closure。

### LV-P1-06 Product Release checker 接受 publish-before-qualification

`scripts/check-product-release.py` 返回通过，但 workflow 的实际 DAG 是：

`cli/images -> publish GitHub Release -> development-profile-qualification`

`publish` 不依赖 qualification，qualification 反而依赖已经产生不可变外部副作用的 publish。

影响：资格失败时，公开 tag/image/signature/Release 已经存在。

建议：先生成 staging content-addressed subjects，完成 qualification 后再执行唯一的 publish job，并将 qualification
evidence 纳入最终 ReleaseBundle。

### LV-P1-07 Sandbox 故障收敛仍缺少实现路径

本次复核确认原整体 review 的下列代码事实未变化：

- detached Sandbox cancel/timeout 只把 Invocation 置为 `Cancelling`，没有 owning Sandbox consumer；
- claim SQL 永久排除 deadline 已过的非终态 Sandbox Job；
- activation authorized 后遇到 runner boot rollover 仍可能使用旧 boot frame；
- runner timeout/output failure只 kill direct child，没有 process group/cgroup kill-all；
- Dispatcher readiness 仍是 authenticated list-only probe。

这些缺口没有对应的本地恶意 Package、boot rollover、expired deadline reconciliation 或 inert create/protocol/delete
资格证据，因此逻辑层 L4 recovery/privilege boundary 不能判定通过。

## 5. 正确工作的边界

- 当前本机不满足真实 L4 前提时，preflight 在 CRD 采集阶段失败，没有生成通过报告；
- production profile 会拒绝缺失 required gate、`requires_multi_node=false` 等明显降级；
- evidence 只要包含 failed outcome，release validator 会拒绝；
- artifact byte length、SHA-256 与普通文件/符号链接边界是 fail closed 的；
- CandidateManifest 与 CapacityProfile 的结构、完整 role/pool/work-class/SLO 集合及 deployment digest binding 可以校验。

## 6. 原始判定

| 层级 | 本地逻辑判定 | 真实资格状态 |
|---|---|---|
| L4 topology / isolation / recovery | **Failed**：存在可复现 false-positive 和未实现收敛路径 | Not run |
| L5 capacity / soak | **Failed**：1 秒 evidence 可通过 86,400 秒门禁 | Not run |
| L6 supply chain / restore / promotion | **Failed**：单一无关 artifact 可覆盖全部 gate，且 publish 顺序错误 | Not run |

本次结果证明“单机可以覆盖逻辑层面”，而且这种覆盖发现了阻止真实验证的门禁缺陷。CR-217 已先修复
LV-P1-01～06；真实环境投入仍应等待 Sandbox LV-P1-07 的状态机与隔离缺口关闭。

## 7. CR-217 修复与复验

| 原发现 | 修复 | 本地复验 |
|---|---|---|
| LV-P1-01 | production profile 至少 86,400 秒；Evidence elapsed time 必须覆盖 profile | 1 秒 evidence 在 CLI 入口被 `completed_at` 拒绝；10 个定向 Rust qualification tests 通过 |
| LV-P1-02 | 每个 gate 至少一个专属 digest；artifact digest 禁止别名，禁止未引用项；CLI 输出明确仅为 structural validity | 单 artifact 跨 26 gate 回归被拒绝；真实执行仍须受保护 CI producer、签名与 GitOps 证据 |
| LV-P1-03 | 从 live BatchSandbox CRD 完整规范化 spec 计算 digest，并与 reviewed render digest 比较 | CRD schema drift 负向测试通过，摘要使用 observed digest |
| LV-P1-04 | 以带标签 Namespace 建立 workload 全闭包；拒绝无 role workload、namespace-wide/unbounded allow；全部 policy 进入摘要 | allow-all 与 privileged unlabelled Deployment 两项负向测试均通过 |
| LV-P1-05 | callback/cleanup 纳入闭包并映射 `mcp_host`；dataset 映射 `context_worker`；计数动态输出 | Helm 静态闭包通过：16 roles / 25 isolated pools |
| LV-P1-06 | workflow 改为 signed candidate → offline exact-cache starter qualification → 重签包含资格 evidence 的 final ReleaseBundle → final OCI tag/GitHub Release | release checker 验证 publish 同时依赖 candidate 与 qualification；final bundle 纳入 development performance digest；workflow YAML 和 shell 语法通过 |
| L4 SA/RBAC/VAP 缺口 | preflight 新增 ServiceAccount、Role/Binding、ClusterRole/Binding、ValidatingAdmissionPolicy/Binding live inventory 与最小权限核验 | RBAC 扩权和 Admission `Deny` 漂移负向测试通过 |

定向复验合计：production topology/workload Python tests 17/17，通过；product release tests 5 项通过、1 项因本机
OpenSSL 不支持 Ed25519 条件跳过；三个受影响 Helm chart 及 Sandbox/Artifact/Security chart lint 通过；生成合同与
`contracts/platform-v1/manifest.json` 一致。没有重复执行全 workspace Rust 测试。

CR-217 只关闭本地已复现的资格假通过和发布顺序问题。它没有生成受保护 CI 签名、24 小时连续运行、真实多节点、
restore 或 promotion 记录，因此 L4、L5、L6 的正式状态保持 `Not run`。
