# ADR-0008：Sandbox runner 的有界 capability 交接与镜像闭包

| 属性 | 值 |
|---|---|
| 状态 | Accepted |
| 日期 | 2026-09-05 |
| 补充 | ADR-0007 |

## 问题

ADR-0007 要求固定 runner 与 Package 使用隔离身份，且 Package 不能控制 runner。OpenSandbox
`execd` 以 Sandbox 容器身份创建 runner 子进程；若该身份没有 `SETUID`、`SETGID` 与 `KILL`，runner
既不能切换到独立 Package 身份，也不能可靠清理整个 Package process group。把这些 capability 直接留给
动态 runner、让 Package 继承 `execd` bearer token，或允许 Package 改写 runner rootfs，都会把短暂的物理权限
变成可被不可信代码利用的第二执行路径。

## 决策

唯一运行链保持为官方 OpenSandbox Server/Controller 与 `execd v1.1.0`，不 fork 上游。Sandbox Pod 使用
containerd/runc、UID/GID `65532:65532`、`RuntimeDefault` seccomp、read-only root filesystem 和严格 supplemental
group policy。容器 capability bounding set 精确为 `KILL,SETGID,SETUID`；`allowPrivilegeEscalation: true` 是仅为下述
file capability 生效的显式例外，不代表通用提权许可。

Platform Sandbox 镜像含两个静态、无解释器和无动态依赖的可信程序：

- `/usr/local/bin/platform-sandbox-runner` 是最小 C launcher，也是整个最终 rootfs 中唯一带 file capability 的文件，
  精确为 `cap_kill,cap_setgid,cap_setuid=ep`；
- `/usr/local/libexec/platform-sandbox-runner-core` 是无 file capability 的静态 Rust core。

launcher 只接受官方 `execd` PID 1 创建的独立 process-group 子进程。它 fail closed 校验 UID/GID、空 supplemental
groups、完整 capability 集、bounding set、securebits、seccomp 与 `no_new_privileges=0`；随后只把既定 capability
经 inheritable/ambient 交给 core，设置 `no_new_privileges=1`，以绝对路径和精确三项新环境执行 core。新环境只有
runner config、config digest 与 `OPENSANDBOX_ID`。

core 在创建异步 runtime 前同步清空 inheritable/ambient，设置 non-dumpable，并重新校验进程边界。启动 Package
的 post-fork 子进程按固定顺序建立新 session、清 supplemental groups、切换到 `65533:65533`、清空 permitted/
effective/inheritable/ambient capability、保持 `no_new_privileges=1`、叠加 Package seccomp 与资源上限并逐项校验，
然后才 `exec` Package。bounding set 保留 `KILL,SETGID,SETUID`，但 Package 的 NNP、零 capability、独立 UID/GID
及严格镜像闭包共同阻止重新执行 launcher 获权。

## 凭证与请求认证

Dispatcher 为每个候选生成 256-bit CSPRNG `EXECD_ACCESS_TOKEN`。它只认证 OpenSandbox 的物理 execd API，
不是业务状态 authority；官方 bootstrap/execd 在 runner 启动前可见它。launcher 验证其形状后用全新 `envp`
执行 core，因此 Platform core 与 Package 不继承该 token。合同不得声称只有 PID 1 曾见过它。

runner 的 state、activate 与 result 路由全部要求独立的、由该候选 activation key 签名的请求证明。证明绑定候选、
冻结的 execution request、HTTP method/path 与原始 request body digest。Dispatcher 持有私钥并生成证明；provider
adapter 只转发 opaque signature；runner 只持有验证公钥。NetworkPolicy 不是同 Pod loopback 的认证边界。
候选与 Admission 的模板身份闭值为 `armed-runner-v2`；旧 `armed-runner-v1` 不再是可接受输入。
这里的 v2 是认证后的 HTTP operation protocol；未改变的 config/state/activation/result frame 仍使用其
独立的 schema v1 与 magic，且不能通过已删除的 `/v1/*` 路由访问。

## Package 镜像闭包

发布资格只接受以已证明 Sandbox runner image layers 为严格前缀的 Package image。新增 layer 只能写入冻结的
`/opt/insight/package` payload，不能改变 OCI runtime config，不能包含 whiteout、路径逃逸、symlink/hardlink、
device、SUID/SGID 或额外 `security.capability`。最终合并 rootfs 全局无 SUID/SGID，唯一 file capability 必须仍是
launcher 的精确值。发布验证绑定每个平台 manifest、config、layer digest 与 xattr；Admission 只校验 Pod 形状，
不能替代镜像证明。

Dispatcher 的全链路 readiness 必须使用同一发布闭包中的精确 Sandbox runner image，而不是通用 Platform runtime
或任意 Package image。这样 readiness 才会经过 launcher/file-capability 边界，又不会把健康状态绑定到业务
Package payload；源码和签名候选路径都必须把该 runner 的 platform manifest 独立导入并记录为环境身份。

## 故障与资格证据

任一 token、签名、环境、身份、capability、NNP、securebits、seccomp、ELF 或镜像闭包不匹配时，候选不得进入
`Armed`。L3 必须在 registry 到 containerd/overlayfs 的真实路径证明 launcher file capability 未丢失，逐阶段记录
进程边界，并证明 Package 不能读取 runner-owned HTTP/state、恢复 execd token、重新获权、向 runner 发信号或逃离
process group。静态检查与单元测试不能替代这组实测证据。

## 否决方案

- 让动态 core 带着 capability 启动：动态 loader/constructor 会在 core 清权前运行；
- 对 Package 暴露 execd token，或关闭 execd 鉴权：形成同 Pod 通用命令面；
- 只依赖 NetworkPolicy 保护 runner：它不隔离同 Pod loopback；
- 用 SUID、root runner、privileged container 或 host runtime socket：权限面不可有界；
- 保留 hardening-on/off 双路径或旧 runner fallback：形成两套进程语义；
- fork OpenSandbox 改 bootstrap：当前最小闭包可由 Platform 镜像与既有 provider 合同实现，无需长期上游分叉。

## Authority 与交叉复核

机器 authority 是 `containerd-runc-runtime-v2.json`、runner protocol、Sandbox owning Rust types 与发布镜像验证器；
Helm Admission 固定其 Pod 投影。本决策的交叉复核覆盖 capability/ambient/NNP 内核语义、静态链接、环境泄漏、
Package layer 路径逃逸、同 Pod loopback、信号清理、xattr 保留与 L3 证据要求。
