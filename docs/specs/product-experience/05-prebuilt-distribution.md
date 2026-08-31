# Spec 05：预构建 CLI、Console 与镜像分发

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-207 |
| 日期 | 2026-08-31 |
| 发行authority | GitHub Release + OCI registry + signed release bundle |

## 1. 目标

普通用户启动本地平台不再编译Rust workspace，也不安装Node.js。源码构建保留给贡献者；release构建、镜像、签名、
SBOM和provenance只对一个exact release revision执行一次，并被所有本地/生产入口复用。

## 2. 发行物

每个release必须同时生成：

### CLI archives

- `insight-<version>-aarch64-apple-darwin.tar.gz`
- `insight-<version>-x86_64-apple-darwin.tar.gz`
- `insight-<version>-x86_64-unknown-linux-gnu.tar.gz`
- `insight-<version>-aarch64-unknown-linux-gnu.tar.gz`

archive只包含`insight`、license和最小版本说明。不得捆绑credential、project state、Docker daemon或自动安装脚本。

### OCI images

- runtime image包含同一release所需Platform role binaries；
- sandbox guest保持独立最小image和exact digest；
- Console为不可变静态bundle，可嵌入runtime static server或独立OCI image，但不能增加BFF；
- manifest list覆盖release声明的平台；每个目标架构绑定exact child digest。

### Metadata

- SHA-256 checksums；
- CycloneDX或SPDX SBOM；
- build provenance；
- keyless或组织key签名；
- canonical ReleaseBundle，绑定Git commit、CLI digest、Console digest、所有image digest、schema/profile digest。

## 3. 构建策略

- PR默认只运行受影响的check/test，不构建和签名production image；
- main可生成未promotion candidate，但同一commit相同输入不得为每个journey重复build；
- tag/release workflow执行一次locked release build，并从同一Cargo invocation产出所需binaries；
- Docker BuildKit缓存Cargo registry/git/target和image layer，cache key绑定toolchain、lockfile、target和build flags；
- sandbox guest变化只重建guest和受影响release index；普通文档/Console变化不重编译全部Rust binary；
- 签名步骤设置bounded timeout和可重试网络边界；超时保持release未发布，不能跳过签名后标记成功。

## 4. 安装与升级

文档首选显式下载、checksum/签名验证后安装。可增加Homebrew tap或包管理器元数据，但它们只指向同一签名archive，
不自行重构建。

```text
insight version
insight update check
insight update apply --version <exact-version>
```

`update apply`必须先下载到临时路径、验证ReleaseBundle和platform匹配，再原子替换CLI。它不自动升级正在运行的production
environment；本地profile升级需显式stop/start并执行兼容检查。数据库仍只允许reviewed forward migration，不由CLI猜测修补。

## 5. 本地profile取用

- `insight dev`默认读取ReleaseBundle并拉取exact digest，禁止mutable tag；
- CLI版本、profile schema和image release不匹配时在启动任何服务前失败；
- `--from-source`是贡献者显式模式，使用当前checkout和Cargo，不污染预构建cache；
- `--offline`只在所有exact artifacts已存在且验证通过时运行，不回退latest或本地未知image；
- registry不可用时显示缺失digest和恢复命令，不静默触发全workspace source build。

## 6. 供应链与权限

- GitHub workflow action、toolchain、base image和生成器固定immutable revision；
- publish权限只授予protected release environment；PR和普通branch没有registry overwrite或release upload权限；
- GitHub Release、OCI signature和GitOps environment使用同一ReleaseBundle digest；
- release资产不可覆盖；修复产生新version/digest；
- provenance不得包含Secret、token、private endpoint或runner环境dump。

## 7. 性能门禁

release报告必须分别记录：

- Rust编译时间与cache hit；
- runtime/guest/Console build时间；
- image push、SBOM、provenance、cosign各自时间；
- archive/image大小与相对上一个release的变化；
- cold pull与warm reuse时间。

单步超过预设预算必须明确失败或标记release blocker，不能在无输出状态无限等待。性能预算由CI配置固定，不能由PR自由放宽。

## 8. 验收

- 四个CLI target在支持的真实/仿真runner上执行`version/doctor`；
- fresh机器不安装Rust/Node即可启动Spec 06 starter并完成first Run；
- archive、image、SBOM、provenance和ReleaseBundle签名可离线重验；
- 相同release中的CLI/profile/image/schema digest完全闭合；
- PR journey复用candidate，不重复构建和签名相同image；
- cosign超时、registry部分push、错误架构、mutable tag和cache poisoning均fail closed；
- 源码模式和预构建模式通过相同L1～L3及北极星旅程。
