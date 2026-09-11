# 持久物理提供者：联合评审记录

状态：运行时物理 adapter 子边界联合接受，适用于已正确初始化并通过精确核验的提供者。Root 与
Product 已共同接受下述职责划分、共享客户端范围、精确版本和失败恢复原则；Root 与 Operations
亦接受首次 bootstrap 的下述失败边界。共享 OpenBao 客户端、Secret adapter 与 Artifact S3/Transit
组合已实现，并通过下述真实物理原语测试与 Compose 受控依赖重建。完整用户业务与生产资格仍待验证；
这些结果不代替真实检索、外部模型和人工审批的验收。F 原数据、授权、密钥和运行配置未被修补。

## 问题与不变边界

F 的完整依赖重启暴露现有开发依赖的物理数据丢失。仅声明兼容 AWS HTTP 不足以证明对象 generation、
KMS 加密、Secret 请求去重或重启持久性。本轮采用具有持久卷的 S3 对象服务，
以及 OpenBao Transit / KV v2 的显式物理适配器；不伪造 AWS ARN，不增加业务表、Security authority
或主密钥兼容分支，不把已丢失的外部材料解释为可自动重新创建。

PostgreSQL 继续拥有 Artifact/Blob、SecretBinding、租约、CAS、Receipt、当前权限与审计事实。
外部服务只拥有对象字节、Secret 版本和加密材料。原 Broker 的前后当前授权、精确引用、内存清零、
容量和期限保持；不持有数据库事务跨网络调用，不缓存业务授权，不将外部 200 当作业务已提交。

现有承接点：

- Artifact 的 `ArtifactObjectReferenceUnsealer` 与 `InstalledArtifactObjectStore` 继续分离定位符解密
  和 exact generation HEAD/read/delete。[S3 上传实现](../../../crates/adapters/platform-artifact-broker/src/aws.rs)
  已通过独立的 [reference key 实现](../../../crates/adapters/platform-artifact-broker/src/reference_key.rs)
  选择真实 AWS KMS 或 OpenBao Transit，保留原 S3 PUT/签名上传/HEAD/读取/删除协议。
- Secret 的 `SecretReferenceSealer`、`SecretReferenceUnsealer`、`InstalledSecretProvider` 与
  `BrokeredPreparedSecretStore` 已容纳物理差异；`SecretProvider` ResourceId、受控 purpose、
  `ExactSecretBindingRef` 和 pinned version digest 不需要新增业务权威。
- `key_id`、密文定位符及 opaque reference 已是有界物理字段，可承载明确的 OpenBao 身份。
  不能把 `AwsKmsKeyBindingConfig` / `AwsSecretProviderConfig` 的 ARN 验证改成同时接受假 ARN。

## 最小共享组件与接口

Root 拥有最小共享 adapter crate `insight-platform-openbao`。它不依赖 Artifact、Security、
Model、PostgreSQL 或任何业务 domain，不接收业务 ID，不决定授权、Receipt 或请求重放。
Product 拥有 Secret Broker 的 OpenBao adapter 与 Egress factory；Root 拥有 Artifact S3/sealer
解耦；Operations 拥有安装、角色凭据、Compose/Helm 渲染及实际持久重启验证。

实际配置、绑定、错误和界限由
[部署 owning types](../../../crates/deployment/platform-deployment-contracts/src/openbao.rs) 定义；
[共享 crate](../../../crates/adapters/platform-openbao/src/lib.rs) 复用并公开这些类型。
[HTTPS 客户端](../../../crates/adapters/platform-openbao/src/client.rs)、
[Transit](../../../crates/adapters/platform-openbao/src/transit.rs) 和
[KV v2](../../../crates/adapters/platform-openbao/src/kv.rs) 分别实现受控传输、引用加解密和精确版本操作。
本记录不维护另一份字段枚举或候选方法签名。

`install` 只读取有界、固定私有文件并构建 TLS client，不写外部状态。endpoint 是 HTTPS origin；
拒绝 userinfo、query、fragment、非预期 path、redirect、环境代理及调用者指定完整 URL。mount、
auth role、key name 与相对 path 使用各自有界类型，拒绝空段、`.`、`..`、编码分隔符和控制字符。
首版不隐式发送 namespace header；若安装需要 namespace，必须作为显式有界身份加入配置、摘要和测试。

配置、绑定和 opaque 引用均 versioned/closed；固定字段的摘要由单一 owning builder 计算。
部署配置记录私有文件引用与必要公开 TLS 身份，不把私钥、token、API key 或其公开哈希纳入摘要。
secret file 的实际值仅私有读取，不能进入 Debug、日志或 HTTP 错误。传输返回含 Secret 的 JSON
必须为不可随意 Debug/Clone、drop 清零的有界容器；不会暴露任意 URL 或行政 API escape hatch。

cert login 显式发送固定 role name。短 token 可仅缓存内存，按服务端 TTL 和本地单调期限失效，
不自动延长；这个缓存不是 Security 授权缓存。登录、TLS、请求和 body 读取共用调用者的绝对 deadline。
不自动重试 POST/PUT，不因 401 重新发送可能已执行的写入。缓存命中后发生拒绝，当前调用失败。

错误闭集由部署 owning type 定义。禁止返回原始响应、token、URL、底层异常或 Secret。
不是每个 HTTP 400 都代表 CAS
冲突，也不是每个 404 都证明不存在；密封、认证失败、缺失 mount、错误 cluster、截断与超量响应
不能进入“新建”分支。可能发出的写入在无法获得精确读回证据时返回 UnknownOutcome。

## Transit：真实加密与身份

真实原语测试使用已发布的 OpenBao v2.6.1，物理 fixture 固定实际镜像身份，未跟随 latest。
官方 [v2.6.1 release](https://github.com/openbao/openbao/releases/tag/v2.6.1) 是版本来源；
该提供者原语证据不等于完整平台或生产资格。

Transit 显式使用 `aes256-gcm96` 随机 AEAD；不启用 convergent encryption，不传自制
nonce，不导出密钥或允许 plaintext backup。encrypt 指定非零 key_version，验证响应实际版本；
decrypt 只接受该绑定允许的 `vault:v<version>:` 密文，不能改用当前 latest key 或明文 fallback。
服务端配置的密钥类型、version 范围、exportable/backup/deletion 设置必须通过 readiness 检查。
OpenBao 的 [Transit API](https://openbao.org/docs/api/secret/transit/) 明确区分 `associated_data`
与用于 derived key 的 `context`；本方案使用前者承接 AEAD 绑定，不能仅传普通业务字段后声称已认证。

AAD 由现各 Broker owning 层构建为 canonical bytes，含域分隔和版本：Artifact 保留 tenant、blob、
storage binding、encryption domain、key identity；Secret 保留 tenant、binding、provider、generation、
key identity。共享客户端只传输这些字节。跨租户、代次、key、storage binding 或两类引用交换必须解密失败。

物理身份包含观察到的 cluster_id、mount accessor、key name/version 及配置摘要。runtime 不可创建、
轮换、删除或重配 Transit key/mount。丢失或替换密钥会使旧密文明确不可解密；不以同名新 key 重建旧身份。

## KV v2：固定请求、精确版本与销毁

每个 preparation 使用固定 namespace 下由 tenant 与原 preparation digest 派生的独立 path。
Model import 的 digest 继续只来自既有请求身份，不含 API key；MCP PKCE/token 继续消费各自既有
preparation 身份。共享准备 envelope、校验和敏感值清零逻辑从当前 AWS 私有实现提取到 Secret Broker
自己的模块，避免第二份身份/内容业务判定。

第一次写只允许 `options.cas=0`，只接受版本 1；后续请求绝不写版本 2。读回总是指定 version=1，
严格核对 envelope kind、完整冻结身份和候选敏感内容；不同内容复用同一请求必须拒绝。
并发写、CAS 冲突和写响应丢失均转为同一 exact path/version 的有界读取，不能换 path/UUID/token。
未读到确定结果或 deadline 已过保持 UnknownOutcome，原 Receipt/恢复记录不变。
写入已成功或可能执行后，读回拒绝、权限过期、无效响应或不可读都不能证明写入未发生；
这些路径保留 WriteUncertain 并由公共 Broker 返回 OutcomeUnknown。实际读回并验证出的内容冲突
仍明确拒绝，写前授权拒绝也不转成写入不确定。
服务端 [KV v2 API](https://openbao.org/docs/api/secret/kv/kv-v2/) 提供 CAS0、指定版本读取及 exact
version destroy；不能用 latest 读取或逻辑 key 删除替代这些语义。

`opaque_version_identity_digest` 不能是 `sha256("1")`：KV 数字版本只在一个 path 内有意义。
新的 closed opaque reference 绑定 provider/config、cluster/mount 身份、固定 path、version、material
kind；摘要覆盖该完整物理身份，不覆盖原 API key。解密后的 path 仍须验证在安装前缀及当前 tenant
范围内。readiness 按已冻结 accessor 验证 mount 的 KV v2 类型，避免 remount 后旧 version1 重绑定。

仅 pinned policy 可调用该准备和删除路径。MCP 已选的 PKCE 创建/读取、token prepare/load/resolve
与 exact delete 必须实现；若某种 provider rotation 或 material kind 未支持，安装/配置时明确拒绝，
不能接受配置后在真实执行中用 trait 默认 Rejected，也不能把 FollowProviderRotation 默认为 pinned。

物理清理只调用 `destroy` 的单个精确 version，随后核验 metadata 显示该版本 destroyed。
soft delete 不等于 destroyed；未知 404 不足以证明已销毁。允许已确认的同一墓碑返回 AlreadyAbsent，
无法证明时仍 OutcomeUncertain。禁止 metadata DELETE、latest DELETE、批量 version 销毁和 undelete；
墓碑保留，避免 CAS0 重新创建旧 path。普通 SecretBinding revoke 仍只改变原 PostgreSQL 状态。

## 工厂、权限与 S3

Process catalog 使用显式物理类型分支，保留真实 AWS 和 OpenBao 各自的严格 decoder；不自动侦测
endpoint 协议，不试一个失败后 fallback 另一个。
[Artifact catalog](../../../crates/adapters/platform-artifact-broker/src/provider_config.rs) 和
[Secret catalog](../../../crates/adapters/platform-secret-broker/src/provider_config.rs) 已采用新的顶层版本，
Artifact Gateway/Data/Maintenance、Egress、initializer、CLI 与共享 renderer 按同一 owner 接入；
共享 one-shot 安装接线已通过下述有明确范围的 Compose 与 Kind 实测。
上传 request/staged/evidence 类型去除不真实的 AWS 名称；stage evidence 仍由现 Artifact owner
验证真实 generation、HEAD/GET bytes/hash 和 locator context，PG material 不出现假 ARN。

S3 继续要求版本化、非空真实 generation、If-None-Match 条件写、精确 HEAD/read/delete、实际字节
摘要、限量响应、原 presign 期限与 CORS。换 Transit 不改变 S3 object bytes、上传 CAS、scan、quota
或 Artifact retention/GC。新的 S3 产品在这些能力和完整重启测试通过前不进入默认安装。

本地 profile 的 SeaweedFS 4.46 有已核实的 IAM 限制：动作解析只判断 `versionId` 参数是否出现，
对象处理却把空值当作 latest；因此 `versionId=` 可被分类为版本读取或版本删除，随后执行 latest
读取或创建 delete marker。Root 与 Product 联合接受它作为非生产本地 profile 的明确限制，
不能宣称提供者 IAM 强制执行了逐对象、逐版本的当前授权。IAM 仅承担动作分类和固定 bucket
隔离；Broker/PG 仍是当前授权与精确版本权威。平台 SDK 路径拒绝空 generation，读取验证返回
版本与正文摘要，删除验证原版本且拒绝 delete marker 回执；不新增普通 Write/DeleteObject 权限。
角色凭据不经公共 API 交付 Console/CLI，也不交付不可信执行代码，管理面不因此公开。基础 S3 原语测试不替代最小
IAM 角色矩阵；跨 bucket、匿名、普通写入拒绝和公开签名请求不可篡改仍需独立证据，不能转授生产隔离资格。

服务端 ACL 只允许固定 cert role、固定 namespace 和职责所需的操作：artifact sealer 可 encrypt，
reader/worker/maintenance 按需要 decrypt；Egress 可对专属 KV 前缀 create/read，并对实际 cleanup
路径 destroy/read metadata，不获得 update/patch/metadata-delete/undelete。create 与已有 key 的
update 是不同能力，CAS0 不能替代 ACL。KV v2 不支持用 ACL allowed_parameters 约束这些 JSON，
不能把未经支持的参数规则当安全证据。

readiness 只使用精确的 [GET /sys/mounts/:path](https://openbao.org/docs/api/system/mounts/) 读取所需
mount 身份，不需要全 mount 列表或任何 sys 写权限。业务角色不能持有 initializer/root token、seal
key 或能给自己增权的证书签发者私钥。仅持有实际所需的 mTLS leaf 与最小 ACL，TLS 不依赖跳过验证。

## 初始化与恢复：联合接受的安装子边界与实际证据

OpenBao 使用专属持久存储，例如 [Raft](https://openbao.org/docs/configuration/storage/raft/)，
对象服务、OpenBao 与 PG 分别具有明确 volume 身份；单节点持久化不宣称 HA 或生产恢复资格。
初始私有材料在副作用前写入受控文件并 fsync，重启不能生成新 seal key、cluster 或 namespace。
Ready 后安装 verify 只核验物理身份与不可变种子，合法业务轮换、撤销、默认选择和额度使用不应触发重建。

直接 `/sys/init` 返回的唯一密钥/root 材料若响应丢失，不能靠再次 init 读回。稳定版提供
[static seal](https://openbao.org/docs/configuration/seal/static/) 和
[self-init](https://openbao.org/docs/configuration/self-init/)，可作为本地开发初始化候选：seal key
先由部署者保存并只挂给 Bao，自初始化建立可重新认证的受限部署身份，不向 serving 交付 root。
静态 seal 的安全基础是宿主/部署 Secret 的保管，不能宣称等同外部 HSM。本地 Compose 与本地 Helm
使用同一私有卷信任边界：只有 OpenBao 依赖获得自己的 seal 文件，serving 不持有该文件、初始化
证书私钥或签发者私钥。真正的生产 Helm 消费部署者显式提供的 trust/seal/auth 输入，不把本地
静态 seal 称为生产 KMS 隔离，也不从平台业务 Secret 反向解锁自身。

self-init 不能被假定为跨所有请求的事务或自动恢复协议。上游曾专门修复
[部分初始化失败后重启继续运行的问题](https://github.com/openbao/openbao/pull/2908)。本轮必须在固定
实际版本上证明：各阶段中断/响应丢失后可恢复原身份，或明确失败并保留现场；不能删 volume、忽略
初始化错误、增加 allow_failure 或在已初始化缺材料时悄悄再次创建。

**已确认的首次 bootstrap 失败窗口：** Operations 对 v2.6.1 owning 源码核验表明，Initialize 先持久化，
随后才写 self-init marker 并安装认证。在两者之间 crash，重启不会重复完成该初始化；若此前没有
可用认证，部署者不能通过受控 API 自动修复；不能在文档或测试中省略这个窗口，也不能声称首次
初始化的所有中断点都可自动恢复。

联合接受的最小边界为：ProviderReady 之前发生异常，以 Incomplete / ExternalOutcomeUnknown
明确失败并保留同一卷、文件和身份；禁止 automatic self-init retry、repair、删除数据或创建新身份。
只有实际认证成功且 cluster、mount、key 全部验收后才确认 ProviderReady，之后平台 one-shot
provision 必须按冻结身份 exact resume。该区别不新增业务表或候选目录发布状态机。
完整重建 dependency container/pod 后必须证明同一 cluster/key/Secret/object 和合法业务状态保持。
运行时 adapter 可独立实施，不根据单测或 /health 宣称默认安装已通过上述完整验收。

此处的只读 verify 允许为当前认证取得短 token，以及提供者自身必要的审计和认证 lease 记录；
不承诺提供者存储逐字节不变。它禁止 mount、policy、cert role、key 的配置写入，以及业务 Secret
或对象写入、续期、重建和修复。任何已发布 ProviderReady 身份失配或原卷缺失均明确失败；不得
重新 self-init、替换 cluster 或以同名新 key/canary 冒充恢复。完整安装 Ready 与物理提供者
ProviderReady 分开判定，后者不能代替 PostgreSQL、角色和业务引用的实际验收。

已完成独立物理证据：固定 OpenBao 2.6.1 与 SeaweedFS 4.46 的真实 SDK 原语通过；专属初次
ProviderReady 后保留原 Transit ciphertext/AAD、KV version 1 与 S3 Artifact generation，再干净
停止并重建两个容器、保留同卷，原材料均可精确解密/读取。对应运行记录为
`insight-provider-recovery-seed.log`、`insight-provider-controlled-recreation.log`、
`insight-provider-recovery-after-recreate.log`；独立空卷仅用 serve 配置启动仍保持未初始化、sealed，
记录为 `insight-provider-empty-volume-negative.log`。这些是受控关闭/容器重建的提供者证据，
不代表掉电、HA 或用户业务恢复已通过；完整安装与依赖重建由下述独立安装验收承接。

## Secret adapter：已接受并实现的消费合同

[Secret provider 配置](../../../crates/adapters/platform-secret-broker/src/provider_config.rs)、
[工厂](../../../crates/adapters/platform-secret-broker/src/catalog.rs)、
[OpenBao 实现](../../../crates/adapters/platform-secret-broker/src/openbao/mod.rs) 与
[opaque reference](../../../crates/adapters/platform-secret-broker/src/openbao/reference.rs)
共同承接已接受的边界；共享准备 envelope 的唯一 owner 是
[prepared.rs](../../../crates/adapters/platform-secret-broker/src/prepared.rs)。

catalog 复用已有有界容量，拒绝重复 provider ID、同一 cluster/mount 下重叠可写前缀和歧义引用。
OpenBao 物理配置摘要绑定 provider、cluster、KV/Transit 身份、前缀和精确 readiness；角色认证、
TLS 文件路径与超时由完整 process 配置冻结，不混入 opaque 物理身份，敏感文件内容不进入摘要。
更换合法角色文件不改变旧引用，cluster、mount、key version 或前缀改变则拒绝旧引用。
共享配置、绑定及校验/摘要方法直接复用部署 owning types，
Secret Broker 不维护另一份地址、TLS、认证或密钥版本规则。readiness 是部署者实际写入、可公开确认
摘要的非秘密 canary；不使用用户 credential 做健康检查，确切版本同样不得回落到 latest。

opaque JSON 存在既有加密引用内，最大 16 KiB；解密后逐一校验 provider、config、tenant、KV binding
和前缀。version 是非零有界整数，准备路径固定为 1。material_kind 闭合为本次实际实现的
ModelCredential / McpOAuthPkce / McpOAuthToken；外部预置 Raw 或 FollowProviderRotation 如需支持，
应另给精确解析/轮换合同，不能仅新增枚举标签接受。已选用它们的部署在安装校验时拒绝。
version identity 是含 domain/schema 的 canonical 物理引用摘要，绝不散列 Secret 正文。

工厂返回现 `SecretReferenceSealer`、`SecretReferenceUnsealer` 和 `InstalledSecretProviderCatalog`。
多物理后端的 sealer/unsealer 按当前 provider_id 与冻结 key/config 精确选择，不按密文前缀尝试所有
密钥。当前 `AwsSecretProviderCatalogConfig` 的旧无标签外形不做自动探测：process owner 消费新顶层
版本，现 AWS 子配置仍按原 owner 验证，全部 renderer 和 owning fixtures 同次切换。
Model prepare/resolve/delete 和已选 MCP PKCE/token 方法已有真实物理实现与下述原语证据；
这些结果不替代现有 Security/PG 当前授权或完整安装链路的验证。

## 已取得的证据与剩余范围

共享客户端的单测与[合成 HTTPS 测试](../../../crates/adapters/platform-openbao/tests/transport.rs)
已通过，证明客户端 TLS、期限、响应界限及未知写入处理；合成服务没有被当作真实 OpenBao。
[Secret 写后故障测试](../../../crates/adapters/platform-secret-broker/src/openbao/transport_tests.rs)
通过公共 Broker 复现旧实现将已存储后的读回 403 误报 Rejected，再验证修复后读回拒绝、损坏、
消失和未知写响应保持 OutcomeUnknown。同请求恢复只读原版本，不重新创建；不同敏感内容仍拒绝。

真实 OpenBao 2.6.1 的
[Secret 物理测试](../../../crates/adapters/platform-secret-broker/src/openbao/physical_tests.rs)
已显式执行并通过（1 项，未忽略，0.22 秒）：Model 并发 CAS0、PKCE/token 重放、真实 Transit
seal/unseal、精确 KV resolve、错误 tenant/generation AAD 拒绝，以及各测试版本的精确销毁、重复
销毁和墓碑防复活。测试使用专属 Egress 叶证书与合成 Secret，未调用用户模型或代行人工批准。

真实 SeaweedFS S3 与 OpenBao Transit 组合的
[Artifact 物理测试](../../../crates/adapters/platform-artifact-broker/src/aws.rs)
也已显式执行并通过（1 项，未忽略，0.17 秒），覆盖真实 installation stage/readback、上传完成证据、
locator 加解密、错误租户拒绝、精确 generation 读取与删除；共享 renderer 的实际配置导出通过。
这是物理 adapter 测试，不是公共 API、scan/Ready、完整窄角色链路或 PostgreSQL 事务的端到端证明。

同卷重建后的冻结 ciphertext/AAD、KV1、Artifact locator 与对象保持已通过上文列明的独立提供者测试。
最终 I 镜像的 Compose 首次启动失败记录仍保留，原错误未捕获，不倒推原因；随后经明确授权，
同一 input、镜像与私有目录完成首次安装、内部模型 Policy Artifact stage/readback、只读验证和配置
漂移拒绝。所有服务及四个依赖按原容器身份正常退出后，仅重建容器、保留原卷，再次 Ready 和
只读验证通过，冻结安装 JSON 摘要保持一致。该成功单独记为显式恢复结果，未覆盖原失败报告。

同一镜像的独立 Kind 验收通过首次初始化、常规 OpenBao 服务的本次精确观察、全部基础服务 Ready、
实际 S3 DNS/TLS 正反检查、只读验证、Gateway Pod 替换恢复与私有会话交付，并完成测试集群清理。
Kind 使用空模型目标，不宣称模型 Policy 种子或整组 Kubernetes 依赖重建已验证。完整用户业务恢复、
生产资格、真实检索和人工审批未由这些安装测试关闭。下列矩阵保留完整验收要求，不表示每项已完成。

## 必要证据矩阵

1. 共享传输：真实 CA/SAN 与 cert role；错误 CA/角色/cluster/accessor，redirect、TLS/读 body
   超时、body 超量/截断、重复 JSON key、错误字段/整数；deadline 覆盖 login 和所有读回；写入不重试；
   token/Secret/key/ciphertext/canonical opaque reference 不进入 stdout、stderr、Debug 或错误正文。
2. Transit：真实 encrypt/decrypt、明确版本、全部 AAD 维度变异、错误密钥/算法/cipher version；
   重启后原密文可解；非授权角色不得导出、备份、删除、轮换或创建 key。
3. KV：同请求并发同内容只产生 version1；不同内容冲突；成功写丢响应后 exact winner；写前/写后
   deadline、拒绝和不可读分支；mount/tenant/path/version/material 漂移；destroy/readback、重复
   destroy、soft delete 和 metadata 缺失不误报；墓碑阻止旧请求复活。
4. Secret Broker：真实 Model import→PG commit→resolve，失联恢复、最终权限撤销、Receipt 过期
   后现有 Binding authority；MCP transient/token 的全部已选路径；不以 adapter 单测替代 Security
   当前授权，不读取或调用用户真实模型作为凭据测试。
5. Artifact：实际非零 prepare→PUT→complete→scan→Ready→reader，installation stage/readback，
   Context/MCP internal stage 和 exact generation delete；所有实际窄角色；原过期/CAS/hash 拒绝不变。
6. 安装：固定镜像、多架构、初次及中断恢复；完整停止/重建所有依赖进程但保留卷，证明 S3 generation、
   原 Secret 版本、Transit 旧密文、PG exact refs 仍有效。缺 key/volume、错误权限和错误身份均 fail
   closed；证据只输出安全 IDs、状态与摘要。最后再跑原完整业务/清理与性能门禁，不改预算。

完成审查与证据后，将耐久决策简写进 ADR-0010/current operations，并删除本临时设计文件；不得将
本评审、外部产品文档、模拟 HTTP 或单次 /health 成功记作平台持久性验收。
