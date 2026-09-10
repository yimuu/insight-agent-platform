# 发布文档检索与人工审核 Agent

以下命令从仓库根目录执行。`WORK` 是本次作者工作目录，`CONSOLE_ORIGIN`、`TENANT_ID`、
`SESSION_FILE` 和 `PUBLIC_CA` 使用[统一安装工具](../../../docs/current/installation.md)真实交付的值。
路径使用绝对路径；session 只从私有文件读取。公开服务证书已受信任时可以省略附加 CA，默认本地安装的 S3 上传需要它。

```bash
umask 077
mkdir -m 700 "$WORK"
insight connect --path "$WORK" --endpoint "$CONSOLE_ORIGIN" \
  --tenant "$TENANT_ID" --token-file "$SESSION_FILE" --ca-file "$PUBLIC_CA"
cargo build -p insight-platform-contract-tooling --bin document_review_source
SOURCE_TOOL="$PWD/target/debug/document_review_source"
```

## 1. 部署实际文档服务并声明目的地

先按 [README](README.md) 将服务和冻结 corpus 部署到自己的公开 HTTPS DNS 名。
平台不允许将 loopback/内网或临时 DNS 绕过当作 Remote Context 部署。
创建 `endpoint.json`，内容使用当前 `CanonicalHttpEndpoint`：`scheme` 为 `https`、
`host` 为自己的 DNS 名、`port` 为实际 TLS 端口、`base_path` 为 `/v1/query`。
证书根只含公共 PEM；`REGION` 是操作者明确选择的实际部署标签。

```bash
"$SOURCE_TOOL" --destination "$WORK/endpoint.json" "$REGION" \
  "$DOCUMENT_ROOT_CA" "$WORK/document-destination.json"
jq '[.]' "$WORK/document-destination.json" > "$WORK/remote-context-destinations.json"
```

该命令由 Foundation owner 计算协议、结果映射与 endpoint 摘要，固定本例匿名映射和字节限额。
它只构造类型，不证明 DNS 可达、证书有效或检索通过。三个安装输入工厂 `compose-input`、
`kubernetes-input`、`native-input` 都可在原有参数后增加
`--remote-context-destinations /absolute/remote-context-destinations.json`；容器入口须将该公共文件只读挂载到对应路径。
首次 `up` 前生成输入；非空列表由共享 renderer 启用 ContextRemote 及其角色配置。已准备的安装不可编辑输入。
安装本身不访问文档服务，也不创建下面的业务资源。

## 2. 取得真实协议证据

此命令对指定公开服务执行六个有界请求，复用生产 Remote Search wire codec、公开地址检查、精确 DNS pinning 和显式 TLS 根。
没有模型请求或业务身份，不伪造 Context 授权。缺少环境变量时明确失败。

```bash
mkdir -m 700 "$WORK/protocol-evidence"
PLATFORM_DOCUMENT_REVIEW_DESTINATION_FILE="$WORK/document-destination.json" \
PLATFORM_DOCUMENT_REVIEW_REPORT_FILE="$WORK/protocol-evidence/conformance.json" \
cargo test -p insight-platform-egress --lib \
  remote_context::document_qualification::live_document_provider_protocol_conformance \
  -- --ignored --exact
```

首次网络操作前会独占创建 `.attempt.json` 和 `.progress.jsonl` 并 fsync；每次发送前记录 `maybe_dispatched`，
完成后记录安全摘要。超时或失败保留已有记录，不重试、不重用路径；只有全部通过才产生 `conformance.json`。
该报告只证明实际端点的 TLS、wire 和 corpus 一致性，不证明平台授权、Agent 能力或人的决定。
保留失败证据；需要另一轮测试时由操作者明确选择新路径，不能把旧 unknown 记录删除后当作首次请求。

上传通过的原始报告，保留 CLI 返回的真实 Artifact ID、内容摘要、大小和媒体类型：

```bash
insight advanced artifact upload --path "$WORK" \
  --file "$WORK/protocol-evidence/conformance.json" \
  --purpose authoring_document --classification internal --media-type application/json \
  > "$WORK/conformance-upload.json"
jq '{artifact_id,content_digest,byte_length,media_type,classification:"internal",display_name:null}' \
  "$WORK/conformance-upload.json" > "$WORK/conformance-artifact.json"
```

未产生通过报告时停止这条发布流程，不用本地单测、source digest 或任意 JSON 冒充已部署提供方证据。

## 3. 发布 Policy、Interface、Implementation

Context 使用自己的实际 PolicyRevision。可以选择已有且适用于本端点的策略，或通过下面的构造器发布声明。
`policy-input.json` 的形状为 `{"kind":"policy","display_name":"...","policy_kind":"parser","rules":{...}}`。
`rules` 必须是操作者审阅的完整真实声明；构造器计算其摘要并把原文留在 authoring Artifact 中，不凭摘要声称执行了新策略引擎。

| 用途 | Policy kind | 本例声明应对应的事实 |
| --- | --- | --- |
| slot 授权、接口 entitlement、cache | 各自独立 `authorization` | 当前 ContextQuery 授权；仅该公开 corpus；不承诺跨 Query 缓存 |
| parser / chunker | `parser` / `chunker` | 冻结 UTF-8 文本；服务实际按空行和字节上限分段 |
| ranking | `ranking` | 英文词项/中文双字词项交集比例，整数百万分，locator 升序破同分 |
| data | `data_flow` | 请求可含 Internal 问题，返回公开原文；明确本端点 region 与限额 |
| network / TLS / trust | `network` / `tls` / `trust` | 本例 endpoint、HTTPS/证书根、实际 provider 和 ObservationOnly 引用 |

每份声明都通过相同普通生命周期。下面用 `parser` 举例；`--resource-source` 输出 canonical bytes，上传后不要改写或增加换行：

```bash
"$SOURCE_TOOL" --resource-source "$WORK/parser-input.json" "$WORK/parser-source.json"
insight advanced artifact upload --path "$WORK" --file "$WORK/parser-source.json" \
  --purpose authoring_document --classification internal --media-type application/json \
  > "$WORK/parser-upload.json"
jq '{artifact_id,content_digest,byte_length,media_type,classification:"internal",display_name:null}' \
  "$WORK/parser-upload.json" > "$WORK/parser-artifact.json"
"$SOURCE_TOOL" --resource-publication "$WORK/parser-source.json" "$WORK/parser-artifact.json" \
  "$WORK/parser-requests.json"
jq '.create' "$WORK/parser-requests.json" > "$WORK/parser-create-body.json"
jq '.publish' "$WORK/parser-requests.json" > "$WORK/parser-publish-body.json"
```

这是本地请求文件集合，内含现有公开 create/publish 正文，**不是 `insight advanced apply` 输入**。
Policy 和 Interface 都可部署，`apply` 要求同时提供 Deployment；本流程必须先取得 InterfaceRevision 再发布引用它的 Implementation，
因此这三类资源统一通过原有公开 HTTP 分步发布。发送方法和 token 隔离见第 4 节 curl 片段；GET 不带正文、Receipt 或 If-Match。

1. 对 `POST /v1/{resource_noun}` 发送 `.create`（这里为 `policies`），使用首次发送前已持久冻结的独立 create Receipt。
   成功为 201；保存真实 Resource ID、Enabled 状态、draft generation、version 和 body/header ETag。
2. 预先持久保存另一 validation Receipt、该 Resource path 和原 ETag，然后空正文
   `POST /v1/{resource_noun}/{resource_id}/draft:validate`（202）。保存 Operation ID，
   用 `insight advanced operation wait "$VALIDATION_JOB_ID" --path "$WORK"` 等待原 Operation。
3. 仅原 Operation succeeded 后 GET 当前 Resource，核对身份、原 draft generation、完整 document 和本次 validation 结果，
   version 必须是原版本加一次 validation 更新；漂移停止。保存新的 body/header ETag，不能从 opaque ETag 自行提取版本。
4. 预先冻结 publish Receipt、上述 ETag 与 `.publish` bytes，然后
   `POST /v1/{resource_noun}/{resource_id}/draft:publish`（200）。保存响应，核对 Resource、版本、原 Artifact 和内容摘要。
   当前响应中的 `published_versions[0]` 就是本例的真实版本；所有后续 exact refs 使用它。

每步发送前把 body/path/Receipt/ETag 持久保存并 fsync，失败保留；未知结果只重放原身份，不新建 Resource/Receipt 或换 CAS。
对 Policy 的实际 publish response（下例保存为 `parser-published.json`）提取：

```bash
jq --arg kind policy_revision \
  '.published_versions[0] | {revision_id:.resource_version_id,resource_kind:$kind,semantic_digest:.content_digest}' \
  "$WORK/parser-published.json" > "$WORK/parser-exact.json"
```

Interface 与 Implementation 分别使用 owner kind `context_source_interface_revision` 和 `context_source_implementation_revision`。
新 constructor 输出文件拒绝覆盖；它不发送 HTTP，不保存业务恢复状态，也不代表服务器已接受这些声明。

接着用相同命令序列发布 `interface`，再发布 `implementation`：

```bash
jq -n --arg region "$REGION" --slurpfile entitlement "$WORK/entitlement-exact.json" \
  --slurpfile cache "$WORK/cache-exact.json" \
  '{kind:"interface",display_name:"Document review corpus",region:$region,
    entitlement_policy:$entitlement[0],cache_policy:$cache[0]}' > "$WORK/interface-input.json"
# 使用上面 source → upload → ArtifactRef → public create/validate/publish → exact 的序列。
jq -n --slurpfile interface "$WORK/interface-exact.json" \
  '{kind:"implementation",display_name:"Document review Remote Search",
    interface_revision:$interface[0]}' > "$WORK/implementation-input.json"
# 同样发布 implementation；它冻结已发布的 interface-exact.json。
```

构造器产生与 Agent 匹配的 query/item/observation schema、ExternalObservation/RemoteOpaque 合同和 Remote Search 协议。
它验证 Artifact 确实绑定这些 source bytes，也拒绝规则或 schema 摘要与声明原文失配。
样例只取一条结果、最多一条引用；原文大小不能代替包含引用元数据、问题、平台/作者指令与 schema 的完整模型预算。
默认问题的本地 corpus 测试同时验证真实条目能进入 Domain 的观察形状和实际 Compute 输出；这不是一次 live Context Run。

## 4. 创建并激活已有接口的 Deployment

构造 `closure.json`，采用现有
[`ContextDeploymentClosure`](../../../crates/foundation/platform-contracts/src/resource.rs)：
`interface`、`implementation` 用上述真实 exact refs；`backend` 用安装的同一个 RemoteSearch endpoint、endpoint 摘要和 region；
本例 `secret_bindings` 为空、`embedding_model_deployment` 为 null。七项 parser/chunker/ranking/data/network/TLS/trust refs 必须各自正确且互异；
`conformance_evidence` 使用刚才真实上传的报告 ArtifactRef。
输入 `closure.json` 暂不包含 `required_worker_manifest_digest`；构造器只允许从显式交付的实际 WorkerManifest 推导它。
部署操作者从本安装 `<output>/roles/context-remote/config/context-remote.json` 仅提取公开的 `worker_manifest` 字段
为 `context-worker-manifest.json`，交付时核对当前安装、角色和实际二进制。不要复制 environment 或 credentials 目录。
构造器用现有 WorkerManifest owner 验证当前 Remote Context capability 并计算 canonical 摘要，
不接受调用方额外填写或覆盖这个字段，也不用镜像 digest 或整个配置文件摘要代替。

```bash
"$SOURCE_TOOL" --context-deployment "$WORK/closure.json" "$WORK/context-worker-manifest.json" \
  development "$WORK/deployment-body.json"
```

最终两步使用既有公开 HTTP 路由。不要再对 Interface 执行一次带 deployment 的 `apply`：那会再次发布接口，而 Implementation 冻结的是原 InterfaceRevision。

1. 当前认证下 GET `/v1/contexts/{resource_id}`，核对 Tenant、Resource、Enabled、接口 head 及 body/header ETag。保存返回 body 和完整 ETag。
2. 在任何发送前，把独立随机 `Idempotency-Key`、原 ETag、固定 URL path 与 `deployment-body.json` 持久写入 0600 文件并 fsync（目录也 fsync）。
   POST `/v1/contexts/{resource_id}/deployments`，`Content-Type: application/json`，携原 `If-Match` 和 Receipt。成功应为 201。
3. 保存并核验 Deployment 的 Resource、原 InterfaceRevision、closure、environment 和 body/header ETag。重新 GET Resource，要求 version 恰为第 1 步的 version+1，再冻结该 ETag 和另一独立 activation Receipt。
4. POST `/v1/contexts/{resource_id}/deployments/{deployment_id}:activate`，正文为空，携冻结的 activation ETag/Receipt。成功应为 200。
   最后独立 GET 当前 Resource，要求 Enabled 且 active_deployment_id 为目标 Deployment。Receipt 回放的旧成功响应不证明当前 active head。

发送未知或连接中断时保留原 path/body/ETag/Receipt；在当前认证下重放原命令，不换键、不读取新的 CAS 后自动重试。
当前 head 漂移或 409/412 停止并交操作者处理。可显式续发同安装 session，但不能将续发当作重建业务意图。

可以用 curl 发上述请求。token 不放 argv、URL、环境变量或命令历史；下面将它从私有文件经 stdin 交给 curl 的配置读取器。
`METHOD`、`PUBLIC_PATH`、`RECEIPT`、`ETAG` 都来自先前已持久冻结的非秘密请求文件；GET 不发送后两个头。
`BODY_FILE` 是固定正文路径，activation 使用明确的空文件。每次尝试使用新的 response 文件名，保留旧响应。

```bash
python3 - "$SESSION_FILE" <<'PY' | curl --config - --silent --show-error \
  --noproxy '*' --proto '=http,https' --connect-timeout 5 --max-time 30 \
  --cacert "$PUBLIC_CA" --request "$METHOD" \
  --header "Idempotency-Key: $RECEIPT" --header "If-Match: $ETAG" \
  --header 'Content-Type: application/json' --data-binary "@$BODY_FILE" \
  --dump-header "$WORK/attempt-headers.txt" --output "$WORK/attempt-body.json" \
  --write-out '%{http_code}\n' "$CONSOLE_ORIGIN$PUBLIC_PATH"
import os, re, stat, sys
fd = os.open(sys.argv[1], os.O_RDONLY | os.O_NOFOLLOW)
with os.fdopen(fd, 'rb') as f:
    m = os.fstat(f.fileno())
    assert stat.S_ISREG(m.st_mode) and m.st_nlink == 1 and m.st_mode & 0o077 == 0
    token = f.read(16385).removesuffix(b'\n')
assert len(token) <= 16384 and re.fullmatch(rb'[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+', token)
print('header = "Authorization: Bearer ' + token.decode('ascii') + '"')
PY
```

不要使用 `-k`、redirect 或 `--retry`。该片段只负责发送；按上面的 owner 检查判断结果，不能凭 curl exit0 宣称成功。
跨 HTTP 的闭合响应、ETag、Receipt 合同见 [OpenAPI](../../../contracts/platform-v1/openapi.yaml)。

## 5. 解析依赖、发布和运行

先完成[模型配置](../../../docs/current/model-configuration.md)并设置有限额度和默认模型。
查询 `insight agent dependencies context --path "$WORK" --environment development` 验证真实 Context 可见且可调用。
从公开 `GET /v1/agent-authoring-profile` 取得模型的实际 `selection_policy`；用第 4 步的 actual Context Deployment 和已发布策略生成选择器：

```bash
jq -n --arg resource "$CONTEXT_RESOURCE_ID" \
  --slurpfile authorization "$WORK/authorization-exact.json" \
  --slurpfile ranking "$WORK/ranking-exact.json" \
  --slurpfile profile "$WORK/public-authoring-profile.json" \
  '{documents:{kind:"context",deployment:{kind:"active",resource_id:$resource,environment:"development"},
    consistency:{mode:"external_observation"},allowed_projection:[],
    authorization_policy:$authorization[0],ranking_policy:$ranking[0]},
    model:{kind:"model",candidates:[{kind:"default_model",environment:"development"}],
    selection_policy:$profile[0].models[0].selection_policy}}' > "$WORK/selectors.json"
"$SOURCE_TOOL" --bindings "$WORK/selectors.json" "$WORK/.insight/agent-binding-selections.json"
cp examples/productization/document-review/agent/*.json "$WORK/"
insight agent publish --path "$WORK" --file "$WORK/agent.json"
insight agent run document-review --path "$WORK" \
  --input '{"question":"平台如何处理持久状态和人工确认？"}' --detach
```

公开 resolver 在当前授权下返回 exact bindings 和 required-feature evidence；constructor 不生成这些证据。
保留返回的 Run ID，查看 `insight agent logs "$RUN_ID" --path "$WORK" --follow`。
如果检索、模型或发布失败，保留原 Run 与 journal；没有输出的失败 Run 不等于模型已经作答。

## 6. 人实际审核并提交

Run 到达 HumanWork 后，用 Console 的当前 Run values 查看 `retrieve` 的原观察和 `answer` 的模型草稿。
其公开路径是 `/v1/runs/{run_id}/values` 及 `/v1/runs/{run_id}/values/{value_id}/content`；对照
corpus 的 URI、source_revision、原始文件 SHA256、行范围和实际 excerpt，确认每条引用支持回答。
这些是原文件字节摘要；JSON canonical digest 和平台 ObservationOnly 引用有不同含义。

```bash
insight advanced task list --path "$WORK" --run-id "$RUN_ID" --purpose respondable
insight advanced task get "$TASK_ID" --path "$WORK" --purpose respondable
```

人审阅实际 Task schema、当前授权和服务器给出的 deadline，然后自行写 `review.json`，包含 `decision`（`approve` 或 `reject`）
和自己的 `comment`。不要复制一个预填批准值作为自动验收。该 Task 是 HumanWork，使用 typed input 提交：

```bash
insight advanced task submit-input "$TASK_ID" --file "$WORK/review.json" --path "$WORK"
insight agent result "$RUN_ID" --path "$WORK" --output json
```

最终结果分别保留 `draft` 与 `review`。Task 到期、权限变化或失败继续按原状态处理，不新造人的响应。
完整样例通过需要真实 Context dispatch/来源结果、真实模型答案、原 Task 的人工提交和原 Run 终态共同证明；
目前本地测试与安装启动证据不代替这些待外部端点和用户参与的结果。
