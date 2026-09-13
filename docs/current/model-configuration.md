# 配置模型来源

在 Console 的 **Models** 页面或 `insight model` 中管理模型。两者调用相同的公开 API：来源对应
ModelProvider，模型对应 ModelProfile，默认模型保存在 TenantConfig；没有另一套配置数据库。
模型服务的 HTTPS 地址、协议、区域和凭据直接在页面配置，不需要预先编辑安装文件。
地址随 Registry 来源部署冻结，出站服务从已经授权的版本读取，继续执行公网 DNS、TLS 和策略检查。
同一个来源可以配置多个模型，不同账号的来源互不覆盖，也不会自动切换或共用凭据。
软件升级保留模型冻结配置。发布时的 Worker 标识是来源证据；本次执行与取消按 PostgreSQL
Job 中实际领取时的 build 与租约校验，并仍要求当前 Worker 满足冻结适配器契约及执行能力。
已有来源在后续编辑模型时，同样按适配器名称和契约校验兼容性；软件构建标识变化不要求重发来源。
历史厂商调用记录属于对应版本的资格证据，不代表本次部署或所有厂商均已验证。

## Console

打开 Console，首次创建管理员账号，之后使用邮箱和密码登录。进入“模型配置”：

1. **添加模型**：没有已连接服务时，弹窗先选择模型服务并填写 API Key，再进入模型配置。
   提供阿里百炼、OpenAI、Anthropic 预设，也可以填写自定义兼容 HTTPS 地址和协议。服务名称自动填写，内部别名自动生成；“更多设置”可修改地址、名称和区域。
2. 填写厂商的 **模型 ID**。显示名称自动跟随输入，“高级设置”可调整名称、输入输出限制和累计执行额度。
   已连接的服务可用于多个模型；“连接服务”也可单独添加账户。新建模型默认输出预算为 2,048 Token，
   页面直接显示当前预算；回答还受 Agent 输出字段约束，修改模型不会重写已发布 Agent 的冻结引用。
3. **Test connection**：对这个已部署模型发送一次短请求，查看响应、拒绝、限流或超时等结果。
4. **Use as default**：将这个精确部署设为新 Agent 编写时使用的默认模型。

默认模型变化不改写已发布 Agent 或已有 Run 的依赖。新 Agent 的作者 profile 使用 `project/default` 这个可直接编译的模型引用；
发布时解析并冻结实际部署。未设置默认模型时 profile 不提供模型选项，失效的默认指针会明确报错。
无工具的 Model chat 使用明确的零工具预算；编译器冻结该预算，运行时仍核验实际模型能力。有 Skill 或 Capability slot 的计划必须配置正工具预算。
ModelLoop 回答其自身输出端口的封闭对象 schema。完整 Agent 可在后续人工任务或计算后组成另一份最终结果，
模型不会因此被要求提前生成尚未发生的人工决定。响应 schema 的指令计入原有提示和 token 预算。
失败诊断区分非法 JSON、输出约束不匹配和输出容量超限，并区分连接、流式空闲及总时限超时。
诊断只使用固定安全说明，不回传厂商文本；结构化结果仍严格校验，不自动修补、截断或按成功保存。

连接检测可能产生一次厂商调用费用。它仅检查协议响应；不能证明 Agent、文档检索、工具、流式输出、tokenizer
或数据保留承诺。基础配置启用文本和本地验证的 JSON 文本回退，工具与原生结构化输出关闭。
输入输出限制由操作者声明，出站分类上限为 Internal；未知的厂商训练、保留、定价和 tokenizer 信息保持未知。
未验证的用量报告能力保持未知；实际模型响应提供完整输入、输出 token 用量时按该观测结算，并校验
原准入额度。缺失或非法用量不能按零用量成功处理，派发后失败仍使用原有保守结算规则。
Responses 已识别的文本计费用量明细经过严格校验后丢弃，不与顶层 token 用量重复相加，也不代表
费用已知。完整终态证据保留实际明细；不支持或非法的扩展仍明确拒绝。
基础来源的响应预算预留 Inline 输出封装空间；网络、RPC 和规范化后的完整输出各自保留字节限制。
超过实际输出容量会明确失败，不截断内容、不自动转存 Artifact，也不为本地存储失败重放厂商请求。
模型 SSE 解析器接受标准换行和开头 BOM，丢弃 `id`、`retry` 传输元数据，不据此保存身份或重连。
业务事件仍须通过严格 JSON 与协议校验；连接结束不会把未完成事件补成成功响应。
Responses 空文本增量不产生可见事件，空文本片段仍须在完整响应中通过原结果校验。
可选空 fingerprint 只表示未观测；必需模型身份与用量校验保持不变，诊断不返回供应商字段值。
连接检测收到合法但因短输出预算而截断的 reasoning 响应，也会显示已收到协议响应；这不表示最终答案已完成，
reasoning 内容不会展示或写入诊断结果。

**Edit / rotate key** 使用新密钥创建新的精确凭据绑定并发布来源。旧的已发布依赖仍引用原绑定；如需阻止继续
使用旧密钥，在 **Credential** 中查看绑定后执行 **Revoke this credential**。撤销不可恢复，影响所有引用该绑定
的模型，不自动改写默认模型或切换来源。底层厂商密钥的注销仍由厂商账号管理。

添加与编辑在弹窗内完成，保存失败后保留原操作并在表单旁显示原因；关闭弹窗后可通过“继续添加”恢复。
额度和密钥管理仅在选择对应模型或服务后展开，操作结束会清除处理中状态。
发布、导入、默认选择、额度调整和撤销都有恢复入口。中断后沿用原命令身份、版本条件和已冻结输入；不能通过刷新版本号
覆盖他人的修改。浏览器仅在当前会话保存恢复元数据，API key 不写入浏览器存储；导入未确认时需重新输入同一密钥。

## CLI 与批量配置

会话文件必须是单链接的普通文件，权限为 `0600`，内容是一行 token。不要把 token 或 API key 放进命令参数。
下面的 shell 数组只保存非敏感的地址、身份和文件路径：

```bash
connection=(--endpoint http://127.0.0.1:8088 --tenant ten_REPLACE_WITH_ACTUAL_ID \
  --token-file /absolute/path/private-installation/session-token \
  --ca-file /absolute/path/private-installation/public-ca.pem)
insight model configure "${connection[@]}" --file models.json --state-dir /private/path/model-state
insight model sources "${connection[@]}"
insight model source "${connection[@]}" --source dashscope.work
insight model list "${connection[@]}"
insight model get "${connection[@]}" --model qwen.work
insight model quota "${connection[@]}" --model qwen.work
insight model probe "${connection[@]}" --model qwen.work
insight model default "${connection[@]}" --model qwen.work
insight model default "${connection[@]}"
```

替换为安装实际输出的 origin、Tenant ID 和会话文件；仅明确的 loopback HTTP origin 被允许，其他地址要求 HTTPS。
默认 Compose 安装的公共 CA 文件由安装工具交付；已有安装可按[公共 CA 导出说明](installation.md#obtain-the-public-ca)
执行 `public-trust`。这份 CA 同时用于对象上传。使用公开受信任证书的部署可以省略 `--ca-file`。
会话过期后使用部署工具显式续发。配置恢复目录权限为 `0700`，父目录必须存在；保留该目录以恢复中断的操作。

`models.json` 可以为每个来源单独映射环境变量名。下面沿用已有 `OPENAI_*` 名称接入 DashScope，第二个来源
使用自己的变量名。没有要求厂商必须使用某一组环境变量：

```json
{
  "schema_version": 1,
  "default_model": "qwen.work",
  "sources": [
    {
      "alias": "dashscope.work",
      "display_name": "DashScope 工作账号",
      "region": "cn-beijing",
      "protocol": "open_ai_responses",
      "environment": {
        "schema_version": 1,
        "api_key": "OPENAI_API_KEY",
        "base_url": "OPENAI_BASE_URL",
        "model": "OPENAI_DEFAULT_MODEL"
      },
      "models": [{ "alias": "qwen.work", "display_name": "工作 Qwen", "quota": { "requests": 20, "tokens": 204800, "cost_microunits": 20000000 } }]
    },
    {
      "alias": "anthropic.work",
      "display_name": "另一来源",
      "region": "global",
      "protocol": "anthropic_messages",
      "environment": {
        "schema_version": 1,
        "api_key": "TEAM_ANTHROPIC_KEY",
        "base_url": "TEAM_ANTHROPIC_URL"
      },
      "models": [{
        "alias": "team.chat",
        "display_name": "团队模型",
        "model_environment_variable": "TEAM_CHAT_MODEL",
        "quota": { "requests": 20, "tokens": 204800, "cost_microunits": 20000000 }
      }]
    }
  ]
}
```

只读取配置点名的输入，每个变量或文件在一次配置操作中读取一次。显式 `environment` 替换整个 preset 映射；
不扫描环境、不按厂商名猜测 API key。preset `openai`、`dashscope`、`anthropic` 的映射由
[CLI 输入类型](../../apps/insight-cli/src/model/configuration.rs) 定义；preset 不补造默认地址或模型。
来源间也禁止把同一个环境变量同时用作密钥和公开模型元数据。

每个模型可使用 `model` 字面值或 `model_environment_variable`，二者不能同时设置。同一来源的多个模型共享其
账号绑定。别名以小写字母开头，只能包含小写字母、数字、点、下划线和连字符，创建后不可改；显示名称可修改。
再次配置同一别名会通过当前资源的版本条件更新。

无人值守部署可使用 `api_key_file`，相对路径以配置文件所在目录为基准：

```json
{
  "schema_version": 1,
  "default_model": "qwen.work",
  "sources": [{
    "alias": "dashscope.work",
    "display_name": "DashScope 工作账号",
      "region": "cn-beijing",
    "protocol": "open_ai_responses",
    "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
      "region": "cn-beijing",
    "api_key_file": "/run/keys/dashscope",
    "models": [{
      "alias": "qwen.work",
      "display_name": "工作 Qwen",
      "model": "qwen3.8-flash",
      "maximum_input_tokens": 8192,
      "maximum_output_tokens": 1024,
      "quota": { "requests": 20, "tokens": 204800, "cost_microunits": 20000000 }
    }]
  }]
}
```

密钥文件必须是 `0400` 或 `0600` 的普通单链接文件，内容是可见 ASCII 密钥，可带一个末尾换行。
符号链接、硬链接、目录和超限内容会被拒绝；Kubernetes Secret 可通过 `subPath` 挂载为普通文件。
`api_key_file` 与显式 `environment` 互斥；其余公开字段可用字面值或模型的单独环境变量映射。

该 DashScope 地址是北京接入点，官方 Responses 文档列出 `qwen3.8-flash`；实际账号可用性仍由连接检测及真实
运行验证。[阿里云地址说明](https://www.alibabacloud.com/help/en/model-studio/base-url)，
[Responses API](https://www.alibabacloud.com/help/en/model-studio/qwen-api-via-openai-responses)。
安装端保存归一化地址 `/compatible-mode`，适配器追加 `/v1/responses`；用户配置继续填写 SDK base URL。

```bash
insight model default "${connection[@]}" --clear
insight model credential "${connection[@]}" --binding sbd_REPLACE_WITH_ACTUAL_ID
insight model revoke "${connection[@]}" --binding sbd_REPLACE_WITH_ACTUAL_ID --state-dir /private/path/model-state
```

`configure` 与默认选择完成后，再以相同输入执行会核对当前结果。若其他人已经改动当前状态，使用
`--new-attempt` 明确发起新意图；未完成操作必须先用原输入恢复。密钥轮换也需要新的 configure 意图：
用相同账号位置提供新密钥并显式使用 `--new-attempt`，不会通过比较或记录原密钥来自动推测轮换。
元数据读取和撤销只需要对应 Secret 权限，不额外依赖 ModelRead。

公开契约见 [OpenAPI](../../contracts/platform-v1/openapi.yaml)；资源的生命周期、精确部署、Artifact、Receipt、
权限和 ETag 仍使用各自已有 authority。

使用私有 CA 的安装时，所有 `insight model` 命令可显式传入 `--ca-file /absolute/path/public-ca.pem`。
命令只读取一次有界公共 PEM 证书包，将证书加入本次 API 和对象上传客户端的信任根；仍校验证书名称、
拒绝跳转且不转发平台令牌到对象存储。证书不会写入业务资源、Receipt 或系统信任设置。
普通公开 HTTPS 证书不需要此选项。Console 上传由同源服务使用部署 CA 转发，浏览器无需安装存储 CA。


## 执行额度

每个模型配置必须显式提供有限的 `quota`。上面的数值是示例；根据部署的实际准入估算设置上限。
额度属于精确 Model Deployment，分为请求数、token 和平台 cost microunits。它们是平台累计限制，
不是厂商的价格或账单。零表示不允许新增相应预留，不表示无限。租户模型并发限制由安装初始化。

Console 的 **Quota** 显示当前上限、已预留和已使用量，可单独调整上限；CLI 使用：

```bash
insight model quota "${connection[@]}" --model qwen.work
insight model quota "${connection[@]}" --model qwen.work --file limits.json --state-dir /private/path/quota-state
```

`limits.json` 只包含 `requests`、`tokens`、`cost_microunits` 三个非负安全整数。`--model` 也可填写已有的
精确 `mdep_...` ID，以管理仍被运行依赖的旧部署。读取需要 ModelRead，设置需要独立的 TenantManage；
发布权限不授予额度。调整不得低于当前使用量加预留量，也不会清空 ledger 或补充已消耗预算。

发布新版本会产生新的精确部署，额度不从旧部署转移。中断的额度操作保留原部署、上限、ETag 和 Receipt；
同一恢复不追随资源的新 active deployment。已完成操作后如需新的同参数意图，显式使用 `--new-attempt`。
