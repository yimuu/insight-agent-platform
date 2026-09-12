# ADR 0013: Console 同源对象上传

Status: Accepted

## 决策

Console 已有透明传输进程同时承接浏览器的对象 PUT。浏览器不再直接访问安装内部的 S3，
也不需要信任安装 CA。CLI 仍按原有公开 Artifact 契约直接消费签名地址。

Console transport 配置由 `apps/console/server/config.ts` 的 V2 类型拥有，完整替换 V1。
部署 renderer 输出固定 HTTPS 对象 origin、桶路径前缀及公共 CA PEM；不交付存储密钥或
安装私钥。未配置对象传输时三个字段必须同时为空，端口拒绝上传。

浏览器使用 `PUT /_console/v1/object-upload`，请求头 `X-Insight-Upload-Target` 携带原签名地址，
正文为原对象字节。该端口是 Console transport 的私有边界，不增加 Gateway 公共 API。
地址必须属于部署固定 origin 和桶路径前缀，并具有无重复字段的 SigV4 查询参数。
HTTP 方法、签名 Host、原路径及查询保持不变；仅向 S3 发送内容类型、长度和对象正文。
平台 Bearer、Cookie、任意转发头和重定向均不传递。TLS 强制验证安装 CA 和对端名称。
请求复用既有每请求、总缓冲、连接、超时和取消限制；响应只保留上传成功或有界错误，
不暴露签名、上游响应正文或 Location。不允许跨站浏览器请求。

## 契约与架构联合核对（实现前）

本决策已对照 ADR 0010、公开 `SecretBearingUploadTargetV1`、当前 `schema.sql` 的 Artifact
及 upload grant 所有权、现有 Console transport 配置与转发实现核对：

- 所有权、身份、schema：PostgreSQL 与 Artifact 服务继续拥有授权、状态、grant 与内容校验；
  Console 不保存签名、身份映射、凭据或业务状态，无 DDL 及公共 wire 变更。
- 错误、安全：拒绝任意主机/桶、HTTP、用户信息、重复签名参数、跨站请求和非 PUT 方法；
  失效或伪造签名由原 S3 校验，Console 不替代它签发授权。
- 事务、事件、恢复：仍使用原 prepare / PUT / complete / verify 顺序和 Receipt，未知结果不
  自动重试。浏览器恢复原 grant；传输进程不创建额外事务或事件，也不修改终态。
- 容量：使用现有有界缓冲与连接预算，在完整请求准入之后才建立上游连接；取消释放缓冲并停止传输。
- 验证要求：真实 HTTPS 上游验证可信 CA / 错误名称 / 不可信 CA、精确 Host 和字节、禁止
  credential 转发、主机/路径逃逸、重复参数、跨站/重定向/超限拒绝，以及本机模型保存恢复。

现有安装可由部署者将 V2 Console 配置与公共 CA 作为只读配置交付后更新 Console 镜像。
不修改冻结的安装身份或已有数据库；新安装由共享 renderer 生成相同 V2 配置。

## 实测证据（2026-09-11）

Console 的真实 HTTPS transport 测试通过，覆盖可信 CA、错误名称、不可信 CA、原始字节、
转发头剥离、越界地址、跨站请求、重定向及缓冲释放；部署 renderer 单元测试通过。
现有 `my-platform` 安装已更新 Console 镜像与只读 V2 配置，在原 `8088` 端口完成公开
Artifact prepare（201）、同源 PUT（204）、complete（202）及异步内容校验。
Operation 为 `succeeded`、Artifact 为 `ready`，实际内容摘要与提交摘要一致。
这验证了真实上传链路；不扩大为阿里百炼模型调用或 Kubernetes 本次重验的结论。
