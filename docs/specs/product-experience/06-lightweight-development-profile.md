# Spec 06：轻量单节点开发模式

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-214 |
| 日期 | 2026-09-01 |
| 默认入口 | `insight dev` |
| 部署性质 | 单机开发；不是production或L4证据 |

## 1. 目标

用户在一台macOS或Linux开发机上使用预构建发行物启动最小真实Platform closure。所有业务请求仍经过相同`/v1`、
PostgreSQL authority、Receipt/CAS、durable Run和独立执行边界；不得用内存mock或第二套development API换取速度。

“轻量”定义为不编译源码、只启动必要角色、可增量启用功能和可预测资源占用，不等于把所有role合并为一个进程。

## 2. Profile clean cut

现有`base/full`在本阶段clean-cut为：

```text
starter
starter + model
starter + remote-capability
starter + context
starter + mcp
starter + wasi
all（上述feature的canonical union）
qualification（独立，不属于dev feature）
```

CLI：

```bash
insight dev
insight dev --features model,context
insight dev --features all
```

不保留`base/full`兼容别名。project config保存closed、排序后的feature集合和profile digest；unknown、重复、冲突feature
在拉取image或启动dependency前失败。

## 3. Starter closure

starter只承担deterministic Agent发布与Run：

- PostgreSQL 16；
- NATS；
- HTTPS S3/KMS-compatible local dependency；
- Management Gateway与Runtime Gateway；
- Registry Validation、Orchestration、Native Capability；
- Artifact Gateway与Artifact Data Worker。

这些role保持独立identity、配置、连接池和permit。不能将Gateway、Scheduler、Artifact或Capability执行合并进CLI。
Console是可选静态资源，不要求本机Node.js。

## 4. Feature closure

每个feature声明：

- 追加的role和OCI image digest；
- required config/identity/certificate；
- dependency、端口和资源预算；
- 提供的manifest execution/binding能力；
- readiness和disable前置条件。

feature只能增加既有Platform role，不新增authority。组合必须确定性排序并计算profile digest。相同feature集合在不同命令顺序下
产生相同closure。

发布Agent前，Spec 01 compiler输出`required_features`。缺失时CLI返回例如：

```text
This agent requires: model
Restart with: insight dev --features model
```

默认不自动扩大环境或启动外部访问能力。只有用户显式使用`--enable-required-features`时，CLI才可确认后重启到exact closure。

## 5. 生命周期与状态

- `init`只创建project-local non-production identity/config，不启动服务；
- 首次`dev`显式provision fresh schema和bootstrap；运行时role仍无DDL权限；
- 后续`dev`验证并复用现有PostgreSQL、object和identity，不旋转project ID、CA、port或Secret；
- 增加feature只追加其身份/config并重新计算闭包，不重建既有authority；
- `stop`停止exact Platform/依赖closure；`start`恢复时重验schema、release和profile；
- `reset`属于单独破坏性命令，明确列出将删除的project-local数据并要求确认；不得藏在`dev`失败恢复中。

## 6. 资源与时间预算

资格runner固定一台4 vCPU/8 GiB、100 Mbps等效机器，记录而不是推断资源：

| 指标 | Starter目标 |
|---|---:|
| 已缓存warm ready | <= 60秒 |
| fresh cold pull到ready | <= 300秒 |
| idle RSS总和 | <= 6 GiB |
| idle CPU（稳定5分钟后） | <= 10%单核等效 |
| project固定磁盘（不含用户Artifact） | <= 8 GiB |
| source compilation | 0 |

预算不包括首次下载的公网速度波动，但报告必须单列download bytes/time。超限时输出最大image、process、volume和启动阶段，
不能通过减少一致性检查、关闭TLS或换mock来过门禁。

## 7. 故障与缺失能力

- feature未启用：发布前返回`feature_not_enabled`，零外部I/O；
- role未ready：整体profile不报告ready；
- dependency重启：Run/Job authority保持PostgreSQL current state并按现有恢复语义继续；
- profile/image/schema drift：启动前fail closed并给出exact update命令；
- Docker资源不足、端口冲突、磁盘不足：`doctor`指出资源和建议，不自动删除用户容器/volume；
- macOS没有runsc：starter/WASI能力不冒充gVisor，qualification继续显示unavailable。

## 8. 验收

- fresh预构建CLI在支持的macOS/Linux完成`init -> dev -> agent publish -> agent run`；
- starter和每个单feature、all组合均有closed config、readiness和negative fixture；
- feature顺序重排得到相同profile digest，unknown组合在零I/O前失败；
- warm/cold/idle资源门禁产生机器可读报告；
- stop/start、Gateway/Worker kill、dependency短暂不可用后现有Run不丢失或重复effect；
- starter与all对相同deterministic Agent产生相同public结果和authority不变量；
- 本地profile报告明确写明single-node、non-production、L4～L6 Not run。
