# Pre-1.0 Durable Schema 预置与运行时零 DDL 规范

> **状态：已实施并验收通过。** 本文定义的一次 1.0 前 clean cutover 已完成；当前运行与部署
> 行为以本规范及 `docs/current/` 为准。

| 属性 | 值 |
|---|---|
| 日期 | 2026-07-25 |
| 验收日期 | 2026-07-25 |
| 目标阶段 | 1.0 之前 |
| 变更类型 | Database Schema Lifecycle / Deployment Boundary / Clean Cutover |
| 数据兼容 | 不保留现有开发数据库，不提供旧 Schema 升级路径 |

## 1. 决策摘要

项目在 1.0 之前没有已发布、必须保留的旧数据库，也没有向旧 Schema 提供兼容升级的要求。因此：

1. 当前 `migrations/durable/{postgres,sqlite}` 的 23 对 SQL 不再作为 migration 历史保留；
2. 两个后端分别收敛为一个从空数据库创建当前完整结构的权威 Schema 文件；
3. 目录和代码不得继续将该资产称为 migration；
4. PostgreSQL 和 SQLite Schema 必须在服务进程启动前由显式部署或测试步骤安装；
5. 服务进程不得创建、修改、修复或升级数据库对象；
6. 服务启动只执行一次最小、只读的 Schema contract ID 校验；
7. 1.0 发布后，只有出现真实的已有数据库升级需求时，才建立独立的 migration 目录和执行器；
8. 即使 1.0 后引入 migration，migration 仍由独立部署步骤执行，业务服务继续保持运行时零 DDL。

本次收敛删除的是未产生用户价值的历史升级机制，不删除 durable repository 已有的数据库级安全
约束。现有表、索引、外键、`CHECK`、不可变性触发器、fencing、幂等收据和 public-event authority
必须完整保留。

## 2. 规范性术语

- **必须**：实现、部署和 CI 都必须满足；
- **不得**：明确禁止；
- **应**：除非后续设计记录给出可验证替代方案，否则必须满足；
- **可以**：不破坏本文不变量时允许的实现选择；
- **Schema**：从空数据库创建当前完整数据库结构的声明式 DDL；
- **Schema provisioner**：在服务启动前安装 Schema 的部署步骤、工具或测试 fixture；
- **Schema contract ID**：程序与数据库之间用于识别结构合同的一枚不透明标识；
- **Migration**：将必须保留数据的已有数据库从一个已发布 Schema 升级到另一个 Schema 的过程；
- **运行时零 DDL**：业务服务进程的正常启动和运行路径不执行 `CREATE`、`ALTER`、`DROP` 或等价
  Schema 修改。

业务表 `run_migration_intents` 表达 Run 在工作流定义或部署版本之间的迁移，不属于数据库 Schema
migration，不因本规范改名。

## 3. 背景与问题

### 3.1 当前实现

当前仓库维护：

```text
migrations/
└── durable/
    ├── postgres/   # 23 个 SQL
    └── sqlite/     # 23 个 SQL
```

`crates/storage/src/repository/migration_manifest.rs` 将两套 SQL 通过 `include_str!` 编译进程序，并定义
23 项有序 manifest。

PostgreSQL 服务启动路径会：

1. 获取 transaction advisory lock；
2. 创建或读取 `schema_migrations`；
3. 校验 version、文件名和 SQL SHA-256；
4. 执行尚未登记的 DDL；
5. 在同一事务写入 migration ledger。

SQLite repository 连接路径会：

1. 使用 `create_if_missing(true)` 打开数据库文件；
2. 根据表或列是否存在选择 migration；
3. 在每次打开时重新执行标记为 `Always` 的 SQL。

### 3.2 当前模型的成本

在没有旧数据库的前提下，当前机制引入了不必要的合同和维护面：

1. 23 个版本全部形成于同一 pre-1.0 阶段，却被当作永久升级历史冻结；
2. 初始 Schema 已包含部分后续文件再次处理的对象，基线和升级路径职责重叠；
3. PostgreSQL checksum、版本前缀、空洞检测和无 ledger Schema 拒绝逻辑保护了一个并不存在的
   兼容需求；
4. SQLite guard 和触发器重放让 repository 连接同时承担数据库修复职责；
5. 业务服务需要超出正常 DML 的数据库权限；
6. 多实例启动被迫处理 DDL 并发，而 Schema 安装本应由部署编排串行完成；
7. `migration` 命名误导维护者，使 pre-1.0 Schema 修改被错误理解为必须兼容旧数据库；
8. 测试大量验证历史 migration 布局，而不是验证从空数据库安装的最终合同。

### 3.3 不改变的复杂度

合并 SQL 不代表删除数据库中的业务安全模型。以下复杂度属于 durable runtime 本身，必须保留：

- execution event 不可变和每 Run 单调序号；
- projection checkpoint 和 projection ledger 的封闭合同；
- scheduler、task、attempt 和 model-tool claim fencing；
- Signal/Timer first-winner；
- public outbox、永久 receipt、projection decision 和 delivery head；
- terminal Run、response snapshot 和 Artifact retention 的原子权威；
- recovery lineage、reuse provenance 和 transition idempotency；
- PostgreSQL 与 SQLite 对同一 durable port 的语义一致性。

本规范只改变这些对象如何安装，不改变它们的运行语义。

## 4. 目标与非目标

### 4.1 目标

1. 为 PostgreSQL 和 SQLite 各提供一个清晰、可审查、从空数据库执行的完整 Schema；
2. 从业务服务中删除全部数据库对象创建和升级逻辑；
3. 将 Schema 安装变成明确的部署前置条件；
4. 让 PostgreSQL 服务账号可以在无 DDL 权限下完整运行；
5. 让 SQLite 在数据库文件不存在时明确失败，不静默创建空库；
6. 用一条最小只读查询在 HTTP bind 和 RunService 启动前识别错误数据库或错误 Schema；
7. 将 CI 的关注点从历史 migration 字节转向最终 Schema、双后端语义和零 DDL 边界；
8. 为 1.0 后真实 migration 留下清晰但不提前实现的演进边界。

### 4.2 非目标

- 不改变 durable 表的业务含义、字段语义或状态机；
- 不弱化现有外键、`CHECK`、唯一索引、不可变性触发器或 fail-closed 行为；
- 不保留任何现有本地、CI 或开发 PostgreSQL/SQLite 数据；
- 不提供从当前 23 项 manifest 到新基线的在线升级；
- 不在业务服务中保留隐藏的“首次启动自动初始化”模式；
- 不通过 feature flag 同时支持 old migration 和 new Schema 两条路径；
- 不在本次变更中设计 1.0 后的具体 migration 工具或版本协商协议；
- 不把完整 Schema introspection 或自动修复引入启动路径。

## 5. 权威目录与命名

### 5.1 目标布局

```text
database/
└── durable/
    ├── README.md
    ├── postgres/
    │   └── schema.sql
    └── sqlite/
        └── schema.sql
```

路径语义如下：

| 路径 | 职责 |
|---|---|
| `database/` | 仓库拥有的数据库资产 |
| `database/durable/` | durable repository 的数据库合同 |
| `postgres/schema.sql` | 从空 PostgreSQL Schema 创建完整结构 |
| `sqlite/schema.sql` | 从空 SQLite 数据库创建完整结构 |
| `README.md` | 安装前提、执行方式、权限、失败和版本策略 |

1.0 之前文件名使用 `schema.sql`，不得用连续 migration 编号模拟尚未存在的兼容历史。

### 5.2 代码命名

实现应采用以下语义替换：

| 当前名称 | 目标名称或处理 |
|---|---|
| `migration_manifest.rs` | 删除；必要的运行时常量移入 `schema_contract.rs` |
| `DURABLE_MIGRATIONS` | 删除 |
| `DurableMigration` | 删除 |
| `SqliteMigrationGuard` | 删除 |
| `migrate_schema()` | 删除 |
| `initialize_schema()` | 从生产 repository API 删除 |
| `schema_migrations` | 删除 |
| `migration_layout.rs` | 重写为 `schema_layout.rs` |
| `postgres_migrations.rs` | 重写为 PostgreSQL Schema provisioning/validation 测试 |
| `durable-migrations.tsv` | 删除 |

生产 library 不得通过 `include_str!` 嵌入 DDL。测试支持或独立 provisioner 可以读取或嵌入
`schema.sql`，但业务服务 binary 不得拥有执行该 DDL 的路径。

## 6. Schema 文件合同

### 6.1 空数据库前提

两个 `schema.sql` 都只支持空目标：

- PostgreSQL 的目标 database/schema 中不得已有 durable 受管对象；
- SQLite 目标文件必须是由 provisioner 新建的空数据库；
- Schema 文件不要求重复执行幂等；
- 对非空、部分初始化或对象冲突的目标，provisioning 必须失败；
- 不得通过 `IF NOT EXISTS` 掩盖对象漂移或部分安装。

### 6.2 原子安装

PostgreSQL Schema 必须在单个事务中安装：

```sql
BEGIN;

-- tables, indexes, functions, triggers
-- durable schema contract row is inserted last

COMMIT;
```

SQLite 必须先启用 foreign keys，再在显式写事务中安装：

```sql
PRAGMA foreign_keys = ON;
BEGIN IMMEDIATE;

-- tables, indexes, triggers
-- durable schema contract row is inserted last

COMMIT;
```

Schema contract 行必须是事务中的最后一个逻辑安装步骤。任一 DDL 或约束创建失败时，不得留下能被
服务识别为有效的 metadata。

### 6.3 纯最终状态

新 Schema 文件必须描述最终结构，不得包含只对旧数据有意义的步骤：

- 不得包含字段 backfill；
- 不得包含历史 backlog 重建；
- 不得包含用于兼容旧表的 `ALTER TABLE ADD COLUMN`；
- 不得包含 migration-time 临时验证表；
- 不得先创建旧触发器再 `DROP` 和替换；
- 不得保留 legacy registration 默认值或过渡状态，除非该值仍是当前运行合同的一部分；
- 不得保留只用于旧 Schema adoption 的探测逻辑。

最终 Schema 中仍可且应直接创建当前需要的函数、触发器、索引和约束。

### 6.4 生成方法

不得简单连接当前 23 个 SQL 文件作为新基线。实现必须：

1. 在空 PostgreSQL 16 和空 SQLite 上按当前顺序应用全部 23 项；
2. 导出两个后端的最终数据库结构；
3. 按依赖关系整理为声明式 Schema；
4. 去除 backfill、兼容 guard、临时验证和旧对象替换步骤；
5. 从新的空目标执行整理后的 Schema；
6. 对比新旧最终表、列、索引、外键、触发器和函数；
7. 运行共享 repository 合同和真实 PostgreSQL 测试。

导出结果是审计输入，不是可不经整理直接提交的最终文件。

## 7. Schema Contract Metadata

### 7.1 最小数据模型

两个后端必须创建单例 metadata 表。概念模型为：

```sql
CREATE TABLE durable_schema_contract (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    contract_id TEXT NOT NULL,
    backend TEXT NOT NULL,
    installed_at TEXT_OR_TIMESTAMP NOT NULL,
    CHECK (backend IN ('postgres', 'sqlite'))
);
```

具体时间类型按后端选择。`contract_id` 是不透明、编译期固定的字符串，不是 migration version。
pre-1.0 每次不兼容 Schema 修改都必须：

1. 修改对应 Schema 文件；
2. 同步修改两个后端的 contract ID；
3. 更新程序期望的 contract ID；
4. 删除并重新 provision 开发和 CI 数据库。

两个后端使用相同 contract ID，但各自的 `backend` 必须准确。

### 7.2 不使用 Schema hash

运行时不得计算或比较完整 SQL/Schema hash，原因是：

- SQL 文件字节 hash 不等于数据库最终结构 hash；
- metadata 不能证明管理员没有手工修改其他对象；
- 完整结构 canonicalization 在两个数据库后端间复杂且脆弱；
- CI、最小权限和空库安装测试更适合承担结构正确性责任。

contract ID 只用于识别“这个数据库声明实现哪个合同”，不替代 CI 和权限隔离。

## 8. 服务启动合同

### 8.1 PostgreSQL

服务必须：

1. 使用运行时账号连接已存在的数据库；
2. 在创建 RunService、启动 scheduler/worker pump 或绑定 HTTP 前读取
   `durable_schema_contract`；
3. 精确比较 `contract_id` 和 `backend='postgres'`；
4. 校验成功后才继续启动。

服务不得：

- 创建 metadata 表；
- 创建 `schema_migrations`；
- 获取 migration advisory lock；
- 执行任何 Schema SQL；
- 在缺少对象时自动修复或 adoption；
- 在校验失败后进入部分 Ready 状态。

### 8.2 SQLite

服务必须：

1. 使用 `create_if_missing(false)` 打开配置指定的数据库文件；
2. 要求文件在进程启动前已经存在；
3. 读取并校验 `durable_schema_contract`；
4. 精确比较 contract ID 和 `backend='sqlite'`；
5. 失败时终止启动。

SQLite 正常写事务可能创建 `-wal`、`-shm` 或 journal sidecar 文件；这属于数据库运行时写入，不等于
创建数据库 Schema，不违反运行时零 DDL。

### 8.3 最小只读校验

启动校验只能是一条或一组固定、有界、只读查询。不得：

- 扫描 `information_schema` 或 `sqlite_master` 验证全部对象；
- 枚举和比较所有字段、索引、外键或触发器；
- 执行自动补表、补列、补索引或触发器恢复；
- 将完整 Schema introspection 加入 readiness 周期。

建议错误分类：

| 错误 | 含义 |
|---|---|
| `DATABASE_SCHEMA_NOT_INITIALIZED` | metadata 表或单例行不存在 |
| `DATABASE_SCHEMA_CONTRACT_MISMATCH` | contract ID 与程序要求不一致 |
| `DATABASE_SCHEMA_BACKEND_MISMATCH` | 数据库声明的后端与 repository 不一致 |

错误消息不得泄露数据库凭据或非必要连接信息。

### 8.4 Readiness

Schema contract 校验是启动 gate，不是周期性 migration 或修复任务：

- 校验未完成或失败时不得绑定业务 HTTP，或 readiness 必须保持失败；
- 校验成功后，普通 readiness 只检查 repository 可达性和现有运行时条件；
- 运行过程中发现“表不存在”或“列不存在”仍是不可恢复的部署错误，不得触发自动 DDL。

## 9. Provisioning 与权限边界

### 9.1 Provisioner 所有权

Schema provisioner 可以是：

- 部署流水线中的显式 SQL 步骤；
- Kubernetes Job 或 init container；
- Docker 初始化步骤；
- DBA 执行流程；
- 独立、显式调用的 Schema 安装工具；
- 测试 fixture。

具体包装工具不属于本文强制接口，但必须满足：

1. 在服务启动前完成；
2. 失败会阻止服务部署；
3. 与业务服务进程相互独立；
4. 不允许服务在 provisioner 缺失时自行降级为初始化模式。

### 9.2 PostgreSQL 权限

provisioner 使用的数据库角色必须拥有创建表、索引、函数、触发器和约束所需的 owner/DDL 权限。

业务服务角色不得拥有：

- database/schema 对象创建权限；
- `ALTER` 或 `DROP` 受管对象的权限；
- 替换 migration/Schema 函数和触发器的权限。

业务服务角色只获得 durable repository 正常 DML、查询和必要函数执行权限。CI 必须以一个显式拒绝
DDL 的服务角色证明生产 repository 可以启动并完成代表性工作流。

### 9.3 部署顺序

部署编排必须显式表达：

```text
创建空数据库或 SQLite 文件
        ↓
Schema provisioner 成功
        ↓
授予/确认运行时最小权限
        ↓
启动业务服务
        ↓
只读 contract 校验
        ↓
Ready
```

不得依赖“服务先启动，随后自己把数据库补齐”。

## 10. Repository API 与测试支持边界

### 10.1 生产 API

生产 repository API 不得暴露通用 Schema 安装方法。以下能力必须移除：

- `PostgresDurableRepository::migrate_schema`;
- `PostgresDurableRepository::initialize_schema`;
- SQLite connect 内的隐式 SQL 执行；
- 生产可调用的“初始化内存数据库并自动建表”捷径。

repository 构造可以内部执行最小 contract 校验，也可以由 composition root 在构造后立即调用只读
`validate_schema_contract()`；无论采用哪种形态，都必须保证无效 Schema 不会进入 RunService。

### 10.2 测试支持

测试必须显式区分 provisioning 和连接：

```text
create empty database
        ↓
install schema fixture
        ↓
connect repository
        ↓
run contract test
```

测试专用 helper 可以读取 Schema 文件并安装到临时数据库，但必须位于 test support 或 dev-only
边界，不得被生产启动代码调用。

SQLite `in_memory()` 若继续存在，必须移到明确的测试支持 API，或改为接收已经 provision 完成的
连接。生产 repository 构造不得因为调用 `in_memory()` 而隐式创建表。

## 11. 测试与 CI 合同

### 11.1 Schema 安装测试

两个后端都必须证明：

1. Schema 可在空数据库上一次性成功执行；
2. 所有核心表、索引、外键和触发器存在；
3. contract metadata 最后成功写入；
4. 对非空或部分安装目标执行会失败；
5. Schema 不包含数据 backfill 或旧版本 adoption；
6. PostgreSQL 和 SQLite 保持现有 durable 语义一致性；
7. PostgreSQL 16 真实数据库门禁不得在 CI 静默跳过。

### 11.2 启动失败测试

必须覆盖：

- PostgreSQL metadata 表不存在；
- SQLite 文件不存在；
- SQLite 文件存在但未初始化；
- contract ID 不匹配；
- backend 不匹配；
- Schema 安装中途失败后服务拒绝启动；
- 连接到了错误 database/schema 或错误 SQLite 文件；
- 校验失败时不绑定业务 HTTP、不启动 scheduler/worker。

### 11.3 零 DDL 测试

必须至少提供以下证据：

1. PostgreSQL 使用无 DDL 权限的运行时角色启动成功；
2. 该角色可以完成代表性的 Run 创建、调度、事件、终态和响应读取；
3. repository 连接和服务启动不会写入 Schema metadata；
4. 生产代码不引用或嵌入 `schema.sql`；
5. SQLite 已存在数据库可以启动，缺失文件不会被创建；
6. 重启服务不会改变表、索引、触发器或 contract metadata。

### 11.4 现有测试处理

| 当前测试/资产 | 处理 |
|---|---|
| `tests/phase0_migration_baseline.rs` | 删除或替换为双后端 Schema contract 测试 |
| `tests/baselines/durable-migrations.tsv` | 删除 |
| `crates/storage/tests/migration_layout.rs` | 重写为最终 Schema 语义和布局测试 |
| `crates/storage/tests/postgres_migrations.rs` | 重写为 PostgreSQL provisioning、contract 校验和无 DDL 权限测试 |
| 依赖 `initialize_schema()` 的 repository 测试 | 改为显式 test fixture provisioning |
| binary smoke | 在启动进程前显式 provision 数据库 |

测试不得为了减少改动而保留生产 repository 的隐式建表入口。

## 12. Clean Cutover

### 12.1 数据策略

本次变更明确不提供数据迁移：

- 删除本地 SQLite 数据库并重新 provision；
- 删除或重建开发 PostgreSQL schema/database；
- CI 每次从空目标安装；
- 不读取旧 `schema_migrations`；
- 不尝试识别或收养由当前 23 项 manifest 创建的数据库；
- 旧数据库连接新服务时必须因 contract metadata 缺失而失败。

### 12.2 实施顺序

实施应按以下顺序完成：

1. 冻结当前 23 项最终结构作为审计输入；
2. 生成并整理 PostgreSQL、SQLite 最终 Schema；
3. 加入 contract metadata；
4. 建立显式 test provisioning helper；
5. 将全部 repository 测试切换到显式 provisioning；
6. 将生产 repository 构造改为只读 contract 校验；
7. 删除 PostgreSQL migration coordinator 和 SQLite guards；
8. 删除旧 migration 目录、manifest、ledger baseline 和过时测试；
9. 更新 Quickstart、Docker、生产部署、开发指南和 CI；
10. 删除并重建所有开发/测试数据库；
11. 运行第 13 节完整门禁；
12. 确认没有 old/new 双路径后一次性合入。

不得先删除 migration 资产，再依赖未完成的 Schema 文件或隐式测试初始化维持暂时可用状态。

## 13. 验收标准

只有同时满足以下条件，本规范才视为实施完成：

1. `migrations/durable/` 不再存在；
2. `database/durable/{postgres,sqlite}/schema.sql` 是唯一权威 DDL；
3. 两个 Schema 都能在空目标原子安装；
4. 当前全部 durable repository 行为和安全约束仍通过；
5. 生产 Rust 代码中不存在 `DURABLE_MIGRATIONS`、`SqliteMigrationGuard`、
   `schema_migrations` 或 `migrate_schema()`；
6. PostgreSQL 服务账号无 DDL 权限仍能通过启动和代表性 E2E；
7. SQLite 不会在服务启动时创建数据库文件或表；
8. 缺失、错误 contract ID 或错误 backend 会在 HTTP bind 前 fail-closed；
9. 服务启动不会修改 Schema 或 metadata；
10. 当前文档不再描述服务自动 migration；
11. Quickstart 和生产部署都明确包含服务启动前的 Schema provisioning；
12. 所有 migration checksum/连续性门禁已删除，最终 Schema 语义门禁已建立；
13. `cargo fmt --all -- --check`、workspace check/test、真实 PostgreSQL 16、binary smoke、
    real-process restart/shutdown 和文档/路径残留检查全部通过。

### 13.1 实施验收记录

2026-07-25 已完成以下验收：

- 旧 23 项 manifest 最终状态与两个新 Schema 基线逐对象比对；旧表、列、约束、索引、触发器和
  PostgreSQL function 全部保留，另新增 contract metadata，并将 SQLite 文本主键显式收紧为
  `NOT NULL` 以匹配 PostgreSQL 主键语义；
- PostgreSQL 16 与 SQLite 均从空目标原子安装成功，部分安装失败时不发布 contract metadata；
- 实际无 DDL 权限 PostgreSQL LOGIN 角色完成服务启动、代表性工作流、重启读取和对象清单不变验证；
- 缺失及空 SQLite 目标均在 HTTP bind 前失败，且缺失文件不会被服务创建；
- 全工作区 `check`、Clippy、测试、文档测试、公共 API、crate boundary、残留扫描和格式门禁通过。

## 14. 1.0 后演进规则

1. 1.0 发布时必须冻结当时的 Schema 基线和 contract ID；
2. 1.0 后如果仍允许删除重建数据库，可以继续发布新的完整 Schema，但必须明确这是部署合同；
3. 只有第一次出现“必须保留已有数据库数据并原地升级”的真实需求时，才建立
   `database/migrations/`；
4. migration 文件必须描述两个已发布合同之间的转换，不得用 migration 目录记录 pre-1.0 编辑历史；
5. migration 必须由独立 provisioner/upgrade job 执行；
6. 业务服务继续只读校验目标 contract ID，不获得 DDL 权限；
7. 滚动部署、向前/向后兼容窗口、锁策略和失败恢复必须在首个真实 migration 前由独立设计确定，
   不由本规范预设。

## 15. 风险与控制

| 风险 | 控制 |
|---|---|
| 合并时遗漏后续 migration 中的对象 | 在两个后端应用全部 23 项后导出最终结构并逐项比较 |
| 删除 migration 测试后约束回退 | 用最终 Schema 语义测试和共享 repository 合同替换 |
| provisioner 部分成功 | 单事务安装，contract metadata 最后写入 |
| 服务连接错误数据库 | 启动时精确校验 contract ID 和 backend |
| metadata 存在但管理员手工漂移对象 | 最小权限禁止服务修改；CI 从空库验证；运维变更走受控 provisioning |
| SQLite 缺失文件被意外创建 | `create_if_missing(false)` 和缺失文件启动失败测试 |
| Quickstart 变复杂 | 提供明确、单步但独立于服务启动的 provisioning 命令 |
| 1.0 后继续随意重写基线 | 发布流程冻结 1.0 contract，真实兼容需求触发独立 migration 设计 |

## 16. 最终不变量

完成后，数据库生命周期必须满足：

```text
空数据库
    │
    │  显式、部署前、原子 Schema provisioning
    ▼
已安装 durable Schema + contract metadata
    │
    │  业务服务只读校验
    ▼
运行时 DML / 查询
```

业务服务与数据库结构之间的边界是：

> Schema 由部署系统创建；Schema 正确性由 Schema 文件、CI 和权限隔离保证；服务只确认自己连接到
> 声明实现预期 contract 的数据库，绝不创建、迁移或修复表。
