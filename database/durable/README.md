# 持久化数据库 Schema

本目录包含 1.0 版本之前各个受支持数据库后端的权威持久化仓储
Schema：

- `postgres/schema.sql`：在空的 PostgreSQL 16 数据库或 Schema 中完成结构部署。
- `sqlite/schema.sql`：在一个全新、空白的 SQLite 数据库文件中完成结构部署。

这些文件描述的是当前版本的最终数据库结构。它们不是 migration，也不支持升级、
接管或修复已有数据库。在 1.0 版本之前，如果发生不兼容的结构变更，需要同时修改
两个 Schema 文件、分配一个新的共享契约 ID，并基于空目标重新部署开发环境和 CI
使用的数据库。

## 生命周期边界

Schema 部署是一个明确的部署步骤或测试夹具准备步骤，必须在服务启动之前完成。
服务只读取 `durable_schema_contract` 并执行正常的运行时 DML；服务绝不能执行这些
Schema 文件，也不能创建、修改、修复或升级任何数据库对象。

两个 Schema 文件都会以原子事务完成安装，并且只有在所有其他受管理对象均创建
成功后才写入契约记录。对于非空目标或仅完成部分初始化的目标，部署会主动失败，
而不是通过 `IF NOT EXISTS` 掩盖结构漂移。

当前使用的不透明契约 ID 为：

```text
durable-schema-eb07a629-e22a-4935-9bba-4835c7b027f1
```

两个后端共享同一个契约 ID。元数据记录通过 `postgres` 或 `sqlite` 单独标识实际
使用的数据库后端。

## PostgreSQL 16

请使用拥有目标 Schema，并且能够创建表、索引、函数和触发器的部署角色执行
PostgreSQL Schema。例如：

```sh
psql --set=ON_ERROR_STOP=1 "$DATABASE_URL" \
  --file database/durable/postgres/schema.sql
```

部署连接的 `search_path` 中排在第一位的 Schema 就是部署目标；该 Schema 必须已经
存在，并且不能包含任何持久化仓储对象。运行时连接必须解析到同一个 Schema。

部署角色与服务运行角色必须分离。安装完成后，只向服务角色授予仓储正常工作所需的
DML、查询和函数执行权限；不要授予数据库或 Schema 创建权限，也不要授予
`ALTER` 或 `DROP` 权限。

## SQLite

在启动服务之前，创建一个新的空文件并执行 SQLite Schema：

```sh
sqlite3 /path/to/durable.sqlite \
  < database/durable/sqlite/schema.sql
```

服务会打开这个已经完成部署的文件，并且不会启用 `create_if_missing`。SQLite 的
WAL、共享内存或 journal 辅助文件属于正常的运行时存储，不视为创建数据库结构。

## 契约校验

服务会执行一次有界的只读查询，其效果等同于：

```sql
SELECT contract_id, backend
FROM durable_schema_contract
WHERE singleton = 1;
```

只有在查询结果恰好包含一条记录，并且其中声明的契约 ID 与仓储使用的后端标识都
符合预期时，服务才会继续启动。元数据缺失、契约 ID 不匹配或后端不匹配均属于
部署错误，服务必须在进入就绪状态之前失败。

## run-stream/v1 clean-cut 边界

`run-stream/v1` 将 terminal snapshot 固定为 `run_stream_snapshots.run_payload`，并把协议版本纳入
snapshot hash 域。旧 `response_snapshots` 的 split `response/workflow` 结构不支持在线或原地升级；
部署新版本前必须停止旧写入，并在全新空目标安装当前 Schema。确需保留历史数据时，应通过单独、
离线且经过校验的导出/导入流程迁移，而不是让服务在启动时猜测或修复旧结构。契约 ID 或对象布局
不匹配时，启动校验必须 fail closed。
