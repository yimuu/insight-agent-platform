# Text2SQL 工作流

根据表结构和自然语言问题生成 PostgreSQL SQL。三个模型步骤依次分析需求、生成草稿、复核并输出结果；不连接或执行数据库。

## 文件与使用

- `agent.yaml`：模型引用、静态指令、输入输出接口。
- `workflow.yaml`：三个步骤、各步输出 Schema、变量引用和最终输出映射。
- `input.schema.json` / `output.schema.json`：公开接口。
- `input.json`：客户与订单表的示例问题。

在 Console 导入 `agent.yaml`、`workflow.yaml` 和两份 Schema 文件，选择已经配置的模型，再校验和发布。`project/default-model` 是需要实际绑定的逻辑引用，示例不包含 API Key。运行时提交 `input.json` 对象。

输入包含两个必填字符串：

```json
{
  "schema_text": "PostgreSQL 表结构、字段含义、关联关系与业务规则",
  "question": "需要查询的问题"
}
```

`input.json` 中的具体问题是查询 2025 年 1 月已付款订单金额最高的前 10 位客户。模型可以生成类似查询：

```sql
SELECT c.id, c.name, SUM(o.total_amount) AS total_amount
FROM customers AS c
JOIN orders AS o ON o.customer_id = c.id
WHERE o.status = 'paid'
  AND o.created_at >= TIMESTAMP '2025-01-01 00:00:00'
  AND o.created_at < TIMESTAMP '2025-02-01 00:00:00'
GROUP BY c.id, c.name
ORDER BY total_amount DESC, c.id ASC
LIMIT 10;
```

这段 SQL 是说明性示例，不是已执行的模型结果。最终响应是包含 `sql` 和 `explanation` 两个字符串字段的 JSON 对象；缺少必要信息时，提示词要求返回空 SQL 并说明需要补充什么。

## 数据传递

```yaml
# 生成步骤
prompt: |
  表结构：{{ input.schema_text }}
  问题：{{ input.question }}
  分析：{{ nodes.analyze.output.text }}

# 审阅步骤可以分别引用结构化输出字段
prompt: |
  SQL 草稿：{{ nodes.generate.output.sql }}
  草稿说明：{{ nodes.generate.output.explanation }}
```

上面是两个不同步骤的节选，完整合法文件见 `workflow.yaml`。最后通过 `output` 映射返回审阅步骤的 SQL 和说明。

步骤共用一个已配置模型；每步预算沿用平台模型循环配置，300 秒期限覆盖整次运行。输入、上游结果和模板长度均受 Schema 和编译限制约束。

## 验证边界

编译与源码恢复测试验证文件、Schema、依赖和跨步骤引用。`review` 是模型复核，不是 SQL 解析器或数据库验证器；只读要求由提示词表达，不能当作执行权限控制。如果后续接入执行，应单独实现 SQL 校验并使用受限数据库权限。本例没有数据库执行步骤。
