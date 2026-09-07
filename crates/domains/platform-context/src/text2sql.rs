use crate::{digest, digest_without_field, ContextQueryError};
use insight_platform_contracts::{
    CapabilityName, ClosedJsonValue, Effect, ExactDeploymentRef, ExactVersionRef, ResourceId,
    ResourceKind, Sha256Digest,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const READONLY_DATABASE_CAPABILITY: &str = "database.query.readonly";
pub const TEXT2SQL_PLAN_VALUE_KIND: &str = "text2sql_readonly_plan";
const MAX_SQL_IDENTIFIER_BYTES: usize = 128;
const MAX_SQL_SOURCES: usize = 32;
const MAX_SQL_PROJECTIONS: usize = 256;
const MAX_SQL_PREDICATES: usize = 512;
const MAX_SQL_ORDER_ITEMS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlObjectName {
    pub schema: String,
    pub object: String,
}

impl SqlObjectName {
    fn validate(&self) -> Result<(), ContextQueryError> {
        if is_identifier(&self.schema) && is_identifier(&self.object) {
            Ok(())
        } else {
            Err(ContextQueryError::InvalidTextToSql)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlColumnRef {
    pub source_alias: String,
    pub column: String,
}

impl SqlColumnRef {
    fn validate(&self) -> Result<(), ContextQueryError> {
        if is_identifier(&self.source_alias) && is_identifier(&self.column) {
            Ok(())
        } else {
            Err(ContextQueryError::InvalidTextToSql)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlSource {
    pub object: SqlObjectName,
    pub alias: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqlAggregate {
    Count,
    CountDistinct,
    Sum,
    Average,
    Minimum,
    Maximum,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SqlProjectionExpression {
    Column {
        column: SqlColumnRef,
    },
    Aggregate {
        function: SqlAggregate,
        column: Option<SqlColumnRef>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlProjection {
    pub expression: SqlProjectionExpression,
    pub output_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqlComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    IsNull,
    IsNotNull,
    In,
    Between,
    LikeLiteral,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlPredicate {
    pub column: SqlColumnRef,
    pub operator: SqlComparisonOperator,
    pub parameter_ordinals: Vec<u16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqlJoinKind {
    Inner,
    Left,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlJoin {
    pub kind: SqlJoinKind,
    pub source: SqlSource,
    pub left: SqlColumnRef,
    pub right: SqlColumnRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqlOrderDirection {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlOrderItem {
    pub column: SqlColumnRef,
    pub direction: SqlOrderDirection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadOnlySqlExecutionBinding {
    pub capability_name: CapabilityName,
    pub capability_deployment: ExactDeploymentRef,
    pub interface_revision: ExactVersionRef,
    pub effect: Effect,
    pub database_identity_digest: Sha256Digest,
    pub dialect: String,
    pub allowed_schemas: Vec<String>,
    pub statement_timeout_milliseconds: u64,
    pub row_limit: u32,
    pub byte_limit: u64,
    pub cost_gate_digest: Sha256Digest,
}

impl ReadOnlySqlExecutionBinding {
    pub fn validate(&self) -> Result<(), ContextQueryError> {
        self.capability_deployment
            .validate()
            .map_err(|_| ContextQueryError::InvalidTextToSql)?;
        self.interface_revision
            .validate()
            .map_err(|_| ContextQueryError::InvalidTextToSql)?;
        if self.capability_name.as_str() != READONLY_DATABASE_CAPABILITY
            || self.capability_deployment.resource_kind != ResourceKind::CapabilityDeployment
            || self.interface_revision.resource_kind != ResourceKind::CapabilityInterfaceRevision
            || self.effect != Effect::ReadOnly
            || !is_identifier(&self.dialect)
            || self.allowed_schemas.is_empty()
            || self.allowed_schemas.len() > MAX_SQL_SOURCES
            || !is_sorted_unique(&self.allowed_schemas)
            || self
                .allowed_schemas
                .iter()
                .any(|schema| !is_identifier(schema))
            || self.statement_timeout_milliseconds == 0
            || self.statement_timeout_milliseconds > 300_000
            || self.row_limit == 0
            || self.row_limit > 100_000
            || self.byte_limit == 0
            || self.byte_limit > 1_073_741_824
        {
            return Err(ContextQueryError::InvalidTextToSql);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadOnlySqlPlan {
    pub schema_version: u32,
    pub catalog_context_query_id: ResourceId,
    pub catalog_observation_id: ResourceId,
    pub catalog_observation_digest: Sha256Digest,
    pub catalog_projection_digest: Sha256Digest,
    pub execution: ReadOnlySqlExecutionBinding,
    pub from: SqlSource,
    pub joins: Vec<SqlJoin>,
    pub projections: Vec<SqlProjection>,
    pub predicates: Vec<SqlPredicate>,
    pub group_by: Vec<SqlColumnRef>,
    pub order_by: Vec<SqlOrderItem>,
    pub parameters: Vec<ClosedJsonValue>,
    pub limit: u32,
    pub offset: u32,
    pub generated_sql_digest: Sha256Digest,
    pub validation_evidence_digest: Sha256Digest,
    pub canonical_digest: Sha256Digest,
}

impl ReadOnlySqlPlan {
    pub fn validate(&self) -> Result<(), ContextQueryError> {
        self.execution.validate()?;
        self.from.object.validate()?;
        if self.schema_version != 1
            || self.catalog_context_query_id.kind() != ResourceKind::ContextQuery
            || self.catalog_observation_id.kind() != ResourceKind::ContextObservation
            || !is_identifier(&self.from.alias)
            || self.joins.len() >= MAX_SQL_SOURCES
            || self.projections.is_empty()
            || self.projections.len() > MAX_SQL_PROJECTIONS
            || self.predicates.len() > MAX_SQL_PREDICATES
            || self.group_by.len() > MAX_SQL_PROJECTIONS
            || self.order_by.len() > MAX_SQL_ORDER_ITEMS
            || self.parameters.len() > MAX_SQL_PREDICATES * 2
            || self.limit == 0
            || self.limit > self.execution.row_limit
            || self.offset > 10_000_000
            || !self
                .execution
                .allowed_schemas
                .contains(&self.from.object.schema)
        {
            return Err(ContextQueryError::InvalidTextToSql);
        }
        let mut aliases = BTreeSet::from([self.from.alias.as_str()]);
        for join in &self.joins {
            join.source.object.validate()?;
            join.left.validate()?;
            join.right.validate()?;
            let new_alias = join.source.alias.as_str();
            let connects_existing_source = (join.left.source_alias == new_alias
                && aliases.contains(join.right.source_alias.as_str()))
                || (join.right.source_alias == new_alias
                    && aliases.contains(join.left.source_alias.as_str()));
            if !is_identifier(new_alias)
                || !self
                    .execution
                    .allowed_schemas
                    .contains(&join.source.object.schema)
                || aliases.contains(new_alias)
                || !connects_existing_source
            {
                return Err(ContextQueryError::InvalidTextToSql);
            }
            aliases.insert(new_alias);
        }
        let valid_column = |column: &SqlColumnRef| {
            column.validate().is_ok() && aliases.contains(column.source_alias.as_str())
        };
        let mut output_names = BTreeSet::new();
        for projection in &self.projections {
            if !is_identifier(&projection.output_name)
                || !output_names.insert(&projection.output_name)
            {
                return Err(ContextQueryError::InvalidTextToSql);
            }
            match &projection.expression {
                SqlProjectionExpression::Column { column } if valid_column(column) => {}
                SqlProjectionExpression::Aggregate {
                    function: SqlAggregate::Count,
                    column: None,
                } => {}
                SqlProjectionExpression::Aggregate {
                    column: Some(column),
                    ..
                } if valid_column(column) => {}
                _ => return Err(ContextQueryError::InvalidTextToSql),
            }
        }
        for predicate in &self.predicates {
            if !valid_column(&predicate.column)
                || !valid_parameter_arity(predicate.operator, &predicate.parameter_ordinals)
                || predicate
                    .parameter_ordinals
                    .iter()
                    .any(|ordinal| usize::from(*ordinal) >= self.parameters.len())
            {
                return Err(ContextQueryError::InvalidTextToSql);
            }
        }
        if self.group_by.iter().any(|column| !valid_column(column))
            || self.order_by.iter().any(|item| !valid_column(&item.column))
            || self
                .parameters
                .iter()
                .any(|parameter| parameter.validate().is_err())
            || digest_without_field(self, "canonical_digest")? != self.canonical_digest
        {
            return Err(ContextQueryError::InvalidTextToSql);
        }
        Ok(())
    }
}

/// Durable facts required to authorize a Text2SQL plan as a generic CapabilityInvocation input.
///
/// Both facts are read under the same caller-owned PostgreSQL transaction that admits the
/// CapabilityInvocation. This function stays pure: it proves the catalog observation and exact
/// read-only execution binding without creating a Text2SQL-specific persistence authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextToSqlAdmissionFacts {
    pub run_id: ResourceId,
    pub input_value_id: ResourceId,
    pub input_value_kind: String,
    pub input_content_digest: Sha256Digest,
    pub selected_capability_name: CapabilityName,
    pub selected_capability_deployment: ExactDeploymentRef,
    pub selected_interface_revision: ExactVersionRef,
    pub selected_effect: Effect,
    pub catalog_query_id: ResourceId,
    pub catalog_run_id: ResourceId,
    pub catalog_context_deployment: ExactDeploymentRef,
    pub catalog_backend_database_identity_digest: Sha256Digest,
    pub catalog_backend_dialect: String,
    pub catalog_observation_id: ResourceId,
    pub catalog_observation_digest: Sha256Digest,
    pub catalog_projection_digest: Sha256Digest,
}

pub fn validate_text_to_sql_admission(
    plan: &ReadOnlySqlPlan,
    facts: &TextToSqlAdmissionFacts,
) -> Result<(), ContextQueryError> {
    plan.validate()?;
    facts
        .selected_capability_deployment
        .validate()
        .map_err(|_| ContextQueryError::InvalidTextToSql)?;
    facts
        .selected_interface_revision
        .validate()
        .map_err(|_| ContextQueryError::InvalidTextToSql)?;
    facts
        .catalog_context_deployment
        .validate()
        .map_err(|_| ContextQueryError::InvalidTextToSql)?;
    if facts.run_id.kind() != ResourceKind::Run
        || facts.input_value_id.kind() != ResourceKind::RunValue
        || facts.input_value_kind != TEXT2SQL_PLAN_VALUE_KIND
        || facts.input_content_digest != digest(plan)?
        || facts.selected_capability_name.as_str() != READONLY_DATABASE_CAPABILITY
        || facts.selected_capability_name != plan.execution.capability_name
        || facts.selected_capability_deployment != plan.execution.capability_deployment
        || facts.selected_interface_revision != plan.execution.interface_revision
        || facts.selected_effect != Effect::ReadOnly
        || facts.selected_effect != plan.execution.effect
        || facts.catalog_query_id != plan.catalog_context_query_id
        || facts.catalog_query_id.kind() != ResourceKind::ContextQuery
        || facts.catalog_run_id != facts.run_id
        || facts.catalog_context_deployment.resource_kind != ResourceKind::ContextDeployment
        || facts.catalog_backend_database_identity_digest != plan.execution.database_identity_digest
        || facts.catalog_backend_dialect != plan.execution.dialect
        || facts.catalog_observation_id != plan.catalog_observation_id
        || facts.catalog_observation_digest != plan.catalog_observation_digest
        || facts.catalog_projection_digest != plan.catalog_projection_digest
    {
        return Err(ContextQueryError::InvalidTextToSql);
    }
    Ok(())
}

fn valid_parameter_arity(operator: SqlComparisonOperator, ordinals: &[u16]) -> bool {
    match operator {
        SqlComparisonOperator::IsNull | SqlComparisonOperator::IsNotNull => ordinals.is_empty(),
        SqlComparisonOperator::Between => ordinals.len() == 2,
        SqlComparisonOperator::In => !ordinals.is_empty() && ordinals.len() <= 1_000,
        SqlComparisonOperator::Equal
        | SqlComparisonOperator::NotEqual
        | SqlComparisonOperator::LessThan
        | SqlComparisonOperator::LessThanOrEqual
        | SqlComparisonOperator::GreaterThan
        | SqlComparisonOperator::GreaterThanOrEqual
        | SqlComparisonOperator::LikeLiteral => ordinals.len() == 1,
    }
}

fn is_identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    value.len() <= MAX_SQL_IDENTIFIER_BYTES
        && (first.is_ascii_lowercase() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn is_sorted_unique<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::canonical_digest;
    use serde_json::json;

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1c9-32e4-75e1-a9e8-d95ca0f4{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn digest(label: &str) -> Sha256Digest {
        canonical_digest(&json!({"text2sql": label}))
            .unwrap()
            .parse()
            .unwrap()
    }

    fn exact_version(kind: ResourceKind, suffix: u16) -> ExactVersionRef {
        ExactVersionRef::new(id(kind, suffix), digest(&format!("version-{suffix}"))).unwrap()
    }

    fn valid_plan() -> ReadOnlySqlPlan {
        let deployment = ExactDeploymentRef::new(
            id(ResourceKind::CapabilityDeployment, 1),
            digest("deployment"),
        )
        .unwrap();
        let parameter = ClosedJsonValue::build(digest("parameter-schema"), json!(42)).unwrap();
        let mut plan = ReadOnlySqlPlan {
            schema_version: 1,
            catalog_context_query_id: id(ResourceKind::ContextQuery, 2),
            catalog_observation_id: id(ResourceKind::ContextObservation, 3),
            catalog_observation_digest: digest("catalog-observation"),
            catalog_projection_digest: digest("catalog-projection"),
            execution: ReadOnlySqlExecutionBinding {
                capability_name: READONLY_DATABASE_CAPABILITY.parse().unwrap(),
                capability_deployment: deployment,
                interface_revision: exact_version(ResourceKind::CapabilityInterfaceRevision, 4),
                effect: Effect::ReadOnly,
                database_identity_digest: digest("database"),
                dialect: "postgres".to_owned(),
                allowed_schemas: vec!["analytics".to_owned()],
                statement_timeout_milliseconds: 5_000,
                row_limit: 1_000,
                byte_limit: 1_048_576,
                cost_gate_digest: digest("cost-gate"),
            },
            from: SqlSource {
                object: SqlObjectName {
                    schema: "analytics".to_owned(),
                    object: "orders".to_owned(),
                },
                alias: "orders".to_owned(),
            },
            joins: vec![],
            projections: vec![SqlProjection {
                expression: SqlProjectionExpression::Column {
                    column: SqlColumnRef {
                        source_alias: "orders".to_owned(),
                        column: "total".to_owned(),
                    },
                },
                output_name: "total".to_owned(),
            }],
            predicates: vec![SqlPredicate {
                column: SqlColumnRef {
                    source_alias: "orders".to_owned(),
                    column: "customer_id".to_owned(),
                },
                operator: SqlComparisonOperator::Equal,
                parameter_ordinals: vec![0],
            }],
            group_by: vec![],
            order_by: vec![],
            parameters: vec![parameter],
            limit: 100,
            offset: 0,
            generated_sql_digest: digest("generated-sql"),
            validation_evidence_digest: digest("validation"),
            canonical_digest: digest("placeholder"),
        };
        plan.canonical_digest = digest_without_field(&plan, "canonical_digest").unwrap();
        plan
    }

    #[test]
    fn closed_read_only_plan_accepts_parameterized_select() {
        let plan = valid_plan();
        plan.validate().unwrap();
        assert_eq!(plan.execution.effect, Effect::ReadOnly);
        assert_eq!(plan.parameters[0].value, json!(42));
    }

    #[test]
    fn write_effect_and_identifier_injection_fail_closed() {
        let mut write = valid_plan();
        write.execution.effect = Effect::IdempotentWrite;
        write.canonical_digest = digest_without_field(&write, "canonical_digest").unwrap();
        assert_eq!(write.validate(), Err(ContextQueryError::InvalidTextToSql));

        let mut injection = valid_plan();
        injection.from.object.object = "orders;drop_table".to_owned();
        injection.canonical_digest = digest_without_field(&injection, "canonical_digest").unwrap();
        assert_eq!(
            injection.validate(),
            Err(ContextQueryError::InvalidTextToSql)
        );
    }

    #[test]
    fn schema_escape_parameter_mismatch_and_disconnected_join_are_rejected() {
        let mut schema_escape = valid_plan();
        schema_escape.from.object.schema = "private".to_owned();
        schema_escape.canonical_digest =
            digest_without_field(&schema_escape, "canonical_digest").unwrap();
        assert_eq!(
            schema_escape.validate(),
            Err(ContextQueryError::InvalidTextToSql)
        );

        let mut missing_parameter = valid_plan();
        missing_parameter.predicates[0].parameter_ordinals = vec![];
        missing_parameter.canonical_digest =
            digest_without_field(&missing_parameter, "canonical_digest").unwrap();
        assert_eq!(
            missing_parameter.validate(),
            Err(ContextQueryError::InvalidTextToSql)
        );

        let mut disconnected = valid_plan();
        disconnected.joins.push(SqlJoin {
            kind: SqlJoinKind::Inner,
            source: SqlSource {
                object: SqlObjectName {
                    schema: "analytics".to_owned(),
                    object: "customers".to_owned(),
                },
                alias: "customers".to_owned(),
            },
            left: SqlColumnRef {
                source_alias: "customers".to_owned(),
                column: "id".to_owned(),
            },
            right: SqlColumnRef {
                source_alias: "customers".to_owned(),
                column: "parent_id".to_owned(),
            },
        });
        disconnected.canonical_digest =
            digest_without_field(&disconnected, "canonical_digest").unwrap();
        assert_eq!(
            disconnected.validate(),
            Err(ContextQueryError::InvalidTextToSql)
        );
    }

    #[test]
    fn composed_admission_binds_catalog_and_exact_readonly_capability() {
        let plan = valid_plan();
        let facts = TextToSqlAdmissionFacts {
            run_id: id(ResourceKind::Run, 5),
            input_value_id: id(ResourceKind::RunValue, 6),
            input_value_kind: TEXT2SQL_PLAN_VALUE_KIND.to_owned(),
            input_content_digest: super::digest(&plan).unwrap(),
            selected_capability_name: READONLY_DATABASE_CAPABILITY.parse().unwrap(),
            selected_capability_deployment: plan.execution.capability_deployment.clone(),
            selected_interface_revision: plan.execution.interface_revision.clone(),
            selected_effect: Effect::ReadOnly,
            catalog_query_id: plan.catalog_context_query_id.clone(),
            catalog_run_id: id(ResourceKind::Run, 5),
            catalog_context_deployment: ExactDeploymentRef::new(
                id(ResourceKind::ContextDeployment, 7),
                digest("catalog-deployment"),
            )
            .unwrap(),
            catalog_backend_database_identity_digest: plan
                .execution
                .database_identity_digest
                .clone(),
            catalog_backend_dialect: plan.execution.dialect.clone(),
            catalog_observation_id: plan.catalog_observation_id.clone(),
            catalog_observation_digest: plan.catalog_observation_digest.clone(),
            catalog_projection_digest: plan.catalog_projection_digest.clone(),
        };
        validate_text_to_sql_admission(&plan, &facts).unwrap();

        let mut write = facts.clone();
        write.selected_effect = Effect::IdempotentWrite;
        assert_eq!(
            validate_text_to_sql_admission(&plan, &write),
            Err(ContextQueryError::InvalidTextToSql)
        );
        let mut wrong_capability = facts.clone();
        wrong_capability.selected_capability_name = "database.query.write".parse().unwrap();
        assert_eq!(
            validate_text_to_sql_admission(&plan, &wrong_capability),
            Err(ContextQueryError::InvalidTextToSql)
        );
        let mut foreign_run = facts.clone();
        foreign_run.catalog_run_id = id(ResourceKind::Run, 8);
        assert_eq!(
            validate_text_to_sql_admission(&plan, &foreign_run),
            Err(ContextQueryError::InvalidTextToSql)
        );
        let mut drifted_catalog = facts;
        drifted_catalog.catalog_observation_digest = digest("drifted-observation");
        assert_eq!(
            validate_text_to_sql_admission(&plan, &drifted_catalog),
            Err(ContextQueryError::InvalidTextToSql)
        );
    }
}
