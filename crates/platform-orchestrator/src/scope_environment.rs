use crate::{ExactDataPortRef, OrchestratorError};
use insight_platform_contracts::{
    canonical_digest, HardLimitProfile, ResourceId, ResourceKind, Sha256Digest,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScopeEnvironmentLimits {
    pub maximum_bindings_per_scope: usize,
    pub maximum_lexical_depth: usize,
}

impl ScopeEnvironmentLimits {
    pub fn from_profile(profile: &HardLimitProfile) -> Result<Self, OrchestratorError> {
        profile
            .validate()
            .map_err(|_| OrchestratorError::InvalidScopeEnvironment)?;
        let maximum_bindings_per_scope =
            usize::try_from(profile.run_scheduler.value_refs_per_run.q1_default)
                .map_err(|_| OrchestratorError::InvalidScopeEnvironment)?;
        let maximum_lexical_depth = usize::try_from(profile.registry_plan.plan_nodes.q1_default)
            .map_err(|_| OrchestratorError::InvalidScopeEnvironment)?;
        if maximum_bindings_per_scope == 0 || maximum_lexical_depth == 0 {
            return Err(OrchestratorError::InvalidScopeEnvironment);
        }
        Ok(Self {
            maximum_bindings_per_scope,
            maximum_lexical_depth,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactRunValueRef {
    pub value_id: ResourceId,
    pub schema_digest: Sha256Digest,
    pub content_digest: Sha256Digest,
}

impl ExactRunValueRef {
    pub fn validate_for_port(&self, port: &ExactDataPortRef) -> Result<(), OrchestratorError> {
        if self.value_id.kind() != ResourceKind::RunValue
            || &self.schema_digest != port.schema_digest()
        {
            return Err(OrchestratorError::InvalidScopeEnvironment);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeDataBinding {
    pub port: ExactDataPortRef,
    pub value: ExactRunValueRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeDataEnvironmentSnapshot {
    pub schema_version: u32,
    pub bindings: Vec<ScopeDataBinding>,
    pub canonical_digest: Sha256Digest,
}

impl ScopeDataEnvironmentSnapshot {
    pub fn build(
        bindings: BTreeMap<ExactDataPortRef, ExactRunValueRef>,
        limits: ScopeEnvironmentLimits,
    ) -> Result<Self, OrchestratorError> {
        let bindings = bindings
            .into_iter()
            .map(|(port, value)| ScopeDataBinding { port, value })
            .collect::<Vec<_>>();
        validate_bindings(&bindings, limits)?;
        let canonical_digest = environment_digest(1, &bindings)?;
        Ok(Self {
            schema_version: 1,
            bindings,
            canonical_digest,
        })
    }

    pub fn empty(limits: ScopeEnvironmentLimits) -> Result<Self, OrchestratorError> {
        Self::build(BTreeMap::new(), limits)
    }

    pub fn validate(&self, limits: ScopeEnvironmentLimits) -> Result<(), OrchestratorError> {
        if self.schema_version != 1
            || validate_bindings(&self.bindings, limits).is_err()
            || environment_digest(self.schema_version, &self.bindings)? != self.canonical_digest
        {
            return Err(OrchestratorError::InvalidScopeEnvironment);
        }
        Ok(())
    }

    pub fn bind_new(
        &mut self,
        port: ExactDataPortRef,
        value: ExactRunValueRef,
        limits: ScopeEnvironmentLimits,
    ) -> Result<(), OrchestratorError> {
        value.validate_for_port(&port)?;
        if self.bindings.iter().any(|binding| binding.port == port)
            || self.bindings.len() >= limits.maximum_bindings_per_scope
        {
            return Err(OrchestratorError::ScopePortConflict);
        }
        self.bindings.push(ScopeDataBinding { port, value });
        self.bindings
            .sort_by(|left, right| left.port.cmp(&right.port));
        self.canonical_digest = environment_digest(self.schema_version, &self.bindings)?;
        Ok(())
    }
}

pub fn resolve_scope_inputs(
    required_ports: &[ExactDataPortRef],
    lexical_chain_nearest_first: &[ScopeDataEnvironmentSnapshot],
    limits: ScopeEnvironmentLimits,
) -> Result<Vec<ExactRunValueRef>, OrchestratorError> {
    if lexical_chain_nearest_first.is_empty()
        || lexical_chain_nearest_first.len() > limits.maximum_lexical_depth
        || required_ports.len() > limits.maximum_bindings_per_scope
        || required_ports.iter().collect::<BTreeSet<_>>().len() != required_ports.len()
    {
        return Err(OrchestratorError::InvalidScopeEnvironment);
    }
    for environment in lexical_chain_nearest_first {
        environment.validate(limits)?;
    }
    required_ports
        .iter()
        .map(|port| {
            lexical_chain_nearest_first
                .iter()
                .find_map(|environment| {
                    environment
                        .bindings
                        .iter()
                        .find(|binding| &binding.port == port)
                        .map(|binding| binding.value.clone())
                })
                .ok_or(OrchestratorError::ScopePortUnbound)
        })
        .collect()
}

fn validate_bindings(
    bindings: &[ScopeDataBinding],
    limits: ScopeEnvironmentLimits,
) -> Result<(), OrchestratorError> {
    if limits.maximum_bindings_per_scope == 0
        || limits.maximum_lexical_depth == 0
        || bindings.len() > limits.maximum_bindings_per_scope
        || bindings.windows(2).any(|pair| pair[0].port >= pair[1].port)
        || bindings
            .iter()
            .any(|binding| binding.value.validate_for_port(&binding.port).is_err())
    {
        return Err(OrchestratorError::InvalidScopeEnvironment);
    }
    Ok(())
}

#[derive(Serialize)]
struct UnsignedEnvironment<'a> {
    schema_version: u32,
    bindings: &'a [ScopeDataBinding],
}

fn environment_digest(
    schema_version: u32,
    bindings: &[ScopeDataBinding],
) -> Result<Sha256Digest, OrchestratorError> {
    let value = serde_json::to_value(UnsignedEnvironment {
        schema_version,
        bindings,
    })
    .map_err(|_| OrchestratorError::Canonicalization)?;
    canonical_digest(&value)
        .map_err(|_| OrchestratorError::Canonicalization)?
        .parse()
        .map_err(|_| OrchestratorError::Canonicalization)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DataPortKey, PlanNodeKey};
    use std::str::FromStr;

    fn digest(character: char) -> Sha256Digest {
        Sha256Digest::from_str(&format!("sha256:{}", character.to_string().repeat(64))).unwrap()
    }

    fn port(name: &str) -> ExactDataPortRef {
        ExactDataPortRef::NodeOutput {
            producer_node_id: PlanNodeKey::new("compute".to_owned()).unwrap(),
            port_id: DataPortKey::new(name.to_owned()).unwrap(),
            schema_digest: digest('1'),
        }
    }

    fn value(suffix: &str) -> ExactRunValueRef {
        ExactRunValueRef {
            value_id: format!("val_0198f1c5-0787-75e1-a9e8-d95ca0f3{suffix}")
                .parse()
                .unwrap(),
            schema_digest: digest('1'),
            content_digest: digest('2'),
        }
    }

    fn limits() -> ScopeEnvironmentLimits {
        ScopeEnvironmentLimits {
            maximum_bindings_per_scope: 4,
            maximum_lexical_depth: 3,
        }
    }

    #[test]
    fn nearest_scope_shadows_without_copying_the_parent_value() {
        let target = port("state");
        let root = ScopeDataEnvironmentSnapshot::build(
            BTreeMap::from([(target.clone(), value("a001"))]),
            limits(),
        )
        .unwrap();
        let child = ScopeDataEnvironmentSnapshot::build(
            BTreeMap::from([(target.clone(), value("a002"))]),
            limits(),
        )
        .unwrap();
        let resolved = resolve_scope_inputs(&[target], &[child, root], limits()).unwrap();
        assert_eq!(resolved, vec![value("a002")]);
    }

    #[test]
    fn duplicate_local_binding_digest_drift_and_depth_fail_closed() {
        let target = port("result");
        let mut environment = ScopeDataEnvironmentSnapshot::empty(limits()).unwrap();
        environment
            .bind_new(target.clone(), value("b001"), limits())
            .unwrap();
        assert_eq!(
            environment.bind_new(target.clone(), value("b002"), limits()),
            Err(OrchestratorError::ScopePortConflict)
        );
        let mut forged = environment.clone();
        forged.canonical_digest = digest('f');
        assert_eq!(
            forged.validate(limits()),
            Err(OrchestratorError::InvalidScopeEnvironment)
        );
        assert_eq!(
            resolve_scope_inputs(
                &[target],
                &[
                    environment.clone(),
                    environment.clone(),
                    environment.clone(),
                    environment
                ],
                limits(),
            ),
            Err(OrchestratorError::InvalidScopeEnvironment)
        );
    }
}
