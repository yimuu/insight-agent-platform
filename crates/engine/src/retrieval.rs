//! Frozen first-class Retrieval execution and publication authority.
//!
//! This module is intentionally independent from SQL repositories and worker
//! adapters. It decodes the exact deployment evidence shared by both and
//! carries only a typed, already-public completion sidecar across the worker
//! boundary. Raw retrieval input, model output, and public candidates have no
//! representation here.

use std::{collections::BTreeSet, fmt};

use semver::Version;
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    resource_policy::RetrievalPublicPolicy,
    response::{WorkflowRetrieval, WorkflowRetrievalPublicProjection},
    ActivationId, EffectIdempotency, RunId, WorkerCancellation, WorkerEffectClass,
    WorkerEffectPolicy,
};

pub const RETRIEVAL_BINDING_INVALID: &str = "ENGINE_RETRIEVAL_BINDING_INVALID";
pub const RETRIEVAL_COMPLETION_INVALID: &str = "ENGINE_RETRIEVAL_COMPLETION_INVALID";

const MAX_RETRIEVAL_PUBLIC_PROJECTION_BYTES: usize = 1024 * 1024;
const RETRIEVAL_ID_DOMAIN: &[u8] = b"insight-agent-platform/retrieval-public-id/v1\0";

/// Workspace-internal, read-only evidence required to prove that a registered
/// Retrieval still matches an immutable deployment binding.
///
/// Resource registries implement this view without moving provider objects or
/// registry ownership into the engine crate.
#[doc(hidden)]
pub trait RegisteredRetrievalView {
    fn resource_id(&self) -> &str;
    fn resource_version(&self) -> &Version;
    fn descriptor_hash(&self) -> &str;
    fn input_schema(&self) -> &Value;
    fn output_schema(&self) -> &Value;
    fn query_field(&self) -> &str;
    fn effect(&self) -> &str;
    fn idempotency(&self) -> &str;
    fn cancellation(&self) -> &str;
    fn required_capabilities(&self) -> Vec<&str>;
    fn public_policy(&self) -> &RetrievalPublicPolicy;
}

/// Safe worker sidecar for one completed Retrieval task.
///
/// `public=None` is the required representation of a fully private frozen
/// policy. This type can never contain raw input or a raw executor result.
#[derive(Clone, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RetrievalCompletion {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    public: Option<WorkflowRetrieval>,
}

impl fmt::Debug for RetrievalCompletion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetrievalCompletion")
            .field("public", &self.public.is_some())
            .field(
                "public_result_count",
                &self.public.as_ref().map(|value| value.results().len()),
            )
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetrievalCompletionWire {
    #[serde(default)]
    public: Option<WorkflowRetrieval>,
}

impl RetrievalCompletion {
    pub fn new(public: Option<WorkflowRetrieval>) -> Result<Self, &'static str> {
        if public
            .as_ref()
            .is_some_and(|retrieval| match serde_jcs::to_vec(retrieval) {
                Ok(encoded) => encoded.len() > MAX_RETRIEVAL_PUBLIC_PROJECTION_BYTES,
                Err(_) => true,
            })
        {
            return Err(RETRIEVAL_COMPLETION_INVALID);
        }
        Ok(Self { public })
    }

    pub fn private() -> Self {
        Self { public: None }
    }

    pub fn public(&self) -> Option<&WorkflowRetrieval> {
        self.public.as_ref()
    }
}

impl<'de> Deserialize<'de> for RetrievalCompletion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = RetrievalCompletionWire::deserialize(deserializer)?;
        Self::new(wire.public).map_err(D::Error::custom)
    }
}

/// Stable caller-visible identity for one logical Retrieval activation.
/// Attempts are deliberately excluded so retries cannot mint duplicates.
pub fn deterministic_retrieval_id(run_id: &RunId, activation_id: &ActivationId) -> String {
    let mut hasher = Sha256::new();
    hasher.update(RETRIEVAL_ID_DOMAIN);
    hash_part(&mut hasher, run_id.as_str().as_bytes());
    hash_part(&mut hasher, activation_id.as_str().as_bytes());
    let digest = hasher.finalize();
    let mut output = String::with_capacity(68);
    output.push_str("ret_");
    for byte in digest {
        use fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn hash_part(hasher: &mut Sha256, value: &[u8]) {
    hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(value);
}

/// Exact immutable Retrieval target frozen in a Deployment Revision.
#[derive(Debug, Clone, PartialEq)]
pub struct FrozenRetrievalTarget {
    resource_id: String,
    resource_version: Version,
    descriptor_hash: String,
    input_schema: Value,
    output_schema: Value,
    query_field: String,
    effect: String,
    idempotency: String,
    cancellation: String,
    required_capabilities: Vec<String>,
    publish: bool,
    descriptor_public_policy: Value,
    effective_public_policy: Value,
}

impl FrozenRetrievalTarget {
    pub fn from_deployment_binding(binding: &Value) -> Result<Self, &'static str> {
        let object = binding.as_object().ok_or(RETRIEVAL_BINDING_INVALID)?;
        let expected = BTreeSet::from([
            "adapter",
            "cancellation",
            "descriptor_hash",
            "effective_public_policy",
            "effect",
            "idempotency",
            "input_schema",
            "output_schema",
            "public",
            "publish",
            "query_field",
            "required_capabilities",
            "retrieval_id",
            "retrieval_version",
        ]);
        if object.keys().map(String::as_str).collect::<BTreeSet<_>>() != expected
            || object.get("adapter").and_then(Value::as_str) != Some("native_retrieval")
        {
            return Err(RETRIEVAL_BINDING_INVALID);
        }

        let resource_id = bounded_label(object, "retrieval_id", 128)?;
        let resource_version = Version::parse(&bounded_label(object, "retrieval_version", 64)?)
            .map_err(|_| RETRIEVAL_BINDING_INVALID)?;
        let descriptor_hash = object
            .get("descriptor_hash")
            .and_then(Value::as_str)
            .filter(|value| valid_lower_hex(value, 64))
            .ok_or(RETRIEVAL_BINDING_INVALID)?
            .to_owned();
        let input_schema = canonical_object(object, "input_schema")?;
        let output_schema = canonical_object(object, "output_schema")?;
        let query_field = bounded_label(object, "query_field", 128)?;
        let effect = one_of(object, "effect", &["pure", "read_only", "mutating"])?;
        let idempotency = one_of(object, "idempotency", &["idempotent", "non_idempotent"])?;
        let cancellation = one_of(object, "cancellation", &["cooperative", "not_supported"])?;
        let required_capabilities = object
            .get("required_capabilities")
            .and_then(Value::as_array)
            .ok_or(RETRIEVAL_BINDING_INVALID)?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .filter(|value| !value.is_empty() && value.len() <= 128)
                    .map(ToOwned::to_owned)
                    .ok_or(RETRIEVAL_BINDING_INVALID)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if required_capabilities
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(RETRIEVAL_BINDING_INVALID);
        }
        let publish = object
            .get("publish")
            .and_then(Value::as_bool)
            .ok_or(RETRIEVAL_BINDING_INVALID)?;
        let descriptor_public_policy =
            normalized_policy(object.get("public").ok_or(RETRIEVAL_BINDING_INVALID)?)?;
        let effective_public_policy = normalized_policy(
            object
                .get("effective_public_policy")
                .ok_or(RETRIEVAL_BINDING_INVALID)?,
        )?;
        let private_policy = serde_json::to_value(RetrievalPublicPolicy::private())
            .map_err(|_| RETRIEVAL_BINDING_INVALID)?;
        let expected_effective = if publish {
            &descriptor_public_policy
        } else {
            &private_policy
        };
        if &effective_public_policy != expected_effective
            || WorkflowRetrievalPublicProjection::from_frozen_effective_policy(
                &effective_public_policy,
                &query_field,
            )
            .is_err()
        {
            return Err(RETRIEVAL_BINDING_INVALID);
        }

        Ok(Self {
            resource_id,
            resource_version,
            descriptor_hash,
            input_schema,
            output_schema,
            query_field,
            effect,
            idempotency,
            cancellation,
            required_capabilities,
            publish,
            descriptor_public_policy,
            effective_public_policy,
        })
    }

    pub fn validate_registered<R: RegisteredRetrievalView + ?Sized>(
        &self,
        registered: &R,
    ) -> Result<(), &'static str> {
        let public = serde_json::to_value(registered.public_policy())
            .map_err(|_| RETRIEVAL_BINDING_INVALID)?;
        if registered.resource_id() != self.resource_id
            || registered.resource_version() != &self.resource_version
            || registered.descriptor_hash() != self.descriptor_hash
            || registered.input_schema() != &self.input_schema
            || registered.output_schema() != &self.output_schema
            || registered.query_field() != self.query_field
            || registered.effect() != self.effect
            || registered.idempotency() != self.idempotency
            || registered.cancellation() != self.cancellation
            || registered
                .required_capabilities()
                .into_iter()
                .ne(self.required_capabilities.iter().map(String::as_str))
            || public != self.descriptor_public_policy
        {
            return Err(RETRIEVAL_BINDING_INVALID);
        }
        Ok(())
    }

    pub fn validate_effect_policy(&self, policy: &WorkerEffectPolicy) -> Result<(), &'static str> {
        let effect = match self.effect.as_str() {
            "pure" => WorkerEffectClass::Pure,
            "read_only" => WorkerEffectClass::ReadOnly,
            "mutating" => WorkerEffectClass::Mutating,
            _ => return Err(RETRIEVAL_BINDING_INVALID),
        };
        let idempotency = match self.idempotency.as_str() {
            "idempotent" => EffectIdempotency::Idempotent,
            "non_idempotent" => EffectIdempotency::NonIdempotent,
            _ => return Err(RETRIEVAL_BINDING_INVALID),
        };
        let cancellation = match self.cancellation.as_str() {
            "cooperative" => WorkerCancellation::Cooperative,
            "not_supported" => WorkerCancellation::LeaseOnly,
            _ => return Err(RETRIEVAL_BINDING_INVALID),
        };
        if policy.effect_class() != effect
            || policy.effect_idempotency() != idempotency
            || policy.cancellation() != cancellation
        {
            return Err(RETRIEVAL_BINDING_INVALID);
        }
        Ok(())
    }

    pub fn validate_completion(
        &self,
        expected_retrieval_id: &str,
        completion: &RetrievalCompletion,
    ) -> Result<(), &'static str> {
        let projection = self.public_projection()?;
        match (
            projection.query_authorized() || projection.result_authorized(),
            completion.public(),
        ) {
            (false, None) => Ok(()),
            (true, Some(public)) => projection
                .validate_frozen_completed(expected_retrieval_id, public)
                .map_err(|_| RETRIEVAL_COMPLETION_INVALID),
            (false, Some(_)) | (true, None) => Err(RETRIEVAL_COMPLETION_INVALID),
        }
    }

    pub fn public_projection(&self) -> Result<WorkflowRetrievalPublicProjection, &'static str> {
        WorkflowRetrievalPublicProjection::from_frozen_effective_policy(
            &self.effective_public_policy,
            &self.query_field,
        )
        .map_err(|_| RETRIEVAL_BINDING_INVALID)
    }

    pub fn resource_id(&self) -> &str {
        &self.resource_id
    }
    pub fn resource_version(&self) -> &Version {
        &self.resource_version
    }
    pub fn descriptor_hash(&self) -> &str {
        &self.descriptor_hash
    }
    pub fn input_schema(&self) -> &Value {
        &self.input_schema
    }
    pub fn output_schema(&self) -> &Value {
        &self.output_schema
    }
    pub fn query_field(&self) -> &str {
        &self.query_field
    }
    pub fn publish(&self) -> bool {
        self.publish
    }
    pub fn descriptor_public_policy(&self) -> &Value {
        &self.descriptor_public_policy
    }
    pub fn effective_public_policy(&self) -> &Value {
        &self.effective_public_policy
    }
}

fn normalized_policy(value: &Value) -> Result<Value, &'static str> {
    let policy = serde_json::from_value::<RetrievalPublicPolicy>(value.clone())
        .map_err(|_| RETRIEVAL_BINDING_INVALID)?;
    let normalized = serde_json::to_value(policy).map_err(|_| RETRIEVAL_BINDING_INVALID)?;
    if &normalized != value {
        return Err(RETRIEVAL_BINDING_INVALID);
    }
    Ok(normalized)
}

fn canonical_object(
    object: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<Value, &'static str> {
    let value = object
        .get(field)
        .filter(|value| value.is_object())
        .ok_or(RETRIEVAL_BINDING_INVALID)?;
    serde_jcs::to_vec(value).map_err(|_| RETRIEVAL_BINDING_INVALID)?;
    Ok(value.clone())
}

fn bounded_label(
    object: &serde_json::Map<String, Value>,
    field: &str,
    max: usize,
) -> Result<String, &'static str> {
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| {
            !value.is_empty()
                && value.len() <= max
                && !value
                    .chars()
                    .any(|character| character.is_control() || character.is_whitespace())
        })
        .map(ToOwned::to_owned)
        .ok_or(RETRIEVAL_BINDING_INVALID)
}

fn one_of(
    object: &serde_json::Map<String, Value>,
    field: &str,
    allowed: &[&str],
) -> Result<String, &'static str> {
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| allowed.contains(value))
        .map(ToOwned::to_owned)
        .ok_or(RETRIEVAL_BINDING_INVALID)
}

fn valid_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn deterministic_identity_is_attempt_independent_and_domain_bounded() {
        let run = RunId::new("run_retrieval").unwrap();
        let activation = ActivationId::new("activation_retrieval").unwrap();
        let identity = deterministic_retrieval_id(&run, &activation);
        assert_eq!(identity.len(), 68);
        assert!(identity.starts_with("ret_"));
        assert_eq!(identity, deterministic_retrieval_id(&run, &activation));
    }

    #[test]
    fn frozen_target_rejects_agent_policy_escalation() {
        let policy = json!({"query": false, "result": null});
        let binding = json!({
            "adapter": "native_retrieval",
            "retrieval_id": "search.docs",
            "retrieval_version": "1.0.0",
            "descriptor_hash": "a".repeat(64),
            "input_schema": {},
            "output_schema": {},
            "query_field": "query",
            "effect": "read_only",
            "idempotency": "idempotent",
            "cancellation": "cooperative",
            "required_capabilities": [],
            "publish": false,
            "public": policy,
            "effective_public_policy": {"query": true, "result": null}
        });
        assert_eq!(
            FrozenRetrievalTarget::from_deployment_binding(&binding).unwrap_err(),
            RETRIEVAL_BINDING_INVALID
        );
    }
}
