//! Backend-neutral durable Retrieval publication preparation and validation.
//!
//! SQL backends call this module inside the fenced task-success transaction.
//! The only payload accepted here is the typed public sidecar already produced
//! by the Retrieval adapter; raw input, model output and executor candidates
//! are structurally unavailable.

use super::RepositoryErrorExt as _;

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use insight_engine::{
    plan::DescriptorValue,
    response::{WorkflowRetrieval, WorkflowRetrievalPublicProjection},
    retrieval::{deterministic_retrieval_id, FrozenRetrievalTarget},
    worker::TaskExecutionOrigin,
    ActivationId, AttemptNo, RunId, SchedulerTaskKind,
};

use super::{RepositoryError, SchedulerTaskClaim, SchedulerTaskSuccess};

const RETRIEVAL_PUBLICATION_HASH_DOMAIN: &str = "insight-agent-platform/retrieval-publication/v1";
const VALUE_HASH_DOMAIN: &str = "insight-agent-platform/retrieval-public-value/v1";

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedRetrievalPublication {
    run_id: RunId,
    retrieval_id: String,
    task_id: String,
    activation_id: ActivationId,
    node_id: String,
    attempt_no: AttemptNo,
    resource_id: String,
    resource_version: String,
    descriptor_hash: String,
    query_field: String,
    effective_public_policy: Value,
    effective_public_policy_hash: String,
    public_projection: Option<Value>,
    public_projection_hash: Option<String>,
    completion_transition_key: String,
    completion_intent_hash: String,
    completion_event_id: String,
    completion_event_seq: u64,
    publication_hash: String,
}

#[allow(dead_code)]
impl PreparedRetrievalPublication {
    pub(crate) fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub(crate) fn retrieval_id(&self) -> &str {
        &self.retrieval_id
    }
    pub(crate) fn task_id(&self) -> &str {
        &self.task_id
    }
    pub(crate) fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }
    pub(crate) fn node_id(&self) -> &str {
        &self.node_id
    }
    pub(crate) fn attempt_no(&self) -> AttemptNo {
        self.attempt_no
    }
    pub(crate) fn resource_id(&self) -> &str {
        &self.resource_id
    }
    pub(crate) fn resource_version(&self) -> &str {
        &self.resource_version
    }
    pub(crate) fn descriptor_hash(&self) -> &str {
        &self.descriptor_hash
    }
    pub(crate) fn query_field(&self) -> &str {
        &self.query_field
    }
    pub(crate) fn effective_public_policy(&self) -> &Value {
        &self.effective_public_policy
    }
    pub(crate) fn effective_public_policy_hash(&self) -> &str {
        &self.effective_public_policy_hash
    }
    pub(crate) fn public_projection(&self) -> Option<&Value> {
        self.public_projection.as_ref()
    }
    pub(crate) fn public_retrieval(&self) -> Result<Option<WorkflowRetrieval>, RepositoryError> {
        self.public_projection
            .as_ref()
            .map(|value| {
                serde_json::from_value(value.clone()).map_err(|_| RepositoryError::invalid_data())
            })
            .transpose()
    }
    pub(crate) fn public_projection_hash(&self) -> Option<&str> {
        self.public_projection_hash.as_deref()
    }
    pub(crate) fn completion_transition_key(&self) -> &str {
        &self.completion_transition_key
    }
    pub(crate) fn completion_intent_hash(&self) -> &str {
        &self.completion_intent_hash
    }
    pub(crate) fn completion_event_id(&self) -> &str {
        &self.completion_event_id
    }
    pub(crate) fn completion_event_seq(&self) -> u64 {
        self.completion_event_seq
    }
    pub(crate) fn publication_hash(&self) -> &str {
        &self.publication_hash
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_retrieval_publication(
    claim: &SchedulerTaskClaim,
    success: &SchedulerTaskSuccess,
    completion_transition_key: &str,
    completion_intent_hash: &str,
    completion_event_id: &str,
    completion_event_seq: u64,
) -> Result<Option<PreparedRetrievalPublication>, RepositoryError> {
    let request = claim.envelope().request();
    if request.task_kind() != SchedulerTaskKind::Retrieval {
        if success.result().retrieval_completion().is_some() {
            return Err(RepositoryError::invalid_data());
        }
        return Ok(None);
    }
    let completion = success
        .result()
        .retrieval_completion()
        .ok_or_else(RepositoryError::invalid_data)?;
    if !matches!(request.origin(), TaskExecutionOrigin::Workflow)
        || request.descriptor_version().as_str() != "1"
        || completion_event_seq == 0
    {
        return Err(RepositoryError::invalid_data());
    }
    let target = FrozenRetrievalTarget::from_deployment_binding(request.deployment_binding())
        .map_err(|_| RepositoryError::invalid_data())?;
    target
        .validate_effect_policy(request.effect_policy())
        .map_err(|_| RepositoryError::invalid_data())?;
    if request.implementation() != target.resource_id()
        || request.worker_version().as_str() != target.resource_version().to_string()
        || request.public_configuration().get("publish")
            != Some(&DescriptorValue::Boolean(target.publish()))
    {
        return Err(RepositoryError::invalid_data());
    }

    let retrieval_id = deterministic_retrieval_id(claim.run_id(), claim.activation_id());
    target
        .validate_completion(&retrieval_id, completion)
        .map_err(|_| RepositoryError::invalid_data())?;
    let effective_public_policy = target.effective_public_policy().clone();
    let effective_public_policy_hash = hash_value(&effective_public_policy)?;
    let public_projection = completion
        .public()
        .map(serde_json::to_value)
        .transpose()
        .map_err(|_| RepositoryError::canonicalization())?;
    let public_projection_hash = public_projection.as_ref().map(hash_value).transpose()?;
    let mut prepared = PreparedRetrievalPublication {
        run_id: claim.run_id().clone(),
        retrieval_id,
        task_id: claim.task_id().as_str().to_owned(),
        activation_id: claim.activation_id().clone(),
        node_id: request.node_id().as_str().to_owned(),
        attempt_no: claim.envelope().attempt_no(),
        resource_id: target.resource_id().to_owned(),
        resource_version: target.resource_version().to_string(),
        descriptor_hash: target.descriptor_hash().to_owned(),
        query_field: target.query_field().to_owned(),
        effective_public_policy,
        effective_public_policy_hash,
        public_projection,
        public_projection_hash,
        completion_transition_key: completion_transition_key.to_owned(),
        completion_intent_hash: completion_intent_hash.to_owned(),
        completion_event_id: completion_event_id.to_owned(),
        completion_event_seq,
        publication_hash: String::new(),
    };
    prepared.publication_hash = publication_hash(publication_hash_document(&prepared))?;
    Ok(Some(prepared))
}

/// Decoded append-only row used by exact replay and terminal aggregation.
/// All fields are public-authority metadata or already-public payload.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredRetrievalPublication {
    pub(crate) run_id: RunId,
    pub(crate) retrieval_id: String,
    pub(crate) task_id: String,
    pub(crate) activation_id: ActivationId,
    pub(crate) node_id: String,
    pub(crate) attempt_no: AttemptNo,
    pub(crate) resource_id: String,
    pub(crate) resource_version: String,
    pub(crate) descriptor_hash: String,
    pub(crate) query_field: String,
    pub(crate) effective_public_policy: Value,
    pub(crate) effective_public_policy_hash: String,
    pub(crate) public_projection: Option<Value>,
    pub(crate) public_projection_hash: Option<String>,
    pub(crate) completion_transition_key: String,
    pub(crate) completion_intent_hash: String,
    pub(crate) completion_event_id: String,
    pub(crate) completion_event_seq: u64,
    pub(crate) publication_hash: String,
}

impl StoredRetrievalPublication {
    pub(crate) fn validate_and_project(
        &self,
    ) -> Result<Option<WorkflowRetrieval>, RepositoryError> {
        if self.retrieval_id != deterministic_retrieval_id(&self.run_id, &self.activation_id)
            || self.attempt_no.get() == 0
            || self.completion_event_seq == 0
            || hash_value(&self.effective_public_policy)? != self.effective_public_policy_hash
            || publication_hash(publication_hash_document(self))? != self.publication_hash
        {
            return Err(RepositoryError::invalid_data());
        }
        match (&self.public_projection, &self.public_projection_hash) {
            (None, None) => {}
            (Some(value), Some(expected)) if hash_value(value)? == *expected => {}
            _ => return Err(RepositoryError::invalid_data()),
        }
        let policy = WorkflowRetrievalPublicProjection::from_frozen_effective_policy(
            &self.effective_public_policy,
            &self.query_field,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        let authorized = policy.query_authorized() || policy.result_authorized();
        match (authorized, self.public_projection.as_ref()) {
            (false, None) => Ok(None),
            (true, Some(value)) => {
                let retrieval = serde_json::from_value::<WorkflowRetrieval>(value.clone())
                    .map_err(|_| RepositoryError::invalid_data())?;
                policy
                    .validate_frozen_completed(&self.retrieval_id, &retrieval)
                    .map_err(|_| RepositoryError::invalid_data())?;
                Ok(Some(retrieval))
            }
            (false, Some(_)) | (true, None) => Err(RepositoryError::invalid_data()),
        }
    }
}

pub(crate) fn validate_exact_retrieval_publication(
    stored: &StoredRetrievalPublication,
    expected: &PreparedRetrievalPublication,
) -> Result<(), RepositoryError> {
    stored.validate_and_project()?;
    if stored.run_id != expected.run_id
        || stored.retrieval_id != expected.retrieval_id
        || stored.task_id != expected.task_id
        || stored.activation_id != expected.activation_id
        || stored.node_id != expected.node_id
        || stored.attempt_no != expected.attempt_no
        || stored.resource_id != expected.resource_id
        || stored.resource_version != expected.resource_version
        || stored.descriptor_hash != expected.descriptor_hash
        || stored.query_field != expected.query_field
        || stored.effective_public_policy != expected.effective_public_policy
        || stored.effective_public_policy_hash != expected.effective_public_policy_hash
        || stored.public_projection != expected.public_projection
        || stored.public_projection_hash != expected.public_projection_hash
        || stored.completion_transition_key != expected.completion_transition_key
        || stored.completion_intent_hash != expected.completion_intent_hash
        || stored.completion_event_id != expected.completion_event_id
        || stored.completion_event_seq != expected.completion_event_seq
        || stored.publication_hash != expected.publication_hash
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

trait PublicationHashFields {
    fn run_id(&self) -> &str;
    fn retrieval_id(&self) -> &str;
    fn task_id(&self) -> &str;
    fn activation_id(&self) -> &str;
    fn node_id(&self) -> &str;
    fn attempt_no(&self) -> u32;
    fn resource_id(&self) -> &str;
    fn resource_version(&self) -> &str;
    fn descriptor_hash(&self) -> &str;
    fn query_field(&self) -> &str;
    fn effective_public_policy_hash(&self) -> &str;
    fn public_projection_hash(&self) -> Option<&str>;
    fn completion_transition_key(&self) -> &str;
    fn completion_intent_hash(&self) -> &str;
    fn completion_event_id(&self) -> &str;
    fn completion_event_seq(&self) -> u64;
}

impl PublicationHashFields for PreparedRetrievalPublication {
    fn run_id(&self) -> &str {
        self.run_id.as_str()
    }
    fn retrieval_id(&self) -> &str {
        &self.retrieval_id
    }
    fn task_id(&self) -> &str {
        &self.task_id
    }
    fn activation_id(&self) -> &str {
        self.activation_id.as_str()
    }
    fn node_id(&self) -> &str {
        &self.node_id
    }
    fn attempt_no(&self) -> u32 {
        self.attempt_no.get()
    }
    fn resource_id(&self) -> &str {
        &self.resource_id
    }
    fn resource_version(&self) -> &str {
        &self.resource_version
    }
    fn descriptor_hash(&self) -> &str {
        &self.descriptor_hash
    }
    fn query_field(&self) -> &str {
        &self.query_field
    }
    fn effective_public_policy_hash(&self) -> &str {
        &self.effective_public_policy_hash
    }
    fn public_projection_hash(&self) -> Option<&str> {
        self.public_projection_hash.as_deref()
    }
    fn completion_transition_key(&self) -> &str {
        &self.completion_transition_key
    }
    fn completion_intent_hash(&self) -> &str {
        &self.completion_intent_hash
    }
    fn completion_event_id(&self) -> &str {
        &self.completion_event_id
    }
    fn completion_event_seq(&self) -> u64 {
        self.completion_event_seq
    }
}

impl PublicationHashFields for StoredRetrievalPublication {
    fn run_id(&self) -> &str {
        self.run_id.as_str()
    }
    fn retrieval_id(&self) -> &str {
        &self.retrieval_id
    }
    fn task_id(&self) -> &str {
        &self.task_id
    }
    fn activation_id(&self) -> &str {
        self.activation_id.as_str()
    }
    fn node_id(&self) -> &str {
        &self.node_id
    }
    fn attempt_no(&self) -> u32 {
        self.attempt_no.get()
    }
    fn resource_id(&self) -> &str {
        &self.resource_id
    }
    fn resource_version(&self) -> &str {
        &self.resource_version
    }
    fn descriptor_hash(&self) -> &str {
        &self.descriptor_hash
    }
    fn query_field(&self) -> &str {
        &self.query_field
    }
    fn effective_public_policy_hash(&self) -> &str {
        &self.effective_public_policy_hash
    }
    fn public_projection_hash(&self) -> Option<&str> {
        self.public_projection_hash.as_deref()
    }
    fn completion_transition_key(&self) -> &str {
        &self.completion_transition_key
    }
    fn completion_intent_hash(&self) -> &str {
        &self.completion_intent_hash
    }
    fn completion_event_id(&self) -> &str {
        &self.completion_event_id
    }
    fn completion_event_seq(&self) -> u64 {
        self.completion_event_seq
    }
}

fn publication_hash_document(value: &impl PublicationHashFields) -> Value {
    json!({
        "domain": RETRIEVAL_PUBLICATION_HASH_DOMAIN,
        "run_id": value.run_id(),
        "retrieval_id": value.retrieval_id(),
        "task_id": value.task_id(),
        "activation_id": value.activation_id(),
        "node_id": value.node_id(),
        "attempt_no": value.attempt_no(),
        "resource_id": value.resource_id(),
        "resource_version": value.resource_version(),
        "descriptor_hash": value.descriptor_hash(),
        "query_field": value.query_field(),
        "effective_public_policy_hash": value.effective_public_policy_hash(),
        "public_projection_hash": value.public_projection_hash(),
        "completion_transition_key": value.completion_transition_key(),
        "completion_intent_hash": value.completion_intent_hash(),
        "completion_event_id": value.completion_event_id(),
        "completion_event_seq": value.completion_event_seq(),
    })
}

fn hash_value(value: &Value) -> Result<String, RepositoryError> {
    let encoded = serde_jcs::to_vec(value).map_err(|_| RepositoryError::canonicalization())?;
    Ok(prefixed_hash(VALUE_HASH_DOMAIN.as_bytes(), &encoded))
}

fn publication_hash(value: Value) -> Result<String, RepositoryError> {
    let encoded = serde_jcs::to_vec(&value).map_err(|_| RepositoryError::canonicalization())?;
    Ok(prefixed_hash(
        RETRIEVAL_PUBLICATION_HASH_DOMAIN.as_bytes(),
        &encoded,
    ))
}

fn prefixed_hash(domain: &[u8], value: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update([0]);
    hasher.update(value);
    let digest = hasher.finalize();
    let mut output = String::with_capacity(71);
    output.push_str("sha256:");
    for byte in digest {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

#[doc(hidden)]
pub mod adapter {
    use super::*;

    fn decode_prepared(value: &Value) -> Result<PreparedRetrievalPublication, RepositoryError> {
        serde_json::from_value(value.clone()).map_err(|_| RepositoryError::invalid_data())
    }

    fn decode_stored(value: &Value) -> Result<StoredRetrievalPublication, RepositoryError> {
        serde_json::from_value(value.clone()).map_err(|_| RepositoryError::invalid_data())
    }

    fn object(value: &Value) -> Result<&Map<String, Value>, RepositoryError> {
        value.as_object().ok_or_else(RepositoryError::invalid_data)
    }

    fn string_field<'a>(
        object: &'a Map<String, Value>,
        field: &str,
    ) -> Result<&'a str, RepositoryError> {
        object
            .get(field)
            .and_then(Value::as_str)
            .ok_or_else(RepositoryError::invalid_data)
    }

    fn u64_field(object: &Map<String, Value>, field: &str) -> Result<u64, RepositoryError> {
        object
            .get(field)
            .and_then(Value::as_u64)
            .ok_or_else(RepositoryError::invalid_data)
    }

    fn optional_value_field<'a>(
        object: &'a Map<String, Value>,
        field: &str,
    ) -> Result<Option<&'a Value>, RepositoryError> {
        match object.get(field) {
            Some(Value::Null) => Ok(None),
            Some(value) => Ok(Some(value)),
            None => Err(RepositoryError::invalid_data()),
        }
    }

    fn optional_string_field<'a>(
        object: &'a Map<String, Value>,
        field: &str,
    ) -> Result<Option<&'a str>, RepositoryError> {
        match object.get(field) {
            Some(Value::Null) => Ok(None),
            Some(Value::String(value)) => Ok(Some(value)),
            _ => Err(RepositoryError::invalid_data()),
        }
    }

    pub fn prepare_retrieval_publication(
        claim: &SchedulerTaskClaim,
        success: &SchedulerTaskSuccess,
        completion_transition_key: &str,
        completion_intent_hash: &str,
        completion_event_id: &str,
        completion_event_seq: u64,
    ) -> Result<Option<Value>, RepositoryError> {
        super::prepare_retrieval_publication(
            claim,
            success,
            completion_transition_key,
            completion_intent_hash,
            completion_event_id,
            completion_event_seq,
        )?
        .map(|publication| {
            serde_json::to_value(publication).map_err(|_| RepositoryError::invalid_data())
        })
        .transpose()
    }

    pub fn prepared_retrieval_identity(
        value: &Value,
    ) -> Result<(&str, &str, &str, &str, &str, u32), RepositoryError> {
        decode_prepared(value)?;
        let object = object(value)?;
        Ok((
            string_field(object, "run_id")?,
            string_field(object, "retrieval_id")?,
            string_field(object, "task_id")?,
            string_field(object, "activation_id")?,
            string_field(object, "node_id")?,
            u32::try_from(u64_field(object, "attempt_no")?)
                .map_err(|_| RepositoryError::invalid_data())?,
        ))
    }

    pub fn prepared_retrieval_resource(
        value: &Value,
    ) -> Result<(&str, &str, &str, &str, &Value, &str), RepositoryError> {
        decode_prepared(value)?;
        let object = object(value)?;
        Ok((
            string_field(object, "resource_id")?,
            string_field(object, "resource_version")?,
            string_field(object, "descriptor_hash")?,
            string_field(object, "query_field")?,
            object
                .get("effective_public_policy")
                .ok_or_else(RepositoryError::invalid_data)?,
            string_field(object, "effective_public_policy_hash")?,
        ))
    }

    pub fn prepared_retrieval_public_projection(
        value: &Value,
    ) -> Result<(Option<&Value>, Option<&str>), RepositoryError> {
        decode_prepared(value)?;
        let object = object(value)?;
        Ok((
            optional_value_field(object, "public_projection")?,
            optional_string_field(object, "public_projection_hash")?,
        ))
    }

    pub fn prepared_retrieval_completion(
        value: &Value,
    ) -> Result<(&str, &str, &str, u64, &str), RepositoryError> {
        decode_prepared(value)?;
        let object = object(value)?;
        Ok((
            string_field(object, "completion_transition_key")?,
            string_field(object, "completion_intent_hash")?,
            string_field(object, "completion_event_id")?,
            u64_field(object, "completion_event_seq")?,
            string_field(object, "publication_hash")?,
        ))
    }

    pub fn prepared_retrieval_public(
        value: &Value,
    ) -> Result<Option<WorkflowRetrieval>, RepositoryError> {
        decode_prepared(value)?.public_retrieval()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn stored_retrieval_publication(
        run_id: String,
        retrieval_id: String,
        task_id: String,
        activation_id: String,
        node_id: String,
        attempt_no: u32,
        resource_id: String,
        resource_version: String,
        descriptor_hash: String,
        query_field: String,
        effective_public_policy: Value,
        effective_public_policy_hash: String,
        public_projection: Option<Value>,
        public_projection_hash: Option<String>,
        completion_transition_key: String,
        completion_intent_hash: String,
        completion_event_id: String,
        completion_event_seq: u64,
        publication_hash: String,
    ) -> Result<Value, RepositoryError> {
        let publication = StoredRetrievalPublication {
            run_id: RunId::new(run_id).map_err(|_| RepositoryError::invalid_data())?,
            retrieval_id,
            task_id,
            activation_id: ActivationId::new(activation_id)
                .map_err(|_| RepositoryError::invalid_data())?,
            node_id,
            attempt_no: AttemptNo::new(attempt_no).map_err(|_| RepositoryError::invalid_data())?,
            resource_id,
            resource_version,
            descriptor_hash,
            query_field,
            effective_public_policy,
            effective_public_policy_hash,
            public_projection,
            public_projection_hash,
            completion_transition_key,
            completion_intent_hash,
            completion_event_id,
            completion_event_seq,
            publication_hash,
        };
        serde_json::to_value(publication).map_err(|_| RepositoryError::invalid_data())
    }

    pub fn stored_retrieval_validate_and_project(
        stored: &Value,
    ) -> Result<Option<WorkflowRetrieval>, RepositoryError> {
        decode_stored(stored)?.validate_and_project()
    }

    pub fn validate_exact_retrieval_publication(
        stored: &Value,
        expected: &Value,
    ) -> Result<(), RepositoryError> {
        super::validate_exact_retrieval_publication(
            &decode_stored(stored)?,
            &decode_prepared(expected)?,
        )
    }
}
