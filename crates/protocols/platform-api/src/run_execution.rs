//! Read-only execution inspection, never an event-derived state projection.
use crate::authentication::AuthenticatedPrincipal;
use crate::run::*;
use axum::{
    extract::{Extension, Path, State},
    http::{header::CACHE_CONTROL, HeaderValue},
    response::{IntoResponse, Response},
    Json,
};
use insight_platform_contracts::*;
pub use insight_platform_orchestrator::store::RunExecutionState;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone)]
pub struct ReadRunExecutionIntent {
    pub principal: AuthenticatedPrincipal,
    pub run_id: ResourceId,
    pub source_kind: PublicRunEventSourceKind,
    pub source_id: ResourceId,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunExecutionDetailV1 {
    pub schema_version: u32,
    pub run_id: ResourceId,
    pub source_kind: PublicRunEventSourceKind,
    pub source_id: ResourceId,
    pub version: u64,
    pub state: RunExecutionState,
    pub node_execution_id: ResourceId,
    pub plan_node_key: String,
    pub node_kind: String,
    pub started_at: Option<UtcTimestamp>,
    pub terminal_at: Option<UtcTimestamp>,
    pub input_value_id: Option<ResourceId>,
    pub output_value_id: Option<ResourceId>,
    pub values: Vec<RunValueMetadataV1>,
    pub values_truncated: bool,
}
impl RunExecutionDetailV1 {
    pub fn validate(&self) -> Result<(), RunApplicationError> {
        let valid_state = match self.source_kind {
            PublicRunEventSourceKind::NodeExecution => {
                self.state.as_str().parse::<NodeExecutionState>().is_ok()
                    && self.source_id == self.node_execution_id
                    && self.input_value_id.is_none()
                    && self.output_value_id.is_none()
                    && self
                        .values
                        .iter()
                        .all(|v| v.node_id.as_ref() == Some(&self.node_execution_id))
            }
            PublicRunEventSourceKind::ModelTurn => {
                self.state.as_str().parse::<ModelTurnState>().is_ok()
                    && self.source_id.kind() == ResourceKind::ModelTurn
                    && !self.values_truncated
                    && self.values.len() <= 2
                    && self.values.iter().all(|v| {
                        self.input_value_id.as_ref() == Some(&v.value_id)
                            || self.output_value_id.as_ref() == Some(&v.value_id)
                    })
            }
            _ => false,
        };
        if self.schema_version != 1
            || self.run_id.kind() != ResourceKind::Run
            || self.node_execution_id.kind() != ResourceKind::NodeExecution
            || self.version == 0
            || !valid_state
            || self.plan_node_key.is_empty()
            || self.plan_node_key.len() > 128
            || self.node_kind.parse::<PlanNodeKind>().is_err()
            || self.values.len() > 64
            || self
                .values
                .iter()
                .any(|v| v.validate().is_err() || v.run_id != self.run_id)
            || [&self.input_value_id, &self.output_value_id]
                .iter()
                .any(|id| {
                    id.as_ref()
                        .is_some_and(|id| !self.values.iter().any(|v| v.value_id == *id))
                })
        {
            return Err(RunApplicationError::Internal);
        }
        Ok(())
    }
}
pub(crate) async fn read(
    State(state): State<RunHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    Path((run, kind, source)): Path<(String, String, String)>,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(RunApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(RunApplicationError::Unauthenticated);
    }
    let (source_kind, id_kind) = match kind.as_str() {
        "node_execution" => (
            PublicRunEventSourceKind::NodeExecution,
            ResourceKind::NodeExecution,
        ),
        "model_turn" => (PublicRunEventSourceKind::ModelTurn, ResourceKind::ModelTurn),
        _ => return problem(RunApplicationError::Invalid),
    };
    let (Ok(run_id), Ok(source_id)) = (
        ResourceId::parse_expected(&run, ResourceKind::Run),
        ResourceId::parse_expected(&source, id_kind),
    ) else {
        return problem(RunApplicationError::NotFound);
    };
    let intent = ReadRunExecutionIntent {
        principal,
        run_id: run_id.clone(),
        source_kind,
        source_id: source_id.clone(),
    };
    match tokio::time::timeout(
        std::time::Duration::from_secs(10),
        state.application.read_run_execution(intent),
    )
    .await
    {
        Ok(Ok(view))
            if view.run_id == run_id
                && view.source_id == source_id
                && view.source_kind == source_kind
                && view.validate().is_ok() =>
        {
            let mut r = Json(view).into_response();
            r.headers_mut().insert(
                CACHE_CONTROL,
                HeaderValue::from_static("no-store, private, max-age=0"),
            );
            r
        }
        Ok(Err(e)) => problem(e),
        Ok(Ok(_)) => problem(RunApplicationError::Internal),
        Err(_) => problem(RunApplicationError::Unavailable),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn id(kind: ResourceKind) -> ResourceId {
        ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
    }
    #[test]
    fn execution_response_preserves_closed_kind_state_and_exact_value_membership() {
        let node = id(ResourceKind::NodeExecution);
        let mut view = RunExecutionDetailV1 {
            schema_version: 1,
            run_id: id(ResourceKind::Run),
            source_kind: PublicRunEventSourceKind::NodeExecution,
            source_id: node.clone(),
            version: 3,
            state: RunExecutionState::Node(NodeExecutionState::Succeeded),
            node_execution_id: node,
            plan_node_key: "answer".into(),
            node_kind: "model_loop".into(),
            started_at: None,
            terminal_at: None,
            input_value_id: None,
            output_value_id: None,
            values: vec![],
            values_truncated: false,
        };
        view.validate().unwrap();
        view.state = RunExecutionState::Model(ModelTurnState::InFlight);
        assert!(view.validate().is_err());
        view.source_kind = PublicRunEventSourceKind::ModelTurn;
        view.source_id = id(ResourceKind::ModelTurn);
        view.validate().unwrap();
        view.output_value_id = Some(id(ResourceKind::RunValue));
        assert!(view.validate().is_err());
        view.output_value_id = None;
        view.state = RunExecutionState::Model(ModelTurnState::Succeeded);
        let decoded: RunExecutionDetailV1 =
            serde_json::from_value(serde_json::to_value(&view).unwrap()).unwrap();
        decoded.validate().unwrap();
        let mut wire = serde_json::to_value(view).unwrap();
        wire["state"] = serde_json::json!("invented_state");
        assert!(serde_json::from_value::<RunExecutionDetailV1>(wire).is_err());
    }
}
