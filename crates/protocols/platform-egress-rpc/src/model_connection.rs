use super::*;
use insight_platform_contracts::{
    ModelConnectionError as Failure, ModelConnectionObservationV1,
    ModelConnectionProbeAuthorizationV1,
};
use insight_platform_security::ModelConnectionProbe;
const REQUEST: &str = "model_connection.probe/v1";
const RESPONSE: &str = "model_connection.observation/v1";
#[async_trait]
impl ModelConnectionProbe for EgressBrokerGrpcClient {
    async fn probe_model_connection(
        &self,
        request: ModelConnectionProbeAuthorizationV1,
    ) -> Result<ModelConnectionObservationV1, Failure> {
        let now = Utc::now();
        if !request.validate_at(now) {
            return Err(Failure::Rejected);
        }
        let mut wire = Request::new(
            encode_metadata(&request, REQUEST, self.limits).map_err(|_| Failure::Rejected)?,
        );
        wire.set_timeout(
            (request.deadline_at() - now)
                .to_std()
                .map_err(|_| Failure::Rejected)?,
        );
        let response = self.client.clone().probe_model_connection(wire).await;
        observe_egress_rpc(&self.dependency_observer, response.is_ok());
        let response = response.map_err(|s| match s.code() {
            tonic::Code::PermissionDenied
            | tonic::Code::Unauthenticated
            | tonic::Code::InvalidArgument => Failure::Rejected,
            _ => Failure::Unavailable,
        })?;
        match decode_metadata::<UnaryOutcome<ModelConnectionObservationV1, Failure>>(
            response.into_inner(),
            RESPONSE,
            self.limits,
        )
        .map_err(|_| Failure::Unavailable)?
        {
            UnaryOutcome::Succeeded(observation)
                if observation.validate()
                    && observation.model_deployment == request.model_deployment =>
            {
                Ok(observation)
            }
            UnaryOutcome::Succeeded(_) => Err(Failure::Unavailable),
            UnaryOutcome::Failed(f) => Err(f),
        }
    }
}
pub(super) async fn serve(
    probe: Option<&dyn ModelConnectionProbe>,
    request: Request<ClosedEgressEnvelope>,
    limits: EgressInternalRpcLimits,
) -> Result<Response<ClosedEgressEnvelope>, Status> {
    require_role(&request, EgressCallerRole::Gateway)?;
    let trace = trace_context(&request)?;
    let request: ModelConnectionProbeAuthorizationV1 =
        decode_metadata(request.into_inner(), REQUEST, limits)?;
    let now = Utc::now();
    if !request.validate_at(now) {
        return Err(Status::invalid_argument("invalid model probe"));
    }
    let probe = probe.ok_or_else(|| Status::unavailable("model probe unavailable"))?;
    let result = tokio::time::timeout(
        (request.deadline_at() - now)
            .to_std()
            .map_err(|_| Status::invalid_argument("invalid model probe"))?,
        scope_trace(trace, probe.probe_model_connection(request.clone())),
    )
    .await;
    let outcome = match result {
        Ok(Ok(v)) if v.validate() && v.model_deployment == request.model_deployment => {
            UnaryOutcome::Succeeded(v)
        }
        Ok(Err(e)) => UnaryOutcome::Failed(e),
        _ => UnaryOutcome::Failed(Failure::Unavailable),
    };
    Ok(Response::new(encode_metadata(&outcome, RESPONSE, limits)?))
}
