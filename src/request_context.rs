use serde::Serialize;

#[derive(Debug, Clone, Default, Serialize)]
pub struct RequestContext {
    pub request_id: String,
    pub caller_service: Option<String>,
    pub tenant_id: Option<String>,
    pub user_id: Option<String>,
}
