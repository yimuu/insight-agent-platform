use serde::Serialize;

pub const CODE_OK: i32 = 0;
pub const CODE_INPUT_ERROR: i32 = 10000;
pub const CODE_NOT_FOUND: i32 = 14004;
pub const CODE_RUN_ERROR: i32 = 20000;
pub const CODE_UPSTREAM_ERROR: i32 = 30000;
pub const CODE_CONFIG_ERROR: i32 = 50000;

#[derive(Debug, Serialize)]
pub struct ApiResponse<T: Serialize> {
    pub code: i32,
    pub message: String,
    pub data: T,
}

impl<T: Serialize> ApiResponse<T> {
    pub fn new(code: i32, message: impl Into<String>, data: T) -> Self {
        Self {
            code,
            message: message.into(),
            data,
        }
    }

    pub fn ok(data: T) -> Self {
        Self::new(CODE_OK, "ok", data)
    }
}
