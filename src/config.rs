use std::{env, net::SocketAddr, path::PathBuf};

use crate::error::AppError;

#[derive(Debug, Clone)]
pub struct PlatformConfig {
    pub bind_addr: SocketAddr,
    pub agents_dir: PathBuf,
    pub openai_api_key: String,
    pub openai_base_url: String,
    pub openai_default_model: String,
}

impl PlatformConfig {
    pub fn from_env() -> Result<Self, AppError> {
        let bind_addr = env::var("BIND_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:3000".to_string())
            .parse()
            .map_err(|err| AppError::Config(format!("invalid BIND_ADDR: {err}")))?;

        let agents_dir = env::var("AGENTS_DIR").unwrap_or_else(|_| "agents".to_string());
        let openai_api_key = env::var("OPENAI_API_KEY")
            .map_err(|_| AppError::Config("OPENAI_API_KEY is required".to_string()))?;
        let openai_base_url = env::var("OPENAI_BASE_URL")
            .unwrap_or_else(|_| "https://dashscope.aliyuncs.com/compatible-mode/v1".to_string());
        let openai_default_model =
            env::var("OPENAI_DEFAULT_MODEL").unwrap_or_else(|_| "qwen3.6-flash".to_string());

        Ok(Self {
            bind_addr,
            agents_dir: PathBuf::from(agents_dir),
            openai_api_key,
            openai_base_url,
            openai_default_model,
        })
    }
}
