//! Bounded physical OpenBao client. It has no business state or authorization authority.
mod client;
mod kv;
mod sensitive;
mod transit;

pub use client::BaoClient;
pub use insight_platform_deployment_contracts::openbao::{
    BaoClientConfigV1, BaoError, BaoSecretPath, KvV2BindingV1, TransitBindingV1,
    MAX_OPERATION_TIMEOUT_MILLISECONDS, MAX_PRIVATE_FILE_BYTES, MAX_PROVIDER_REQUEST_BYTES,
    MAX_PROVIDER_RESPONSE_BYTES, MAX_SECRET_PATH_BYTES,
};
pub use kv::{KvMetadata, KvRead, KvVersionMetadata};
pub use sensitive::SensitiveBytes;
