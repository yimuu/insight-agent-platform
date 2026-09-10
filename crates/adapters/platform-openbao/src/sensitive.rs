use crate::{BaoError, MAX_PROVIDER_RESPONSE_BYTES};
use zeroize::Zeroize;

/// Non-cloneable transport bytes, never rendered in diagnostics.
pub struct SensitiveBytes(Vec<u8>);

impl SensitiveBytes {
    pub fn new(mut bytes: Vec<u8>) -> Result<Self, BaoError> {
        if bytes.is_empty() || bytes.len() > MAX_PROVIDER_RESPONSE_BYTES {
            bytes.zeroize();
            return Err(BaoError::InvalidEvidence);
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(mut self) -> Vec<u8> {
        std::mem::take(&mut self.0)
    }
}

impl std::fmt::Debug for SensitiveBytes {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SensitiveBytes([redacted])")
    }
}

impl Drop for SensitiveBytes {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

/// Provider envelopes also contain encoded secrets and tokens. Clear every value on exit.
pub(crate) struct SensitiveJson(pub(crate) serde_json::Value);

impl Drop for SensitiveJson {
    fn drop(&mut self) {
        clear_json(&mut self.0);
    }
}

fn clear_json(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::String(text) => text.zeroize(),
        serde_json::Value::Array(values) => values.iter_mut().for_each(clear_json),
        serde_json::Value::Object(values) => values.values_mut().for_each(clear_json),
        _ => {}
    }
}
