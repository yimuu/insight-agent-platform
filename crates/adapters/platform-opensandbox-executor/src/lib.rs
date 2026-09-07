//! Physical OpenSandbox orchestration through fenced domain ports.

pub mod dispatcher;

use insight_platform_sandbox::contracts::{OpaqueActivationToken, SandboxContractError};

pub fn generate_activation_token() -> Result<OpaqueActivationToken, SandboxContractError> {
    use ring::rand::{SecureRandom, SystemRandom};
    use std::fmt::Write as _;

    let mut bytes = [0_u8; 32];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| SandboxContractError::InvalidActivation)?;
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").map_err(|_| SandboxContractError::InvalidActivation)?;
    }
    OpaqueActivationToken::parse(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activation_seed_uses_independent_entropy_and_redacted_debug() {
        let first = generate_activation_token().unwrap();
        let second = generate_activation_token().unwrap();
        assert_eq!(first.expose_for_protocol().len(), 64);
        assert_ne!(first, second);
        first.verifying_key().unwrap();
        assert_eq!(format!("{first:?}"), "OpaqueActivationToken([redacted])");
    }
}
