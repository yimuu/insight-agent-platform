//! Shared local OIDC cryptography. Private material is returned only to deployment adapters.
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use insight_platform_deployment_contracts::installation::{
    InstallationError, LocalSessionIdentityV1, LocalSessionKind, INSTALLATION_SESSION_SECONDS,
};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rcgen::{KeyPair, PublicKeyData};
use serde::Serialize;
use serde_json::{json, Value};
use x509_parser::{prelude::FromDer as _, public_key::PublicKey, x509::SubjectPublicKeyInfo};

pub fn jwks_for_key_pair(key_pair: &KeyPair, key_id: &str) -> Result<Value, InstallationError> {
    let public_key_info = key_pair.subject_public_key_info();
    let (_, public_key_info) = SubjectPublicKeyInfo::from_der(&public_key_info)
        .map_err(|_| InstallationError::CredentialInvalid)?;
    let PublicKey::RSA(public_key) = public_key_info
        .parsed()
        .map_err(|_| InstallationError::CredentialInvalid)?
    else {
        return Err(InstallationError::CredentialInvalid);
    };
    let modulus =
        positive_integer(public_key.modulus).ok_or(InstallationError::CredentialInvalid)?;
    let exponent =
        positive_integer(public_key.exponent).ok_or(InstallationError::CredentialInvalid)?;
    Ok(
        json!({"keys":[{"alg":"RS256", "e": URL_SAFE_NO_PAD.encode(exponent), "kid":key_id,
        "kty":"RSA", "n":URL_SAFE_NO_PAD.encode(modulus), "use":"sig"}]}),
    )
}

#[derive(Serialize)]
struct SessionClaims<'a> {
    iss: &'a str,
    aud: &'a str,
    sub: &'a str,
    jti: String,
    iat: i64,
    exp: i64,
    tenant_id: &'a insight_platform_contracts::ResourceId,
    principal_kind: LocalSessionKind,
    authn_strength: &'static str,
}
pub fn sign_session(
    identity: &LocalSessionIdentityV1,
    private_key_der: &[u8],
    issued_at: u64,
) -> Result<String, InstallationError> {
    identity.validate()?;
    let _ = jsonwebtoken::crypto::aws_lc::DEFAULT_PROVIDER.install_default();
    let issued_at = i64::try_from(issued_at).map_err(|_| InstallationError::InvalidInput)?;
    let expires_at = issued_at
        .checked_add(INSTALLATION_SESSION_SECONDS as i64)
        .ok_or(InstallationError::InvalidInput)?;
    let claims = SessionClaims {
        iss: &identity.issuer,
        aud: &identity.audience,
        sub: &identity.subject,
        jti: format!("local-token-{}", uuid::Uuid::now_v7()),
        iat: issued_at,
        exp: expires_at,
        tenant_id: &identity.tenant_id,
        principal_kind: identity.principal_kind,
        authn_strength: "single_factor",
    };
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(identity.key_id.clone());
    header.typ = Some("JWT".into());
    encode(
        &header,
        &claims,
        &EncodingKey::from_rsa_der(private_key_der),
    )
    .map_err(|_| InstallationError::CredentialInvalid)
}
fn positive_integer(value: &[u8]) -> Option<&[u8]> {
    let value = value.strip_prefix(&[0]).unwrap_or(value);
    (!value.is_empty()).then_some(value)
}

pub fn pkcs1_private_key_from_pkcs8(value: &[u8]) -> Option<&[u8]> {
    let (outer, remainder) = der_tlv(value, 0x30)?;
    if !remainder.is_empty() {
        return None;
    }
    let (_, outer) = der_tlv(outer, 0x02)?;
    let (_, outer) = der_tlv(outer, 0x30)?;
    let (private_key, _) = der_tlv(outer, 0x04)?;
    Some(private_key)
}

fn der_tlv(value: &[u8], expected_tag: u8) -> Option<(&[u8], &[u8])> {
    let (&tag, remaining) = value.split_first()?;
    if tag != expected_tag {
        return None;
    }
    let (&first_length, remaining) = remaining.split_first()?;
    let (length, remaining) = if first_length & 0x80 == 0 {
        (usize::from(first_length), remaining)
    } else {
        let length_bytes = usize::from(first_length & 0x7f);
        if length_bytes == 0 || length_bytes > std::mem::size_of::<usize>() {
            return None;
        }
        let (encoded_length, remaining) = remaining.split_at_checked(length_bytes)?;
        let length = encoded_length.iter().try_fold(0usize, |length, byte| {
            length.checked_mul(256)?.checked_add(usize::from(*byte))
        })?;
        (length, remaining)
    };
    let (content, remaining) = remaining.split_at_checked(length)?;
    Some((content, remaining))
}
