use crate::authentication::{
    AuthenticationError, ExternalCredentialVerifier, VerifiedExternalCredential,
};
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, AuthnStrength, PrincipalKind, ResourceId, ResourceKind, Sha256Digest,
};
use jsonwebtoken::{
    decode, decode_header,
    jwk::{Jwk, JwkSet, KeyAlgorithm, KeyOperations, PublicKeyUse},
    Algorithm, DecodingKey, Validation,
};
use serde::Deserialize;

const MAX_OIDC_KEYS: usize = 8;
const MAX_CLOCK_SKEW_SECONDS: u64 = 60;
const MAX_TOKEN_LIFETIME_SECONDS: i64 = 86_400;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledOidcVerifierConfig {
    pub issuer: String,
    pub audience: String,
    pub jwks: serde_json::Value,
    pub jwks_digest: Sha256Digest,
}

impl InstalledOidcVerifierConfig {
    pub fn install(self) -> Result<InstalledOidcVerifier, OidcConfigurationError> {
        if !stable_identifier(&self.issuer)
            || !stable_identifier(&self.audience)
            || canonical_digest(&self.jwks).ok().as_deref() != Some(self.jwks_digest.as_str())
        {
            return Err(OidcConfigurationError::InvalidConfiguration);
        }
        let keys: JwkSet = serde_json::from_value(self.jwks)
            .map_err(|_| OidcConfigurationError::InvalidConfiguration)?;
        if keys.keys.is_empty() || keys.keys.len() > MAX_OIDC_KEYS {
            return Err(OidcConfigurationError::InvalidConfiguration);
        }
        let mut previous: Option<&str> = None;
        for key in &keys.keys {
            let kid = key
                .common
                .key_id
                .as_deref()
                .filter(|value| stable_identifier(value))
                .ok_or(OidcConfigurationError::InvalidConfiguration)?;
            if previous.is_some_and(|value| value >= kid)
                || !jwk_is_valid_rs256_signature_key(key)
                || DecodingKey::from_jwk(key).is_err()
            {
                return Err(OidcConfigurationError::InvalidConfiguration);
            }
            previous = Some(kid);
        }
        Ok(InstalledOidcVerifier {
            issuer: self.issuer,
            audience: self.audience,
            keys,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OidcConfigurationError {
    InvalidConfiguration,
}

#[derive(Debug, Clone)]
pub struct InstalledOidcVerifier {
    issuer: String,
    audience: String,
    keys: JwkSet,
}

impl ExternalCredentialVerifier for InstalledOidcVerifier {
    fn verify(
        &self,
        bearer_token: &str,
        now: DateTime<Utc>,
    ) -> Result<VerifiedExternalCredential, AuthenticationError> {
        let header =
            decode_header(bearer_token).map_err(|_| AuthenticationError::Unauthenticated)?;
        if header.alg != Algorithm::RS256 || header.typ.as_deref() != Some("JWT") {
            return Err(AuthenticationError::Unauthenticated);
        }
        let kid = header
            .kid
            .as_deref()
            .filter(|value| stable_identifier(value))
            .ok_or(AuthenticationError::Unauthenticated)?;
        let key = self
            .keys
            .keys
            .iter()
            .find(|key| key.common.key_id.as_deref() == Some(kid))
            .filter(|key| jwk_is_valid_rs256_signature_key(key))
            .ok_or(AuthenticationError::Unauthenticated)?;
        let decoding_key =
            DecodingKey::from_jwk(key).map_err(|_| AuthenticationError::Unauthenticated)?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_audience(&[self.audience.as_str()]);
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.set_required_spec_claims(&[
            "aud",
            "exp",
            "iat",
            "iss",
            "jti",
            "sub",
            "tenant_id",
        ]);
        validation.leeway = MAX_CLOCK_SKEW_SECONDS;
        validation.validate_nbf = true;
        let claims = decode::<PublicAccessTokenClaims>(bearer_token, &decoding_key, &validation)
            .map_err(|_| AuthenticationError::Unauthenticated)?
            .claims;
        claims.into_verified(&self.issuer, now)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicAccessTokenClaims {
    iss: String,
    aud: String,
    sub: String,
    jti: String,
    iat: i64,
    exp: i64,
    tenant_id: ResourceId,
    principal_kind: PrincipalKind,
    authn_strength: AuthnStrength,
}

impl PublicAccessTokenClaims {
    fn into_verified(
        self,
        expected_issuer: &str,
        now: DateTime<Utc>,
    ) -> Result<VerifiedExternalCredential, AuthenticationError> {
        if self.iss != expected_issuer
            || !stable_identifier(&self.aud)
            || !stable_identifier(&self.sub)
            || !stable_identifier(&self.jti)
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.principal_kind == PrincipalKind::InstallationOperator
            || self.exp <= self.iat
            || self.exp - self.iat > MAX_TOKEN_LIFETIME_SECONDS
            || self.iat > now.timestamp() + i64::try_from(MAX_CLOCK_SKEW_SECONDS).unwrap_or(60)
            || self.exp <= now.timestamp()
        {
            return Err(AuthenticationError::Unauthenticated);
        }
        let issued_at =
            DateTime::from_timestamp(self.iat, 0).ok_or(AuthenticationError::Unauthenticated)?;
        let expires_at =
            DateTime::from_timestamp(self.exp, 0).ok_or(AuthenticationError::Unauthenticated)?;
        Ok(VerifiedExternalCredential {
            tenant_id: self.tenant_id,
            authentication_authority_digest: tagged_digest(
                "oidc_authentication_authority_v1",
                expected_issuer,
            )?,
            subject_digest: tagged_digest("oidc_subject_v1", &self.sub)?,
            credential_digest: tagged_digest(
                "oidc_credential_v1",
                &format!("{expected_issuer}\u{0}{}", self.jti),
            )?,
            principal_kind: self.principal_kind,
            authn_strength: self.authn_strength,
            issued_at,
            expires_at,
        })
    }
}

fn tagged_digest(tag: &str, value: &str) -> Result<Sha256Digest, AuthenticationError> {
    canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "tag": tag,
        "value": value,
    }))
    .map_err(|_| AuthenticationError::Unauthenticated)?
    .parse()
    .map_err(|_| AuthenticationError::Unauthenticated)
}

fn stable_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 512
        && value.is_ascii()
        && !value.chars().any(char::is_control)
}

fn jwk_is_valid_rs256_signature_key(jwk: &Jwk) -> bool {
    let common = &jwk.common;
    if common
        .key_algorithm
        .is_some_and(|declared| declared != KeyAlgorithm::RS256)
        || common
            .public_key_use
            .as_ref()
            .is_some_and(|usage| usage != &PublicKeyUse::Signature)
        || common.key_operations.as_ref().is_some_and(|operations| {
            operations.is_empty()
                || !operations.contains(&KeyOperations::Verify)
                || operations.iter().any(|operation| {
                    !matches!(operation, KeyOperations::Verify | KeyOperations::Sign)
                })
        })
    {
        return false;
    }
    !(common.public_key_use.is_some() && common.key_operations.is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use jsonwebtoken::{encode, EncodingKey, Header};

    const PRIVATE_KEY: &str = r#"-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEA4KoeIFhx35ADyXYT0MpVCFDcWPKi1KUDxNTnPu1uubb9hqbn
pgq68U8YQAGT1Dh1B4lyZmqUvYbGLNBj7CEcuJdms6JkohM50AdwBv6+TCy/uLpZ
zcUs8AGh8zFyVeyceX2CkZptlaP+362KPVB0tnvmjRVO2tJLiiqFBGqe9OKKGL+W
evKFrUlSoaWTova7baKBBIMUx8GckC9NHvSj9oMbaaOTziTSOhonVnzHr1diFh5C
bluUn3ef6KFcO8mssT+prqfqHYnNCEeLRsEUZT79oCVXb2H9RasBv7mU+FNPNwj8
dcWcfUIV6ePEDjAGH+KU1eStSTYxeJEfbgW9zwIDAQABAoIBABZlPu2Qg4F6tLXv
fFgy4zkZ/m0resnhzTdg1dBzELeYozs6BhuKNEp7zPoMbjUYj6n5rJrDAyLFfZnY
CC3wuxE3nnhHtuplKj0vkJ5R5JxpVY9PnEYj4q/mKcO5aSFhndOKjGqBT208VNrt
TLuB+rB6N2hW+G5dykPyqyHekwvsHWRek28z1Jhcyn62k1LUTNQRuFG4espuejiJ
z3YtCkklUNSp6xJlQt56uwvTrD6Ym6manC9cSoCI0GE2JDlMtnI21t/jqpYEqfe8
sRp3OOVExWTy4uN6I8OgxwtDDwIz1iCc9YJnBfetJR7uUfrKTNWNklxZ0BJWZADP
mkHYerkCgYEA+1NCqFxgEMn6TP9rLGAAafRUyJSQh558jqNuVriXBt0gdhc1yGA7
1Hse2Z/WuclZD5qC0qnjUQ+RJgf4wc30agC8jLvklrg9PI4Opikg6oGuhU+SBLjc
vM7mxFxG/fUSQiKVvfihstXVgrQ/oCxsm3e3b59Up2U7gesraoc92BsCgYEA5Nfo
EkvmSF2Nh1uS2A+u0VyFExh+MGmGE7lDNzaoNIFcTHMenFrkW17qAIj4GzYEMJ48
k4uNtDBGXeXjmYy2vq0DcVNfqHDjY/xpD3ghvIqqRHGjHw1Qz9p5DisOL4NbrzZr
tzSqRBEujVQmR39RZqps5v1ELqND/SHPEwIGdF0CgYBWYlHhCI9EdggAezJdOEos
IP0bTGU5GDJ73JTKXfwbMdo8fNHRo7Is4HzEFHp7tUdVY6hfvGETtaQQTGEmTCIc
ZVBplxOE8qKps7I5Tp2vvQ89ZxIraVcF1p/fElCcbaXu8XBCsbjyfSk8GbRc26gg
788vILa6KsN/blOn9AA/zQKBgHqhFDRRxdo7f67sLHlplgWM7aa49k4dDgMdwN4i
hOp1867n9ZxVvI8WApE81K9IN+CRuuZZ3xqSz/JbUaaj1/2/mtuskNMjg0a+KNJo
TrPJHsrElmP6b7aiXUJxYg2l94ihwgEP0Lne9zI2yLiBim5Ynzj8uP/A75sC9gM6
j5jlAoGAPfWKXuhAZ1cW3J1cNbx4hqpaB49fP7wfJvUthxpS8Hm7d83UxdyssnFa
GpNSPprA4sG4wOHwwaJ4S+vy4a8i3oQ5ZPDI+K9jOZI7kpD/daUt7Dm1iYWi5Fux
PTOVecDV20b2Z8Lh/1UrAI6PrMapONiubOBgDD08vhsp41RSmEQ=
-----END RSA PRIVATE KEY-----"#;

    fn jwks() -> serde_json::Value {
        serde_json::json!({"keys": [{
            "kty": "RSA",
            "kid": "public-key-1",
            "use": "sig",
            "alg": "RS256",
            "n": "4KoeIFhx35ADyXYT0MpVCFDcWPKi1KUDxNTnPu1uubb9hqbnpgq68U8YQAGT1Dh1B4lyZmqUvYbGLNBj7CEcuJdms6JkohM50AdwBv6-TCy_uLpZzcUs8AGh8zFyVeyceX2CkZptlaP-362KPVB0tnvmjRVO2tJLiiqFBGqe9OKKGL-WevKFrUlSoaWTova7baKBBIMUx8GckC9NHvSj9oMbaaOTziTSOhonVnzHr1diFh5CbluUn3ef6KFcO8mssT-prqfqHYnNCEeLRsEUZT79oCVXb2H9RasBv7mU-FNPNwj8dcWcfUIV6ePEDjAGH-KU1eStSTYxeJEfbgW9zw",
            "e": "AQAB"
        }]})
    }

    fn verifier() -> InstalledOidcVerifier {
        let keys = jwks();
        InstalledOidcVerifierConfig {
            issuer: "https://issuer.example".to_owned(),
            audience: "insight-platform-public".to_owned(),
            jwks_digest: canonical_digest(&keys).unwrap().parse().unwrap(),
            jwks: keys,
        }
        .install()
        .unwrap()
    }

    fn token(now: DateTime<Utc>, audience: &str, extra: bool) -> String {
        let mut claims = serde_json::json!({
            "iss": "https://issuer.example",
            "aud": audience,
            "sub": "user-123",
            "jti": "credential-456",
            "iat": now.timestamp() - 1,
            "exp": now.timestamp() + 600,
            "tenant_id": "ten_0198f1cc-32e4-75e1-a9e8-d95ca0f80001",
            "principal_kind": "tenant_admin",
            "authn_strength": "multi_factor"
        });
        if extra {
            claims["unreviewed_claim"] = serde_json::json!(true);
        }
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("public-key-1".to_owned());
        encode(
            &header,
            &claims,
            &EncodingKey::from_rsa_der(
                &STANDARD
                    .decode(
                        PRIVATE_KEY
                            .lines()
                            .filter(|line| !line.starts_with("-----"))
                            .collect::<String>(),
                    )
                    .unwrap(),
            ),
        )
        .unwrap()
    }

    #[test]
    fn installation_is_digest_pinned_and_accepts_only_signature_keys() {
        let keys = jwks();
        let config = InstalledOidcVerifierConfig {
            issuer: "https://issuer.example".to_owned(),
            audience: "insight-platform-public".to_owned(),
            jwks_digest: canonical_digest(&keys).unwrap().parse().unwrap(),
            jwks: keys.clone(),
        };
        assert!(config.install().is_ok());

        let mut wrong_digest = InstalledOidcVerifierConfig {
            issuer: "https://issuer.example".to_owned(),
            audience: "insight-platform-public".to_owned(),
            jwks_digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .parse()
                .unwrap(),
            jwks: keys.clone(),
        };
        assert!(matches!(
            wrong_digest.clone().install(),
            Err(OidcConfigurationError::InvalidConfiguration)
        ));
        wrong_digest.jwks["keys"][0]["use"] = serde_json::json!("enc");
        wrong_digest.jwks_digest = canonical_digest(&wrong_digest.jwks)
            .unwrap()
            .parse()
            .unwrap();
        assert!(matches!(
            wrong_digest.install(),
            Err(OidcConfigurationError::InvalidConfiguration)
        ));
    }

    #[test]
    fn verifies_exact_public_claim_contract_and_derives_only_digest_identity() {
        let now = Utc::now();
        let verified = verifier()
            .verify(&token(now, "insight-platform-public", false), now)
            .unwrap();
        assert_eq!(verified.tenant_id.kind(), ResourceKind::Tenant);
        assert_eq!(verified.principal_kind, PrincipalKind::TenantAdmin);
        assert_eq!(verified.authn_strength, AuthnStrength::MultiFactor);
        assert!(verified
            .authentication_authority_digest
            .as_str()
            .starts_with("sha256:"));
        assert!(verified.subject_digest.as_str().starts_with("sha256:"));
        assert!(verified.credential_digest.as_str().starts_with("sha256:"));

        assert_eq!(
            verifier().verify(&token(now, "wrong-audience", false), now),
            Err(AuthenticationError::Unauthenticated)
        );
        assert_eq!(
            verifier().verify(&token(now, "insight-platform-public", true), now),
            Err(AuthenticationError::Unauthenticated)
        );
    }
}
