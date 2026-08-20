use insight_platform_artifact_rpc::AuthenticatedGvisorGuestIdentity;
use insight_platform_contracts::{canonical_digest, Sha256Digest};
use insight_platform_sandbox::GvisorGuestPodIdentity;
use jsonwebtoken::{
    decode, decode_header,
    jwk::{Jwk, JwkSet, KeyAlgorithm, KeyOperations, PublicKeyUse},
    Algorithm, DecodingKey, Validation,
};
use serde::Deserialize;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::{Request, Status};

const MAX_TOKEN_BYTES: usize = 16_384;
const MAX_TOKEN_LIFETIME_SECONDS: u64 = 3_600;
const MAX_CLOCK_SKEW_SECONDS: u64 = 30;
const MAX_JWKS_KEYS: usize = 8;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GvisorGuestIdentityConfig {
    pub issuer: String,
    pub audience: String,
    pub namespace: String,
    pub service_account_name: String,
    pub jwks: serde_json::Value,
    pub jwks_digest: Sha256Digest,
}

impl GvisorGuestIdentityConfig {
    pub(crate) fn install(self) -> Result<GvisorGuestWorkloadIdentity, ()> {
        if !stable_identifier(&self.issuer)
            || !stable_identifier(&self.audience)
            || !dns_name(&self.namespace)
            || !dns_name(&self.service_account_name)
            || canonical_digest(&self.jwks).ok().as_deref() != Some(self.jwks_digest.as_str())
        {
            return Err(());
        }
        let keys: JwkSet = serde_json::from_value(self.jwks).map_err(|_| ())?;
        if keys.keys.is_empty() || keys.keys.len() > MAX_JWKS_KEYS {
            return Err(());
        }
        let mut previous: Option<&str> = None;
        for key in &keys.keys {
            let kid = key
                .common
                .key_id
                .as_deref()
                .filter(|value| stable_identifier(value))
                .ok_or(())?;
            if previous.is_some_and(|value| value >= kid)
                || !jwk_is_valid_rs256_signature_key(key)
                || DecodingKey::from_jwk(key).is_err()
            {
                return Err(());
            }
            previous = Some(kid);
        }
        Ok(GvisorGuestWorkloadIdentity {
            issuer: self.issuer,
            audience: self.audience,
            namespace: self.namespace,
            service_account_name: self.service_account_name,
            keys,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct GvisorGuestWorkloadIdentity {
    issuer: String,
    audience: String,
    namespace: String,
    service_account_name: String,
    keys: JwkSet,
}

impl tonic::service::Interceptor for GvisorGuestWorkloadIdentity {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let authorization = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .ok_or_else(rejected)?;
        let token = authorization.strip_prefix("Bearer ").ok_or_else(rejected)?;
        if token.is_empty()
            || token.len() > MAX_TOKEN_BYTES
            || !token.is_ascii()
            || token.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(rejected());
        }
        let identity = self.verify(token)?;
        request
            .extensions_mut()
            .insert(AuthenticatedGvisorGuestIdentity(identity));
        Ok(request)
    }
}

impl GvisorGuestWorkloadIdentity {
    fn verify(&self, token: &str) -> Result<GvisorGuestPodIdentity, Status> {
        let header = decode_header(token).map_err(|_| rejected())?;
        if header.alg != Algorithm::RS256 || header.typ.as_deref() != Some("JWT") {
            return Err(rejected());
        }
        let kid = header
            .kid
            .as_deref()
            .filter(|value| stable_identifier(value))
            .ok_or_else(rejected)?;
        let key = self
            .keys
            .keys
            .iter()
            .find(|key| key.common.key_id.as_deref() == Some(kid))
            .filter(|key| jwk_is_valid_rs256_signature_key(key))
            .ok_or_else(rejected)?;
        let decoding_key = DecodingKey::from_jwk(key).map_err(|_| rejected())?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_audience(&[self.audience.as_str()]);
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.set_required_spec_claims(&["aud", "exp", "iat", "iss", "sub"]);
        validation.leeway = MAX_CLOCK_SKEW_SECONDS;
        validation.validate_nbf = true;
        let claims = decode::<KubernetesServiceAccountClaims>(token, &decoding_key, &validation)
            .map_err(|_| rejected())?
            .claims;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| rejected())?
            .as_secs();
        if claims.exp <= claims.iat
            || claims.exp <= now
            || claims.iat > now.saturating_add(MAX_CLOCK_SKEW_SECONDS)
            || claims.exp.saturating_sub(claims.iat) > MAX_TOKEN_LIFETIME_SECONDS
            || claims.exp > now.saturating_add(MAX_TOKEN_LIFETIME_SECONDS)
            || claims.kubernetes.namespace != self.namespace
            || claims.kubernetes.service_account.name != self.service_account_name
            || claims.sub
                != format!(
                    "system:serviceaccount:{}:{}",
                    self.namespace, self.service_account_name
                )
        {
            return Err(rejected());
        }
        let identity = GvisorGuestPodIdentity {
            namespace: claims.kubernetes.namespace,
            pod_name: claims.kubernetes.pod.name,
            pod_uid: claims.kubernetes.pod.uid,
            service_account_name: claims.kubernetes.service_account.name,
        };
        identity.validate().map_err(|_| rejected())?;
        Ok(identity)
    }
}

#[derive(Debug, Deserialize)]
struct KubernetesServiceAccountClaims {
    exp: u64,
    iat: u64,
    sub: String,
    #[serde(rename = "kubernetes.io")]
    kubernetes: KubernetesPrivateClaims,
}

#[derive(Debug, Deserialize)]
struct KubernetesPrivateClaims {
    namespace: String,
    pod: KubernetesObjectIdentity,
    #[serde(rename = "serviceaccount")]
    service_account: KubernetesObjectIdentity,
}

#[derive(Debug, Deserialize)]
struct KubernetesObjectIdentity {
    name: String,
    uid: String,
}

fn rejected() -> Status {
    Status::unauthenticated("gVisor guest token rejected")
}

fn stable_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 512
        && value.is_ascii()
        && !value.chars().any(char::is_control)
}

fn dns_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label.bytes().enumerate().all(|(index, byte)| match byte {
                    b'a'..=b'z' | b'0'..=b'9' => true,
                    b'-' => index > 0 && index + 1 < label.len(),
                    _ => false,
                })
        })
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

    fn verifier() -> GvisorGuestWorkloadIdentity {
        let jwks = serde_json::json!({"keys": [{
            "kty": "RSA",
            "kid": "guest-key-1",
            "use": "sig",
            "alg": "RS256",
            "n": "4KoeIFhx35ADyXYT0MpVCFDcWPKi1KUDxNTnPu1uubb9hqbnpgq68U8YQAGT1Dh1B4lyZmqUvYbGLNBj7CEcuJdms6JkohM50AdwBv6-TCy_uLpZzcUs8AGh8zFyVeyceX2CkZptlaP-362KPVB0tnvmjRVO2tJLiiqFBGqe9OKKGL-WevKFrUlSoaWTova7baKBBIMUx8GckC9NHvSj9oMbaaOTziTSOhonVnzHr1diFh5CbluUn3ef6KFcO8mssT-prqfqHYnNCEeLRsEUZT79oCVXb2H9RasBv7mU-FNPNwj8dcWcfUIV6ePEDjAGH-KU1eStSTYxeJEfbgW9zw",
            "e": "AQAB"
        }]});
        GvisorGuestIdentityConfig {
            issuer: "https://kubernetes.default.svc.cluster.local".to_owned(),
            audience: "insight-platform-gvisor-guest".to_owned(),
            namespace: "insight-platform-sandbox-guests".to_owned(),
            service_account_name: "insight-platform-gvisor-guest".to_owned(),
            jwks_digest: canonical_digest(&jwks).unwrap().parse().unwrap(),
            jwks,
        }
        .install()
        .unwrap()
    }

    fn token(audience: &str, service_account: &str) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let claims = serde_json::json!({
            "iss": "https://kubernetes.default.svc.cluster.local",
            "aud": [audience],
            "sub": format!("system:serviceaccount:insight-platform-sandbox-guests:{service_account}"),
            "iat": now,
            "exp": now + 600,
            "kubernetes.io": {
                "namespace": "insight-platform-sandbox-guests",
                "pod": {
                    "name": "insight-gv-0123456789abcdef0123456789abcdef",
                    "uid": "765c6658-49d2-4f1c-8319-dde65dca6664"
                },
                "serviceaccount": {
                    "name": service_account,
                    "uid": "3b9f3b8c-4148-4310-b87c-339a2095745c"
                }
            }
        });
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("guest-key-1".to_owned());
        let der = STANDARD
            .decode(
                PRIVATE_KEY
                    .lines()
                    .filter(|line| !line.starts_with("-----"))
                    .collect::<String>(),
            )
            .unwrap();
        encode(&header, &claims, &EncodingKey::from_rsa_der(&der)).unwrap()
    }

    fn intercepted(token: &str) -> Result<Request<()>, Status> {
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        tonic::service::Interceptor::call(&mut verifier(), request)
    }

    #[test]
    fn accepts_only_exact_pod_bound_projected_token() {
        let accepted = intercepted(&token(
            "insight-platform-gvisor-guest",
            "insight-platform-gvisor-guest",
        ))
        .unwrap();
        let identity = &accepted
            .extensions()
            .get::<AuthenticatedGvisorGuestIdentity>()
            .unwrap()
            .0;
        assert_eq!(identity.namespace, "insight-platform-sandbox-guests");
        assert_eq!(identity.pod_uid, "765c6658-49d2-4f1c-8319-dde65dca6664");

        assert!(intercepted(&token("wrong-audience", "insight-platform-gvisor-guest")).is_err());
        assert!(intercepted(&token(
            "insight-platform-gvisor-guest",
            "different-service-account"
        ))
        .is_err());
    }
}
