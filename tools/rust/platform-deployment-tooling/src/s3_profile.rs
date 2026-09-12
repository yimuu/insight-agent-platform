//! Closed SeaweedFS development profile. Static IAM grants bucket capability classes.
//!
//! Exact, nonempty generations remain the Artifact Broker's authority. SeaweedFS 4.46 accepts
//! an empty versionId as a version action but applies a delete marker; this profile does not
//! claim IAM enforces a nonempty generation. No legacy Write action or IAM management API is used.
use insight_platform_deployment_contracts::installation::InstallationError;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use zeroize::{Zeroize, Zeroizing};

pub const S3_IMAGE: &str =
    "chrislusf/seaweedfs@sha256:08d516132314207d10c8e37cbffc1f32b147d870169688734cc61c6231625b62";
pub const S3_DIRECTORY: &str = "/run/insight/s3";
pub const S3_DATA_DIRECTORY: &str = "/data";
pub const S3_STOP_GRACE_SECONDS: u32 = 45;
pub const S3_CONFIGURATION_FILE: &str = "s3.json";
pub const S3_SECURITY_FILE: &str = "security.toml";
pub const S3_API_CA_FILE: &str = "ca.pem";
pub const S3_SERVER_CERTIFICATE_FILE: &str = "server.crt";
pub const S3_SERVER_KEY_FILE: &str = "server.key";
pub const S3_GRPC_CA_FILE: &str = "grpc-ca.pem";
pub const S3_GRPC_CERTIFICATE_FILE: &str = "grpc-server.crt";
pub const S3_GRPC_KEY_FILE: &str = "grpc-server.key";

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum S3IdentityRole {
    Initializer,
    ArtifactGateway,
    ArtifactData,
    ArtifactMaintenance,
}

impl S3IdentityRole {
    pub const ALL: [Self; 4] = [
        Self::Initializer,
        Self::ArtifactGateway,
        Self::ArtifactData,
        Self::ArtifactMaintenance,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::Initializer => "initializer",
            Self::ArtifactGateway => "artifact-gateway",
            Self::ArtifactData => "artifact-data",
            Self::ArtifactMaintenance => "artifact-maintenance",
        }
    }

    pub const fn credential_filename(self) -> &'static str {
        match self {
            Self::Initializer => "s3-initializer-credentials",
            Self::ArtifactGateway => "s3-artifact-gateway-credentials",
            Self::ArtifactData => "s3-artifact-data-credentials",
            Self::ArtifactMaintenance => "s3-artifact-maintenance-credentials",
        }
    }
}

/// Deliberately neither Debug nor Serialize. Only the private server document exposes these keys.
pub struct S3RoleCredentials {
    access_key: [u8; 16],
    secret_key: [u8; 32],
}

impl Drop for S3RoleCredentials {
    fn drop(&mut self) {
        self.access_key.zeroize();
        self.secret_key.zeroize();
    }
}

fn decode_hex<const N: usize>(raw: &str) -> Result<[u8; N], InstallationError> {
    if raw.len() != N * 2
        || !raw
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(InstallationError::CredentialInvalid);
    }
    let mut output = [0; N];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&raw[index * 2..index * 2 + 2], 16)
            .map_err(|_| InstallationError::CredentialInvalid)?;
    }
    Ok(output)
}

impl S3RoleCredentials {
    /// Expose borrowed, fixed-format values only to the explicit SDK credential constructor.
    /// The callback must not log them; the shared producer stays independent of the AWS SDK.
    pub fn with_keys<T>(&self, use_keys: impl FnOnce(&str, &str) -> T) -> T {
        let access = Zeroizing::new(crate::lower_hex(&self.access_key));
        let secret = Zeroizing::new(crate::lower_hex(&self.secret_key));
        use_keys(&access, &secret)
    }

    /// Accept only the exact private INI emitted by prepare, never ambient profiles or overrides.
    pub fn decode(bytes: &[u8]) -> Result<Self, InstallationError> {
        if bytes.len() > 256 {
            return Err(InstallationError::CredentialInvalid);
        }
        let raw = std::str::from_utf8(bytes).map_err(|_| InstallationError::CredentialInvalid)?;
        let body = raw
            .strip_prefix("[default]\naws_access_key_id=")
            .ok_or(InstallationError::CredentialInvalid)?;
        let (access, secret) = body
            .split_once("\naws_secret_access_key=")
            .ok_or(InstallationError::CredentialInvalid)?;
        let secret = secret
            .strip_suffix('\n')
            .ok_or(InstallationError::CredentialInvalid)?;
        Ok(Self {
            access_key: decode_hex(access)?,
            secret_key: decode_hex(secret)?,
        })
    }
}

/// Contains private credentials. The publisher must enforce private modes and exact inventory.
pub struct S3ServerProfile {
    pub configuration_json: Vec<u8>,
    pub security_toml: Vec<u8>,
    pub arguments: Vec<String>,
}

fn policy(bucket: &str, role: S3IdentityRole) -> Value {
    let bucket_arn = format!("arn:aws:s3:::{bucket}");
    let object_arn = format!("{bucket_arn}/v1/*");
    let mut statements = vec![
        json!({"Effect":"Allow","Action":["s3:ListBucket"],"Resource":[bucket_arn],
               "Condition":{"StringEquals":{"s3:RequestMethod":"HEAD"}}}),
        json!({"Effect":"Allow","Action":["s3:GetBucketVersioning"],"Resource":[bucket_arn]}),
    ];
    if role == S3IdentityRole::Initializer {
        // The pinned Seaweed action constants spell Cors, unlike the AWS IAM CORS spelling.
        statements.push(json!({"Effect":"Allow","Resource":[bucket_arn],"Action":[
            "s3:CreateBucket","s3:GetBucketTagging","s3:PutBucketTagging","s3:PutBucketVersioning",
            "s3:GetBucketCors","s3:PutBucketCors"
        ]}));
    } else {
        let reads = if role == S3IdentityRole::ArtifactMaintenance {
            vec!["s3:GetObjectVersion"]
        } else {
            // Conditional-create recovery needs one unversioned HEAD to discover the generation.
            vec!["s3:GetObject", "s3:GetObjectVersion"]
        };
        statements.push(
            json!({"Effect":"Allow","Action":reads,"Resource":[object_arn],
            "Condition":{"StringEquals":{"s3:RequestMethod":["GET","HEAD"]}}}),
        );
        if role == S3IdentityRole::ArtifactMaintenance {
            statements.push(
                json!({"Effect":"Allow","Action":["s3:DeleteObjectVersion"],"Resource":[object_arn],
                "Condition":{"StringEquals":{"s3:RequestMethod":"DELETE"}}}),
            );
        } else {
            // Seaweed implicitly matches multipart actions against PutObject. Limit the actual
            // method so this grant cannot authorize DELETE abort or POST multipart initiation.
            statements.push(
                json!({"Effect":"Allow","Action":["s3:PutObject"],"Resource":[object_arn],
                "Condition":{"StringEquals":{"s3:RequestMethod":"PUT"}}}),
            );
        }
    }
    json!({"Version":"2012-10-17","Statement":statements})
}

pub fn render_s3_profile(
    bucket: &str,
    credentials: &BTreeMap<S3IdentityRole, S3RoleCredentials>,
) -> Result<S3ServerProfile, InstallationError> {
    let nonce = bucket
        .strip_prefix("insight-platform-artifacts-")
        .ok_or(InstallationError::InvalidInput)?;
    decode_hex::<16>(nonce).map_err(|_| InstallationError::InvalidInput)?;
    if credentials.len() != S3IdentityRole::ALL.len()
        || !S3IdentityRole::ALL
            .iter()
            .all(|role| credentials.contains_key(role))
        || credentials
            .values()
            .map(|value| value.access_key)
            .collect::<BTreeSet<_>>()
            .len()
            != credentials.len()
        || credentials
            .values()
            .map(|value| value.secret_key)
            .collect::<BTreeSet<_>>()
            .len()
            != credentials.len()
    {
        return Err(InstallationError::CredentialInvalid);
    }
    let mut identities = Vec::new();
    let mut policies = Vec::new();
    for role in S3IdentityRole::ALL {
        let credential = &credentials[&role];
        identities.push(json!({"name":role.name(),"actions":[],"policyNames":[role.name()],
            "credentials":[{"accessKey":crate::lower_hex(&credential.access_key),"secretKey":crate::lower_hex(&credential.secret_key)}]}));
        policies.push(json!({"name":role.name(),"content":serde_json::to_string(&policy(bucket, role)).map_err(|_| InstallationError::InvalidInput)?}));
    }
    let configuration_json =
        serde_json::to_vec(&json!({"identities":identities,"policies":policies}))
            .map_err(|_| InstallationError::InvalidInput)?;
    if configuration_json.len() > 16_384 {
        return Err(InstallationError::InvalidInput);
    }
    // A separate gRPC authority is mandatory. No platform role receives its client key.
    // advancedtls 1.0.0 checks Min > Max before defaulting Max: set BOTH bounds explicitly.
    let security_toml = br#"[tls]
min_version = "TLS 1.2"
max_version = "TLS 1.3"
[grpc]
ca = "/run/insight/s3/grpc-ca.pem"
[grpc.s3]
cert = "/run/insight/s3/grpc-server.crt"
key = "/run/insight/s3/grpc-server.key"
allowed_commonNames = "Insight Local Workload"
"#
    .to_vec();
    Ok(S3ServerProfile {
        configuration_json,
        security_toml,
        arguments: server_arguments(),
    })
}

pub fn server_arguments() -> Vec<String> {
    [
        "-config_dir=/run/insight/s3",
        "server",
        "-dir=/data",
        "-ip=127.0.0.1",
        "-ip.bind=127.0.0.1",
        "-master=true",
        "-volume=true",
        "-filer=true",
        "-s3=true",
        "-iam=false",
        "-master.volumeSizeLimitMB=64",
        "-master.telemetry=false",
        // Artifact buckets and the filer metadata log use separate collections. Leave room
        // for both; four volumes are consumed by the first bucket alone.
        "-volume.max=16",
        "-s3.ip.bind=0.0.0.0",
        "-s3.port=8333",
        "-s3.port.https=0",
        "-s3.port.grpc=18333",
        "-s3.port.iceberg=0",
        "-s3.port.lance=0",
        "-s3.iam=false",
        "-s3.autoCreateBucket=false",
        "-s3.allowDeleteBucketNotEmpty=false",
        "-s3.allowedOrigins=",
        "-s3.config=/run/insight/s3/s3.json",
        "-s3.key.file=/run/insight/s3/server.key",
        "-s3.cert.file=/run/insight/s3/server.crt",
        "-s3.concurrentUploadLimitMB=16",
        "-s3.concurrentFileUploadLimit=4",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    const BUCKET: &str = "insight-platform-artifacts-0123456789abcdef0123456789abcdef";
    fn credentials() -> BTreeMap<S3IdentityRole, S3RoleCredentials> {
        S3IdentityRole::ALL
            .into_iter()
            .enumerate()
            .map(|(index, role)| {
                (
                    role,
                    S3RoleCredentials {
                        access_key: [index as u8; 16],
                        secret_key: [(index + 4) as u8; 32],
                    },
                )
            })
            .collect()
    }
    #[test]
    fn private_ini_has_no_profiles_duplicates_or_ambient_extension() {
        let valid = format!(
            "[default]\naws_access_key_id={}\naws_secret_access_key={}\n",
            "a".repeat(32),
            "b".repeat(64)
        );
        assert!(S3RoleCredentials::decode(valid.as_bytes()).is_ok());
        for invalid in [
            valid.replace("[default]", "[other]"),
            valid.replace("aaaa", "AAAA"),
            valid.trim_end().to_owned(),
            format!("{valid}[other]\n"),
            format!("{valid}aws_session_token=x\n"),
            valid.replace("\n", "\r\n"),
            valid.replace("aws_access_key_id=", "aws_access_key_id= "),
        ] {
            assert!(S3RoleCredentials::decode(invalid.as_bytes()).is_err());
        }
    }
    #[test]
    fn shared_keys_missing_roles_and_arbitrary_bucket_paths_are_rejected() {
        let mut material = credentials();
        for bucket in [
            "*",
            "insight-platform-artifacts-*",
            "insight-platform-artifacts-0123456789abcdef0123456789abcdef/v1",
            "foreign",
        ] {
            assert!(render_s3_profile(bucket, &material).is_err());
        }
        material
            .get_mut(&S3IdentityRole::ArtifactData)
            .unwrap()
            .access_key = [0; 16];
        assert!(render_s3_profile(BUCKET, &material).is_err());
        material = credentials();
        material
            .get_mut(&S3IdentityRole::ArtifactData)
            .unwrap()
            .secret_key = [4; 32];
        assert!(render_s3_profile(BUCKET, &material).is_err());
        material = credentials();
        material.remove(&S3IdentityRole::Initializer);
        assert!(render_s3_profile(BUCKET, &material).is_err());
    }
    #[test]
    fn runtime_capability_classes_cannot_change_bucket_or_cross_roles() {
        let documents = render_s3_profile(BUCKET, &credentials()).unwrap();
        let config: Value = serde_json::from_slice(&documents.configuration_json).unwrap();
        for identity in config["identities"].as_array().unwrap() {
            assert_eq!(identity["actions"], json!([]));
            assert_eq!(identity["policyNames"], json!([identity["name"]]));
        }
        for role in S3IdentityRole::ALL {
            let rendered = policy(BUCKET, role);
            let statements = rendered["Statement"].as_array().unwrap();
            let actions = statements
                .iter()
                .flat_map(|statement| statement["Action"].as_array().unwrap())
                .map(|action| action.as_str().unwrap())
                .collect::<Vec<_>>();
            assert!(!actions.iter().any(|action| action.contains('*')
                || action.contains("Policy")
                || action.contains("Acl")));
            assert_eq!(
                statements[0]["Condition"],
                json!({"StringEquals":{"s3:RequestMethod":"HEAD"}})
            );
            if role == S3IdentityRole::Initializer {
                assert!(statements
                    .iter()
                    .all(|statement| statement["Resource"]
                        == json!([format!("arn:aws:s3:::{BUCKET}")])));
                assert!(actions.contains(&"s3:PutBucketCors"));
            } else {
                assert!(!actions.contains(&"s3:PutBucketVersioning"));
                assert!(!actions.contains(&"s3:CreateBucket"));
                assert!(!actions.contains(&"s3:DeleteObject"));
                if role == S3IdentityRole::ArtifactMaintenance {
                    assert!(!actions.contains(&"s3:PutObject"));
                    assert!(actions.contains(&"s3:DeleteObjectVersion"));
                    assert_eq!(
                        statements.last().unwrap()["Condition"],
                        json!({"StringEquals":{"s3:RequestMethod":"DELETE"}})
                    );
                } else {
                    assert!(
                        actions.contains(&"s3:PutObject")
                            && actions.contains(&"s3:GetObjectVersion")
                            && actions.contains(&"s3:GetObject")
                    );
                    assert!(!actions.contains(&"s3:DeleteObjectVersion"));
                    assert_eq!(
                        statements.last().unwrap()["Condition"],
                        json!({"StringEquals":{"s3:RequestMethod":"PUT"}})
                    );
                }
            }
        }
    }
    #[test]
    fn explicit_tls_bounds_and_closed_process_flags_are_deterministic() {
        let material = credentials();
        let first = render_s3_profile(BUCKET, &material).unwrap();
        let second = render_s3_profile(BUCKET, &material).unwrap();
        assert_eq!(first.configuration_json, second.configuration_json);
        assert_eq!(first.security_toml, second.security_toml);
        let tls = std::str::from_utf8(&first.security_toml).unwrap();
        assert!(
            tls.contains("min_version = \"TLS 1.2\"") && tls.contains("max_version = \"TLS 1.3\"")
        );
        assert!(tls.contains("/grpc-ca.pem") && !tls.contains("client.key"));
        for argument in [
            "-s3.iam=false",
            "-iam=false",
            "-s3.port.iceberg=0",
            "-s3.port.lance=0",
            "-s3.allowedOrigins=",
            "-s3.autoCreateBucket=false",
            "-ip.bind=127.0.0.1",
        ] {
            assert!(first.arguments.iter().any(|value| value == argument));
        }
    }
}
