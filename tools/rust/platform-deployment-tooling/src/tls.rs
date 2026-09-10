//! Shared certificate material generation. Callers own persistence and exact topology selection.
use insight_platform_deployment_contracts::installation::InstallationError;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};

pub struct CertificateMaterial {
    pub certificate_pem: String,
    pub private_key_pem: String,
}
pub fn authority_parameters() -> Result<CertificateParams, InstallationError> {
    let mut params = CertificateParams::new(Vec::<String>::new())
        .map_err(|_| InstallationError::CredentialInvalid)?;
    params
        .distinguished_name
        .push(DnType::CommonName, "Insight Local Installation CA");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    Ok(params)
}
pub fn create_authority() -> Result<CertificateMaterial, InstallationError> {
    let key = KeyPair::generate().map_err(|_| InstallationError::CredentialInvalid)?;
    let certificate = authority_parameters()?
        .self_signed(&key)
        .map_err(|_| InstallationError::CredentialInvalid)?;
    Ok(CertificateMaterial {
        certificate_pem: certificate.pem(),
        private_key_pem: key.serialize_pem(),
    })
}
pub fn create_leaf(
    dns_names: &[&str],
    workload_identity: Option<&str>,
    usage: ExtendedKeyUsagePurpose,
    issuer: &Issuer<'_, KeyPair>,
) -> Result<CertificateMaterial, InstallationError> {
    let mut params = CertificateParams::new(
        dns_names
            .iter()
            .map(|name| (*name).to_owned())
            .collect::<Vec<_>>(),
    )
    .map_err(|_| InstallationError::CredentialInvalid)?;
    // Names distinguish issuer and subject for strict X.509 path builders. Authorization still
    // uses the exact SAN identities, key usages and trusted CA, never the common name.
    params
        .distinguished_name
        .push(DnType::CommonName, "Insight Local Workload");
    if let Some(identity) = workload_identity {
        params.subject_alt_names.push(SanType::URI(
            identity
                .try_into()
                .map_err(|_| InstallationError::CredentialInvalid)?,
        ));
    }
    params.use_authority_key_identifier_extension = true;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![usage];
    let key = KeyPair::generate().map_err(|_| InstallationError::CredentialInvalid)?;
    let certificate = params
        .signed_by(&key, issuer)
        .map_err(|_| InstallationError::CredentialInvalid)?;
    Ok(CertificateMaterial {
        certificate_pem: certificate.pem(),
        private_key_pem: key.serialize_pem(),
    })
}

use crate::{full_profile, DevProfile};
use std::collections::BTreeMap;
pub const RUNTIME_ARTIFACT_DATA_CERTIFICATE_FILE: &str = "artifact-data.pem";
pub const RUNTIME_ARTIFACT_DATA_PRIVATE_KEY_FILE: &str = "artifact-data-key.pem";
pub const RUNTIME_ARTIFACT_GATEWAY_CERTIFICATE_FILE: &str = "artifact-gateway.pem";
pub const RUNTIME_ARTIFACT_GATEWAY_PRIVATE_KEY_FILE: &str = "artifact-gateway-key.pem";
pub const RUNTIME_CA_CERTIFICATE_FILE: &str = "ca.pem";
pub const RUNTIME_CA_PRIVATE_KEY_FILE: &str = "ca-key.pem";
pub const RUNTIME_GATEWAY_CLIENT_CERTIFICATE_FILE: &str = "gateway-client.pem";
pub const RUNTIME_GATEWAY_CLIENT_PRIVATE_KEY_FILE: &str = "gateway-client-key.pem";
pub const RUNTIME_NATS_CLIENT_CERTIFICATE_FILE: &str = "nats-client.pem";
pub const RUNTIME_NATS_CLIENT_PRIVATE_KEY_FILE: &str = "nats-client-key.pem";
pub const RUNTIME_NATS_SERVER_CERTIFICATE_FILE: &str = "nats-server.pem";
pub const RUNTIME_NATS_SERVER_PRIVATE_KEY_FILE: &str = "nats-server-key.pem";
pub const RUNTIME_ORCHESTRATION_CLIENT_CERTIFICATE_FILE: &str = "orchestration-client.pem";
pub const RUNTIME_ORCHESTRATION_CLIENT_PRIVATE_KEY_FILE: &str = "orchestration-client-key.pem";
pub const RUNTIME_REGISTRY_VALIDATION_CLIENT_CERTIFICATE_FILE: &str =
    "registry-validation-client.pem";
pub const RUNTIME_REGISTRY_VALIDATION_CLIENT_PRIVATE_KEY_FILE: &str =
    "registry-validation-client-key.pem";
pub const PUBLIC_GATEWAY_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/public-gateway";
pub const SCHEDULER_WORKLOAD_IDENTITY: &str = "spiffe://insight.platform/workload/scheduler";
#[derive(Clone, Copy)]
pub enum LocalTlsUsage {
    Server,
    Client,
}

#[derive(Clone, Copy)]
pub struct LocalTlsIdentitySpec {
    pub certificate: &'static str,
    pub private_key: &'static str,
    pub dns_names: &'static [&'static str],
    pub workload_identity: Option<&'static str>,
    pub usage: LocalTlsUsage,
}

pub fn native_identity_specs(
    selected_profile: DevProfile,
) -> BTreeMap<&'static str, LocalTlsIdentitySpec> {
    let mut identities = BTreeMap::from([
        (
            RUNTIME_ARTIFACT_GATEWAY_CERTIFICATE_FILE,
            LocalTlsIdentitySpec {
                certificate: RUNTIME_ARTIFACT_GATEWAY_CERTIFICATE_FILE,
                private_key: RUNTIME_ARTIFACT_GATEWAY_PRIVATE_KEY_FILE,
                dns_names: &["localhost"],
                workload_identity: None,
                usage: LocalTlsUsage::Server,
            },
        ),
        (
            RUNTIME_ARTIFACT_DATA_CERTIFICATE_FILE,
            LocalTlsIdentitySpec {
                certificate: RUNTIME_ARTIFACT_DATA_CERTIFICATE_FILE,
                private_key: RUNTIME_ARTIFACT_DATA_PRIVATE_KEY_FILE,
                dns_names: &["localhost"],
                workload_identity: None,
                usage: LocalTlsUsage::Server,
            },
        ),
        (
            RUNTIME_GATEWAY_CLIENT_CERTIFICATE_FILE,
            LocalTlsIdentitySpec {
                certificate: RUNTIME_GATEWAY_CLIENT_CERTIFICATE_FILE,
                private_key: RUNTIME_GATEWAY_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(PUBLIC_GATEWAY_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
        ),
        (
            RUNTIME_ORCHESTRATION_CLIENT_CERTIFICATE_FILE,
            LocalTlsIdentitySpec {
                certificate: RUNTIME_ORCHESTRATION_CLIENT_CERTIFICATE_FILE,
                private_key: RUNTIME_ORCHESTRATION_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(SCHEDULER_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
        ),
        (
            RUNTIME_NATS_SERVER_CERTIFICATE_FILE,
            LocalTlsIdentitySpec {
                certificate: RUNTIME_NATS_SERVER_CERTIFICATE_FILE,
                private_key: RUNTIME_NATS_SERVER_PRIVATE_KEY_FILE,
                dns_names: &["localhost"],
                workload_identity: None,
                usage: LocalTlsUsage::Server,
            },
        ),
        (
            RUNTIME_NATS_CLIENT_CERTIFICATE_FILE,
            LocalTlsIdentitySpec {
                certificate: RUNTIME_NATS_CLIENT_CERTIFICATE_FILE,
                private_key: RUNTIME_NATS_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some("spiffe://insight.platform/workload/local-nats-client"),
                usage: LocalTlsUsage::Client,
            },
        ),
    ]);
    identities.insert(
        full_profile::GATEWAY_EGRESS_CLIENT_CERTIFICATE_FILE,
        LocalTlsIdentitySpec {
            certificate: full_profile::GATEWAY_EGRESS_CLIENT_CERTIFICATE_FILE,
            private_key: full_profile::GATEWAY_EGRESS_CLIENT_PRIVATE_KEY_FILE,
            dns_names: &[],
            workload_identity: Some(insight_platform_contracts::GATEWAY_WORKLOAD_IDENTITY),
            usage: LocalTlsUsage::Client,
        },
    );
    for spec in outbox_identity_specs() {
        identities.insert(spec.certificate, spec);
    }
    identities.insert(
        RUNTIME_REGISTRY_VALIDATION_CLIENT_CERTIFICATE_FILE,
        LocalTlsIdentitySpec {
            certificate: RUNTIME_REGISTRY_VALIDATION_CLIENT_CERTIFICATE_FILE,
            private_key: RUNTIME_REGISTRY_VALIDATION_CLIENT_PRIVATE_KEY_FILE,
            dns_names: &[],
            workload_identity: Some(
                "spiffe://insight.platform/workload/registry-validation-worker",
            ),
            usage: LocalTlsUsage::Client,
        },
    );
    let mut insert = |spec: LocalTlsIdentitySpec| {
        identities.insert(spec.certificate, spec);
    };
    if selected_profile.needs_egress() {
        for spec in [
            LocalTlsIdentitySpec {
                certificate: full_profile::SECURITY_AUTHORITY_CERTIFICATE_FILE,
                private_key: full_profile::SECURITY_AUTHORITY_PRIVATE_KEY_FILE,
                dns_names: &["localhost"],
                workload_identity: None,
                usage: LocalTlsUsage::Server,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::EGRESS_BROKER_CLIENT_CERTIFICATE_FILE,
                private_key: full_profile::EGRESS_BROKER_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(full_profile::EGRESS_BROKER_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::EGRESS_BROKER_CERTIFICATE_FILE,
                private_key: full_profile::EGRESS_BROKER_PRIVATE_KEY_FILE,
                dns_names: &["localhost"],
                workload_identity: None,
                usage: LocalTlsUsage::Server,
            },
        ] {
            insert(spec);
        }
    }
    if selected_profile.has_model() {
        insert(LocalTlsIdentitySpec {
            certificate: full_profile::MODEL_WORKER_CLIENT_CERTIFICATE_FILE,
            private_key: full_profile::MODEL_WORKER_CLIENT_PRIVATE_KEY_FILE,
            dns_names: &[],
            workload_identity: Some(full_profile::MODEL_WORKER_WORKLOAD_IDENTITY),
            usage: LocalTlsUsage::Client,
        });
    }
    if selected_profile.has_context() {
        for spec in [
            LocalTlsIdentitySpec {
                certificate: full_profile::CONTEXT_WORKER_CLIENT_CERTIFICATE_FILE,
                private_key: full_profile::CONTEXT_WORKER_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(full_profile::CONTEXT_WORKER_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::CONTEXT_DATASET_CLIENT_CERTIFICATE_FILE,
                private_key: full_profile::CONTEXT_DATASET_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(full_profile::CONTEXT_DATASET_WORKER_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::CONTEXT_SUBSCRIPTION_CLIENT_CERTIFICATE_FILE,
                private_key: full_profile::CONTEXT_SUBSCRIPTION_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(full_profile::CONTEXT_WORKER_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::MCP_RESOURCE_HOST_CERTIFICATE_FILE,
                private_key: full_profile::MCP_RESOURCE_HOST_PRIVATE_KEY_FILE,
                dns_names: &["localhost"],
                workload_identity: None,
                usage: LocalTlsUsage::Server,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::MCP_RESOURCE_EGRESS_CLIENT_CERTIFICATE_FILE,
                private_key: full_profile::MCP_RESOURCE_EGRESS_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(full_profile::MCP_HOST_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
        ] {
            insert(spec);
        }
    }
    if selected_profile.has_remote_capability() {
        insert(LocalTlsIdentitySpec {
            certificate: full_profile::CAPABILITY_REMOTE_CLIENT_CERTIFICATE_FILE,
            private_key: full_profile::CAPABILITY_REMOTE_CLIENT_PRIVATE_KEY_FILE,
            dns_names: &[],
            workload_identity: Some(full_profile::CAPABILITY_WORKER_WORKLOAD_IDENTITY),
            usage: LocalTlsUsage::Client,
        });
    }
    if selected_profile.has_mcp() {
        for spec in [
            LocalTlsIdentitySpec {
                certificate: full_profile::MCP_HOST_CERTIFICATE_FILE,
                private_key: full_profile::MCP_HOST_PRIVATE_KEY_FILE,
                dns_names: &["localhost"],
                workload_identity: None,
                usage: LocalTlsUsage::Server,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::MCP_RESOURCE_HOST_CERTIFICATE_FILE,
                private_key: full_profile::MCP_RESOURCE_HOST_PRIVATE_KEY_FILE,
                dns_names: &["localhost"],
                workload_identity: None,
                usage: LocalTlsUsage::Server,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::MCP_HOST_EGRESS_CLIENT_CERTIFICATE_FILE,
                private_key: full_profile::MCP_HOST_EGRESS_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(full_profile::MCP_HOST_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::MCP_RESOURCE_EGRESS_CLIENT_CERTIFICATE_FILE,
                private_key: full_profile::MCP_RESOURCE_EGRESS_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(full_profile::MCP_HOST_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::MCP_DISCOVERY_CLIENT_CERTIFICATE_FILE,
                private_key: full_profile::MCP_DISCOVERY_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(full_profile::MCP_DISCOVERY_WORKER_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::MCP_SUBSCRIPTION_CLIENT_CERTIFICATE_FILE,
                private_key: full_profile::MCP_SUBSCRIPTION_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(full_profile::MCP_SUBSCRIPTION_WORKER_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::MCP_CLEANUP_CLIENT_CERTIFICATE_FILE,
                private_key: full_profile::MCP_CLEANUP_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(full_profile::MCP_CLEANUP_WORKER_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::CALLBACK_CLIENT_CERTIFICATE_FILE,
                private_key: full_profile::CALLBACK_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(full_profile::MCP_CALLBACK_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
        ] {
            insert(spec);
        }
    }
    identities
}

pub fn outbox_identity_specs() -> [LocalTlsIdentitySpec; 2] {
    [
        LocalTlsIdentitySpec {
            certificate: "outbox-client.pem",
            private_key: "outbox-client-key.pem",
            dns_names: &[],
            workload_identity: Some(insight_platform_contracts::OUTBOX_WORKER_WORKLOAD_IDENTITY),
            usage: LocalTlsUsage::Client,
        },
        LocalTlsIdentitySpec {
            certificate: "outbox-provision-client.pem",
            private_key: "outbox-provision-client-key.pem",
            dns_names: &[],
            workload_identity: Some("spiffe://insight.platform/workload/local-outbox-provisioner"),
            usage: LocalTlsUsage::Client,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use x509_parser::{extensions::GeneralName, prelude::*};

    #[test]
    fn issuer_and_workload_subjects_are_distinct_and_exact_san_chain_remains_authoritative() {
        let authority = create_authority().unwrap();
        let key = KeyPair::from_pem(&authority.private_key_pem).unwrap();
        let issuer = Issuer::new(authority_parameters().unwrap(), key);
        let identity = "spiffe://insight.platform/workload/test-gateway";
        let leaf = create_leaf(
            &["localstack.installation-test.svc.cluster.local"],
            Some(identity),
            ExtendedKeyUsagePurpose::ServerAuth,
            &issuer,
        )
        .unwrap();
        let (_, authority_pem) =
            x509_parser::pem::parse_x509_pem(authority.certificate_pem.as_bytes()).unwrap();
        let (_, authority) = X509Certificate::from_der(&authority_pem.contents).unwrap();
        let (_, leaf_pem) =
            x509_parser::pem::parse_x509_pem(leaf.certificate_pem.as_bytes()).unwrap();
        let (_, leaf) = X509Certificate::from_der(&leaf_pem.contents).unwrap();
        assert_ne!(leaf.subject(), leaf.issuer());
        assert_eq!(leaf.issuer(), authority.subject());
        assert_eq!(
            authority
                .subject()
                .iter_common_name()
                .next()
                .unwrap()
                .as_str()
                .unwrap(),
            "Insight Local Installation CA"
        );
        assert_eq!(
            leaf.subject()
                .iter_common_name()
                .next()
                .unwrap()
                .as_str()
                .unwrap(),
            "Insight Local Workload"
        );
        assert_eq!(
            leaf.subject_alternative_name()
                .unwrap()
                .unwrap()
                .value
                .general_names,
            vec![
                GeneralName::DNSName("localstack.installation-test.svc.cluster.local"),
                GeneralName::URI(identity)
            ]
        );
        assert!(
            leaf.extended_key_usage()
                .unwrap()
                .unwrap()
                .value
                .server_auth
        );
        assert!(
            !leaf
                .extended_key_usage()
                .unwrap()
                .unwrap()
                .value
                .client_auth
        );
        assert!(authority.basic_constraints().unwrap().unwrap().value.ca);
        assert!(authority
            .key_usage()
            .unwrap()
            .unwrap()
            .value
            .key_cert_sign());
        leaf.verify_signature(Some(authority.public_key())).unwrap();
        assert!(leaf.verify_signature(Some(leaf.public_key())).is_err());
    }
}
