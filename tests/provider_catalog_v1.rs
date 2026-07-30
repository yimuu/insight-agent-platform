use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use insight_agent_platform::{
    config::{
        ModelInputModality, ModelPolicyConfig, ModelSelectorConfig, ProviderExtensionConfig,
        ProviderExtensionSource, ProviderModelProfileConfig, ProviderTransportPolicy,
        ProvidersConfig,
    },
    resources::{
        config::load_model_registry_with_env,
        models::{ModelCapability, ModelRequestCapability, ModelSelector},
    },
};

fn selector(provider: &str, id: &str) -> ModelSelector {
    ModelSelector::new(provider, id).unwrap()
}

fn load_builtin(secret: Option<&str>) -> insight_agent_platform::resources::models::ModelRegistry {
    load_model_registry_with_env(&ProvidersConfig::default(), None, |name| {
        (name == "DASHSCOPE_API_KEY")
            .then(|| secret.map(str::to_owned))
            .flatten()
    })
    .unwrap()
}

#[test]
fn builtin_catalog_resolves_provider_model_capabilities_and_official_secret() {
    let registry = load_builtin(Some("never-log-this-key"));
    let text = registry
        .resolve(&selector("dashscope-cn", "qwen3.6-flash"))
        .unwrap();
    let vision = registry
        .resolve(&selector("dashscope-cn", "qwen-vl-plus"))
        .unwrap();

    assert!(!text.capabilities().contains(&ModelCapability::Vision));
    assert_eq!(text.capabilities(), BTreeSet::new());
    assert!(vision.capabilities().contains(&ModelCapability::Vision));
    assert_eq!(
        text.request_capabilities(),
        BTreeSet::from([
            ModelRequestCapability::Complete,
            ModelRequestCapability::Streaming,
        ])
    );
    assert!(!format!("{text:?}").contains("never-log-this-key"));
}

#[test]
fn missing_or_empty_secret_fails_only_when_the_model_is_resolved() {
    for (secret, code) in [
        (None, "MODEL_SECRET_MISSING"),
        (Some("  "), "MODEL_SECRET_EMPTY"),
    ] {
        let registry = load_builtin(secret);
        let error = registry
            .resolve(&selector("dashscope-cn", "qwen3.6-flash"))
            .unwrap_err();
        assert_eq!(error.code(), code);
        assert!(error.to_string().contains("DASHSCOPE_API_KEY"));
    }

    let registry = load_builtin(None);
    assert!(registry.selectors().count() >= 4);
}

#[test]
fn deployment_identity_pins_route_catalog_and_output_policy_but_not_secret_value() {
    let first = load_builtin(Some("secret-one"));
    let second = load_builtin(Some("secret-two"));
    let selector = selector("dashscope-cn", "qwen3.6-flash");
    let first = first.deployment_identity(&selector).unwrap();
    let second = second.deployment_identity(&selector).unwrap();

    assert_eq!(first.binding_hash(), second.binding_hash());
    assert_eq!(first.evidence(), second.evidence());
    assert_eq!(first.worker_version(), "openai-chat-adapter-2.1.0");
    assert_eq!(first.evidence()["provider_route"], "dashscope-cn");
    assert_eq!(first.evidence()["model_id"], "qwen3.6-flash");
    assert_eq!(first.evidence()["credential_env"], "DASHSCOPE_API_KEY");
    assert_eq!(
        first.evidence()["output_policy"],
        "prompt-local-validation-v1"
    );
    assert!(first.evidence().get("native_structured_output").is_none());
    assert!(first.evidence()["catalog_digest"]
        .as_str()
        .unwrap()
        .starts_with("sha256:"));
    let rendered = serde_json::to_string(first.evidence()).unwrap();
    assert!(!rendered.contains("secret-one"));
    assert!(!rendered.contains("secret-two"));
}

#[test]
fn regional_routes_freeze_distinct_endpoint_identity_without_implicit_selection() {
    let registry = load_builtin(Some("secret"));
    let cn = registry
        .deployment_identity(&selector("dashscope-cn", "qwen3.6-flash"))
        .unwrap();
    let intl = registry
        .deployment_identity(&selector("dashscope-intl", "qwen3.6-flash"))
        .unwrap();

    assert_ne!(
        cn.evidence()["endpoint_identity"],
        intl.evidence()["endpoint_identity"]
    );
    assert_ne!(cn.binding_hash(), intl.binding_hash());
}

#[test]
fn catalog_is_fail_closed_for_unknown_provider_and_model() {
    let registry = load_builtin(Some("secret"));
    for selector in [
        selector("unknown", "qwen3.6-flash"),
        selector("dashscope-cn", "qwen-does-not-exist"),
    ] {
        assert_eq!(
            registry.resolve(&selector).unwrap_err().code(),
            "MODEL_NOT_FOUND"
        );
    }
}

#[test]
fn custom_provider_supports_opaque_model_ids_and_input_modalities() {
    let providers = ProvidersConfig {
        extensions: BTreeMap::from([(
            "company-llm".to_owned(),
            ProviderExtensionConfig {
                source: ProviderExtensionSource::OpenAiCompatible,
                endpoint: Some("http://localhost:11434/v1".to_owned()),
                credential_env: Some("COMPANY_LLM_API_KEY".to_owned()),
                models: BTreeMap::from([(
                    "vendor/internal-chat/v1".to_owned(),
                    ProviderModelProfileConfig {
                        input: BTreeSet::from([
                            ModelInputModality::Text,
                            ModelInputModality::Image,
                        ]),
                    },
                )]),
                connect_timeout: Duration::from_secs(2),
                request_timeout: Duration::from_secs(30),
                transport: ProviderTransportPolicy::AllowLoopbackHttp,
            },
        )]),
    };
    let registry = load_model_registry_with_env(&providers, None, |name| {
        (name == "COMPANY_LLM_API_KEY").then(|| "private-key".to_owned())
    })
    .unwrap();
    let selector = selector("company-llm", "vendor/internal-chat/v1");
    let model = registry.resolve(&selector).unwrap();
    let identity = registry.deployment_identity(&selector).unwrap();

    assert!(model.capabilities().contains(&ModelCapability::Vision));
    assert!(identity.evidence()["provider_extension_digest"]
        .as_str()
        .unwrap()
        .starts_with("sha256:"));
    assert_eq!(
        identity.evidence()["endpoint_identity"],
        "http://localhost:11434/v1"
    );
    assert!(!serde_json::to_string(identity.evidence())
        .unwrap()
        .contains("private-key"));
    assert!(!serde_json::to_string(identity.evidence())
        .unwrap()
        .contains("native_structured_output"));

    let mut endpoint_with_query = providers.clone();
    endpoint_with_query
        .extensions
        .get_mut("company-llm")
        .unwrap()
        .endpoint = Some("https://llm.example/v1?api_key=forbidden".to_owned());
    assert_eq!(
        load_model_registry_with_env(&endpoint_with_query, None, |_| Some("key".to_owned()))
            .err()
            .unwrap()
            .code(),
        "PROVIDER_ENDPOINT_INVALID"
    );
}

#[test]
fn inherited_route_uses_a_distinct_identity_and_can_add_models() {
    let providers = ProvidersConfig {
        extensions: BTreeMap::from([(
            "dashscope-cn-team-a".to_owned(),
            ProviderExtensionConfig {
                source: ProviderExtensionSource::Extends {
                    provider: "dashscope-cn".to_owned(),
                },
                endpoint: None,
                credential_env: Some("TEAM_A_DASHSCOPE_API_KEY".to_owned()),
                models: BTreeMap::from([(
                    "qwen-new-model".to_owned(),
                    ProviderModelProfileConfig {
                        input: BTreeSet::from([ModelInputModality::Text]),
                    },
                )]),
                connect_timeout: Duration::from_secs(5),
                request_timeout: Duration::from_secs(120),
                transport: ProviderTransportPolicy::HttpsOnly,
            },
        )]),
    };
    let registry = load_model_registry_with_env(&providers, None, |name| {
        (name == "TEAM_A_DASHSCOPE_API_KEY").then(|| "team-key".to_owned())
    })
    .unwrap();

    let inherited = selector("dashscope-cn-team-a", "qwen3.6-flash");
    let added = selector("dashscope-cn-team-a", "qwen-new-model");
    assert!(registry.resolve(&inherited).is_ok());
    assert!(registry.resolve(&added).is_ok());
    assert_eq!(
        registry.deployment_identity(&added).unwrap().evidence()["credential_env"],
        "TEAM_A_DASHSCOPE_API_KEY"
    );
    assert_ne!(
        registry
            .deployment_identity(&inherited)
            .unwrap()
            .binding_hash(),
        load_builtin(Some("team-key"))
            .deployment_identity(&selector("dashscope-cn", "qwen3.6-flash"))
            .unwrap()
            .binding_hash()
    );

    let route_conflict = ProvidersConfig {
        extensions: BTreeMap::from([(
            "dashscope-cn".to_owned(),
            providers.extensions["dashscope-cn-team-a"].clone(),
        )]),
    };
    assert_eq!(
        load_model_registry_with_env(&route_conflict, None, |_| Some("key".to_owned()))
            .err()
            .unwrap()
            .code(),
        "PROVIDER_ROUTE_CONFLICT"
    );

    let mut model_override = providers.clone();
    model_override
        .extensions
        .get_mut("dashscope-cn-team-a")
        .unwrap()
        .models
        .insert(
            "qwen3.6-flash".to_owned(),
            ProviderModelProfileConfig {
                input: BTreeSet::from([ModelInputModality::Text]),
            },
        );
    assert_eq!(
        load_model_registry_with_env(&model_override, None, |_| Some("key".to_owned()))
            .err()
            .unwrap()
            .code(),
        "PROVIDER_MODEL_CONFLICT"
    );
}

#[test]
fn model_policy_only_narrows_existing_models() {
    let allowed = ModelSelectorConfig {
        provider: "dashscope-cn".to_owned(),
        id: "qwen3.6-flash".to_owned(),
    };
    let policy = ModelPolicyConfig {
        allow: BTreeSet::from([allowed]),
    };
    let registry = load_model_registry_with_env(&ProvidersConfig::default(), Some(&policy), |_| {
        Some("secret".to_owned())
    })
    .unwrap();

    assert_eq!(registry.selectors().count(), 1);
    assert!(registry
        .resolve(&selector("dashscope-cn", "qwen3.6-flash"))
        .is_ok());
    assert_eq!(
        registry
            .resolve(&selector("dashscope-cn", "qwen-vl-plus"))
            .unwrap_err()
            .code(),
        "MODEL_NOT_FOUND"
    );

    let invalid = ModelPolicyConfig {
        allow: BTreeSet::from([ModelSelectorConfig {
            provider: "dashscope-cn".to_owned(),
            id: "missing".to_owned(),
        }]),
    };
    let error = load_model_registry_with_env(&ProvidersConfig::default(), Some(&invalid), |_| None)
        .err()
        .unwrap();
    assert_eq!(error.code(), "MODEL_POLICY_MODEL_NOT_FOUND");
}
