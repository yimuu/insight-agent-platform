//! Explicit POST-query and credential operations. These are not arbitrary command exemptions.
use super::*;
use insight_platform_api::model_configuration::{
    CompileModelConfigurationRequestV1, DeclareModelConfigurationRequestV1,
};
use insight_platform_api::model_credentials::ImportModelCredentialResponseV1;
use insight_platform_contracts::{ModelCredentialOperationId, SensitiveModelApiKey};
use insight_platform_registry::model_configuration::{
    CompiledModelConfigurationV1, ModelConfigurationDeclarationV1,
};

impl PublicHttpClient {
    pub fn probe_model_connection(
        &self,
        request: &insight_platform_contracts::ModelConnectionProbeRequestV1,
    ) -> Result<insight_platform_contracts::ModelConnectionObservationV1, PublicClientError> {
        if !request.validate() {
            return Err(PublicClientError::InvalidConfiguration(
                "invalid model connection target",
            ));
        }
        let result: insight_platform_contracts::ModelConnectionObservationV1 =
            self.model_configuration_query("/v1/model-configuration:probe", request)?;
        if !result.validate() || result.model_deployment != request.model_deployment {
            return Err(PublicClientError::InvalidResponse(
                "model connection observation differs from its target".to_owned(),
            ));
        }
        Ok(result)
    }
    pub fn declare_model_configuration(
        &self,
        request: &DeclareModelConfigurationRequestV1,
    ) -> Result<ModelConfigurationDeclarationV1, PublicClientError> {
        self.model_configuration_query("/v1/model-configuration:declare", request)
    }
    pub fn compile_model_configuration(
        &self,
        request: &CompileModelConfigurationRequestV1,
    ) -> Result<CompiledModelConfigurationV1, PublicClientError> {
        self.model_configuration_query("/v1/model-configuration:compile", request)
    }
    fn model_configuration_query<T: Serialize, R: DeserializeOwned>(
        &self,
        path: &str,
        request: &T,
    ) -> Result<R, PublicClientError> {
        let body = serde_json::to_vec(request).map_err(|_| {
            PublicClientError::InvalidConfiguration("model configuration cannot be serialized")
        })?;
        if body.len() > 16_384 {
            return Err(PublicClientError::InvalidConfiguration(
                "model configuration exceeds its bound",
            ));
        }
        let response = self
            .client
            .post(format!("{}{}", self.base_url, path))
            .header(ACCEPT, JSON_CONTENT_TYPE)
            .header(CONTENT_TYPE, JSON_CONTENT_TYPE)
            .header(AUTHORIZATION, format!("Bearer {}", self.bearer_token))
            .body(body)
            .send()
            .map_err(|_| {
                PublicClientError::Transport("model configuration query failed".to_owned())
            })?;
        decode_body_response(response, StatusCode::OK).map(|result| result.body)
    }
    pub fn import_model_credential(
        &self,
        operation_id: &ModelCredentialOperationId,
        provider_id: &ResourceId,
        key: SensitiveModelApiKey,
    ) -> Result<ImportModelCredentialResponseV1, PublicClientError> {
        struct PrivateKey<'a>(&'a SensitiveModelApiKey);
        impl Serialize for PrivateKey<'_> {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(
                    std::str::from_utf8(self.0.expose())
                        .map_err(|_| serde::ser::Error::custom("invalid credential"))?,
                )
            }
        }
        #[derive(Serialize)]
        struct Request<'a> {
            schema_version: u16,
            operation_id: &'a ModelCredentialOperationId,
            provider_id: &'a ResourceId,
            api_key: PrivateKey<'a>,
        }
        // reqwest owns the reader while transmitting. Its Drop clears our serialized key buffer;
        // keys never enter a request URL, Receipt, CLI report or recovery journal.
        struct PrivateBody(std::io::Cursor<Vec<u8>>);
        impl std::io::Read for PrivateBody {
            fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
                self.0.read(bytes)
            }
        }
        impl Drop for PrivateBody {
            fn drop(&mut self) {
                self.0.get_mut().fill(0);
            }
        }
        if provider_id.kind() != ResourceKind::SecretProvider {
            return Err(PublicClientError::InvalidConfiguration(
                "credential provider kind",
            ));
        }
        let body = serde_json::to_vec(&Request {
            schema_version: 1,
            operation_id,
            provider_id,
            api_key: PrivateKey(&key),
        })
        .map_err(|_| PublicClientError::InvalidConfiguration("invalid credential input"))?;
        let length = body.len() as u64;
        let response = self
            .client
            .post(format!("{}/v1/model-credentials", self.base_url))
            .header(ACCEPT, JSON_CONTENT_TYPE)
            .header(CONTENT_TYPE, JSON_CONTENT_TYPE)
            .header(CONTENT_LENGTH, length)
            .header(AUTHORIZATION, format!("Bearer {}", self.bearer_token))
            .body(reqwest::blocking::Body::new(PrivateBody(
                std::io::Cursor::new(body),
            )))
            .send()
            .map_err(|_| {
                PublicClientError::Transport(
                    "credential import outcome may be unknown; retry the same operation and input"
                        .to_owned(),
                )
            })?;
        let response: PublicBodyResponse<ImportModelCredentialResponseV1> =
            decode_body_response(response, StatusCode::OK)?;
        if response.body.schema_version != 1
            || response.body.binding.validate().is_err()
            || &response.body.binding.provider_id != provider_id
            || response.body.binding.purpose.as_str()
                != insight_platform_contracts::MODEL_API_KEY_PURPOSE
        {
            return Err(PublicClientError::InvalidResponse(
                "credential import exact binding is invalid".to_owned(),
            ));
        }
        Ok(response.body)
    }
}
