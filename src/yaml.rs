use serde::{de::DeserializeOwned, Serialize};

#[derive(Debug, Clone, Copy)]
pub(crate) enum YamlSurface {
    Platform,
    ProviderCatalog,
}

impl YamlSurface {
    fn label(self) -> &'static str {
        match self {
            Self::Platform => "platform YAML",
            Self::ProviderCatalog => "provider catalog YAML",
        }
    }
}

pub(crate) fn from_str<T>(source: &str, surface: YamlSurface) -> Result<T, String>
where
    T: DeserializeOwned,
{
    yaml_serde::from_str(source)
        .map_err(|error| format!("{} parse failed: {error}", surface.label()))
}

#[allow(dead_code)]
pub(crate) fn to_value<T>(value: T) -> Result<yaml_serde::Value, String>
where
    T: Serialize,
{
    yaml_serde::to_value(value).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{from_str, YamlSurface};
    use serde::Deserialize;

    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    struct SingleDocument {
        value: u32,
    }

    #[test]
    fn rejects_multi_document_streams() {
        let error =
            from_str::<SingleDocument>("value: 1\n---\nvalue: 2\n", YamlSurface::ProviderCatalog)
                .unwrap_err();

        assert!(error.contains("provider catalog YAML"));
        assert!(error.contains("more than one document"));
    }

    #[test]
    fn maps_errors_through_surface_labels() {
        let error = from_str::<SingleDocument>("value: [", YamlSurface::Platform).unwrap_err();

        assert!(error.contains("platform YAML"));
    }
}
