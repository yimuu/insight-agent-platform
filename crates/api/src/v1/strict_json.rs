use std::{collections::BTreeSet, fmt};

use axum::{
    body::{to_bytes, Body},
    http::Request,
};
use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};

pub(crate) async fn validate_request(
    request: Request<Body>,
    max_bytes: usize,
) -> Result<Request<Body>, ()> {
    let (parts, body) = request.into_parts();
    let bytes = to_bytes(body, max_bytes).await.map_err(|_| ())?;
    if parts.method == axum::http::Method::DELETE {
        if !bytes.is_empty() {
            return Err(());
        }
    } else if !bytes.is_empty() {
        validate_bytes(&bytes)?;
    }
    Ok(Request::from_parts(parts, Body::from(bytes)))
}

fn validate_bytes(bytes: &[u8]) -> Result<(), ()> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let root = StrictJson::deserialize(&mut deserializer).map_err(|_| ())?;
    deserializer.end().map_err(|_| ())?;
    root.object.then_some(()).ok_or(())
}

struct StrictJson {
    object: bool,
}

impl<'de> Deserialize<'de> for StrictJson {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(StrictJsonVisitor)
    }
}

struct StrictJsonVisitor;

impl<'de> Visitor<'de> for StrictJsonVisitor {
    type Value = StrictJson;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("strict JSON")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut keys = BTreeSet::new();
        while let Some(key) = map.next_key::<String>()? {
            if !keys.insert(key) {
                return Err(de::Error::custom("duplicate JSON object key"));
            }
            let _: StrictJson = map.next_value()?;
        }
        Ok(StrictJson { object: true })
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while sequence.next_element::<StrictJson>()?.is_some() {}
        Ok(StrictJson { object: false })
    }

    fn visit_bool<E>(self, _: bool) -> Result<Self::Value, E> {
        Ok(StrictJson { object: false })
    }

    fn visit_i64<E>(self, _: i64) -> Result<Self::Value, E> {
        Ok(StrictJson { object: false })
    }

    fn visit_u64<E>(self, _: u64) -> Result<Self::Value, E> {
        Ok(StrictJson { object: false })
    }

    fn visit_f64<E>(self, _: f64) -> Result<Self::Value, E> {
        Ok(StrictJson { object: false })
    }

    fn visit_str<E>(self, _: &str) -> Result<Self::Value, E> {
        Ok(StrictJson { object: false })
    }

    fn visit_string<E>(self, _: String) -> Result<Self::Value, E> {
        Ok(StrictJson { object: false })
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(StrictJson { object: false })
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(StrictJson { object: false })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_duplicate_keys_at_every_depth_and_trailing_documents() {
        assert!(validate_bytes(br#"{"a":1,"nested":{"b":2}}"#).is_ok());
        assert!(validate_bytes(br#"{"a":1,"a":2}"#).is_err());
        assert!(validate_bytes(br#"{"nested":{"b":1,"b":2}}"#).is_err());
        assert!(validate_bytes(br#"{"a":1} {"b":2}"#).is_err());
        assert!(validate_bytes(br#"[1,2,3]"#).is_err());
    }
}
