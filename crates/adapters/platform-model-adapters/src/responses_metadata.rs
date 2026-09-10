use serde_json::Value;

/// Optional provider metadata never changes canonical requests or execution results.
pub(crate) fn valid_response_metadata(response: &Value) -> bool {
    response
        .get("completed_at")
        .is_none_or(|value| value.is_null() || value.as_u64().is_some())
        && ["frequency_penalty", "presence_penalty"]
            .iter()
            .all(|field| {
                response.get(field).is_none_or(|value| {
                    value.is_null()
                        || value.as_f64().is_some_and(|number| {
                            number.is_finite() && (-2.0..=2.0).contains(&number)
                        })
                })
            })
}
