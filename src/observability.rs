use std::time::{Duration, Instant};

pub(crate) fn json_size_bytes<T: serde::Serialize>(value: &T) -> usize {
    serde_json::to_vec(value).map_or(0, |bytes| bytes.len())
}

pub(crate) fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

pub(crate) fn elapsed_ms(started: Instant) -> u64 {
    duration_ms(started.elapsed())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;
    use serde_json::json;

    #[test]
    fn json_size_counts_serialized_bytes_without_exposing_body() {
        let value = json!({"secret":"body-free-secret","n":1});
        assert_eq!(
            json_size_bytes(&value),
            serde_json::to_vec(&value).unwrap().len()
        );
    }

    #[test]
    fn json_size_returns_zero_when_serialization_fails() {
        struct FailingValue;

        impl Serialize for FailingValue {
            fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                Err(serde::ser::Error::custom(
                    "intentional serialization failure",
                ))
            }
        }

        let value = FailingValue;
        assert_eq!(json_size_bytes(&value), 0);
    }

    #[test]
    fn duration_ms_saturates_to_u64_max() {
        let huge = Duration::from_millis(u64::MAX).saturating_add(Duration::from_millis(1));
        assert_eq!(duration_ms(huge), u64::MAX);
    }

    #[test]
    fn elapsed_ms_counts_elapsed_time_in_milliseconds() {
        let started = Instant::now()
            .checked_sub(Duration::from_millis(5))
            .expect("recent instant should be representable");
        assert!(elapsed_ms(started) >= 5);
    }
}
