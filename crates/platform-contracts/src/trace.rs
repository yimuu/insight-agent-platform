use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::{error::Error, fmt, str::FromStr};
use uuid::Uuid;

pub const TRACE_ID_HEX_LENGTH: usize = 32;
pub const SPAN_ID_HEX_LENGTH: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TraceId([u8; 16]);

impl TraceId {
    pub fn new() -> Self {
        Self(*Uuid::new_v4().as_bytes())
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Result<Self, TraceContractError> {
        if bytes == [0; 16] {
            return Err(TraceContractError::ZeroTraceId);
        }
        Ok(Self(bytes))
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl Default for TraceId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TraceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

impl FromStr for TraceId {
    type Err = TraceContractError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::from_bytes(parse_exact_lower_hex::<16>(
            value,
            TraceContractError::InvalidTraceId,
        )?)
    }
}

impl Serialize for TraceId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for TraceId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SpanId([u8; 8]);

impl SpanId {
    pub fn new() -> Self {
        let source = Uuid::new_v4();
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(&source.as_bytes()[..8]);
        // A v4 UUID cannot have an all-zero first half because its version bits are non-zero.
        Self(bytes)
    }

    pub fn from_bytes(bytes: [u8; 8]) -> Result<Self, TraceContractError> {
        if bytes == [0; 8] {
            return Err(TraceContractError::ZeroSpanId);
        }
        Ok(Self(bytes))
    }

    pub const fn as_bytes(&self) -> &[u8; 8] {
        &self.0
    }
}

impl Default for SpanId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SpanId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

impl FromStr for SpanId {
    type Err = TraceContractError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::from_bytes(parse_exact_lower_hex::<8>(
            value,
            TraceContractError::InvalidSpanId,
        )?)
    }
}

impl Serialize for SpanId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for SpanId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TraceFlags {
    NotSampled,
    Sampled,
}

impl TraceFlags {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotSampled => "00",
            Self::Sampled => "01",
        }
    }
}

impl fmt::Display for TraceFlags {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for TraceFlags {
    type Err = TraceContractError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "00" => Ok(Self::NotSampled),
            "01" => Ok(Self::Sampled),
            _ => Err(TraceContractError::InvalidTraceFlags),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct W3cTraceParent {
    pub trace_id: TraceId,
    pub parent_span_id: SpanId,
    pub flags: TraceFlags,
}

impl W3cTraceParent {
    pub const VERSION: &'static str = "00";

    pub const fn new(trace_id: TraceId, parent_span_id: SpanId, flags: TraceFlags) -> Self {
        Self {
            trace_id,
            parent_span_id,
            flags,
        }
    }

    pub fn child(self) -> Self {
        Self::new(self.trace_id, SpanId::new(), self.flags)
    }
}

impl fmt::Display for W3cTraceParent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}-{}-{}-{}",
            Self::VERSION,
            self.trace_id,
            self.parent_span_id,
            self.flags
        )
    }
}

impl FromStr for W3cTraceParent {
    type Err = TraceContractError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 55 {
            return Err(TraceContractError::InvalidTraceParent);
        }
        let mut components = value.split('-');
        let version = components.next();
        let trace_id = components.next();
        let parent_span_id = components.next();
        let flags = components.next();
        if version != Some(Self::VERSION) || components.next().is_some() {
            return Err(TraceContractError::InvalidTraceParent);
        }
        Ok(Self {
            trace_id: trace_id
                .ok_or(TraceContractError::InvalidTraceParent)?
                .parse()?,
            parent_span_id: parent_span_id
                .ok_or(TraceContractError::InvalidTraceParent)?
                .parse()?,
            flags: flags
                .ok_or(TraceContractError::InvalidTraceParent)?
                .parse()?,
        })
    }
}

impl Serialize for W3cTraceParent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for W3cTraceParent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraceIdentityV1 {
    pub schema_version: u32,
    pub trace_id: TraceId,
}

impl TraceIdentityV1 {
    pub const SCHEMA_VERSION: u32 = 1;

    pub const fn new(trace_id: TraceId) -> Self {
        Self {
            schema_version: Self::SCHEMA_VERSION,
            trace_id,
        }
    }

    pub fn generate() -> Self {
        Self::new(TraceId::new())
    }

    pub fn validate(&self) -> Result<(), TraceContractError> {
        if self.schema_version != Self::SCHEMA_VERSION {
            return Err(TraceContractError::UnsupportedSchemaVersion);
        }
        Ok(())
    }

    pub fn traceparent(&self, flags: TraceFlags) -> W3cTraceParent {
        W3cTraceParent::new(self.trace_id, SpanId::new(), flags)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceContractError {
    InvalidTraceId,
    ZeroTraceId,
    InvalidSpanId,
    ZeroSpanId,
    InvalidTraceFlags,
    InvalidTraceParent,
    UnsupportedSchemaVersion,
}

impl fmt::Display for TraceContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidTraceId => "trace ID must be exactly 32 lowercase hexadecimal digits",
            Self::ZeroTraceId => "trace ID must not be all zero",
            Self::InvalidSpanId => "span ID must be exactly 16 lowercase hexadecimal digits",
            Self::ZeroSpanId => "span ID must not be all zero",
            Self::InvalidTraceFlags => "trace flags must be exactly 00 or 01",
            Self::InvalidTraceParent => "traceparent must be the exact W3C version-00 form",
            Self::UnsupportedSchemaVersion => "trace identity schema_version must be exactly 1",
        })
    }
}

impl Error for TraceContractError {}

fn parse_exact_lower_hex<const N: usize>(
    value: &str,
    invalid: TraceContractError,
) -> Result<[u8; N], TraceContractError> {
    if value.len() != N * 2
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid);
    }
    let mut decoded = [0_u8; N];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        decoded[index] = (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]);
    }
    Ok(decoded)
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => unreachable!("input was validated as lowercase hexadecimal"),
    }
}

fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        formatter.write_str(
            std::str::from_utf8(&[HEX[(byte >> 4) as usize], HEX[(byte & 0x0f) as usize]])
                .expect("hexadecimal is UTF-8"),
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const TRACE: &str = "0af7651916cd43dd8448eb211c80319c";
    const SPAN: &str = "b7ad6b7169203331";

    #[test]
    fn exact_w3c_parent_round_trips_canonically() {
        let wire = format!("00-{TRACE}-{SPAN}-01");
        let parent = wire.parse::<W3cTraceParent>().unwrap();
        assert_eq!(parent.to_string(), wire);
        assert_eq!(parent.trace_id.to_string(), TRACE);
        assert_eq!(parent.parent_span_id.to_string(), SPAN);
        assert_eq!(parent.flags, TraceFlags::Sampled);
        assert_ne!(parent.child().parent_span_id, parent.parent_span_id);
    }

    #[test]
    fn malformed_or_extensible_parents_fail_closed() {
        for value in [
            format!("01-{TRACE}-{SPAN}-01"),
            format!("00-{}-{SPAN}-01", TRACE.to_uppercase()),
            format!("00-{TRACE}-0000000000000000-01"),
            format!("00-00000000000000000000000000000000-{SPAN}-01"),
            format!("00-{TRACE}-{SPAN}-03"),
            format!("00-{TRACE}-{SPAN}-01-extra"),
        ] {
            assert!(value.parse::<W3cTraceParent>().is_err(), "{value}");
        }
    }

    #[test]
    fn trace_identity_is_closed_and_versioned() {
        let identity = TraceIdentityV1::new(TRACE.parse().unwrap());
        assert!(identity.validate().is_ok());
        assert_eq!(
            serde_json::to_string(&identity).unwrap(),
            format!(r#"{{"schema_version":1,"trace_id":"{TRACE}"}}"#)
        );
        assert!(serde_json::from_str::<TraceIdentityV1>(&format!(
            r#"{{"schema_version":1,"trace_id":"{TRACE}","tenant_id":"forged"}}"#
        ))
        .is_err());
        let unsupported = serde_json::from_str::<TraceIdentityV1>(&format!(
            r#"{{"schema_version":2,"trace_id":"{TRACE}"}}"#
        ))
        .unwrap();
        assert_eq!(
            unsupported.validate(),
            Err(TraceContractError::UnsupportedSchemaVersion)
        );
    }

    #[test]
    fn generated_identifiers_are_nonzero_and_canonical() {
        let trace_id = TraceId::new();
        let span_id = SpanId::new();
        assert_eq!(trace_id.to_string().len(), TRACE_ID_HEX_LENGTH);
        assert_eq!(span_id.to_string().len(), SPAN_ID_HEX_LENGTH);
        assert_ne!(trace_id.as_bytes(), &[0; 16]);
        assert_ne!(span_id.as_bytes(), &[0; 8]);
    }
}
