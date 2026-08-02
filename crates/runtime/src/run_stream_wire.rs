//! Closed private wire for live-only Run Stream bus adapters.
//!
//! This codec carries only already-authorized public observations. It has no
//! replay identity and never contains the durable terminal Run snapshot.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use insight_engine::run_stream::adapter::{publication_from_source, publication_payload};
use insight_engine::run_stream::{
    LiveRunObservationIdentity, LiveRunStreamDelivery, LiveRunStreamGap, LiveRunStreamItemIdentity,
    LiveRunStreamPayload, LiveRunStreamPublication, LiveRunStreamSeal, LiveRunStreamSealStatus,
    LiveRunStreamSourceIdentity, RunOutputContentPart, RunOutputItem, RunPublicError,
    RunRetrievalResult, RunToolContent, RunToolProgressContent,
};
use insight_engine::{ActivationId, AttemptNo, RunId};

pub(crate) const LIVE_RUN_STREAM_BUS_WIRE_VERSION: u8 = 1;
pub(crate) const LIVE_RUN_STREAM_BUS_MAX_FRAME_BYTES: usize = 64 * 1_024;

#[derive(Debug, Clone, Copy)]
pub(crate) struct WireError;

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum LiveRunStreamBusWireRef<'a> {
    Publication {
        schema_version: u8,
        source: WireSourceRef<'a>,
        local_sequence: u64,
        payload: WirePayloadRef<'a>,
    },
    Gap {
        schema_version: u8,
        source: WireSourceRef<'a>,
        missing_from: u64,
        missing_to: Option<u64>,
        unknown_tail: bool,
    },
    Seal {
        schema_version: u8,
        source: WireSourceRef<'a>,
        last_local_sequence: Option<u64>,
        status: WireSealStatus,
    },
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum LiveRunStreamBusWire {
    Publication {
        schema_version: u8,
        source: WireSource,
        local_sequence: u64,
        payload: WirePayload,
    },
    Gap {
        schema_version: u8,
        source: WireSource,
        missing_from: u64,
        missing_to: Option<u64>,
        unknown_tail: bool,
    },
    Seal {
        schema_version: u8,
        source: WireSource,
        last_local_sequence: Option<u64>,
        status: WireSealStatus,
    },
}

pub(crate) fn decode(
    encoded: &str,
    expected_run_id: &RunId,
    max_frame_bytes: usize,
) -> Result<LiveRunStreamDelivery, WireError> {
    LiveRunStreamBusWire::decode(encoded, expected_run_id, max_frame_bytes)
}

impl LiveRunStreamBusWire {
    pub(crate) fn decode(
        encoded: &str,
        expected_run_id: &RunId,
        max_frame_bytes: usize,
    ) -> Result<LiveRunStreamDelivery, WireError> {
        if encoded.len() > max_frame_bytes || encoded.len() > LIVE_RUN_STREAM_BUS_MAX_FRAME_BYTES {
            return Err(WireError);
        }
        let wire: Self = serde_json::from_str(encoded).map_err(|_| WireError)?;
        match wire {
            Self::Publication {
                schema_version,
                source,
                local_sequence,
                payload,
            } => {
                validate_wire_version(schema_version)?;
                let source = source.into_live(expected_run_id)?;
                publication_from_source(source, local_sequence, payload.into_live())
                    .map(LiveRunStreamDelivery::Publication)
                    .map_err(|_| WireError)
            }
            Self::Gap {
                schema_version,
                source,
                missing_from,
                missing_to,
                unknown_tail,
            } => {
                validate_wire_version(schema_version)?;
                let identity = source.into_output_item(expected_run_id)?;
                let gap = match (missing_to, unknown_tail) {
                    (Some(missing_to), false) => {
                        LiveRunStreamGap::known(identity, missing_from, missing_to)
                            .map_err(|_| WireError)?
                    }
                    (None, true) => LiveRunStreamGap::unknown_tail(identity, missing_from),
                    (Some(_), true) | (None, false) => return Err(WireError),
                };
                Ok(LiveRunStreamDelivery::Gap(gap))
            }
            Self::Seal {
                schema_version,
                source,
                last_local_sequence,
                status,
            } => {
                validate_wire_version(schema_version)?;
                let identity = source.into_output_item(expected_run_id)?;
                let status = match status {
                    WireSealStatus::Completed => LiveRunStreamSealStatus::Completed,
                    WireSealStatus::Incomplete => LiveRunStreamSealStatus::Incomplete,
                };
                Ok(LiveRunStreamDelivery::Seal(LiveRunStreamSeal::new(
                    identity,
                    last_local_sequence,
                    status,
                )))
            }
        }
    }
}

pub(crate) fn encode_publication(
    publication: &LiveRunStreamPublication,
    max_frame_bytes: usize,
) -> Result<String, WireError> {
    encode_wire(
        &LiveRunStreamBusWireRef::Publication {
            schema_version: LIVE_RUN_STREAM_BUS_WIRE_VERSION,
            source: WireSourceRef::from_live(publication.source()),
            local_sequence: publication.local_sequence(),
            payload: WirePayloadRef::from_live(publication_payload(publication)),
        },
        max_frame_bytes,
    )
}

pub(crate) fn encode_gap(
    gap: &LiveRunStreamGap,
    max_frame_bytes: usize,
) -> Result<String, WireError> {
    encode_wire(
        &LiveRunStreamBusWireRef::Gap {
            schema_version: LIVE_RUN_STREAM_BUS_WIRE_VERSION,
            source: WireSourceRef::OutputItem {
                identity: WireOutputItemIdentityRef::from_live(gap.identity()),
            },
            missing_from: gap.missing_from(),
            missing_to: gap.missing_to(),
            unknown_tail: gap.has_unknown_tail(),
        },
        max_frame_bytes,
    )
}

pub(crate) fn encode_seal(
    seal: &LiveRunStreamSeal,
    max_frame_bytes: usize,
) -> Result<String, WireError> {
    let status = match seal.status() {
        LiveRunStreamSealStatus::Completed => WireSealStatus::Completed,
        LiveRunStreamSealStatus::Incomplete => WireSealStatus::Incomplete,
    };
    encode_wire(
        &LiveRunStreamBusWireRef::Seal {
            schema_version: LIVE_RUN_STREAM_BUS_WIRE_VERSION,
            source: WireSourceRef::OutputItem {
                identity: WireOutputItemIdentityRef::from_live(seal.identity()),
            },
            last_local_sequence: seal.last_local_sequence(),
            status,
        },
        max_frame_bytes,
    )
}

fn encode_wire(
    wire: &LiveRunStreamBusWireRef<'_>,
    max_frame_bytes: usize,
) -> Result<String, WireError> {
    if max_frame_bytes == 0 || max_frame_bytes > LIVE_RUN_STREAM_BUS_MAX_FRAME_BYTES {
        return Err(WireError);
    }
    let mut buffer = BoundedWireBuffer::new(max_frame_bytes);
    serde_json::to_writer(&mut buffer, wire).map_err(|_| WireError)?;
    String::from_utf8(buffer.into_bytes()).map_err(|_| WireError)
}

struct BoundedWireBuffer {
    bytes: Vec<u8>,
    limit: usize,
}

impl BoundedWireBuffer {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit),
            limit,
        }
    }

    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl std::io::Write for BoundedWireBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other(
                "live Run stream bus wire limit exceeded",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn validate_wire_version(schema_version: u8) -> Result<(), WireError> {
    (schema_version == LIVE_RUN_STREAM_BUS_WIRE_VERSION)
        .then_some(())
        .ok_or(WireError)
}

#[derive(Serialize)]
#[serde(tag = "source_kind", rename_all = "snake_case")]
enum WireSourceRef<'a> {
    OutputItem {
        #[serde(flatten)]
        identity: WireOutputItemIdentityRef<'a>,
    },
    RunObservation {
        #[serde(flatten)]
        identity: WireRunObservationIdentityRef<'a>,
    },
}

impl<'a> WireSourceRef<'a> {
    fn from_live(source: &'a LiveRunStreamSourceIdentity) -> Self {
        match source {
            LiveRunStreamSourceIdentity::OutputItem(identity) => Self::OutputItem {
                identity: WireOutputItemIdentityRef::from_live(identity),
            },
            LiveRunStreamSourceIdentity::RunObservation(identity) => Self::RunObservation {
                identity: WireRunObservationIdentityRef::from_live(identity),
            },
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "source_kind", rename_all = "snake_case", deny_unknown_fields)]
enum WireSource {
    OutputItem {
        run_id: RunId,
        activation_id: ActivationId,
        attempt_no: AttemptNo,
        model_call_no: u32,
        item_id: String,
        output_index: u32,
    },
    RunObservation {
        run_id: RunId,
        activation_id: ActivationId,
        attempt_no: AttemptNo,
        source_id: String,
    },
}

impl WireSource {
    fn into_live(self, expected_run_id: &RunId) -> Result<LiveRunStreamSourceIdentity, WireError> {
        match self {
            Self::OutputItem {
                run_id,
                activation_id,
                attempt_no,
                model_call_no,
                item_id,
                output_index,
            } => {
                if &run_id != expected_run_id {
                    return Err(WireError);
                }
                LiveRunStreamItemIdentity::new(
                    run_id,
                    activation_id,
                    attempt_no,
                    model_call_no,
                    item_id,
                    output_index,
                )
                .map(LiveRunStreamSourceIdentity::OutputItem)
                .map_err(|_| WireError)
            }
            Self::RunObservation {
                run_id,
                activation_id,
                attempt_no,
                source_id,
            } => {
                if &run_id != expected_run_id {
                    return Err(WireError);
                }
                LiveRunObservationIdentity::new(run_id, activation_id, attempt_no, source_id)
                    .map(LiveRunStreamSourceIdentity::RunObservation)
                    .map_err(|_| WireError)
            }
        }
    }

    fn into_output_item(
        self,
        expected_run_id: &RunId,
    ) -> Result<LiveRunStreamItemIdentity, WireError> {
        match self.into_live(expected_run_id)? {
            LiveRunStreamSourceIdentity::OutputItem(identity) => Ok(identity),
            LiveRunStreamSourceIdentity::RunObservation(_) => Err(WireError),
        }
    }
}

#[derive(Serialize)]
struct WireOutputItemIdentityRef<'a> {
    run_id: &'a RunId,
    activation_id: &'a ActivationId,
    attempt_no: AttemptNo,
    model_call_no: u32,
    item_id: &'a str,
    output_index: u32,
}

impl<'a> WireOutputItemIdentityRef<'a> {
    fn from_live(identity: &'a LiveRunStreamItemIdentity) -> Self {
        Self {
            run_id: identity.run_id(),
            activation_id: identity.activation_id(),
            attempt_no: identity.attempt_no(),
            model_call_no: identity.model_call_no(),
            item_id: identity.item_id(),
            output_index: identity.output_index(),
        }
    }
}

#[derive(Serialize)]
struct WireRunObservationIdentityRef<'a> {
    run_id: &'a RunId,
    activation_id: &'a ActivationId,
    attempt_no: AttemptNo,
    source_id: &'a str,
}

impl<'a> WireRunObservationIdentityRef<'a> {
    fn from_live(identity: &'a LiveRunObservationIdentity) -> Self {
        Self {
            run_id: identity.run_id(),
            activation_id: identity.activation_id(),
            attempt_no: identity.attempt_no(),
            source_id: identity.source_id(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
enum WireSealStatus {
    Completed,
    Incomplete,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum WirePayloadRef<'a> {
    OutputItemAdded {
        item: &'a RunOutputItem,
    },
    ContentPartAdded {
        content_index: u32,
        part: &'a RunOutputContentPart,
    },
    OutputTextDelta {
        content_index: u32,
        delta: &'a str,
    },
    OutputTextDone {
        content_index: u32,
        text: &'a str,
    },
    ContentPartDone {
        content_index: u32,
        part: &'a RunOutputContentPart,
    },
    FunctionCallArgumentsDelta {
        delta: &'a str,
    },
    FunctionCallArgumentsDone {
        name: &'a str,
        arguments: &'a str,
    },
    OutputItemDone {
        item: &'a RunOutputItem,
    },
    FileSearchCallInProgress,
    FileSearchCallSearching,
    FileSearchCallCompleted,
    ToolStarted {
        call_id: &'a str,
        tool_name: &'a str,
        arguments: &'a Option<Value>,
    },
    ToolProgress {
        call_id: &'a str,
        tool_name: &'a str,
        content: &'a [RunToolProgressContent],
    },
    ToolCompleted {
        call_id: &'a str,
        tool_name: &'a str,
        duration_ms: u64,
        content: &'a [RunToolContent],
    },
    ToolFailed {
        call_id: &'a str,
        tool_name: &'a str,
        duration_ms: u64,
        error: &'a RunPublicError,
    },
    RetrievalCompleted {
        retrieval_id: &'a str,
        query: &'a Option<String>,
        results: &'a [RunRetrievalResult],
    },
}

impl<'a> WirePayloadRef<'a> {
    fn from_live(payload: &'a LiveRunStreamPayload) -> Self {
        match payload {
            LiveRunStreamPayload::OutputItemAdded { item } => Self::OutputItemAdded { item },
            LiveRunStreamPayload::ContentPartAdded {
                content_index,
                part,
            } => Self::ContentPartAdded {
                content_index: *content_index,
                part,
            },
            LiveRunStreamPayload::OutputTextDelta {
                content_index,
                delta,
            } => Self::OutputTextDelta {
                content_index: *content_index,
                delta,
            },
            LiveRunStreamPayload::OutputTextDone {
                content_index,
                text,
            } => Self::OutputTextDone {
                content_index: *content_index,
                text,
            },
            LiveRunStreamPayload::ContentPartDone {
                content_index,
                part,
            } => Self::ContentPartDone {
                content_index: *content_index,
                part,
            },
            LiveRunStreamPayload::FunctionCallArgumentsDelta { delta } => {
                Self::FunctionCallArgumentsDelta { delta }
            }
            LiveRunStreamPayload::FunctionCallArgumentsDone { name, arguments } => {
                Self::FunctionCallArgumentsDone { name, arguments }
            }
            LiveRunStreamPayload::OutputItemDone { item } => Self::OutputItemDone { item },
            LiveRunStreamPayload::FileSearchCallInProgress => Self::FileSearchCallInProgress,
            LiveRunStreamPayload::FileSearchCallSearching => Self::FileSearchCallSearching,
            LiveRunStreamPayload::FileSearchCallCompleted => Self::FileSearchCallCompleted,
            LiveRunStreamPayload::ToolStarted {
                call_id,
                tool_name,
                arguments,
            } => Self::ToolStarted {
                call_id,
                tool_name,
                arguments,
            },
            LiveRunStreamPayload::ToolProgress {
                call_id,
                tool_name,
                content,
            } => Self::ToolProgress {
                call_id,
                tool_name,
                content,
            },
            LiveRunStreamPayload::ToolCompleted {
                call_id,
                tool_name,
                duration_ms,
                content,
            } => Self::ToolCompleted {
                call_id,
                tool_name,
                duration_ms: *duration_ms,
                content,
            },
            LiveRunStreamPayload::ToolFailed {
                call_id,
                tool_name,
                duration_ms,
                error,
            } => Self::ToolFailed {
                call_id,
                tool_name,
                duration_ms: *duration_ms,
                error,
            },
            LiveRunStreamPayload::RetrievalCompleted {
                retrieval_id,
                query,
                results,
            } => Self::RetrievalCompleted {
                retrieval_id,
                query,
                results,
            },
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum WirePayload {
    OutputItemAdded {
        item: RunOutputItem,
    },
    ContentPartAdded {
        content_index: u32,
        part: RunOutputContentPart,
    },
    OutputTextDelta {
        content_index: u32,
        delta: String,
    },
    OutputTextDone {
        content_index: u32,
        text: String,
    },
    ContentPartDone {
        content_index: u32,
        part: RunOutputContentPart,
    },
    FunctionCallArgumentsDelta {
        delta: String,
    },
    FunctionCallArgumentsDone {
        name: String,
        arguments: String,
    },
    OutputItemDone {
        item: RunOutputItem,
    },
    FileSearchCallInProgress,
    FileSearchCallSearching,
    FileSearchCallCompleted,
    ToolStarted {
        call_id: String,
        tool_name: String,
        arguments: Option<Value>,
    },
    ToolProgress {
        call_id: String,
        tool_name: String,
        content: Vec<RunToolProgressContent>,
    },
    ToolCompleted {
        call_id: String,
        tool_name: String,
        duration_ms: u64,
        content: Vec<RunToolContent>,
    },
    ToolFailed {
        call_id: String,
        tool_name: String,
        duration_ms: u64,
        error: RunPublicError,
    },
    RetrievalCompleted {
        retrieval_id: String,
        query: Option<String>,
        results: Vec<RunRetrievalResult>,
    },
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::*;

    fn run(value: &str) -> RunId {
        RunId::new(value).unwrap()
    }

    fn output_identity(run_id: RunId) -> LiveRunStreamItemIdentity {
        LiveRunStreamItemIdentity::new(
            run_id,
            ActivationId::new("activation_wire_test").unwrap(),
            AttemptNo::FIRST,
            1,
            "item_wire_test",
            0,
        )
        .unwrap()
    }

    fn observation_identity(run_id: RunId) -> LiveRunObservationIdentity {
        LiveRunObservationIdentity::new(
            run_id,
            ActivationId::new("activation_wire_observation").unwrap(),
            AttemptNo::FIRST,
            "tool_source_wire_test",
        )
        .unwrap()
    }

    #[test]
    fn private_wire_round_trips_both_source_classes_and_controls() {
        let run_id = run("run_wire_round_trip");
        let output = LiveRunStreamPublication::new(
            output_identity(run_id.clone()),
            7,
            LiveRunStreamPayload::OutputTextDelta {
                content_index: 0,
                delta: "public delta".to_owned(),
            },
        )
        .unwrap();
        let encoded = encode_publication(&output, 4 * 1_024).unwrap();
        let LiveRunStreamDelivery::Publication(decoded) =
            decode(&encoded, &run_id, 4 * 1_024).unwrap()
        else {
            panic!("output publication must round trip")
        };
        assert_eq!(decoded, output);

        let observation = LiveRunStreamPublication::new_run_observation(
            observation_identity(run_id.clone()),
            3,
            LiveRunStreamPayload::ToolStarted {
                call_id: "call_wire".to_owned(),
                tool_name: "lookup".to_owned(),
                arguments: Some(json!({"public": true})),
            },
        )
        .unwrap();
        let encoded = encode_publication(&observation, 4 * 1_024).unwrap();
        let LiveRunStreamDelivery::Publication(decoded) =
            decode(&encoded, &run_id, 4 * 1_024).unwrap()
        else {
            panic!("run observation must round trip")
        };
        assert_eq!(decoded, observation);

        let identity = output_identity(run_id.clone());
        let gap = LiveRunStreamGap::known(identity.clone(), 4, 6).unwrap();
        let LiveRunStreamDelivery::Gap(decoded_gap) =
            decode(&encode_gap(&gap, 4 * 1_024).unwrap(), &run_id, 4 * 1_024).unwrap()
        else {
            panic!("gap must round trip")
        };
        assert_eq!(decoded_gap, gap);
        let seal = LiveRunStreamSeal::new(identity, Some(7), LiveRunStreamSealStatus::Completed);
        let LiveRunStreamDelivery::Seal(decoded_seal) =
            decode(&encode_seal(&seal, 4 * 1_024).unwrap(), &run_id, 4 * 1_024).unwrap()
        else {
            panic!("seal must round trip")
        };
        assert_eq!(decoded_seal, seal);
    }

    #[test]
    fn private_wire_is_closed_versioned_run_scoped_and_bounded() {
        let run_id = run("run_wire_closed");
        let publication = LiveRunStreamPublication::new(
            output_identity(run_id.clone()),
            0,
            LiveRunStreamPayload::OutputTextDelta {
                content_index: 0,
                delta: "answer".to_owned(),
            },
        )
        .unwrap();
        let encoded = encode_publication(&publication, 4 * 1_024).unwrap();

        let mut unknown: Value = serde_json::from_str(&encoded).unwrap();
        unknown["unexpected"] = json!(true);
        assert!(decode(
            &serde_json::to_string(&unknown).unwrap(),
            &run_id,
            4 * 1_024
        )
        .is_err());

        let mut future: Value = serde_json::from_str(&encoded).unwrap();
        future["schema_version"] = json!(LIVE_RUN_STREAM_BUS_WIRE_VERSION + 1);
        assert!(decode(&serde_json::to_string(&future).unwrap(), &run_id, 4 * 1_024).is_err());
        assert!(decode(&encoded, &run("run_wire_other"), 4 * 1_024).is_err());
        assert!(decode(&encoded, &run_id, encoded.len() - 1).is_err());
        assert!(encode_publication(&publication, encoded.len() - 1).is_err());
        assert!(encode_publication(&publication, LIVE_RUN_STREAM_BUS_MAX_FRAME_BYTES + 1).is_err());
    }

    #[test]
    fn private_wire_rejects_source_payload_and_control_mismatches() {
        let run_id = run("run_wire_mismatch");
        let publication = LiveRunStreamPublication::new(
            output_identity(run_id.clone()),
            0,
            LiveRunStreamPayload::OutputTextDelta {
                content_index: 0,
                delta: "answer".to_owned(),
            },
        )
        .unwrap();
        let encoded = encode_publication(&publication, 4 * 1_024).unwrap();
        let mut wrong_payload: Value = serde_json::from_str(&encoded).unwrap();
        wrong_payload["payload"] = json!({
            "type": "tool_started",
            "call_id": "call_wrong",
            "tool_name": "lookup",
            "arguments": null
        });
        assert!(decode(
            &serde_json::to_string(&wrong_payload).unwrap(),
            &run_id,
            4 * 1_024
        )
        .is_err());

        let seal = LiveRunStreamSeal::new(
            output_identity(run_id.clone()),
            None,
            LiveRunStreamSealStatus::Completed,
        );
        let mut wrong_control: Value =
            serde_json::from_str(&encode_seal(&seal, 4 * 1_024).unwrap()).unwrap();
        wrong_control["source"] = json!({
            "source_kind": "run_observation",
            "run_id": run_id,
            "activation_id": "activation_wire_observation",
            "attempt_no": 1,
            "source_id": "tool_source_wire_test"
        });
        assert!(decode(
            &serde_json::to_string(&wrong_control).unwrap(),
            &run("run_wire_mismatch"),
            4 * 1_024
        )
        .is_err());
    }
}

impl WirePayload {
    fn into_live(self) -> LiveRunStreamPayload {
        match self {
            Self::OutputItemAdded { item } => LiveRunStreamPayload::OutputItemAdded { item },
            Self::ContentPartAdded {
                content_index,
                part,
            } => LiveRunStreamPayload::ContentPartAdded {
                content_index,
                part,
            },
            Self::OutputTextDelta {
                content_index,
                delta,
            } => LiveRunStreamPayload::OutputTextDelta {
                content_index,
                delta,
            },
            Self::OutputTextDone {
                content_index,
                text,
            } => LiveRunStreamPayload::OutputTextDone {
                content_index,
                text,
            },
            Self::ContentPartDone {
                content_index,
                part,
            } => LiveRunStreamPayload::ContentPartDone {
                content_index,
                part,
            },
            Self::FunctionCallArgumentsDelta { delta } => {
                LiveRunStreamPayload::FunctionCallArgumentsDelta { delta }
            }
            Self::FunctionCallArgumentsDone { name, arguments } => {
                LiveRunStreamPayload::FunctionCallArgumentsDone { name, arguments }
            }
            Self::OutputItemDone { item } => LiveRunStreamPayload::OutputItemDone { item },
            Self::FileSearchCallInProgress => LiveRunStreamPayload::FileSearchCallInProgress,
            Self::FileSearchCallSearching => LiveRunStreamPayload::FileSearchCallSearching,
            Self::FileSearchCallCompleted => LiveRunStreamPayload::FileSearchCallCompleted,
            Self::ToolStarted {
                call_id,
                tool_name,
                arguments,
            } => LiveRunStreamPayload::ToolStarted {
                call_id,
                tool_name,
                arguments,
            },
            Self::ToolProgress {
                call_id,
                tool_name,
                content,
            } => LiveRunStreamPayload::ToolProgress {
                call_id,
                tool_name,
                content,
            },
            Self::ToolCompleted {
                call_id,
                tool_name,
                duration_ms,
                content,
            } => LiveRunStreamPayload::ToolCompleted {
                call_id,
                tool_name,
                duration_ms,
                content,
            },
            Self::ToolFailed {
                call_id,
                tool_name,
                duration_ms,
                error,
            } => LiveRunStreamPayload::ToolFailed {
                call_id,
                tool_name,
                duration_ms,
                error,
            },
            Self::RetrievalCompleted {
                retrieval_id,
                query,
                results,
            } => LiveRunStreamPayload::RetrievalCompleted {
                retrieval_id,
                query,
                results,
            },
        }
    }
}
