use super::{permanent, ModelAdapterFailure, ModelProviderWireEvent, ModelProviderWireStream};
use futures::{stream, stream::BoxStream, StreamExt};
use insight_platform_contracts::{parse_strict_json, JsonLimits};
use std::collections::VecDeque;

/// Raw response-body chunks returned by the role-scoped HTTP/Egress implementation.
pub type ModelProviderByteStream = BoxStream<'static, Result<Vec<u8>, ModelAdapterFailure>>;

/// Incremental, bounded SSE decoder shared by all Provider HTTP connectors.
///
/// It rejects ambiguous SSE fields and parses every data payload with Platform strict JSON rules,
/// including duplicate-key rejection, before constructing a [`ModelProviderWireEvent`].
pub struct ModelProviderSseDecoder {
    maximum_response_bytes: usize,
    observed_response_bytes: usize,
    buffer: Vec<u8>,
    event_name: Option<String>,
    data: Vec<u8>,
    done_marker: bool,
}

impl ModelProviderSseDecoder {
    pub fn new(maximum_response_bytes: u32) -> Result<Self, ModelAdapterFailure> {
        let maximum_response_bytes = usize::try_from(maximum_response_bytes)
            .map_err(|_| permanent("model_sse_invalid_limit"))?;
        if maximum_response_bytes == 0 {
            return Err(permanent("model_sse_invalid_limit"));
        }
        Ok(Self {
            maximum_response_bytes,
            observed_response_bytes: 0,
            buffer: Vec::new(),
            event_name: None,
            data: Vec::new(),
            done_marker: false,
        })
    }

    pub fn push(
        &mut self,
        chunk: &[u8],
    ) -> Result<Vec<ModelProviderWireEvent>, ModelAdapterFailure> {
        if self.done_marker && !chunk.is_empty() {
            return Err(permanent("model_sse_bytes_after_done"));
        }
        self.observed_response_bytes = self
            .observed_response_bytes
            .checked_add(chunk.len())
            .filter(|total| *total <= self.maximum_response_bytes)
            .ok_or_else(|| permanent("model_sse_response_too_large"))?;
        if self
            .buffer
            .len()
            .checked_add(chunk.len())
            .is_none_or(|total| total > self.maximum_response_bytes)
        {
            return Err(permanent("model_sse_line_too_large"));
        }
        self.buffer.extend_from_slice(chunk);
        self.consume_complete_lines()
    }

    pub fn finish(&mut self) -> Result<Vec<ModelProviderWireEvent>, ModelAdapterFailure> {
        if self.done_marker {
            if self.buffer.is_empty() && self.event_name.is_none() && self.data.is_empty() {
                return Ok(Vec::new());
            }
            return Err(permanent("model_sse_invalid_done"));
        }
        let mut events = self.consume_complete_lines()?;
        if !self.buffer.is_empty() {
            let line = std::mem::take(&mut self.buffer);
            self.consume_line(&line, &mut events)?;
        }
        if self.event_name.is_some() || !self.data.is_empty() {
            if let Some(event) = self.dispatch_event()? {
                events.push(event);
            }
        }
        Ok(events)
    }

    pub const fn observed_response_bytes(&self) -> usize {
        self.observed_response_bytes
    }

    fn consume_complete_lines(
        &mut self,
    ) -> Result<Vec<ModelProviderWireEvent>, ModelAdapterFailure> {
        let mut events = Vec::new();
        while let Some(newline) = self.buffer.iter().position(|byte| *byte == b'\n') {
            if newline > self.maximum_response_bytes {
                return Err(permanent("model_sse_line_too_large"));
            }
            let mut remaining = self.buffer.split_off(newline + 1);
            std::mem::swap(&mut remaining, &mut self.buffer);
            remaining.truncate(newline);
            if remaining.last() == Some(&b'\r') {
                remaining.pop();
            }
            self.consume_line(&remaining, &mut events)?;
        }
        Ok(events)
    }

    fn consume_line(
        &mut self,
        line: &[u8],
        events: &mut Vec<ModelProviderWireEvent>,
    ) -> Result<(), ModelAdapterFailure> {
        if self.done_marker && !line.is_empty() {
            return Err(permanent("model_sse_bytes_after_done"));
        }
        if line.len() > self.maximum_response_bytes {
            return Err(permanent("model_sse_line_too_large"));
        }
        if line.is_empty() {
            if let Some(event) = self.dispatch_event()? {
                events.push(event);
            }
            return Ok(());
        }
        if line.starts_with(b":") {
            return Ok(());
        }
        let separator = line
            .iter()
            .position(|byte| *byte == b':')
            .ok_or_else(|| permanent("model_sse_invalid_field"))?;
        let field = &line[..separator];
        let mut value = &line[separator + 1..];
        if value.first() == Some(&b' ') {
            value = &value[1..];
        }
        match field {
            b"event" => {
                if self.event_name.is_some() {
                    return Err(permanent("model_sse_duplicate_event_field"));
                }
                let name = std::str::from_utf8(value)
                    .ok()
                    .filter(|name| valid_event_name(name))
                    .ok_or_else(|| permanent("model_sse_invalid_event_name"))?;
                self.event_name = Some(name.to_owned());
            }
            b"data" => {
                let separator = usize::from(!self.data.is_empty());
                if self
                    .data
                    .len()
                    .checked_add(value.len())
                    .and_then(|total| total.checked_add(separator))
                    .is_none_or(|total| total > self.maximum_response_bytes)
                {
                    return Err(permanent("model_sse_event_too_large"));
                }
                if separator == 1 {
                    self.data.push(b'\n');
                }
                self.data.extend_from_slice(value);
            }
            _ => return Err(permanent("model_sse_unknown_field")),
        }
        Ok(())
    }

    fn dispatch_event(&mut self) -> Result<Option<ModelProviderWireEvent>, ModelAdapterFailure> {
        if self.event_name.is_none() && self.data.is_empty() {
            return Ok(None);
        }
        let data = std::mem::take(&mut self.data);
        let declared_name = self.event_name.take();
        if data == b"[DONE]" {
            if declared_name.is_some() {
                return Err(permanent("model_sse_invalid_done"));
            }
            self.done_marker = true;
            return Ok(None);
        }
        if data.is_empty() {
            return Err(permanent("model_sse_missing_data"));
        }
        let value = parse_strict_json(
            &data,
            JsonLimits {
                max_bytes: self.maximum_response_bytes,
                max_depth: JsonLimits::CONTRACT_FIXTURE.max_depth,
                max_properties_per_object: JsonLimits::CONTRACT_FIXTURE.max_properties_per_object,
                max_items_per_array: JsonLimits::CONTRACT_FIXTURE.max_items_per_array,
                max_string_bytes: self.maximum_response_bytes,
            },
        )
        .map_err(|_| permanent("model_sse_invalid_json"))?;
        let data_name = value
            .get("type")
            .and_then(serde_json::Value::as_str)
            .filter(|name| valid_event_name(name))
            .ok_or_else(|| permanent("model_sse_missing_event_type"))?;
        if declared_name
            .as_deref()
            .is_some_and(|declared| declared != data_name)
        {
            return Err(permanent("model_sse_event_type_mismatch"));
        }
        Ok(Some(ModelProviderWireEvent {
            event_name: declared_name.unwrap_or_else(|| data_name.to_owned()),
            data: value,
        }))
    }
}

/// Converts a raw HTTP response body into strict Provider events without buffering the stream.
pub fn decode_model_provider_sse(
    upstream: ModelProviderByteStream,
    maximum_response_bytes: u32,
) -> Result<ModelProviderWireStream, ModelAdapterFailure> {
    struct State {
        upstream: ModelProviderByteStream,
        decoder: ModelProviderSseDecoder,
        pending: VecDeque<ModelProviderWireEvent>,
        upstream_done: bool,
    }

    let decoder = ModelProviderSseDecoder::new(maximum_response_bytes)?;
    Ok(Box::pin(stream::unfold(
        State {
            upstream,
            decoder,
            pending: VecDeque::new(),
            upstream_done: false,
        },
        |mut state| async move {
            loop {
                if let Some(event) = state.pending.pop_front() {
                    return Some((Ok(event), state));
                }
                if state.upstream_done {
                    return None;
                }
                match state.upstream.next().await {
                    Some(Ok(chunk)) => match state.decoder.push(&chunk) {
                        Ok(events) => state.pending.extend(events),
                        Err(failure) => {
                            state.upstream_done = true;
                            return Some((Err(failure), state));
                        }
                    },
                    Some(Err(failure)) => {
                        state.upstream_done = true;
                        return Some((Err(failure), state));
                    }
                    None => {
                        state.upstream_done = true;
                        match state.decoder.finish() {
                            Ok(events) => state.pending.extend(events),
                            Err(failure) => return Some((Err(failure), state)),
                        }
                    }
                }
            }
        },
    )))
}

fn valid_event_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_chunks_decode_named_and_inferred_events() {
        let mut decoder = ModelProviderSseDecoder::new(4_096).unwrap();
        let first = b"event: response.output_text.delta\r\nda";
        let second = b"ta: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\r\n\r\ndata: {\"type\":\"ping\"}\n\n";
        assert!(decoder.push(first).unwrap().is_empty());
        let events = decoder.push(second).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_name, "response.output_text.delta");
        assert_eq!(events[1].event_name, "ping");
        assert_eq!(
            decoder.observed_response_bytes(),
            first.len() + second.len()
        );
    }

    #[test]
    fn duplicate_json_keys_and_ambiguous_sse_fields_fail_closed() {
        let mut duplicate_json = ModelProviderSseDecoder::new(4_096).unwrap();
        let failure = duplicate_json
            .push(b"data: {\"type\":\"ping\",\"type\":\"error\"}\n\n")
            .unwrap_err();
        assert_eq!(failure.safe_code, "model_sse_invalid_json");

        let mut duplicate_event = ModelProviderSseDecoder::new(4_096).unwrap();
        let failure = duplicate_event
            .push(b"event: ping\nevent: ping\ndata: {\"type\":\"ping\"}\n\n")
            .unwrap_err();
        assert_eq!(failure.safe_code, "model_sse_duplicate_event_field");

        let mut unknown_field = ModelProviderSseDecoder::new(4_096).unwrap();
        let failure = unknown_field
            .push(b"id: secret-handle\ndata: {\"type\":\"ping\"}\n\n")
            .unwrap_err();
        assert_eq!(failure.safe_code, "model_sse_unknown_field");
    }

    #[test]
    fn response_limit_and_done_marker_are_closed() {
        let mut oversized = ModelProviderSseDecoder::new(16).unwrap();
        let failure = oversized.push(&[b'x'; 17]).unwrap_err();
        assert_eq!(failure.safe_code, "model_sse_response_too_large");

        let mut decoder = ModelProviderSseDecoder::new(128).unwrap();
        assert!(decoder.push(b"data: [DONE]\n\n").unwrap().is_empty());
        let failure = decoder.push(b"data: {\"type\":\"ping\"}\n\n").unwrap_err();
        assert_eq!(failure.safe_code, "model_sse_bytes_after_done");

        let mut same_chunk = ModelProviderSseDecoder::new(128).unwrap();
        let failure = same_chunk
            .push(b"data: [DONE]\n\ndata: {\"type\":\"ping\"}\n\n")
            .unwrap_err();
        assert_eq!(failure.safe_code, "model_sse_bytes_after_done");
    }

    #[tokio::test]
    async fn byte_stream_is_decoded_incrementally_without_terminal_invention() {
        let raw: ModelProviderByteStream = Box::pin(stream::iter(vec![
            Ok(b"event: ping\nda".to_vec()),
            Ok(b"ta: {\"type\":\"ping\"}\n\n".to_vec()),
        ]));
        let decoded = decode_model_provider_sse(raw, 4_096)
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].as_ref().unwrap().event_name, "ping");
    }
}
