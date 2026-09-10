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
    stream_start: bool,
    pending_cr: bool,
    event_name: Option<String>,
    data: Vec<u8>,
    data_field_seen: bool,
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
            stream_start: true,
            pending_cr: false,
            event_name: None,
            data: Vec::new(),
            data_field_seen: false,
            done_marker: false,
        })
    }

    pub fn push(
        &mut self,
        chunk: &[u8],
    ) -> Result<Vec<ModelProviderWireEvent>, ModelAdapterFailure> {
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
        let events = self.consume_complete_lines()?;
        // EOF is not a line or event delimiter. A CR has already ended its line; an optional
        // following LF is the only pending framing byte that may be absent at EOF.
        if !self.buffer.is_empty() || self.event_name.is_some() || self.data_field_seen {
            return Err(permanent("model_sse_incomplete_event"));
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
        if self.stream_start {
            const BOM: &[u8] = b"\xef\xbb\xbf";
            if self.buffer.len() < BOM.len() && BOM.starts_with(&self.buffer) {
                return Ok(events);
            }
            if self.buffer.starts_with(BOM) {
                self.buffer.drain(..BOM.len());
            }
            self.stream_start = false;
        }
        loop {
            if self.pending_cr {
                let Some(first) = self.buffer.first() else {
                    break;
                };
                if *first == b'\n' {
                    self.buffer.remove(0);
                }
                self.pending_cr = false;
            }
            if self.done_marker && !self.buffer.is_empty() {
                return Err(permanent("model_sse_bytes_after_done"));
            }
            let Some(newline) = self
                .buffer
                .iter()
                .position(|byte| matches!(byte, b'\r' | b'\n'))
            else {
                break;
            };
            if newline > self.maximum_response_bytes {
                return Err(permanent("model_sse_line_too_large"));
            }
            self.pending_cr = self.buffer[newline] == b'\r';
            let mut remaining = self.buffer.split_off(newline + 1);
            std::mem::swap(&mut remaining, &mut self.buffer);
            remaining.truncate(newline);
            self.consume_line(&remaining, &mut events)?;
        }
        Ok(events)
    }

    fn consume_line(
        &mut self,
        line: &[u8],
        events: &mut Vec<ModelProviderWireEvent>,
    ) -> Result<(), ModelAdapterFailure> {
        if line.len() > self.maximum_response_bytes {
            return Err(permanent("model_sse_line_too_large"));
        }
        std::str::from_utf8(line).map_err(|_| permanent("model_sse_invalid_field"))?;
        if line.is_empty() {
            if let Some(event) = self.dispatch_event()? {
                events.push(event);
            }
            return Ok(());
        }
        if line.starts_with(b":") {
            return Ok(());
        }
        let (field, mut value) = match line.iter().position(|byte| *byte == b':') {
            Some(separator) => (&line[..separator], &line[separator + 1..]),
            None => (line, &b""[..]),
        };
        if value.first() == Some(&b' ') {
            value = &value[1..];
        }
        match field {
            // Transport-only metadata: never an identity, timer, evidence or reconnect input.
            b"id" | b"retry" => {}
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
                let separator = usize::from(self.data_field_seen);
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
                self.data_field_seen = true;
            }
            _ => return Err(permanent("model_sse_unknown_field")),
        }
        Ok(())
    }

    fn dispatch_event(&mut self) -> Result<Option<ModelProviderWireEvent>, ModelAdapterFailure> {
        if self.event_name.is_none() && !self.data_field_seen {
            return Ok(None);
        }
        self.data_field_seen = false;
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

    fn decode_chunks(chunks: &[&[u8]]) -> Vec<ModelProviderWireEvent> {
        let total = chunks.iter().map(|chunk| chunk.len()).sum::<usize>();
        let mut decoder = ModelProviderSseDecoder::new(u32::try_from(total).unwrap()).unwrap();
        let mut events = Vec::new();
        for chunk in chunks {
            events.extend(decoder.push(chunk).unwrap());
        }
        events.extend(decoder.finish().unwrap());
        assert_eq!(decoder.observed_response_bytes(), total);
        events
    }

    #[test]
    fn standard_transport_metadata_is_discarded() {
        let input = b"id: transport-canary\nid\nid: nul\0id\nretry: 42\nretry: not-a-duration\nretry\ndata: {\"type\":\"ping\"}\n\n";
        let events = decode_chunks(&[input]);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_name, "ping");
        assert_eq!(events[0].data, serde_json::json!({"type": "ping"}));
        assert!(!format!("{events:?}").contains("transport-canary"));
    }

    #[test]
    fn standard_bom_and_newlines_are_invariant_at_every_chunk_boundary() {
        for newline in ["\n", "\r", "\r\n"] {
            let input = format!(
                "\u{feff}: comment{newline}id: canary{newline}event: ping{newline}data: {{\"type\":\"ping\",\"text\":\"你好\"}}{newline}{newline}retry: 999999999999999999999999999999999999999999{newline}data: [DONE]{newline}{newline}"
            );
            let bytes = input.as_bytes();
            let expected = decode_chunks(&[bytes]);
            assert_eq!(expected.len(), 1);
            for split in 0..=bytes.len() {
                let actual = decode_chunks(&[&bytes[..split], &bytes[split..]]);
                assert_eq!(actual, expected, "split={split}, newline={newline:?}");
            }
            assert_eq!(
                decode_chunks(&bytes.chunks(1).collect::<Vec<_>>()),
                expected
            );
        }
    }

    #[test]
    fn eof_never_invents_a_data_event_delimiter() {
        for bytes in [
            b"data: {\"type\":\"ping\"}".as_slice(),
            b"data: {\"type\":\"ping\"}\n",
            b"event: ping\n",
            b"data:\n",
            b"id: unfinished",
            b"\xef\xbb",
        ] {
            let mut decoder = ModelProviderSseDecoder::new(4_096).unwrap();
            assert!(decoder.push(bytes).unwrap().is_empty());
            assert_eq!(
                decoder.finish().unwrap_err().safe_code,
                "model_sse_incomplete_event"
            );
        }
        assert!(decode_chunks(&[b": complete comment\rid\rretry\r"]).is_empty());
        assert!(decode_chunks(&[b"\xef\xbb\xbf"]).is_empty());
        assert!(decode_chunks(&[b"data: [DONE]\r\r"]).is_empty());
    }

    #[test]
    fn discarded_metadata_remains_utf8_and_byte_bounded() {
        for bytes in [
            b"id: \xff\n".as_slice(),
            b"retry: \xff\n",
            b": \xff\n",
            b"\xef\xbb\xbf\xef\xbb\xbfdata: {\"type\":\"ping\"}\n\n",
            b": comment\n\xef\xbb\xbfdata: {\"type\":\"ping\"}\n\n",
        ] {
            let mut decoder = ModelProviderSseDecoder::new(4_096).unwrap();
            let failure = decoder.push(bytes).unwrap_err();
            assert!(matches!(
                failure.safe_code.as_str(),
                "model_sse_invalid_field" | "model_sse_unknown_field"
            ));
        }
        let input = b"id: discarded\r\nretry: 999999999999999999999999\r\n";
        let mut decoder =
            ModelProviderSseDecoder::new(u32::try_from(input.len() - 1).unwrap()).unwrap();
        let failure = decoder.push(input).unwrap_err();
        assert_eq!(failure.safe_code, "model_sse_response_too_large");

        let events =
            decode_chunks(&["data: {\"type\":\"ping\",\"text\":\"\u{feff}\"}\n\n".as_bytes()]);
        assert_eq!(events[0].data["text"], "\u{feff}");
    }

    #[test]
    fn malformed_fields_and_post_done_bytes_fail_at_every_split() {
        for (input, expected) in [
            (
                b"id: canary\nunknown: value\n".as_slice(),
                "model_sse_unknown_field",
            ),
            (
                b"id: canary\nevent: ping\nevent: ping\n",
                "model_sse_duplicate_event_field",
            ),
            (
                b"retry: 1\ndata: {\"type\":\"ping\",\"type\":\"error\"}\n\n",
                "model_sse_invalid_json",
            ),
            (
                b"event: error\ndata: {\"type\":\"ping\"}\n\n",
                "model_sse_event_type_mismatch",
            ),
            (b"data: [DONE]\r\n\r\n\n", "model_sse_bytes_after_done"),
            (
                b"data: [DONE]\r\rdata: {\"type\":\"ping\"}\n\n",
                "model_sse_bytes_after_done",
            ),
            (b"data: [DONE]\n\n: comment\n", "model_sse_bytes_after_done"),
            (b"id: canary\xff\n", "model_sse_invalid_field"),
        ] {
            for split in 0..=input.len() {
                let mut decoder = ModelProviderSseDecoder::new(4_096).unwrap();
                let failure = decoder
                    .push(&input[..split])
                    .and_then(|_| decoder.push(&input[split..]))
                    .and_then(|_| decoder.finish())
                    .unwrap_err();
                assert_eq!(failure.safe_code, expected, "split={split}");
                assert!(!format!("{failure:?}").contains("canary"));
            }
        }
    }

    #[test]
    fn mixed_delimiters_and_empty_data_lines_preserve_actual_json() {
        let input = b"id:\rretry\nevent: ping\r\ndata:\rdata: {\"type\":\"ping\"}\ndata:\r\n\r";
        let expected = decode_chunks(&[input]);
        assert_eq!(expected[0].data, serde_json::json!({"type":"ping"}));
        assert_eq!(
            decode_chunks(&input.chunks(1).collect::<Vec<_>>()),
            expected
        );
        let mut decoder = ModelProviderSseDecoder::new(128).unwrap();
        assert_eq!(
            decoder.push(b"data:\n\n").unwrap_err().safe_code,
            "model_sse_missing_data"
        );
        // Empty data lines are retained in the SSE data buffer; they cannot turn a malformed
        // transport marker into a legal marker by being silently removed.
        let mut decoder = ModelProviderSseDecoder::new(128).unwrap();
        assert_eq!(
            decoder
                .push(b"data:\ndata: [DONE]\n\n")
                .unwrap_err()
                .safe_code,
            "model_sse_invalid_json"
        );
    }

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
            .push(b"future-field: transport-canary\ndata: {\"type\":\"ping\"}\n\n")
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
