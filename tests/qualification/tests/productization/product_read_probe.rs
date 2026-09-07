//! Bounded local Gateway observations. These measurements are not production capacity evidence.
use super::*;
use insight_platform_api::{
    product::ListPageV1,
    run::{
        RunResultViewV1, MAX_RUN_REQUEST_BYTES, RUN_HIGH_WATER_HEADER,
        RUN_HISTORY_TRUNCATED_HEADER, RUN_REPLAY_FLOOR_HEADER,
    },
    task::TaskViewV2,
};
use insight_platform_contracts::{OpaqueRunEventCursor, PublicRunEvent, ResourceId, ValueRef};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
};

const CLIENTS: usize = 4;
const ROUNDS: usize = 16;
const MAX_INBOX_PAGES: usize = 16;

#[derive(Default)]
struct Observations {
    requests: BTreeMap<&'static str, Vec<(u64, usize)>>,
    event_count: usize,
    empty_event_pages: usize,
    task_count: usize,
    empty_task_pages: usize,
}
impl Observations {
    fn record(&mut self, endpoint: &'static str, started: Instant, bytes: usize) {
        self.requests.entry(endpoint).or_default().push((
            u64::try_from(started.elapsed().as_micros()).expect("bounded probe duration"),
            bytes,
        ));
    }
    fn report(&self) -> Value {
        let endpoints = self.requests.iter().map(|(name, entries)| {
            let mut latencies = entries.iter().map(|(elapsed, _)| *elapsed).collect::<Vec<_>>();
            latencies.sort_unstable();
            let percentile = |percent: usize| latencies[(latencies.len() * percent).div_ceil(100).saturating_sub(1)];
            ((*name).to_owned(), json!({"requests": entries.len(), "response_bytes": entries.iter().map(|(_, bytes)| *bytes).sum::<usize>(), "p50_microseconds": percentile(50), "p95_microseconds": percentile(95), "max_microseconds": latencies.last()}))
        }).collect::<BTreeMap<_, _>>();
        json!({"endpoints":endpoints,"event_count":self.event_count,"empty_event_pages":self.empty_event_pages,"task_count":self.task_count,"empty_task_pages":self.empty_task_pages})
    }
}
fn read_bytes(response: reqwest::blocking::Response) -> Vec<u8> {
    let mut bytes = Vec::new();
    response
        .take((MAX_RUN_REQUEST_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .expect("bounded public response reads");
    assert!(
        bytes.len() <= MAX_RUN_REQUEST_BYTES,
        "probe response exceeds its bounded read"
    );
    bytes
}
fn checked_get(request: reqwest::blocking::RequestBuilder) -> reqwest::blocking::Response {
    let response = request.send().expect("public read transport completes");
    assert!(
        response.status() == StatusCode::OK,
        "public read {} returned non-success status {}",
        response.url().path(),
        response.status()
    );
    assert!(
        response
            .headers()
            .get("cache-control")
            .is_some_and(|value| value.to_str().is_ok_and(|value| value.contains("no-store"))),
        "public content remains no-store"
    );
    response
}
fn header_u64(response: &reqwest::blocking::Response, name: &str) -> u64 {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .expect("bounded public history position header")
}
fn events(bytes: &[u8]) -> Vec<PublicRunEvent> {
    let text = std::str::from_utf8(bytes).expect("public SSE is UTF-8");
    text.split("\n\n")
        .filter(|frame| !frame.trim().is_empty())
        .map(|frame| {
            let mut id = None;
            let mut kind = None;
            let mut data = Vec::new();
            for line in frame.lines() {
                let (key, value) = line.split_once(':').expect("closed public SSE field");
                let value = value.strip_prefix(' ').unwrap_or(value);
                match key {
                    "id" => {
                        assert!(id.replace(value).is_none(), "one SSE id");
                    }
                    "event" => {
                        assert!(kind.replace(value).is_none(), "one SSE event type");
                    }
                    "data" => data.push(value),
                    _ => panic!("unexpected public SSE field"),
                }
            }
            let event: PublicRunEvent =
                serde_json::from_str(&data.join("\n")).expect("owning closed PublicRunEvent DTO");
            event.validate().expect("public event validates");
            assert!(
                event.cursor.as_ref().map(OpaqueRunEventCursor::as_str) == id,
                "frame and event opaque cursor agree"
            );
            assert!(
                Some(event.event_type.as_str()) == kind,
                "frame and event kind agree"
            );
            event
        })
        .collect()
}
fn terminal_result(bytes: &[u8], run_id: &ResourceId) -> RunResultViewV1 {
    let view: RunResultViewV1 = serde_json::from_slice(bytes).expect("owning closed result DTO");
    view.validate().expect("result DTO validates");
    assert!(
        &view.run_id == run_id,
        "result belongs to the requested Run"
    );
    match &view.value {
        ValueRef::Inline { value } => assert!(
            canonical_digest(value) == view.content_digest.as_str(),
            "Inline content digest is exact"
        ),
        ValueRef::Artifact { artifact } => {
            artifact.validate().expect("exact Artifact reference");
            assert!(
                artifact.content_digest() == &view.content_digest,
                "Artifact digest is exact"
            );
        }
    }
    view
}
fn client_probe(
    client: Client,
    base: &str,
    token: &str,
    run_id: &ResourceId,
) -> (Observations, String) {
    let mut measurements = Observations::default();
    let mut cursor: Option<OpaqueRunEventCursor> = None;
    let mut previous_sequence = 0;
    let mut observed_high_water = None;
    let mut expected_result = None;
    for _ in 0..ROUNDS {
        let started = Instant::now();
        let mut request = client
            .get(format!("{base}/v1/runs/{run_id}/events"))
            .bearer_auth(token)
            .header("accept", "text/event-stream");
        if let Some(cursor) = &cursor {
            request = request.header("last-event-id", cursor.as_str());
        }
        let response = checked_get(request);
        assert!(
            response
                .headers()
                .get("content-type")
                .is_some_and(|value| value
                    .to_str()
                    .is_ok_and(|value| value.starts_with("text/event-stream"))),
            "SSE content type"
        );
        let floor = header_u64(&response, RUN_REPLAY_FLOOR_HEADER);
        let high_water = header_u64(&response, RUN_HIGH_WATER_HEADER);
        assert!(floor <= high_water, "ordered history bounds");
        assert!(
            response
                .headers()
                .get(RUN_HISTORY_TRUNCATED_HEADER)
                .is_some_and(|value| value == "false"),
            "fresh terminal history is complete"
        );
        if let Some(previous) = observed_high_water {
            assert!(previous == high_water, "terminal Run high water is stable");
        }
        observed_high_water = Some(high_water);
        let bytes = read_bytes(response);
        measurements.record("run_events", started, bytes.len());
        let page = events(&bytes);
        if page.is_empty() {
            measurements.empty_event_pages += 1;
        }
        for event in page {
            assert!(&event.run_id == run_id, "event Run identity");
            let sequence = event.sequence.expect("durable sequence");
            assert!(
                sequence > previous_sequence && sequence > floor && sequence <= high_water,
                "durable sequence advances within history bounds"
            );
            previous_sequence = sequence;
            cursor = event.cursor;
            measurements.event_count += 1;
        }
        let started = Instant::now();
        let bytes = read_bytes(checked_get(
            client
                .get(format!("{base}/v1/runs/{run_id}/result"))
                .bearer_auth(token),
        ));
        measurements.record("run_result", started, bytes.len());
        let result = terminal_result(&bytes, run_id);
        if let Some(expected) = &expected_result {
            assert!(&result == expected, "terminal typed result is stable");
        }
        let started = Instant::now();
        let content = read_bytes(checked_get(
            client
                .get(format!(
                    "{base}/v1/runs/{run_id}/values/{}/content",
                    result.value_id
                ))
                .bearer_auth(token),
        ));
        measurements.record("run_value_content", started, content.len());
        assert!(
            terminal_result(&content, run_id) == result,
            "result and value-content share the same typed result"
        );
        expected_result = Some(result);
    }
    assert!(
        previous_sequence == observed_high_water.expect("real history read"),
        "all terminal public events were consumed"
    );
    assert!(
        measurements.empty_event_pages > 0,
        "real follow reaches empty pages without resetting the cursor"
    );
    let mut next_cursor: Option<String> = None;
    let mut seen_tasks = BTreeSet::new();
    for page_no in 0..MAX_INBOX_PAGES {
        let started = Instant::now();
        let mut url =
            reqwest::Url::parse(&format!("{base}/v1/tasks")).expect("fixed same-origin Task route");
        url.query_pairs_mut().append_pair("page_size", "1");
        if let Some(cursor) = &next_cursor {
            url.query_pairs_mut().append_pair("cursor", cursor);
        }
        let request = client.get(url).bearer_auth(token);
        let bytes = read_bytes(checked_get(request));
        measurements.record("task_inbox", started, bytes.len());
        let page: ListPageV1<TaskViewV2> =
            serde_json::from_slice(&bytes).expect("owning closed Task inbox DTO");
        assert!(
            page.schema_version == 1 && page.items.len() <= 1,
            "bounded inbox response"
        );
        if page.items.is_empty() {
            measurements.empty_task_pages += 1;
        }
        for item in &page.items {
            item.validate().expect("Task metadata validates");
            assert!(
                seen_tasks.insert(item.task_id.clone()),
                "Task pagination does not duplicate a candidate"
            );
        }
        measurements.task_count += page.items.len();
        match page.next_cursor {
            None => break,
            Some(cursor) => {
                assert!(
                    page_no + 1 < MAX_INBOX_PAGES,
                    "probe cannot silently truncate the bounded fixture inbox"
                );
                next_cursor = Some(cursor.as_str().to_owned());
            }
        }
    }
    (
        measurements,
        expected_result
            .expect("real result reads")
            .content_digest
            .to_string(),
    )
}
pub(super) fn run(project: &Path, run_id: &str) {
    let started_at = Utc::now();
    let started = Instant::now();
    let (client, base, token) = raw_runtime_client(project);
    let run_id: ResourceId = run_id.parse().expect("nominal Run ID");
    let results = thread::scope(|scope| {
        (0..CLIENTS)
            .map(|_| {
                let client = client.clone();
                let base = &base;
                let token = &token;
                let run_id = &run_id;
                scope.spawn(move || client_probe(client, base, token, run_id))
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|thread| thread.join().expect("bounded public read client completes"))
            .collect::<Vec<_>>()
    });
    assert!(
        results.iter().all(|(_, digest)| digest == &results[0].1),
        "all clients observe the same exact content"
    );
    let profile: Value = serde_json::from_slice(
        &fs::read(project.join(".insight/runtime/profile.json")).expect("local profile evidence"),
    )
    .expect("profile JSON");
    let report = json!({
        "schema_version": 1, "kind": "insight.local-product-read-observations/v1", "status": "passed",
        "environment": "fresh native fixture Gateway/PostgreSQL over loopback; no production SLA or capacity claim",
        "source_fingerprint": profile["source_fingerprint"], "profile_digest": profile["profile_digest"],
        "run_id":run_id, "content_digest":results[0].1, "concurrent_clients":CLIENTS, "rounds_per_client":ROUNDS,
        "started_at":started_at.to_rfc3339_opts(SecondsFormat::Micros,true), "finished_at":Utc::now().to_rfc3339_opts(SecondsFormat::Micros,true),
        "elapsed_microseconds":started.elapsed().as_micros(), "clients":results.iter().map(|(observations, _)| observations.report()).collect::<Vec<_>>()
    });
    fs::write(
        project.join(".insight/runtime/logs/product-read-probe.json"),
        serde_jcs::to_vec(&report).expect("bounded safe measurement report"),
    )
    .expect("local measurement evidence writes");
}
