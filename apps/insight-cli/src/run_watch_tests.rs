// Included in run::tests to exercise both consumers with the same actual HTTP fixture.
#[test]
fn watch_drains_events_committed_after_its_first_page() {
    terminal_drain_fixture(false, 1);
}

#[test]
fn journaled_watch_drains_terminal_events_and_resumes_its_cursor() {
    terminal_drain_fixture(true, 1);
}

#[test]
fn terminal_watch_keeps_draining_a_full_page() {
    for journaled in [false, true] {
        terminal_drain_fixture(journaled, 128);
    }
}

fn terminal_drain_fixture(journaled: bool, first_page_size: u64) {
    let run_id = id(ResourceKind::Run);
    let running = running_run(run_id.clone());
    let terminal = RunViewV1 {
        state: RunState::Succeeded,
        version: 2,
        output_value_id: Some(id(ResourceKind::RunValue)),
        terminal_at: Some("2026-08-29T00:00:02.000000Z".parse().unwrap()),
        updated_at: "2026-08-29T00:00:02.000000Z".parse().unwrap(),
        etag: format!("\"{run_id}-2\""),
        ..running.clone()
    };
    let event = |sequence: u64, kind: PublicRunEventType| PublicRunEvent {
        event_id: Some(id(ResourceKind::Event)),
        run_id: run_id.clone(),
        cursor: Some(OpaqueRunEventCursor::new(format!("opaque-event-{sequence}")).unwrap()),
        sequence: Some(sequence),
        schema_version: 1,
        trace_id: TraceId::new(),
        event_type: kind,
        durability: EventDurability::Durable,
        occurred_at: "2026-08-29T00:00:02.000000Z".parse().unwrap(),
        data: serde_json::to_value(DurablePublicRunEventData {
            source_kind: kind.durable_source_kind().unwrap(),
            source_id: id(kind.durable_source_kind().unwrap().resource_kind()),
            source_projection_version: 1,
            safe_summary: None,
        })
        .unwrap(),
    };
    let first = (1..=first_page_size)
        .map(|sequence| event(sequence, PublicRunEventType::NodeStarted))
        .collect::<Vec<_>>();
    let completed = event(first_page_size + 1, PublicRunEventType::ModelCompleted);
    let expected_cursor = completed.cursor.as_ref().unwrap().as_str().to_owned();
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();
    let stopped = Arc::new(AtomicBool::new(false));
    let server_stopped = Arc::clone(&stopped);
    let server_run = run_id.clone();
    let server_terminal = terminal.clone();
    let server_cursor = expected_cursor.clone();
    let server = thread::spawn(move || {
        // The full-page case starts terminal: only the page-size guard can keep it draining.
        let mut committed = first_page_size == 128;
        let mut first_page_served = false;
        let deadline = Instant::now() + Duration::from_secs(30);
        while !server_stopped.load(Ordering::Acquire) && Instant::now() < deadline {
            let (mut stream, _) = match listener.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(2));
                    continue;
                }
                Err(error) => panic!("terminal drain HTTP fixture: {error}"),
            };
            stream.set_nonblocking(false).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let (head, _) = read_request(&mut stream);
            assert_eq!(header_value(&head, "authorization"), Some("Bearer token"));
            if head.starts_with(&format!("GET /v1/runs/{server_run}/events HTTP/1.1")) {
                match header_value(&head, "last-event-id") {
                    None => {
                        assert!(!first_page_served);
                        write_drain_sse_page(&mut stream, &first);
                        first_page_served = true;
                        // In the short-page cases completion commits after this snapshot.
                        // A subsequent Run read sees terminal, but that first page is stale.
                        committed = true;
                    }
                    Some(cursor) if cursor == format!("opaque-event-{first_page_size}") => {
                        assert!(committed);
                        write_sse_response(&mut stream, &completed);
                    }
                    Some(cursor) if cursor == server_cursor => {
                        assert!(committed);
                        write_empty_sse_response(&mut stream);
                    }
                    other => panic!("cursor was lost or replaced: {other:?}"),
                }
            } else {
                assert!(head.starts_with(&format!("GET /v1/runs/{server_run} HTTP/1.1")));
                let view = if committed {
                    &server_terminal
                } else {
                    &running
                };
                write_json_response(
                    &mut stream,
                    "200 OK",
                    "11111111111111111111111111111111",
                    Some(&view.etag),
                    None,
                    view,
                );
            }
        }
    });
    let client = PublicHttpClient::new(
        format!("http://127.0.0.1:{port}"),
        "token".to_owned(),
        Duration::from_secs(2),
    )
    .unwrap();
    let directory = TempDir::new().unwrap();
    let mut output = FlushWriter::default();
    let result = if journaled {
        watch_run_with_cursor_journal(
            &client,
            &run_id,
            Duration::from_secs(10),
            &mut output,
            directory.path(),
        )
    } else {
        watch_run(&client, &run_id, Duration::from_secs(10), &mut output)
    };
    let resumed = if journaled && result.is_ok() {
        let mut resumed = FlushWriter::default();
        let result = watch_run_with_cursor_journal(
            &client,
            &run_id,
            Duration::from_secs(10),
            &mut resumed,
            directory.path(),
        );
        Some((result, resumed.bytes))
    } else {
        None
    };
    stopped.store(true, Ordering::Release);
    server.join().unwrap();
    assert_eq!(result.unwrap(), terminal);
    let records = String::from_utf8(output.bytes)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        records.len(),
        first_page_size as usize + 2,
        "the terminal read must not skip the final event page"
    );
    assert_eq!(
        records[first_page_size as usize]["event"]["event_type"],
        "model.completed"
    );
    assert_eq!(records.last().unwrap()["kind"], "terminal");
    if let Some((result, bytes)) = resumed {
        assert_eq!(result.unwrap(), terminal);
        assert_eq!(String::from_utf8(bytes).unwrap().lines().count(), 1);
        let journal =
            run_journal::load_cursor(&run_journal::cursor_journal_path(directory.path(), &run_id))
                .unwrap()
                .unwrap();
        assert_eq!(journal.last_sequence, first_page_size + 1);
        assert_eq!(journal.cursor.as_deref(), Some(expected_cursor.as_str()));
    }
}

fn write_drain_sse_page(stream: &mut TcpStream, events: &[PublicRunEvent]) {
    let body = events
        .iter()
        .map(|event| {
            format!(
                "id:{}\nevent:{}\ndata:{}\n\n",
                event.cursor.as_ref().unwrap().as_str(),
                event.event_type.as_str(),
                serde_json::to_string(event).unwrap()
            )
        })
        .collect::<String>();
    write!(stream, "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncache-control: no-store, private, max-age=0\r\ntrace-id: 33333333333333333333333333333333\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len()).unwrap();
}
