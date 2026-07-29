#!/usr/bin/env python3
"""Synchronized Attached-SSE/privacy-DELETE qualification probe."""

from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import ssl
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


TERMINAL_EVENTS = {
    "response.completed",
    "response.failed",
    "response.incomplete",
    "workflow.response.timed_out",
    "workflow.response.cancelled",
    "workflow.response.interrupted",
    "error",
}


def route(base_path: str, suffix: str) -> str:
    prefix = base_path.rstrip("/")
    return f"{prefix}{suffix}" if prefix else suffix


def connection_for(base_url: str, timeout: float) -> tuple[http.client.HTTPConnection, str]:
    parsed = urlsplit(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("BASE_URL must be an absolute http(s) URL")
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    if parsed.scheme == "https":
        connection: http.client.HTTPConnection = http.client.HTTPSConnection(
            parsed.hostname,
            port,
            timeout=timeout,
            context=ssl.create_default_context(),
        )
    else:
        connection = http.client.HTTPConnection(
            parsed.hostname, port, timeout=timeout
        )
    return connection, parsed.path


def parse_frame(lines: list[str], observed_at_ns: int) -> dict[str, Any] | None:
    event = "message"
    event_explicit = False
    data_lines: list[str] = []
    for line in lines:
        if line.startswith("event:"):
            event = line.partition(":")[2].lstrip()
            event_explicit = True
        elif line.startswith("data:"):
            data_lines.append(line.partition(":")[2].lstrip())
    if not lines:
        return None
    if not data_lines and not event_explicit:
        event = "comment"
    data = "\n".join(data_lines)
    return {
        "event": event,
        "observed_at_ns": observed_at_ns,
        "data_bytes": len(data.encode("utf-8")),
        "data_sha256": hashlib.sha256(data.encode("utf-8")).hexdigest(),
    }


def evaluate_timeline(
    frames: list[dict[str, Any]],
    delete_completed_at_ns: int,
) -> list[str]:
    failures: list[str] = []
    before = [
        frame
        for frame in frames
        if int(frame["observed_at_ns"]) <= delete_completed_at_ns
    ]
    after = [
        frame
        for frame in frames
        if int(frame["observed_at_ns"]) > delete_completed_at_ns
    ]
    if not any(frame["event"] == "run.output.text.delta" for frame in before):
        failures.append("stream produced no provisional output delta before DELETE")
    terminal_before = [
        frame["event"] for frame in before if frame["event"] in TERMINAL_EVENTS
    ]
    if terminal_before:
        failures.append(
            "stream reached a terminal/error frame before the DELETE race: "
            + ",".join(terminal_before)
        )
    if after:
        failures.append(
            "SSE frames were observed after the successful DELETE response: "
            + ",".join(str(frame["event"]) for frame in after)
        )
    return failures


def run_probe(args: argparse.Namespace) -> int:
    lock = threading.Lock()
    first_delta = threading.Event()
    reader_done = threading.Event()
    frames: list[dict[str, Any]] = []
    transcript_parts: list[bytes] = []
    reader_error: list[str] = []
    stream_status: list[int] = []
    stream_connection: list[http.client.HTTPConnection] = []

    stream_path = route(
        urlsplit(args.base_url).path,
        f"/v1/conversations/{args.conversation_id}/messages/stream",
    )
    stream_body = json.dumps(
        {
            "content": {
                "prompt": "privacy delete race chunk_delay_ms=500",
                "output_scale": "10",
            }
        },
        separators=(",", ":"),
    ).encode("utf-8")

    def read_stream() -> None:
        try:
            connection, _ = connection_for(args.base_url, args.stream_timeout)
            stream_connection.append(connection)
            connection.request(
                "POST",
                stream_path,
                body=stream_body,
                headers={
                    "content-type": "application/json",
                    "x-request-id": args.stream_request_id,
                    "x-tenant-id": args.tenant_id,
                    "x-user-id": args.user_id,
                },
            )
            response = connection.getresponse()
            stream_status.append(response.status)
            if response.status != 200:
                reader_error.append(
                    f"Attached stream returned HTTP {response.status}"
                )
                response.read()
                return
            current_lines: list[str] = []
            while True:
                raw_line = response.readline()
                if not raw_line:
                    if current_lines:
                        with lock:
                            frame = parse_frame(
                                current_lines, time.monotonic_ns()
                            )
                            if frame is not None:
                                frames.append(frame)
                    break
                transcript_parts.append(raw_line)
                line = raw_line.decode("utf-8", errors="strict").rstrip("\r\n")
                if line:
                    current_lines.append(line)
                    continue
                with lock:
                    frame = parse_frame(current_lines, time.monotonic_ns())
                    if frame is not None:
                        frames.append(frame)
                        if frame["event"] == "run.output.text.delta":
                            first_delta.set()
                current_lines = []
        except Exception as error:  # noqa: BLE001 - evidence must record any I/O failure.
            reader_error.append(f"{type(error).__name__}: {error}")
        finally:
            reader_done.set()

    reader = threading.Thread(target=read_stream, name="privacy-sse-reader")
    reader.start()
    if not first_delta.wait(args.start_timeout):
        for connection in stream_connection:
            connection.close()
        reader.join(timeout=2)
        failures = ["Attached stream did not enter the provisional delta window"]
        failures.extend(reader_error)
        report = {
            "passed": False,
            "failures": failures,
            "stream_http_status": stream_status[0] if stream_status else None,
            "frames": frames,
        }
        Path(args.transcript).write_bytes(b"".join(transcript_parts))
        Path(args.report).write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return 1

    delete_started_at_ns = time.monotonic_ns()
    delete_connection, base_path = connection_for(
        args.base_url, args.delete_timeout
    )
    delete_connection.request(
        "DELETE",
        route(
            base_path,
            f"/v1/conversations/{args.conversation_id}",
        ),
        headers={
            "x-request-id": args.delete_request_id,
            "x-tenant-id": args.tenant_id,
            "x-user-id": args.user_id,
        },
    )
    delete_response = delete_connection.getresponse()
    delete_body = delete_response.read()
    with lock:
        delete_completed_at_ns = time.monotonic_ns()
    Path(args.delete_output).write_bytes(delete_body)

    reader.join(timeout=args.close_timeout)
    if reader.is_alive():
        for connection in stream_connection:
            connection.close()
        reader.join(timeout=2)
    Path(args.transcript).write_bytes(b"".join(transcript_parts))

    failures: list[str] = []
    if delete_response.status != 200:
        failures.append(f"privacy DELETE returned HTTP {delete_response.status}")
    try:
        deleted = json.loads(delete_body).get("data", {}).get("deleted") is True
    except (UnicodeDecodeError, json.JSONDecodeError):
        deleted = False
    if not deleted:
        failures.append("privacy DELETE response did not confirm deleted=true")
    if reader.is_alive():
        failures.append("Attached stream remained open after DELETE completed")
    failures.extend(reader_error)
    failures.extend(evaluate_timeline(frames, delete_completed_at_ns))

    frame_counts = Counter(str(frame["event"]) for frame in frames)
    frames_after_delete = sum(
        int(frame["observed_at_ns"]) > delete_completed_at_ns for frame in frames
    )
    report = {
        "passed": not failures,
        "failures": failures,
        "stream_http_status": stream_status[0] if stream_status else None,
        "delete_http_status": delete_response.status,
        "delete_started_at_ns": delete_started_at_ns,
        "delete_completed_at_ns": delete_completed_at_ns,
        "delete_duration_ms": (
            delete_completed_at_ns - delete_started_at_ns
        ) / 1_000_000,
        "stream_closed": reader_done.is_set() and not reader.is_alive(),
        "frame_counts": dict(sorted(frame_counts.items())),
        "frames_before_or_at_delete": len(frames) - frames_after_delete,
        "frames_after_delete": frames_after_delete,
        "frames": frames,
    }
    Path(args.report).write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, sort_keys=True))
    return 0 if not failures else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--conversation-id", required=True)
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--stream-request-id", required=True)
    parser.add_argument("--delete-request-id", required=True)
    parser.add_argument("--transcript", required=True)
    parser.add_argument("--delete-output", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--start-timeout", type=float, default=30)
    parser.add_argument("--delete-timeout", type=float, default=10)
    parser.add_argument("--close-timeout", type=float, default=10)
    parser.add_argument("--stream-timeout", type=float, default=60)
    return run_probe(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
