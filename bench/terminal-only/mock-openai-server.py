#!/usr/bin/env python3
"""Deterministic OpenAI-compatible SSE fixture used only by Gate D."""

from __future__ import annotations

import json
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class FixtureStats:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.started = 0
        self.completed = 0
        self.active = 0
        self.chunks = 0

    def begin(self) -> None:
        with self._lock:
            self.started += 1
            self.active += 1

    def chunk(self) -> None:
        with self._lock:
            self.chunks += 1

    def finish(self) -> None:
        with self._lock:
            self.completed += 1
            self.active -= 1

    def snapshot(self) -> bytes:
        with self._lock:
            document = {
                "started": self.started,
                "completed": self.completed,
                "active": self.active,
                "chunks": self.chunks,
            }
        return json.dumps(document, separators=(",", ":")).encode("utf-8")


STATS = FixtureStats()


class QualificationServer(ThreadingHTTPServer):
    request_queue_size = 128
    daemon_threads = True


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:
        return

    def do_GET(self) -> None:
        if self.path == "/stats":
            body = STATS.snapshot()
        elif self.path == "/health":
            body = b'{"status":"ok"}'
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        if self.path.rstrip("/") != "/v1/chat/completions":
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("content-length", "0"))
            request = json.loads(self.rfile.read(length))
        except (ValueError, json.JSONDecodeError):
            self.send_error(400)
            return
        if request.get("stream") is not True:
            self.send_error(400, "Gate D fixture requires stream=true")
            return

        prompt = json.dumps(request.get("messages", []), separators=(",", ":"))
        match = re.search(r"output_scale=(\d+)", prompt)
        scale = min(max(int(match.group(1)) if match else 1, 1), 10)
        delay_match = re.search(r"chunk_delay_ms=(\d+)", prompt)
        delay_seconds = (
            min(max(int(delay_match.group(1)), 0), 1000) / 1000
            if delay_match
            else 0
        )
        chunk_count = 4 * scale

        STATS.begin()
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.send_header("cache-control", "no-cache")
        self.send_header("connection", "close")
        self.end_headers()
        try:
            for index in range(chunk_count):
                payload = {
                    "id": "chatcmpl-terminal-stream-fixture",
                    "object": "chat.completion.chunk",
                    "choices": [
                        {
                            "index": 0,
                            "delta": {"content": f"fixture-{scale}x-{index:02d} "},
                            "finish_reason": None,
                        }
                    ],
                }
                self.wfile.write(
                    b"data: "
                    + json.dumps(payload, separators=(",", ":")).encode("utf-8")
                    + b"\n\n"
                )
                self.wfile.flush()
                STATS.chunk()
                if delay_seconds:
                    time.sleep(delay_seconds)

            terminal = {
                "id": "chatcmpl-terminal-stream-fixture",
                "object": "chat.completion.chunk",
                "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
            }
            usage = {
                "id": "chatcmpl-terminal-stream-fixture",
                "object": "chat.completion.chunk",
                "choices": [],
                "usage": {
                    "prompt_tokens": 8,
                    "completion_tokens": chunk_count,
                    "total_tokens": 8 + chunk_count,
                    "prompt_tokens_details": {"cached_tokens": 0},
                    "completion_tokens_details": {"reasoning_tokens": 0},
                },
            }
            for payload in (terminal, usage):
                self.wfile.write(
                    b"data: "
                    + json.dumps(payload, separators=(",", ":")).encode("utf-8")
                    + b"\n\n"
                )
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
            self.close_connection = True
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
        finally:
            STATS.finish()


if __name__ == "__main__":
    QualificationServer(("0.0.0.0", 8080), Handler).serve_forever()
