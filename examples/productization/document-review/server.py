#!/usr/bin/env python3
"""Bounded public-document provider for the existing Remote Context JSON wire."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import socketserver
import ssl
import stat
import sys
import threading
import time


MANIFEST_DIGEST = "sha256:c8e1df194188088ccf26b4937d6a094f5fbf2611f409a57d736f47d9b8399344"
SOURCE_REVISION = "b8d9a6e2a4043945eb94cf1bcab52df7c91d3963"
SOURCE_PATHS = ("docs/current/architecture.md", "docs/current/agent-authoring.md")
MAX_MANIFEST_BYTES = 65_536
MAX_CORPUS_BYTES = 32_768
MAX_PARAGRAPHS = 128
MAX_PARAGRAPH_BYTES = 2_048
MAX_HEADER_BYTES = 8_192
MAX_REQUEST_BYTES = 8_192
MAX_QUESTION_BYTES = 1_024
MAX_RESPONSE_BYTES = 65_536
MAX_RESULTS = 8
MAX_IN_FLIGHT = 4
REQUEST_SECONDS = 5.0
CORPUS_DIRECTORY = Path(__file__).resolve().parent / "corpus"


class InvalidInput(ValueError):
    pass


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def digest(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


def reject_number(_):
    raise InvalidInput("non-integer number")


def strict_json(data, maximum):
    if not data or len(data) > maximum:
        raise InvalidInput("JSON size")

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result or len(result) >= 16:
                raise InvalidInput("object fields")
            result[key] = value
        return result

    def integer(text):
        if len(text) > 9:
            raise InvalidInput("integer size")
        return int(text)

    def validate(value, depth=0):
        if depth > 8:
            raise InvalidInput("JSON depth")
        if isinstance(value, dict):
            for key, item in value.items():
                key.encode("utf-8")
                validate(item, depth + 1)
        elif isinstance(value, list):
            if len(value) > 128:
                raise InvalidInput("array size")
            for item in value:
                validate(item, depth + 1)
        elif isinstance(value, str):
            value.encode("utf-8")

    try:
        value = json.loads(data.decode("utf-8"), object_pairs_hook=pairs,
                           parse_int=integer, parse_float=reject_number,
                           parse_constant=reject_number)
        validate(value)
        return value
    except (ValueError, UnicodeError, RecursionError) as error:
        raise InvalidInput("invalid JSON") from error


def closed_object(value, fields):
    if not isinstance(value, dict) or set(value) != set(fields):
        raise InvalidInput("closed object")


def read_bounded(path, maximum):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as source:
        metadata = os.fstat(source.fileno())
        if not stat.S_ISREG(metadata.st_mode) or not 0 < metadata.st_size <= maximum:
            raise InvalidInput("source file")
        data = source.read(maximum + 1)
    if len(data) > maximum:
        raise InvalidInput("source size")
    return data


def tokens(text):
    result = set(re.findall(r"[a-z0-9]+", text.lower()))
    for run in re.findall(r"[\u3400-\u9fff]+", text):
        result.update(run[index:index + 2] for index in range(len(run) - 1))
    return result


class Corpus:
    def __init__(self, directory=CORPUS_DIRECTORY):
        directory = Path(directory)
        if directory.is_symlink() or not directory.is_dir():
            raise InvalidInput("corpus directory")
        manifest_bytes = read_bounded(directory / "manifest.json", MAX_MANIFEST_BYTES)
        if digest(manifest_bytes) != MANIFEST_DIGEST:
            raise InvalidInput("manifest identity")
        manifest = strict_json(manifest_bytes, MAX_MANIFEST_BYTES)
        closed_object(manifest, ("schema_version", "documents"))
        if type(manifest["schema_version"]) is not int or manifest["schema_version"] != 1:
            raise InvalidInput("manifest version")
        documents = manifest["documents"]
        if not isinstance(documents, list) or len(documents) != len(SOURCE_PATHS):
            raise InvalidInput("source set")
        self.paragraphs = []
        self.documents = {}
        total = 0
        for document, expected_path in zip(documents, SOURCE_PATHS):
            closed_object(document, ("path", "source_uri", "source_revision",
                                     "content_digest", "utf8_bytes"))
            uri = ("https://github.com/yimuu/insight-agent-platform/blob/"
                   + SOURCE_REVISION + "/" + expected_path)
            if (document["path"] != expected_path or document["source_uri"] != uri
                    or document["source_revision"] != SOURCE_REVISION):
                raise InvalidInput("source identity")
            data = read_bounded(directory / Path(expected_path).name, MAX_CORPUS_BYTES)
            total += len(data)
            if (total > MAX_CORPUS_BYTES or len(data) != document["utf8_bytes"]
                    or digest(data) != document["content_digest"]):
                raise InvalidInput("source bytes")
            text = data.decode("utf-8")
            if "\r" in text or "\0" in text:
                raise InvalidInput("source encoding")
            lines = text.splitlines(keepends=True)
            self.documents[uri] = (document, lines)
            start, paragraph, size = 1, [], 0
            for line_number, line in enumerate(lines, 1):
                length = len(line.encode("utf-8"))
                if length > MAX_PARAGRAPH_BYTES:
                    raise InvalidInput("source line size")
                if not line.strip() or size + length > MAX_PARAGRAPH_BYTES:
                    if paragraph:
                        self.add_paragraph(document, start, line_number - 1, "".join(paragraph))
                    paragraph, size = [], 0
                if line.strip():
                    if not paragraph:
                        start = line_number
                    paragraph.append(line)
                    size += length
            if paragraph:
                self.add_paragraph(document, start, len(lines), "".join(paragraph))

    def add_paragraph(self, document, start, end, text):
        if len(self.paragraphs) >= MAX_PARAGRAPHS:
            raise InvalidInput("paragraph count")
        uri = document["source_uri"]
        locator = f"{uri}#L{start}-L{end}"
        item = {
            "source_identity": locator,
            "content": text,
            "structured_fields": {
                "source_uri": uri, "source_revision": SOURCE_REVISION,
                "raw_content_digest": document["content_digest"],
                "start_line": start, "end_line": end,
            },
            "score_millionths": 0,
            "locator": locator,
            "display_label": f"{document['path']}:{start}-{end}",
            "classification": "public",
        }
        self.paragraphs.append((tokens(text), item))

    def query(self, body):
        request = strict_json(body, MAX_REQUEST_BYTES)
        closed_object(request, ("schema_version", "query", "normalized_query_digest",
                                "normalized_filter_digest", "requested_projection",
                                "page_size", "cursor_digest"))
        query = request["query"]
        closed_object(query, ("question",))
        question = query["question"]
        page_size = request["page_size"]
        if (type(request["schema_version"]) is not int or request["schema_version"] != 1
                or not isinstance(question, str) or not question.strip()
                or len(question.encode("utf-8")) > MAX_QUESTION_BYTES
                or any(ord(char) < 32 or ord(char) == 127 for char in question)
                or request["normalized_query_digest"] != digest(canonical(query))
                or request["normalized_filter_digest"] != digest(canonical({"schema_version": 1, "filter": None}))
                or request["requested_projection"] != [] or request["cursor_digest"] is not None
                or type(page_size) is not int or not 1 <= page_size <= MAX_RESULTS):
            raise InvalidInput("unsupported query")
        terms = tokens(question)
        ranked = []
        for words, item in self.paragraphs:
            overlap = len(words & terms)
            if overlap:
                score = overlap * 1_000_000 // len(terms)
                ranked.append((score, item))
        ranked.sort(key=lambda pair: (-pair[0], pair[1]["locator"]))
        response = {
            "schema_version": 1,
            "items": [dict(item, score_millionths=score) for score, item in ranked[:page_size]],
            "next_cursor_digest": None,
            "remote_revision_digest": MANIFEST_DIGEST,
        }
        data = canonical(response)
        if len(data) > MAX_RESPONSE_BYTES:
            raise InvalidInput("response size")
        return data


def remaining(deadline):
    seconds = deadline - time.monotonic()
    if seconds <= 0:
        raise TimeoutError("request deadline")
    return seconds


def read_request(connection, deadline):
    data = bytearray()
    while b"\r\n\r\n" not in data:
        connection.settimeout(remaining(deadline))
        chunk = connection.recv(min(2_048, MAX_HEADER_BYTES - len(data)))
        if not chunk:
            raise InvalidInput("incomplete headers")
        data.extend(chunk)
        if len(data) >= MAX_HEADER_BYTES and b"\r\n\r\n" not in data:
            raise InvalidInput("header size")
    raw_headers, body = bytes(data).split(b"\r\n\r\n", 1)
    lines = raw_headers.decode("ascii").split("\r\n")
    if lines[0] != "POST /v1/query HTTP/1.1" or len(lines) > 33:
        raise InvalidInput("HTTP request")
    headers = {}
    for line in lines[1:]:
        name, separator, value = line.partition(":")
        name = name.lower()
        if (not separator or not re.fullmatch(r"[a-z0-9!#$%&'*+.^_`|~-]+", name)
                or name in headers or any(ord(char) < 32 or ord(char) == 127 for char in value)):
            raise InvalidInput("HTTP headers")
        headers[name] = value.strip()
    length = headers.get("content-length", "")
    if (not headers.get("host") or headers.get("content-type") != "application/json"
            or "content-encoding" in headers or "transfer-encoding" in headers
            or "expect" in headers or not re.fullmatch(r"[0-9]{1,5}", length)
            or not 0 < int(length) <= MAX_REQUEST_BYTES):
        raise InvalidInput("HTTP body framing")
    size = int(length)
    if len(body) > size:
        raise InvalidInput("extra buffered request bytes")
    while len(body) < size:
        connection.settimeout(remaining(deadline))
        chunk = connection.recv(size - len(body))
        if not chunk:
            raise InvalidInput("incomplete body")
        body += chunk
    return body


class SearchServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    request_queue_size = MAX_IN_FLIGHT
    allow_reuse_address = False
    daemon_threads = False

    def __init__(self, address, corpus, tls):
        self.corpus = corpus
        self.tls = tls
        self.permits = threading.BoundedSemaphore(MAX_IN_FLIGHT)
        super().__init__(address, SearchHandler)

    def process_request(self, request, client_address):
        if not self.permits.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request((request, time.monotonic() + REQUEST_SECONDS), client_address)
        except BaseException:
            self.permits.release()
            self.shutdown_request(request)
            raise

    def process_request_thread(self, request, client_address):
        connection, _ = request
        try:
            self.finish_request(request, client_address)
        finally:
            self.shutdown_request(connection)
            self.permits.release()


class SearchHandler(socketserver.BaseRequestHandler):
    def handle(self):
        raw_connection, deadline = self.request
        connection = None
        status = 400
        try:
            raw_connection.settimeout(remaining(deadline))
            connection = self.server.tls.wrap_socket(raw_connection, server_side=True,
                                                     do_handshake_on_connect=False)
            connection.settimeout(remaining(deadline))
            connection.do_handshake()
            try:
                body = self.server.corpus.query(read_request(connection, deadline))
                status = 200
            except (InvalidInput, UnicodeError):
                body = b'{"error":"invalid_request"}'
            connection.settimeout(remaining(deadline))
            headers = (f"HTTP/1.1 {status} {'OK' if status == 200 else 'Bad Request'}\r\n"
                       "Content-Type: application/json\r\nConnection: close\r\n"
                       f"Content-Length: {len(body)}\r\n\r\n").encode("ascii")
            connection.sendall(headers + body)
        except (OSError, TimeoutError):
            pass
        finally:
            if connection is not None:
                connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8443)
    parser.add_argument("--certificate", type=Path)
    parser.add_argument("--private-key", type=Path)
    parser.add_argument("--query-stdin", action="store_true", help="one bounded local wire check; no network")
    args = parser.parse_args()
    try:
        corpus = Corpus()
        if args.query_stdin:
            sys.stdout.buffer.write(corpus.query(sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1)))
            return
        if args.certificate is None or args.private_key is None:
            parser.error("HTTPS requires --certificate and --private-key")
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.minimum_version = ssl.TLSVersion.TLSv1_2
        tls.set_alpn_protocols(["http/1.1"])
        tls.load_cert_chain(args.certificate, args.private_key)
        with SearchServer((args.host, args.port), corpus, tls) as server:
            server.serve_forever()
    except (InvalidInput, OSError, UnicodeError):
        print("document search failed validation or I/O", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
