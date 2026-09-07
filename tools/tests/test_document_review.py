import contextlib
import http.client
import importlib.util
import json
from pathlib import Path
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import time
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = ROOT / "examples/productization/document-review"
spec = importlib.util.spec_from_file_location("document_review", EXAMPLE / "server.py")
search = importlib.util.module_from_spec(spec)
spec.loader.exec_module(search)


def request(question="持久状态 PostgreSQL 人工确认", page_size=8):
    query = {"question": question}
    return {
        "schema_version": 1, "query": query,
        "normalized_query_digest": search.digest(search.canonical(query)),
        "normalized_filter_digest": search.digest(search.canonical({"schema_version": 1, "filter": None})),
        "requested_projection": [], "page_size": page_size, "cursor_digest": None,
    }


class CorpusTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.corpus = search.Corpus()

    def test_real_chinese_corpus_has_stable_exact_line_citations(self):
        wire = search.canonical(request())
        data = self.corpus.query(wire)
        self.assertEqual(data, self.corpus.query(wire))
        response = json.loads(data)
        self.assertEqual(response["remote_revision_digest"], search.MANIFEST_DIGEST)
        self.assertIsNone(response["next_cursor_digest"])
        self.assertGreater(len(response["items"]), 0)
        self.assertLessEqual(len(data), search.MAX_RESPONSE_BYTES)
        for item in response["items"]:
            fields = item["structured_fields"]
            path = fields["source_uri"].split(search.SOURCE_REVISION + "/", 1)[1]
            source = (EXAMPLE / "corpus" / Path(path).name).read_bytes()
            lines = source.decode().splitlines(keepends=True)
            excerpt = "".join(lines[fields["start_line"] - 1:fields["end_line"]])
            self.assertEqual(excerpt, item["content"])
            self.assertEqual(search.digest(source), fields["raw_content_digest"])
            self.assertNotEqual(search.digest(search.canonical(excerpt)), fields["raw_content_digest"])
            self.assertEqual(item["classification"], "public")
            self.assertIn(f"#L{fields['start_line']}-L{fields['end_line']}", item["locator"])
            self.assertGreater(item["score_millionths"], 0)
            self.assertLessEqual(item["score_millionths"], 1_000_000)

    def test_one_result_and_no_match_and_escaped_question(self):
        for question in ["PostgreSQL", '持久状态 "PostgreSQL" \\ 原文']:
            response = json.loads(self.corpus.query(search.canonical(request(question, 1))))
            self.assertEqual(len(response["items"]), 1)
            self.assertTrue(all(not isinstance(value, list) for value in response["items"][0]["structured_fields"].values()))
        empty = json.loads(self.corpus.query(search.canonical(request("zzzzunmatchedzzzz", 1))))
        self.assertEqual(empty["items"], [])

    def test_invalid_query_closed_fields_identity_and_limits(self):
        base = request()
        mutations = [
            ("schema_version", True), ("schema_version", 2), ("page_size", True),
            ("page_size", 0), ("page_size", 9), ("query", {"question": 1}),
            ("query", {"question": ""}), ("query", {"question": "x" * 1025}),
            ("query", {"question": "x\ny"}), ("query", {"question": "x", "url": "https://example.com"}),
            ("normalized_query_digest", "sha256:" + "a" * 64),
            ("normalized_filter_digest", "sha256:" + "b" * 64),
            ("requested_projection", ["title"]), ("cursor_digest", "sha256:" + "c" * 64),
            ("job_id", "foreign"),
        ]
        for key, value in mutations:
            with self.subTest(key=key, value=value):
                changed = dict(base, **{key: value})
                if key == "query":
                    changed["normalized_query_digest"] = search.digest(search.canonical(value))
                with self.assertRaises(search.InvalidInput):
                    self.corpus.query(search.canonical(changed))
        for key in base:
            changed = dict(base)
            changed.pop(key)
            with self.assertRaises(search.InvalidInput):
                self.corpus.query(search.canonical(changed))

    def test_json_duplicates_encoding_numbers_and_depth_are_rejected(self):
        valid = search.canonical(request())
        invalid = [
            b'{"schema_version":1,' + valid[1:],
            valid.replace(b'"page_size":8', b'"page_size":8.0'),
            valid.replace(b'"page_size":8', b'"page_size":NaN'),
            valid.replace(b'"page_size":8', b'"page_size":1e999'),
            valid.replace(b'"page_size":8', b'"page_size":999999999999999'),
            b'\xff', b'{"question":"\ud800"}', b'[' * 1000 + b']' * 1000,
            valid + b' ' * search.MAX_REQUEST_BYTES,
        ]
        for data in invalid:
            with self.subTest(data=data[:60]), self.assertRaises(search.InvalidInput):
                self.corpus.query(data)

    def test_frozen_manifest_and_source_tamper_symlinks_are_rejected(self):
        for variant in ("bytes", "manifest", "symlink", "missing", "directory"):
            with self.subTest(variant=variant), tempfile.TemporaryDirectory() as temporary:
                destination = Path(temporary) / "corpus"
                shutil.copytree(EXAMPLE / "corpus", destination)
                source = destination / "architecture.md"
                if variant == "bytes":
                    source.write_bytes(source.read_bytes() + b"\ntampered\n")
                elif variant == "manifest":
                    manifest = destination / "manifest.json"
                    manifest.write_bytes(manifest.read_bytes() + b" ")
                else:
                    source.unlink()
                    if variant == "symlink":
                        source.symlink_to(EXAMPLE / "corpus/architecture.md")
                    elif variant == "directory":
                        source.mkdir()
                with self.assertRaises((search.InvalidInput, OSError)):
                    search.Corpus(destination)

    def test_real_stdin_entrypoint_is_bounded_and_clean(self):
        result = subprocess.run(["python3", str(EXAMPLE / "server.py"), "--query-stdin"],
                                input=search.canonical(request("PostgreSQL", 1)), capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(json.loads(result.stdout)["items"]), 1)
        rejected = subprocess.run(["python3", str(EXAMPLE / "server.py"), "--query-stdin"],
                                  input=b"x" * (search.MAX_REQUEST_BYTES + 1), capture_output=True, timeout=5)
        self.assertNotEqual(rejected.returncode, 0)
        self.assertEqual(rejected.stdout, b"")
        self.assertNotIn(b"xxxxxxxx", rejected.stderr)


class HttpsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix="insight-document-tls-")
        cls.addClassCleanup(cls.directory.cleanup)
        root = Path(cls.directory.name)
        commands = [
            ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=Document Test CA",
             "-keyout", "ca.key", "-out", "ca.pem", "-addext", "basicConstraints=critical,CA:TRUE"],
            ["req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=localhost",
             "-keyout", "server.key", "-out", "server.csr"],
            ["x509", "-req", "-in", "server.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial",
             "-days", "1", "-out", "server.pem", "-extfile", "extensions.cnf"],
        ]
        (root / "extensions.cnf").write_text("subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n")
        for command in commands:
            subprocess.run(["openssl"] + command, cwd=root, check=True, capture_output=True, timeout=15)
        cls.tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        cls.tls.minimum_version = ssl.TLSVersion.TLSv1_2
        cls.tls.load_cert_chain(root / "server.pem", root / "server.key")
        cls.trust = ssl.create_default_context(cafile=str(root / "ca.pem"))
        cls.corpus = search.Corpus()

    @contextlib.contextmanager
    def server(self, seconds=search.REQUEST_SECONDS):
        with patch.object(search, "REQUEST_SECONDS", seconds):
            with search.SearchServer(("127.0.0.1", 0), self.corpus, self.tls) as server:
                worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02})
                worker.start()
                try:
                    yield server
                finally:
                    server.shutdown()
                    worker.join(timeout=6)
                    self.assertFalse(worker.is_alive())

    def connect(self, server, trust=None, hostname="localhost"):
        raw = socket.create_connection(server.server_address, timeout=2)
        try:
            return (trust or self.trust).wrap_socket(raw, server_hostname=hostname)
        except BaseException:
            raw.close()
            raise

    def raw_request(self, body, extra=b""):
        return (b"POST /v1/query HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n"
                + f"Content-Length: {len(body)}\r\n".encode() + extra + b"\r\n" + body)

    def response(self, connection):
        response = http.client.HTTPResponse(connection)
        response.begin()
        body = response.read(search.MAX_RESPONSE_BYTES + 1)
        self.assertEqual(response.getheader("Connection"), "close")
        self.assertEqual(len(body), int(response.getheader("Content-Length")))
        return response.status, body

    def test_real_https_exact_reply_and_empty_results(self):
        with self.server() as server:
            for question in ['持久状态 "PostgreSQL"', "zzzzunmatchedzzzz"]:
                body = search.canonical(request(question, 1))
                with self.connect(server) as connection:
                    connection.sendall(self.raw_request(body))
                    status, response = self.response(connection)
                    self.assertEqual(status, 200)
                    self.assertEqual(response, self.corpus.query(body))

    def test_real_https_rejects_wrong_ca_and_wrong_san(self):
        with self.server() as server:
            with self.assertRaises(ssl.SSLCertVerificationError):
                self.connect(server, ssl.create_default_context())
            with self.assertRaises(ssl.SSLCertVerificationError):
                self.connect(server, hostname="wrong.example.test")
            with self.connect(server) as connection:
                connection.sendall(self.raw_request(search.canonical(request())))
                self.assertEqual(self.response(connection)[0], 200)

    def test_real_https_strict_framing_and_unknown_queries(self):
        valid = self.raw_request(search.canonical(request()))
        invalid = [
            valid.replace(b"POST /v1/query", b"GET /v1/query"),
            valid.replace(b"POST /v1/query", b"POST /v1/query?url=https://example.com"),
            valid.replace(b"Host: localhost", b"Host: localhost\r\nhost: localhost"),
            valid.replace(b"Content-Type: application/json", b"Content-Type: text/plain"),
            self.raw_request(b"{}", b"Content-Encoding: gzip\r\n"),
            self.raw_request(b"{}", b"Transfer-Encoding: chunked\r\n"),
            self.raw_request(b"{}", b"Expect: 100-continue\r\n"),
            self.raw_request(b"{}", b"Content-Length: 2\r\n"),
            self.raw_request(b"{}") + b"SECOND REQUEST",
            self.raw_request(b"{}"),
            b"POST /v1/query HTTP/1.1\r\nX: " + b"x" * search.MAX_HEADER_BYTES,
        ]
        with self.server() as server:
            for wire in invalid:
                with self.subTest(wire=wire[:100]), self.connect(server) as connection:
                    connection.sendall(wire)
                    status, body = self.response(connection)
                    self.assertEqual(status, 400)
                    self.assertEqual(body, b'{"error":"invalid_request"}')

    def test_absolute_deadline_covers_handshake_and_slow_headers_and_body(self):
        with self.server(seconds=0.35) as server:
            for phase in ("handshake", "headers", "body"):
                with self.subTest(phase=phase):
                    started = time.monotonic()
                    connection = (socket.create_connection(server.server_address, timeout=2)
                                  if phase == "handshake" else self.connect(server))
                    with connection:
                        if phase == "body":
                            connection.sendall(self.raw_request(b"0123456789")[:-10])
                        if phase != "handshake":
                            for _ in range(3):
                                connection.sendall(b"x")
                                time.sleep(0.1)
                        self.assertEqual(connection.recv(1), b"")
                    self.assertLess(time.monotonic() - started, 1.0)

    def test_body_truncation_and_capacity_recover(self):
        with self.server(seconds=1.0) as server:
            # TLS connections count before any request headers exist.
            held = [self.connect(server) for _ in range(search.MAX_IN_FLIGHT)]
            try:
                with socket.create_connection(server.server_address, timeout=2) as extra:
                    self.assertEqual(extra.recv(1), b"")
            finally:
                for connection in held:
                    connection.close()
            deadline = time.monotonic() + 2
            while server.permits._value != search.MAX_IN_FLIGHT and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertEqual(server.permits._value, search.MAX_IN_FLIGHT)

            with self.connect(server) as connection:
                connection.sendall(self.raw_request(search.canonical(request("PostgreSQL", 1))))
                self.assertEqual(self.response(connection)[0], 200)
            with self.connect(server) as connection:
                connection.sendall(self.raw_request(b"0123456789")[:-5])
                connection.close()
            deadline = time.monotonic() + 2
            while server.permits._value != search.MAX_IN_FLIGHT and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertEqual(server.permits._value, search.MAX_IN_FLIGHT)

    def test_blocked_diagnostic_sink_cannot_retain_request_capacity(self):
        release = threading.Event()
        entered = threading.Event()

        class BlockedSink:
            def write(self, value):
                entered.set()
                release.wait(timeout=2)
                return len(value)

            def flush(self):
                pass

        with self.server(seconds=0.25) as server:
            try:
                with patch.object(search.sys, "stderr", BlockedSink()):
                    for _ in range(search.MAX_IN_FLIGHT):
                        with self.connect(server) as connection:
                            connection.sendall(self.raw_request(search.canonical(request("PostgreSQL", 1))))
                            self.assertEqual(self.response(connection)[0], 200)
                    deadline = time.monotonic() + 0.5
                    while server.permits._value != search.MAX_IN_FLIGHT and time.monotonic() < deadline:
                        time.sleep(0.01)
                    self.assertFalse(entered.is_set())
                    self.assertEqual(server.permits._value, search.MAX_IN_FLIGHT)
            finally:
                release.set()


if __name__ == "__main__":
    unittest.main()
