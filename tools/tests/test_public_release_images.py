import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "tools/release/verify-public-release-images.py"
SPEC = importlib.util.spec_from_file_location("public_images", SCRIPT)
PUBLIC = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PUBLIC)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


class PublicImageTests(unittest.TestCase):
    def fixture(self, root):
        images, bundle_images, bodies = {}, [], {}
        for name, suffix in PUBLIC.IMAGE_SUFFIXES.items():
            subject = "ghcr.io/owner/project/" + suffix
            body = encoded({"schemaVersion": 2, "test_subject": subject})
            digest = "sha256:" + hashlib.sha256(body).hexdigest()
            platforms = {"linux/amd64": "sha256:" + "a" * 64, "linux/arm64": "sha256:" + "b" * 64}
            images[name] = {"subject": subject, "index_digest": digest, "platforms": platforms}
            bundle_images.append({"name": name, "subject": subject, "index_digest": digest,
                                  "platforms": [{"platform": key, "digest": value} for key, value in platforms.items()]})
            bodies[subject] = body
        bundle = {"schema_version": 1, "version": "1.2.3", "git_commit": "c" * 40, "images": bundle_images}
        (root / "release-bundle.json").write_bytes(encoded(bundle))
        (root / "images.json").write_bytes(encoded(images))
        return images, bodies

    def exact(self, root):
        return PUBLIC.exact_images(root, "owner/project", "v1.2.3", "c" * 40)

    def test_owning_exact_closure_precedes_any_network_and_rejects_drift(self):
        mutations = ("duplicate", "unknown", "foreign", "digest", "platform", "metadata", "bool", "version")
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                images, _ = self.fixture(root)
                self.assertEqual(self.exact(root), images)
                bundle_path = root / "release-bundle.json"
                bundle = json.loads(bundle_path.read_bytes())
                if mutation == "duplicate": bundle["images"][1] = bundle["images"][0]
                elif mutation == "unknown": bundle["images"][0]["name"] = "other"
                elif mutation == "foreign": bundle["images"][0]["subject"] = "ghcr.io/other/project/platform-runtime"
                elif mutation == "digest": bundle["images"][0]["index_digest"] = "sha256:" + "d" * 64
                elif mutation == "platform": bundle["images"][0]["platforms"][1]["platform"] = "linux/amd64"
                elif mutation == "bool": bundle["schema_version"] = True
                elif mutation == "version": bundle["version"] = "1.2.4"
                elif mutation == "metadata":
                    images["console"]["unexpected"] = True
                    (root / "images.json").write_bytes(encoded(images))
                bundle_path.write_bytes(encoded(bundle))
                with patch.object(PUBLIC, "fetch", side_effect=AssertionError("must not dispatch")):
                    with self.assertRaises(ValueError): self.exact(root)

    def test_anonymous_tokens_are_scoped_and_raw_index_bytes_are_verified(self):
        with tempfile.TemporaryDirectory() as temporary:
            images, bodies = self.fixture(Path(temporary))
            calls = []

            def fetch(path, headers, maximum):
                calls.append((path, headers, maximum))
                if path.startswith("/token?"):
                    scope = parse_qs(urlsplit(path).query)
                    self.assertEqual(scope["service"], ["ghcr.io"])
                    self.assertNotIn("Authorization", headers)
                    return encoded({"token": "anonymous.token", "access_token": "anonymous.token"})
                self.assertEqual(headers["Authorization"], "Bearer anonymous.token")
                repository, digest = path.removeprefix("/v2/").split("/manifests/")
                previous_scope = parse_qs(urlsplit(calls[-2][0]).query)["scope"]
                self.assertEqual(previous_scope, [f"repository:{repository}:pull"])
                body = bodies["ghcr.io/" + repository]
                self.assertEqual(digest, "sha256:" + hashlib.sha256(body).hexdigest())
                return body

            with patch.dict(os.environ, {"GH_TOKEN": "not-for-registry", "DOCKER_CONFIG": "/unused", "HTTP_PROXY": "http://unused"}), patch.object(PUBLIC, "fetch", side_effect=fetch):
                report = PUBLIC.verify_indexes(images)
            self.assertEqual(report["status"], "passed")
            self.assertEqual(report["evidence"], "exact_index_anonymous_accessibility")
            self.assertEqual({item["image"] for item in report["indexes"]}, set(bodies))
            self.assertNotIn("anonymous.token", json.dumps(report))

    def test_bad_token_digest_and_unknown_access_fail_without_login_fallback(self):
        with tempfile.TemporaryDirectory() as temporary:
            images, bodies = self.fixture(Path(temporary))
            bad_tokens = [{}, {"token": "a", "access_token": "b"}, {"token": "line\r\nbreak"}, {"token": "x" * 8193}, {"token": None}]
            bad_tokens += [dict(token="anonymous", **{key: value}) for key, value in [
                ("unexpected", True), ("refresh_token", "unrequested"), ("expires_in", False),
                ("expires_in", 0), ("expires_in", -1), ("expires_in", PUBLIC.MAX_SAFE_JSON_INTEGER + 1),
                ("issued_at", ["not-a-timestamp"]), ("issued_at", "2026-02-30T00:00:00Z"),
                ("issued_at", "2026-09-07 12:00:00Z"), ("issued_at", "2026-09-07T00:00:00"),
                ("issued_at", "2026-09-07T00:00:00+99:99"),
                ("issued_at", "2026-09-07T00:00:00+01:99"),
            ]]
            for token in bad_tokens:
                with self.subTest(token=repr(token)[:50]), patch.object(PUBLIC, "fetch", return_value=encoded(token)) as fetch:
                    with self.assertRaises(PUBLIC.Rejected): PUBLIC.verify_indexes(images)
                    self.assertEqual(fetch.call_count, 1)
            with patch.object(PUBLIC, "fetch", side_effect=[encoded({"access_token": "anonymous"}), b"different bytes"]):
                with self.assertRaisesRegex(PUBLIC.Rejected, "index_digest_rejected"): PUBLIC.verify_indexes(images)
            with patch.object(PUBLIC, "fetch", side_effect=[encoded({"token": "wrong.scope"}), PUBLIC.Rejected("anonymous_access_denied")]) as fetch:
                with self.assertRaisesRegex(PUBLIC.Rejected, "anonymous_access_denied"): PUBLIC.verify_indexes(images)
                self.assertEqual(fetch.call_count, 2)

    def test_standard_optional_token_fields_do_not_use_platform_timestamp_format(self):
        for issued in ("2026-09-07T12:34:56Z", "2026-09-07T12:34:56.123456789Z", "2026-09-07t12:34:56z",
                       "2026-09-07T20:34:56+08:00", "2016-12-31T23:59:60Z"):
            with self.subTest(issued=issued):
                self.assertEqual(PUBLIC.anonymous_token({"access_token": "anonymous", "expires_in": 3600, "issued_at": issued}), "anonymous")

    def test_http_status_redirect_encoding_bounds_and_connection_cleanup(self):
        class Response:
            status = 200
            body = b"abc"
            headers = {"Content-Length": "3"}
            def getheader(self, name): return self.headers.get(name)
            def read(self, maximum): return self.body[:maximum]

        class Connection:
            def __init__(self, host, **kwargs):
                self.host = host
                self.closed = False
            def request(self, method, path, headers):
                self.requested = method, path, headers
            def getresponse(self): return response
            def close(self): self.closed = True

        for variant in ("ok", "401", "403", "302", "500", "encoding", "length", "oversize", "truncated"):
            with self.subTest(variant=variant):
                response = Response()
                if variant.isdecimal(): response.status = int(variant)
                elif variant == "encoding": response.headers = {"Content-Encoding": "gzip"}
                elif variant == "length": response.headers = {"Content-Length": "3, 3"}
                elif variant == "oversize": response.body, response.headers = b"x" * 5, {}
                elif variant == "truncated": response.headers = {"Content-Length": "4"}
                connection = Connection("ghcr.io")
                with patch.object(PUBLIC.http.client, "HTTPSConnection", return_value=connection) as constructor:
                    if variant == "ok": self.assertEqual(PUBLIC.fetch("/token?scope=bounded", {}, 4), b"abc")
                    else:
                        with self.assertRaises(PUBLIC.Rejected): PUBLIC.fetch("/token?scope=bounded", {}, 4)
                self.assertEqual(constructor.call_args.args, ("ghcr.io",))
                self.assertEqual(connection.requested[0], "GET")
                self.assertNotIn("Authorization", connection.requested[2])
                self.assertTrue(connection.closed)

    def test_real_subprocess_drops_credentials_and_enforces_total_deadline(self):
        code = '''import json,os
assert all(name not in os.environ for name in ("GH_TOKEN","GITHUB_TOKEN","DOCKER_CONFIG","HTTPS_PROXY","SSL_CERT_FILE","PYTHONPATH"))
print(json.dumps({"status":"passed"}))
'''
        polluted = {name: "test-value" for name in ("GH_TOKEN", "GITHUB_TOKEN", "DOCKER_CONFIG", "HTTPS_PROXY", "SSL_CERT_FILE", "PYTHONPATH")}
        with patch.dict(os.environ, polluted):
            self.assertEqual(PUBLIC.supervise([sys.executable, "-I", "-c", code])["status"], "passed")
        started = time.monotonic()
        with self.assertRaisesRegex(PUBLIC.Rejected, "total_deadline_exceeded"):
            PUBLIC.supervise([sys.executable, "-I", "-c", "import time; time.sleep(30)"], seconds=0.15)
        self.assertLess(time.monotonic() - started, 1)

    def test_total_deadline_also_kills_an_actual_slow_http_header_read(self):
        code = '''import http.client,http.server,runpy,sys,threading,time
module=runpy.run_path(sys.argv[1],run_name="deadline_fixture")
class SlowHeaders(http.server.BaseHTTPRequestHandler):
 def do_GET(self):
  self.connection.sendall(b"HTTP/1.1 200 OK\\r\\n")
  while True:
   self.connection.sendall(b"X-Slow: waiting\\r\\n")
   time.sleep(0.05)
 def log_message(self,*args): pass
server=http.server.HTTPServer(("127.0.0.1",0),SlowHeaders)
threading.Thread(target=server.serve_forever,daemon=True).start()
# The fixture changes only the transport endpoint; production fixes HTTPS/ghcr.io.
original=http.client.HTTPConnection
http.client.HTTPSConnection=lambda host,**kwargs: original("127.0.0.1",server.server_port,timeout=10)
module["fetch"]("/token?fixture",{},16384)
'''
        started = time.monotonic()
        with self.assertRaisesRegex(PUBLIC.Rejected, "total_deadline_exceeded"):
            PUBLIC.supervise([sys.executable, "-I", "-c", code, str(SCRIPT)], seconds=0.25)
        self.assertLess(time.monotonic() - started, 1)

    def test_json_and_safe_main_failure_do_not_expose_inputs(self):
        for data in (b'{"token":"x","token":"y"}', b'{"token":NaN}', b'\xff'):
            with self.assertRaises(PUBLIC.Rejected): PUBLIC.strict_json(data, PUBLIC.MAX_TOKEN_BYTES)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.fixture(root)
            result = subprocess.run([sys.executable, str(SCRIPT), "--assets", str(root), "--repository", "owner/project",
                                     "--release-tag", "v9.9.9", "--revision", "c" * 40], capture_output=True, timeout=5)
            self.assertEqual(result.returncode, 1)
            self.assertEqual(json.loads(result.stdout), {"status": "failed", "reason": "candidate_identity_rejected"})
            self.assertEqual(result.stderr, b"")

    def test_protocol_json_requires_an_object_root_without_relying_on_recursion_limits(self):
        self.assertEqual(PUBLIC.strict_json(b'{"token":"valid"}', PUBLIC.MAX_TOKEN_BYTES),
                         {"token": "valid"})
        for data in (b'null', b'true', b'false', b'1', b'"token"', b'[]',
                     b'[{}]', b'[' * 1000 + b']' * 1000):
            with self.subTest(data_kind=data[:8]), self.assertRaisesRegex(PUBLIC.Rejected, "json_rejected"):
                PUBLIC.strict_json(data, PUBLIC.MAX_TOKEN_BYTES)


class AnonymousGateTests(unittest.TestCase):
    def test_pipeline_rejects_missing_skipped_ignored_rebound_and_late_gate(self):
        workflow = (ROOT / ".github/workflows/product-release.yml").read_text()
        match = re.search(r"^      - name: Verify exact candidate indexes are anonymously readable\n.*?(?=^      - |\Z)", workflow, re.MULTILINE | re.DOTALL)
        self.assertIsNotNone(match)
        block = match[0]
        cases = {
            "valid": workflow,
            "missing": workflow.replace(block, ""),
            "conditional": workflow.replace(block, block.replace("        shell: bash", "        if: false\n        shell: bash")),
            "ignored": workflow.replace(block, block.replace('"$GITHUB_SHA"', '"$GITHUB_SHA" || true')),
            "rebound": workflow.replace(block, block.replace('"$GITHUB_REPOSITORY"', '"other/repository"')),
            "late": workflow.replace(block, "") + block,
        }
        files = ["Cargo.toml", "tools/checks/check-product-release.py", "tools/release/build-product-release.py",
                 "tools/development/build-development-profile-performance.py", "apps/console/scripts/build-agent-compiler.ts",
                 "crates/authoring/platform-agent-compiler-wasm/Cargo.toml", ".github/workflows/ci.yml",
                 "deploy/images/console.Dockerfile", "deploy/images/console.Dockerfile.dockerignore", "deploy/release/performance-budgets-v1.json"]
        for name, changed in cases.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                for relative in files:
                    destination = root / relative
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(ROOT / relative, destination)
                (root / ".github/workflows/product-release.yml").write_text(changed)
                result = subprocess.run([sys.executable, str(root / "tools/checks/check-product-release.py")], capture_output=True, timeout=5)
                self.assertEqual(result.returncode == 0, name == "valid", result.stderr)


if __name__ == "__main__":
    unittest.main()
