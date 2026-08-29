from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import subprocess
import tempfile
import threading
import unittest


ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "examples/productization/http-resource-lifecycle.sh"
TENANT_TOKEN = "fixture-local-oidc-token"
RESOURCE_ID = "pol_0198f127-3540-7f37-8ad4-91e9a141eb1b"
OPERATION_ID = "job_0198f127-3541-7dc5-b882-f0f66e53452a"
VERSION_ID = "prev_0198f127-3542-77d5-8b07-ae73d9248761"
DEPLOYMENT_ID = "pdep_0198f127-3543-7f7e-9420-a71fb6e4bd60"
DIGEST = "sha256:" + "a" * 64


class LifecycleAuthority(BaseHTTPRequestHandler):
    create_body: dict[str, object] | None = None
    deployment_body: dict[str, object] | None = None
    deployed = False
    operation_reads = 0
    observations: list[dict[str, object]] = []

    def log_message(self, _format: str, *_arguments: object) -> None:
        return

    def trace_id(self) -> str:
        traceparent = self.headers.get("traceparent")
        if traceparent:
            return traceparent.split("-")[1]
        return "33333333333333333333333333333333"

    def read_json(self) -> dict[str, object] | None:
        length = int(self.headers.get("content-length", "0"))
        if length == 0:
            return None
        return json.loads(self.rfile.read(length))

    def respond(
        self,
        status: int,
        body: dict[str, object],
        *,
        etag: str | None = None,
        location: str | None = None,
    ) -> None:
        encoded = json.dumps(body, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("cache-control", "no-store, private, max-age=0")
        self.send_header("trace-id", self.trace_id())
        if etag is not None:
            self.send_header("etag", etag)
        if location is not None:
            self.send_header("location", location)
        self.send_header("content-length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    @staticmethod
    def resource(version: int, *, validated: bool) -> dict[str, object]:
        assert LifecycleAuthority.create_body is not None
        validation = None
        if validated:
            validation = {
                "validator_digest": DIGEST,
                "validated_draft_digest": DIGEST,
                "dependency_closure_digest": DIGEST,
                "security_evidence_digest": DIGEST,
                "warnings": [],
            }
        etag = f'"{RESOURCE_ID}-{version}"'
        return {
            "schema_version": 1,
            "resource_id": RESOURCE_ID,
            "resource_kind": "policy",
            "lifecycle_state": "active",
            "gate_state": "enabled",
            "draft_generation": 1,
            "version": version,
            "draft": {
                "display_name": LifecycleAuthority.create_body["display_name"],
                "document": LifecycleAuthority.create_body["document"],
                "validation": validation,
            },
            "etag": etag,
        }

    def observe(self, body: dict[str, object] | None) -> None:
        self.observations.append(
            {
                "method": self.command,
                "path": self.path,
                "receipt": self.headers.get("idempotency-key"),
                "if_match": self.headers.get("if-match"),
                "authorization": self.headers.get("authorization"),
                "body": body,
            }
        )

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
        body = self.read_json()
        self.observe(body)
        if self.path == "/v1/policies":
            if self.create_body is None:
                assert body is not None
                type(self).create_body = body
            elif body != self.create_body:
                self.respond(
                    409,
                    {
                        "type_uri": "https://insight.platform/problems/idempotency_conflict",
                        "title": "idempotency conflict",
                        "status": 409,
                        "code": "idempotency_conflict",
                        "detail": "Receipt input differs",
                        "request_id": "req_0198f127-3544-7ba0-929a-873f546989a5",
                        "trace_id": self.trace_id(),
                        "retryable": False,
                        "retry_after_ms": None,
                        "field_errors": [],
                    },
                )
                return
            resource = self.resource(1, validated=False)
            self.respond(
                201,
                resource,
                etag=str(resource["etag"]),
                location=f"/v1/policies/{RESOURCE_ID}",
            )
            return
        if self.path == f"/v1/policies/{RESOURCE_ID}/draft:validate":
            etag = f'"{OPERATION_ID}-1"'
            self.respond(
                202,
                {
                    "operation_id": OPERATION_ID,
                    "tenant_id": "ten_0198f127-3545-7e84-9ba3-6d09f4173382",
                    "kind": "resource_validation",
                    "target": {
                        "kind": "resource_version",
                        "resource_id": RESOURCE_ID,
                        "resource_version": 1,
                    },
                    "state": "queued",
                    "progress": None,
                    "result": None,
                    "error": None,
                    "created_at": "2026-08-29T00:00:00.000000Z",
                    "updated_at": "2026-08-29T00:00:00.000000Z",
                    "etag": etag,
                },
                etag=etag,
                location=f"/v1/operations/{OPERATION_ID}",
            )
            return
        if self.path == f"/v1/policies/{RESOURCE_ID}/draft:publish":
            etag = f'"{RESOURCE_ID}-3"'
            self.respond(
                200,
                {
                    "schema_version": 1,
                    "resource_id": RESOURCE_ID,
                    "resource_kind": "policy",
                    "draft_generation": 1,
                    "version": 3,
                    "published_versions": [
                        {
                            "resource_version_id": VERSION_ID,
                            "revision_no": 1,
                            "content_digest": DIGEST,
                            "artifact_id": None,
                            "etag": f'"{VERSION_ID}-{DIGEST}"',
                        }
                    ],
                    "etag": etag,
                },
                etag=etag,
            )
            return
        if self.path == f"/v1/policies/{RESOURCE_ID}/deployments":
            assert body is not None
            type(self).deployment_body = body
            type(self).deployed = True
            etag = f'"{DEPLOYMENT_ID}-{DIGEST}"'
            self.respond(
                201,
                {
                    "schema_version": 1,
                    "deployment_id": DEPLOYMENT_ID,
                    "resource_id": RESOURCE_ID,
                    "resource_kind": "policy",
                    "resource_version_id": body["resource_version_id"],
                    "environment": body["environment"],
                    "closure_digest": DIGEST,
                    "closure": body["closure"],
                    "created_at": "2026-08-29T00:00:03.000000Z",
                    "etag": etag,
                },
                etag=etag,
                location=f"/v1/policies/{RESOURCE_ID}/deployments/{DEPLOYMENT_ID}",
            )
            return
        if self.path == (
            f"/v1/policies/{RESOURCE_ID}/deployments/{DEPLOYMENT_ID}:activate"
        ):
            resource = self.resource(5, validated=True)
            self.respond(200, resource, etag=str(resource["etag"]))
            return
        self.send_error(404)

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
        self.observe(None)
        if self.path == f"/v1/operations/{OPERATION_ID}":
            type(self).operation_reads += 1
            state = "running" if self.operation_reads == 1 else "succeeded"
            version = self.operation_reads + 1
            etag = f'"{OPERATION_ID}-{version}"'
            self.respond(
                200,
                {
                    "operation_id": OPERATION_ID,
                    "tenant_id": "ten_0198f127-3545-7e84-9ba3-6d09f4173382",
                    "kind": "resource_validation",
                    "target": {
                        "kind": "resource_version",
                        "resource_id": RESOURCE_ID,
                        "resource_version": 1,
                    },
                    "state": state,
                    "progress": None,
                    "result": {"result_digest": DIGEST} if state == "succeeded" else None,
                    "error": None,
                    "created_at": "2026-08-29T00:00:00.000000Z",
                    "updated_at": "2026-08-29T00:00:02.000000Z",
                    "etag": etag,
                },
                etag=etag,
            )
            return
        if self.path == f"/v1/policies/{RESOURCE_ID}":
            resource = self.resource(4 if self.deployed else 2, validated=True)
            self.respond(200, resource, etag=str(resource["etag"]))
            return
        self.send_error(404)


class ProductizationHttpLifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        LifecycleAuthority.create_body = None
        LifecycleAuthority.deployment_body = None
        LifecycleAuthority.deployed = False
        LifecycleAuthority.operation_reads = 0
        LifecycleAuthority.observations = []

    def test_checked_curl_fixture_executes_exact_lifecycle_and_problem_probe(self) -> None:
        server = ThreadingHTTPServer(("127.0.0.1", 0), LifecycleAuthority)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                project = Path(directory)
                (project / ".insight/runtime").mkdir(parents=True)
                (project / ".insight/identity").mkdir(parents=True)
                (project / ".insight/runtime/profile.json").write_text(
                    json.dumps(
                        {
                            "ports": {
                                "gateway_management": server.server_address[1],
                            }
                        }
                    ),
                    encoding="utf-8",
                )
                (project / ".insight/identity/developer-access-token.jwt").write_text(
                    TENANT_TOKEN,
                    encoding="utf-8",
                )
                manifest = project / "policy.apply.json"
                manifest.write_text(
                    json.dumps(
                        {
                            "schema_version": 1,
                            "kind": "insight.platform.apply/v1",
                            "resource_noun": "policies",
                            "create": {
                                "display_name": "checked raw HTTP policy",
                                "document": {"resource_kind": "policy", "spec": {}},
                            },
                            "publish": {
                                "kind": "single",
                                "revision_no": 1,
                                "content_digest": DIGEST,
                                "artifact_id": None,
                            },
                            "deployment": {
                                "environment": "local",
                                "closure": {
                                    "resource_kind": "policy",
                                    "bindings": {
                                        "applicability_digest": DIGEST,
                                        "qualification_evidence": {"fixture": True},
                                    },
                                },
                            },
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    encoding="utf-8",
                )
                result = subprocess.run(
                    [
                        "bash",
                        str(FIXTURE),
                        "--project",
                        str(project),
                        "--file",
                        str(manifest),
                        "--timeout-seconds",
                        "5",
                    ],
                    cwd=ROOT,
                    check=False,
                    capture_output=True,
                    text=True,
                )
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(
            report["kind"],
            "insight.platform.http-resource-lifecycle-report/v1",
        )
        self.assertEqual(report["resource_id"], RESOURCE_ID)
        self.assertEqual(report["validation_operation_id"], OPERATION_ID)
        self.assertEqual(report["deployment_id"], DEPLOYMENT_ID)
        self.assertEqual(report["receipt_replay"], "same_effect")
        self.assertEqual(report["conflict_problem"], "idempotency_conflict")
        self.assertNotIn(TENANT_TOKEN, result.stdout)
        self.assertNotIn(TENANT_TOKEN, result.stderr)
        self.assertEqual(LifecycleAuthority.operation_reads, 2)

        posts = [
            observation
            for observation in LifecycleAuthority.observations
            if observation["method"] == "POST"
        ]
        self.assertTrue(posts)
        self.assertTrue(
            all(
                observation["authorization"] == f"Bearer {TENANT_TOKEN}"
                for observation in posts
            )
        )
        validation = next(
            observation
            for observation in posts
            if str(observation["path"]).endswith("draft:validate")
        )
        self.assertEqual(validation["if_match"], f'"{RESOURCE_ID}-1"')
        deployment = next(
            observation
            for observation in posts
            if str(observation["path"]).endswith("deployments")
        )
        self.assertEqual(deployment["if_match"], f'"{RESOURCE_ID}-3"')
        activation = next(
            observation
            for observation in posts
            if str(observation["path"]).endswith(":activate")
        )
        self.assertEqual(activation["if_match"], f'"{RESOURCE_ID}-4"')
        self.assertEqual(
            LifecycleAuthority.deployment_body["closure"]["bindings"]["policy_revision"],
            {
                "revision_id": VERSION_ID,
                "resource_kind": "policy_revision",
                "semantic_digest": DIGEST,
            },
        )

    def test_help_and_unknown_option_are_closed(self) -> None:
        help_result = subprocess.run(
            ["bash", str(FIXTURE), "--help"],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(help_result.returncode, 0, help_result.stderr)
        self.assertIn("curl and jq", help_result.stdout)
        unknown = subprocess.run(
            ["bash", str(FIXTURE), "--unknown"],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(unknown.returncode, 2)


if __name__ == "__main__":
    unittest.main()
