#!/usr/bin/env python3
"""Qualify static, bucket-scoped SeaweedFS identities in a fresh S3-only fixture.

Exact nonempty generations remain the Artifact Broker contract. The empty versionId test
records the provider's delete-marker behavior; it does not qualify per-generation IAM.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import re
import secrets
import signal
import subprocess
import tempfile
import uuid

SPEC = importlib.util.spec_from_file_location("s3_physical_fixture", Path(__file__).with_name("qualify-platform-installation-s3.py"))
BASE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BASE)
ROLES = ("initializer", "artifact-gateway", "artifact-data", "artifact-maintenance", "qualification-reader")
SAFE_CODES = frozenset({"iam_input", "iam_readiness", "iam_bucket_create", "iam_versioning", "iam_tagging", "iam_cors",
    "iam_readiness_role", "iam_put", "iam_replay", "iam_exact_read", "iam_head_latest", "iam_denial", "iam_presign",
    "iam_http", "iam_empty_query_missing", "iam_empty_delete", "iam_marker_missing", "iam_empty_version_destroyed",
    "iam_exact_delete", "iam_other_version_destroyed", "iam_signature_query", "iam_anonymous", "iam_evidence",
    "iam_restarted_read", "iam_multipart", "iam_admin_api", "iam_readonly_evidence"})


def read_diagnostics(raw: str) -> list[dict[str, str]]:
    matches = re.findall(r'^IAM_READ object=(main|data|presigned|maintenance) operation=(head|get|body) class=(forbidden|not_found|service|other_status|timeout|dispatch|unknown|evidence)$', raw, re.MULTILINE)
    return [{"object": item[0], "operation": item[1], "class": item[2]} for item in matches[:8]]


def configuration(bucket: str, credentials: dict) -> dict:
    """Independent physical conformance fixture, including a read-only test identity."""
    bucket_arn, object_arn = "arn:aws:s3:::" + bucket, "arn:aws:s3:::" + bucket + "/v1/*"
    identities, policies = [], []
    for role in ROLES:
        statements = [
            {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": [bucket_arn],
             "Condition": {"StringEquals": {"s3:RequestMethod": "HEAD"}}},
            {"Effect": "Allow", "Action": ["s3:GetBucketVersioning"], "Resource": [bucket_arn]},
        ]
        if role == "initializer":
            statements.append({"Effect": "Allow", "Action": ["s3:CreateBucket", "s3:GetBucketTagging", "s3:PutBucketTagging",
                "s3:PutBucketVersioning", "s3:GetBucketCors", "s3:PutBucketCors"], "Resource": [bucket_arn]})
        else:
            reads = ["s3:GetObjectVersion"] if role == "artifact-maintenance" else ["s3:GetObject", "s3:GetObjectVersion"]
            statements.append({"Effect": "Allow", "Action": reads, "Resource": [object_arn],
                               "Condition": {"StringEquals": {"s3:RequestMethod": ["GET", "HEAD"]}}})
            if role in {"artifact-gateway", "artifact-data"}:
                statements.append({"Effect": "Allow", "Action": ["s3:PutObject"], "Resource": [object_arn],
                                   "Condition": {"StringEquals": {"s3:RequestMethod": "PUT"}}})
            if role == "artifact-maintenance":
                statements.append({"Effect": "Allow", "Action": ["s3:DeleteObjectVersion"], "Resource": [object_arn],
                                   "Condition": {"StringEquals": {"s3:RequestMethod": "DELETE"}}})
        policies.append({"name": role, "content": json.dumps({"Version": "2012-10-17", "Statement": statements}, separators=(",", ":"))})
        identities.append({"name": role, "actions": [], "policyNames": [role], "credentials": [credentials[role]]})
    return {"identities": identities, "policies": policies}


class Fixture(BASE.Fixture):
    def __init__(self, root: Path):
        super().__init__(root)
        # Docker names retain the constructor's qualification namespace; the bucket uses
        # the actual closed producer's nonce format.
        self.prefix = "insight-platform-artifacts-" + self.nonce

    def s3_configuration(self, access_key: str, secret_key: str) -> dict:
        self.identities = {role: {"accessKey": secrets.token_hex(16), "secretKey": secrets.token_hex(32)} for role in ROLES}
        self.identities["initializer"] = {"accessKey": access_key, "secretKey": secret_key}
        for role in ROLES[:-1]:
            credential = self.identities[role]
            BASE.write_private(self.root / ("s3-" + role + "-credentials"),
                               f"[default]\naws_access_key_id={credential['accessKey']}\naws_secret_access_key={credential['secretKey']}\n".encode())
        BASE.write_private(self.root / "profile-input.json", json.dumps({"schema_version": 1, "bucket": self.prefix}).encode())
        result = BASE.invoke(["cargo", "test", "--locked", "-q", "-p", "insight-platform-deployment-tooling",
                              "--test", "s3_profile_material", "export_actual_s3_profile", "--", "--ignored", "--exact"],
                             600, env=BASE.environment() | {"INSIGHT_S3_PROFILE_FIXTURE_DIRECTORY": str(self.root)}, check=False)
        if result.returncode:
            raise BASE.QualificationError("shared_iam_producer_failed")
        actual = json.loads((self.root / "profile-s3.json").read_bytes())
        expected = configuration(self.prefix, self.identities)
        # Preserve the exact shared producer's four identities; only a read-only test identity
        # is added, with no management or mutation permissions.
        for key in ("identities", "policies"):
            actual[key].sort(key=lambda value: value["name"])
            wanted = sorted(expected[key][:-1], key=lambda value: value["name"])
            if key == "policies":
                equivalent = [(item["name"], json.loads(item["content"])) for item in actual[key]] == [
                    (item["name"], json.loads(item["content"])) for item in wanted]
            else:
                equivalent = actual[key] == wanted
            if not equivalent:
                raise BASE.QualificationError("shared_iam_producer_drift")
            actual[key].append(expected[key][-1])
        if json.loads((self.root / "profile-arguments.json").read_bytes()) != BASE.server_arguments():
            raise BASE.QualificationError("shared_iam_arguments_drift")
        return actual

    def prepare(self) -> None:
        super().prepare()
        if (self.root / "profile-security.toml").read_bytes() != (self.root / "tls/security.toml").read_bytes():
            raise BASE.QualificationError("shared_iam_tls_drift")
        self.check("actual_shared_four_identity_profile")

    def iam(self, mode: str) -> None:
        self.phase = "iam_" + mode
        print(json.dumps({"event": "phase_started", "phase": self.phase}), flush=True)
        path = self.root / ("iam-input-" + uuid.uuid4().hex + ".json")
        BASE.write_private(path, json.dumps({"base": self.input | {"endpoint": self.endpoint, "mode": mode},
                                            "credentials": self.identities}).encode())
        env = BASE.environment() | {"INSIGHT_S3_IAM_FIXTURE_INPUT": str(path), "SSL_CERT_FILE": str(self.root / "tls/ca.pem"),
                                   "SSL_CERT_DIR": "/etc/ssl/certs", "AWS_EC2_METADATA_DISABLED": "true"}
        with tempfile.TemporaryFile() as output:
            result = subprocess.run([str(self.executable), "actual_s3_static_iam_contract", "--ignored", "--exact", "--nocapture"],
                                    cwd=BASE.ROOT, env=env, stdin=subprocess.DEVNULL, stdout=output,
                                    stderr=subprocess.STDOUT, timeout=180, check=False)
            output.seek(0)
            raw = output.read(65_537).decode(errors="replace")
        if result.returncode or "test result: ok. 1 passed; 0 failed; 0 ignored;" not in raw:
            self.read_failure_diagnostics = read_diagnostics(raw)
            safe = re.search(r'S3 IAM qualification failed with safe code: "([a-z_]{1,64})"', raw)
            raise BASE.QualificationError(safe.group(1) if safe and safe.group(1) in SAFE_CODES else "iam_failed")
        self.check(self.phase)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = {"schema_version": 1, "image": BASE.IMAGE, "source_commit": BASE.COMMIT,
              "scope": "static_iam_bucket_capability_classes_controlled_recreate",
              "generation_authority": "broker_nonempty_generation_and_exact_response_validation",
              "checks": [], "cleanup": False, "passed": False}
    failure = None
    with tempfile.TemporaryDirectory(prefix="insight-s3-iam-qualification-") as temporary:
        fixture = None
        try:
            fixture = Fixture(Path(temporary).resolve())
            fixture.prepare()
            fixture.start(1)
            fixture.iam("seed")
            fixture.private_configuration()
            fixture.tls_boundaries()
            fixture.controlled_recreate()
            fixture.iam("verify")
            fixture.private_configuration()
            fixture.tls_boundaries()
            evidence = json.loads((fixture.root / "evidence.json").read_bytes())
            report["http_evidence"] = {key: evidence[key] for key in ("empty_delete_status", "empty_delete_marker", "original_versions_retained", "denied_status")}
        except (Exception, KeyboardInterrupt) as error:
            failure = str(error) if isinstance(error, BASE.QualificationError) else "qualification_unavailable"
            report["failure"] = failure
            report["phase"] = fixture.phase if fixture else "compile"
            report["read_diagnostics"] = getattr(fixture, "read_failure_diagnostics", [])
        finally:
            if fixture:
                report["checks"] = fixture.checks
                report["fixture_nonce"] = fixture.nonce
                try:
                    fixture.close()
                    report["cleanup"] = True
                except Exception:
                    failure = "cleanup_failed"
                    report["failure"] = failure
            else:
                report["cleanup"] = True
            required = {"iam_seed", "iam_verify", "actual_shared_four_identity_profile", "immutable_image_and_private_shared_tls_material",
                        "actual_uid_private_files_and_readonly_config", "https_ca_san_and_grpc_separate_ca_client_boundaries",
                        "controlled_stop_and_new_container_same_volumes"}
            report["passed"] = failure is None and report["cleanup"] and required.issubset(report["checks"])
            BASE.write_private(args.report.resolve(), json.dumps(report, indent=2).encode() + b"\n")
    print(json.dumps(report), flush=True)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
