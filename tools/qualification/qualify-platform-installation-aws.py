#!/usr/bin/env python3
"""Qualify the existing Artifact AWS adapter on a new, disposable LocalStack instance.

IsolatedAwsFixture can also be imported by another owning qualification: its endpoint,
bucket and key_arn are public physical references. It accepts no existing endpoint or
container. The context manager removes only its own label-verified container.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[2]
TEST = "aws::tests::real_https_s3_and_kms_round_trip_exact_generation"
LABEL = "insight.installation-aws-fixture"


class QualificationError(Exception):
    """Only closed safe error codes are emitted by the harness."""


def fixture_environment() -> dict[str, str]:
    environment = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith(("AWS_", "PLATFORM_TEST_"))
        and name not in {"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"}
    }
    environment.update(
        AWS_ACCESS_KEY_ID="test",
        AWS_SECRET_ACCESS_KEY="test",
        AWS_REGION="us-east-1",
        AWS_DEFAULT_REGION="us-east-1",
        AWS_CONFIG_FILE=os.devnull,
        AWS_SHARED_CREDENTIALS_FILE=os.devnull,
        AWS_EC2_METADATA_DISABLED="true",
        AWS_MAX_ATTEMPTS="1",
    )
    return environment


def command(arguments: list[str], *, timeout: int = 30) -> str:
    try:
        result = subprocess.run(
            arguments, check=False, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, timeout=timeout, env=fixture_environment(), cwd=ROOT,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise QualificationError("fixture_command_unavailable") from error
    if result.returncode != 0 or len(result.stdout) > 65_536:
        raise QualificationError("fixture_command_rejected")
    return result.stdout.strip()


class IsolatedAwsFixture:
    def __init__(self) -> None:
        self.nonce = uuid.uuid4().hex
        self.name = f"insight-installation-aws-{self.nonce}"
        profile = json.loads((ROOT / "deploy/release/development-profile-v1.json").read_text())
        images = [entry["image"] for entry in profile["dependencies"] if entry["name"] == "object_storage"]
        if len(images) != 1 or not re.fullmatch(r"localstack/localstack@sha256:[0-9a-f]{64}", images[0]):
            raise QualificationError("fixture_image_not_pinned")
        self.image = images[0]
        self.container_id: str | None = None
        self.endpoint = ""
        self.bucket = f"installation-fixture-{self.nonce}"
        self.key_arn = ""

    def __enter__(self) -> IsolatedAwsFixture:
        try:
            self.container_id = command([
                "docker", "create", "--name", self.name, "--label", f"{LABEL}={self.nonce}",
                "--publish", "127.0.0.1::4566", "--memory", "1g", "--cpus", "2",
                "--pids-limit", "256", "--tmpfs", "/var/lib/localstack:rw,nosuid,nodev,size=268435456",
                "--env", "SERVICES=s3,kms,secretsmanager", "--env", "AWS_DEFAULT_REGION=us-east-1",
                "--env", "EAGER_SERVICE_LOADING=1", "--env", "PERSISTENCE=0", self.image,
            ], timeout=180)
            if not re.fullmatch(r"[0-9a-f]{64}", self.container_id):
                raise QualificationError("fixture_container_identity_invalid")
            command(["docker", "start", self.container_id])
            info = json.loads(command(["docker", "inspect", self.container_id]))[0]
            bindings = info["NetworkSettings"]["Ports"].get("4566/tcp", [])
            if len(bindings) != 1 or bindings[0]["HostIp"] != "127.0.0.1":
                raise QualificationError("fixture_port_not_loopback")
            if any(mount["Type"] != "tmpfs" for mount in info["Mounts"]):
                raise QualificationError("fixture_persistent_mount_forbidden")
            port = int(bindings[0]["HostPort"])
            if not 1024 <= port <= 65_535:
                raise QualificationError("fixture_port_invalid")
            self.endpoint = f"https://localhost.localstack.cloud:{port}"
            deadline = time.monotonic() + 60
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            while True:
                try:
                    # Default HTTPS validation remains enabled; never install an insecure context.
                    with opener.open(f"{self.endpoint}/_localstack/health", timeout=2) as response:
                        health = json.loads(response.read(65_537))
                    if all(health.get("services", {}).get(service) in {"available", "running"} for service in ("s3", "kms", "secretsmanager")):
                        break
                except (OSError, ValueError, urllib.error.URLError):
                    pass
                if time.monotonic() >= deadline:
                    raise QualificationError("fixture_https_health_unavailable")
                time.sleep(0.25)
            self.aws("s3api", "create-bucket", "--bucket", self.bucket)
            self.aws("s3api", "put-bucket-versioning", "--bucket", self.bucket, "--versioning-configuration", '{"Status":"Enabled"}')
            self.key_arn = self.aws("kms", "create-key", "--query", "KeyMetadata.Arn", "--output", "text")
            if not re.fullmatch(r"arn:aws:kms:us-east-1:000000000000:key/[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", self.key_arn):
                raise QualificationError("fixture_kms_reference_invalid")
            return self
        except BaseException:
            self.close()
            raise

    def aws(self, *arguments: str) -> str:
        if self.container_id is None:
            raise QualificationError("fixture_not_started")
        return command([
            "docker", "exec", "--env", "AWS_ACCESS_KEY_ID=test", "--env", "AWS_SECRET_ACCESS_KEY=test",
            "--env", "AWS_DEFAULT_REGION=us-east-1", self.container_id,
            "awslocal", "--endpoint-url", "https://localhost.localstack.cloud:4566", *arguments,
        ])

    def environment(self) -> dict[str, str]:
        environment = fixture_environment()
        environment.update(PLATFORM_TEST_AWS_ENDPOINT=self.endpoint, PLATFORM_TEST_S3_BUCKET=self.bucket, PLATFORM_TEST_KMS_KEY_ID=self.key_arn)
        return environment

    def close(self) -> None:
        # The unique name also permits cleanup after an uncertain docker-create response.
        try:
            result = command(["docker", "inspect", self.container_id or self.name])
        except QualificationError:
            if self.container_id is not None:
                raise QualificationError("fixture_cleanup_inspection_failed") from None
            return
        info = json.loads(result)[0]
        if info.get("Config", {}).get("Labels", {}).get(LABEL) != self.nonce or info["Config"]["Image"] != self.image:
            raise QualificationError("fixture_cleanup_identity_mismatch")
        command(["docker", "rm", "--force", info["Id"]])
        self.container_id = None

    def __exit__(self, *_: object) -> None:
        self.close()


def main() -> int:
    if len(sys.argv) != 1:
        raise QualificationError("fixture_usage_no_arguments")
    handle, log_name = tempfile.mkstemp(prefix="insight-installation-aws-", suffix=".log")
    print(json.dumps({"schema_version": 1, "event": "qualification_started", "log": log_name}), flush=True)
    with os.fdopen(handle, "w") as log, IsolatedAwsFixture() as fixture:
        print(json.dumps({"schema_version": 1, "event": "isolated_fixture_ready", "endpoint": fixture.endpoint, "bucket": fixture.bucket, "kms_key_arn": fixture.key_arn}), flush=True)
        for package, target, test in [
            ("insight-platform-artifact-broker", ["--lib"], TEST),
        ]:
            try:
                result = subprocess.run(
                    ["cargo", "test", "--locked", "-p", package, *target, test, "--", "--ignored", "--exact", "--nocapture"],
                    cwd=ROOT, env=fixture.environment(), stdin=subprocess.DEVNULL,
                    stdout=log, stderr=subprocess.STDOUT, timeout=600, check=False,
                )
            except (OSError, subprocess.TimeoutExpired) as error:
                raise QualificationError("owning_aws_test_unavailable") from error
            if result.returncode != 0:
                raise QualificationError("owning_aws_test_failed")
            log.flush()
            with open(log_name, "rb") as evidence:
                evidence.seek(max(0, os.fstat(evidence.fileno()).st_size - 16_384))
                result_text = evidence.read().decode("utf-8", errors="replace")
            if f"test {test} ... ok" not in result_text or "test result: ok. 1 passed; 0 failed; 0 ignored;" not in result_text:
                raise QualificationError("owning_aws_test_evidence_missing")
    print(json.dumps({"schema_version": 1, "event": "qualification_passed", "tests": [TEST], "fixture_removed": True, "log": log_name}), flush=True)
    return 0


if __name__ == "__main__":
    def interrupted(_signum: int, _frame: object) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupted)
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("qualification_interrupted", file=sys.stderr)
        raise SystemExit(130) from None
    except QualificationError as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1) from None
