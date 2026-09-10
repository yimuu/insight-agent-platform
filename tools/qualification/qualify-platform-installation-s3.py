#!/usr/bin/env python3
"""Qualify one pinned S3 implementation, with no KMS emulator or existing installation.

The durability claim is controlled shutdown and container recreation with the same volumes;
this does not qualify power-loss durability, HA, or a complete platform installation.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import signal
import socket
import ssl
import stat
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
IMAGE = "chrislusf/seaweedfs@sha256:08d516132314207d10c8e37cbffc1f32b147d870169688734cc61c6231625b62"
COMMIT = "d997fba1575583a89cf0cc50dc0150642286c86d"
LABEL = "insight.s3-qualification"
HOST = "localhost.localstack.cloud"
TEST = "actual_s3_versioned_contract"
CONFIG_FILES = ("ca.pem", "server.crt", "server.key", "grpc-ca.pem", "grpc-server.crt", "grpc-server.key", "s3.json", "security.toml")
SAFE_SDK_CODES = frozenset({
    "bucket_create", "tag_write", "versioning_write", "cors_write", "tag_read", "tag_drift",
    "versioning_read", "versioning_drift", "cors_read", "cors_drift", "presigned_put", "presigned_rejected",
    "version_missing", "head_exact", "head_evidence", "get_exact", "get_evidence", "body_read", "body_drift",
    "conditional_concurrency", "conditional_status", "conditional_replay", "first_put", "second_put",
    "generation_reused", "delete_exact", "delete_evidence", "deleted_version_readable", "cross_bucket_not_denied",
    "wrong_credential_not_denied", "unsigned_transport", "unsigned_not_denied", "cors_transport", "cors_boundary",
    "versions_read", "versions_incomplete", "unexpected_generation_count", "restart_version_drift",
    "readonly_inventory_drift", "sdk_tls_not_rejected", "signed_readiness_timeout", "fixture_input_invalid",
    "head_unavailable", "get_unavailable", "exact_readiness_timeout",
})


class QualificationError(Exception):
    """A closed code only; never include child output, a URL, or a credential."""


def environment() -> dict[str, str]:
    return {key: value for key, value in os.environ.items() if key in {
        "PATH", "HOME", "TMPDIR", "CARGO_HOME", "RUSTUP_HOME", "RUSTUP_TOOLCHAIN",
        "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH",
    }}


def invoke(args: list[str], timeout: int = 30, *, env=None, check=True) -> subprocess.CompletedProcess:
    result = subprocess.run(args, cwd=ROOT, env=env or environment(), stdin=subprocess.DEVNULL,
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=timeout, check=False)
    if len(result.stdout) > 1_048_576 or (check and result.returncode):
        raise QualificationError("child_rejected")
    return result


def command(args: list[str], timeout: int = 30) -> str:
    return invoke(args, timeout).stdout.decode().strip()


def write_private(path: Path, contents: bytes) -> None:
    with os.fdopen(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600), "wb") as output:
        output.write(contents)
        output.flush()
        os.fsync(output.fileno())
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def compile_test() -> Path:
    with tempfile.TemporaryFile() as output:
        result = subprocess.run(["cargo", "test", "--locked", "-p", "insight-platform-installation-tooling",
                                 "--test", "s3_qualification", "--no-run", "--message-format=json"],
                                cwd=ROOT, env=environment(), stdin=subprocess.DEVNULL,
                                stdout=output, stderr=subprocess.DEVNULL, timeout=600, check=False)
        if result.returncode:
            raise QualificationError("sdk_test_compile_failed")
        output.seek(0)
        artifacts = []
        for line in output:
            if len(line) > 1_048_576:
                raise QualificationError("compiler_output_invalid")
            value = json.loads(line)
            if value.get("reason") == "compiler-artifact" and value.get("target", {}).get("name") == "s3_qualification" and value.get("executable"):
                artifacts.append(Path(value["executable"]).resolve())
    if len(artifacts) != 1 or not artifacts[0].is_relative_to(ROOT / "target"):
        raise QualificationError("sdk_test_executable_invalid")
    return artifacts[0]


def owned(metadata: dict, nonce: str, *, container: bool) -> bool:
    if container:
        return (metadata.get("Config", {}).get("Labels", {}).get(LABEL) == nonce
                and metadata.get("Config", {}).get("Image") == IMAGE)
    return metadata.get("Labels", {}).get(LABEL) == nonce


def server_arguments() -> list[str]:
    return ["-config_dir=/run/insight/s3", "server", "-dir=/data", "-ip=127.0.0.1", "-ip.bind=127.0.0.1",
            "-master=true", "-volume=true", "-filer=true", "-s3=true", "-iam=false",
            "-master.volumeSizeLimitMB=64", "-master.telemetry=false", "-volume.max=4", "-s3.ip.bind=0.0.0.0", "-s3.port=8333",
            "-s3.port.https=0", "-s3.port.grpc=18333", "-s3.port.iceberg=0", "-s3.port.lance=0",
            "-s3.iam=false", "-s3.autoCreateBucket=false", "-s3.allowDeleteBucketNotEmpty=false",
            "-s3.allowedOrigins=", "-s3.config=/run/insight/s3/s3.json",
            "-s3.key.file=/run/insight/s3/server.key", "-s3.cert.file=/run/insight/s3/server.crt",
            "-s3.concurrentUploadLimitMB=16", "-s3.concurrentFileUploadLimit=4"]


class Fixture:
    def __init__(self, root: Path):
        self.root = root
        self.nonce = uuid.uuid4().hex
        self.prefix = "insight-s3-qualification-" + self.nonce
        self.data_volume = self.prefix + "-data"
        self.config_volume = self.prefix + "-config"
        self.network = self.prefix + "-network"
        self.containers: list[str] = []
        self.current = ""
        self.endpoint = ""
        self.port = 0
        self.grpc_port = 0
        self.checks: list[str] = []
        self.phase = "prepare"
        self.input = {}
        self.references_published = False
        self.executable = compile_test()

    def check(self, name: str) -> None:
        self.checks.append(name)
        print(json.dumps({"event": "passed", "check": name}), flush=True)

    def inspect(self, name: str, kind="container") -> dict:
        values = json.loads(command(["docker", kind, "inspect", name]))
        if len(values) != 1 or not owned(values[0], self.nonce, container=kind == "container"):
            raise QualificationError("fixture_identity_drift")
        return values[0]

    def s3_configuration(self, access_key: str, secret_key: str) -> dict:
        """Physical baseline identity. Narrow IAM qualification overrides this before publication."""
        return {"identities": [{"name": "installation-qualification",
                 "credentials": [{"accessKey": access_key, "secretKey": secret_key}],
                 "actions": ["Admin:" + self.prefix]}]}

    def prepare(self) -> None:
        self.phase = "image_inspection"
        image = invoke(["docker", "image", "inspect", IMAGE], check=False)
        if image.returncode:
            command(["docker", "pull", IMAGE], 240)
        metadata = json.loads(command(["docker", "image", "inspect", IMAGE]))[0]
        if metadata.get("Config", {}).get("Labels", {}).get("org.opencontainers.image.revision") != COMMIT:
            raise QualificationError("image_revision_invalid")
        if not any(item.endswith("@" + IMAGE.split("@", 1)[1]) for item in metadata.get("RepoDigests", [])):
            raise QualificationError("image_digest_invalid")
        tls = self.root / "tls"
        tls.mkdir(mode=0o700)
        self.phase = "tls_fixture_generation"
        env = environment() | {"INSIGHT_S3_TLS_FIXTURE_DIRECTORY": str(tls)}
        result = invoke(["cargo", "test", "--locked", "-q", "-p", "insight-platform-deployment-tooling",
                         "--test", "s3_fixture_material", "export_isolated_s3_fixture_material",
                         "--", "--ignored", "--exact"], 600, env=env, check=False)
        if result.returncode:
            raise QualificationError("shared_tls_producer_failed")
        for path in tls.iterdir():
            metadata = path.lstat()
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1 or stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_uid != os.geteuid():
                raise QualificationError("private_tls_invalid")
        access_key = secrets.token_hex(16)
        secret_key = secrets.token_hex(32)
        write_private(tls / "s3.json", json.dumps(self.s3_configuration(access_key, secret_key)).encode())
        write_private(tls / "security.toml", b'''[tls]
min_version = "TLS 1.2"
max_version = "TLS 1.3"
[grpc]
ca = "/run/insight/s3/grpc-ca.pem"
[grpc.s3]
cert = "/run/insight/s3/grpc-server.crt"
key = "/run/insight/s3/grpc-server.key"
allowed_commonNames = "Insight Local Workload"
''')
        for name in (self.data_volume, self.config_volume):
            self.phase = "private_volume_create"
            command(["docker", "volume", "create", "--label", LABEL + "=" + self.nonce, name])
        self.phase = "private_network_create"
        # A private bridge name isolates this fixture; --internal prevents Docker from allocating
        # the host loopback mappings required by the actual host SDK. Only those two ports publish.
        command(["docker", "network", "create", "--label", LABEL + "=" + self.nonce, self.network])
        helper = self.prefix + "-initialize"
        self.containers.append(helper)
        script = '''set -eu
test -z "$(ls -A /data)" || exit 41
test -z "$(ls -A /config)" || exit 42
for name in ca.pem server.crt server.key grpc-ca.pem grpc-server.crt grpc-server.key s3.json security.toml; do
  test -f "/input/$name" || exit 43
  cp "/input/$name" "/config/$name" || exit 44
  chmod 600 "/config/$name" || exit 45
  chown 10001:10001 "/config/$name" || exit 46
done
chmod 700 /data /config || exit 47
chown 10001:10001 /data /config || exit 48
command -v weed
'''
        self.phase = "private_initializer_create"
        command(["docker", "create", "--name", helper, "--label", LABEL + "=" + self.nonce,
                 "--network", "none", "--entrypoint", "/bin/sh", "--read-only",
                 "--mount", f"type=volume,source={self.data_volume},target=/data,volume-nocopy",
                 "--mount", f"type=volume,source={self.config_volume},target=/config,volume-nocopy",
                 "--mount", f"type=bind,source={tls},target=/input,readonly", IMAGE, "-ec", script])
        self.phase = "private_initializer_run"
        result = invoke(["docker", "start", "--attach", helper], 30, check=False)
        if result.returncode:
            status = self.inspect(helper).get("State", {}).get("ExitCode")
            codes = {41: "data_not_empty", 42: "config_not_empty", 43: "source_missing", 44: "copy_failed",
                     45: "file_mode_failed", 46: "file_owner_failed", 47: "directory_mode_failed", 48: "directory_owner_failed"}
            raise QualificationError("initializer_" + codes.get(status, "unavailable"))
        binary = result.stdout.decode().strip()
        if binary not in {"/usr/bin/weed", "/usr/local/bin/weed"}:
            raise QualificationError("fixed_binary_missing")
        helper_info = self.inspect(helper)
        if helper_info.get("State", {}).get("ExitCode") != 0:
            raise QualificationError("private_volume_initialization_failed")
        self.binary = binary
        self.input = {"schema_version": 1, "bucket": self.prefix, "access_key": access_key, "secret_key": secret_key,
                      "ca_file": str(tls / "ca.pem"), "evidence_file": str(self.root / "evidence.json")}
        self.check("immutable_image_and_private_shared_tls_material")

    def start(self, generation: int) -> None:
        self.phase = "start_" + str(generation)
        name = self.prefix + "-server-" + str(generation)
        self.containers.append(name)
        command(["docker", "create", "--name", name, "--label", LABEL + "=" + self.nonce,
                 "--network", self.network, "--user", "10001:10001", "--read-only", "--cap-drop", "ALL",
                 "--security-opt", "no-new-privileges", "--memory", "1g", "--cpus", "2", "--pids-limit", "256",
                 "--tmpfs", "/tmp:rw,nosuid,nodev,size=67108864,mode=1777",
                 "--mount", f"type=volume,source={self.data_volume},target=/data,volume-nocopy",
                 "--mount", f"type=volume,source={self.config_volume},target=/run/insight/s3,readonly,volume-nocopy",
                 "--publish", f"127.0.0.1:{self.port or ''}:8333",
                 "--publish", f"127.0.0.1:{self.grpc_port or ''}:18333", "--entrypoint", self.binary,
                 IMAGE, *server_arguments()])
        command(["docker", "start", name])
        self.current = name
        info = self.inspect(name)
        if not info.get("State", {}).get("Running"):
            raise QualificationError("server_exited_before_port_ready")
        for target, attribute in [("8333/tcp", "port"), ("18333/tcp", "grpc_port")]:
            mappings = info["NetworkSettings"]["Ports"][target]
            if not isinstance(mappings, list) or len(mappings) != 1 or mappings[0]["HostIp"] != "127.0.0.1":
                raise QualificationError("port_exposure_invalid")
            setattr(self, attribute, int(mappings[0]["HostPort"]))
        mounts = {value["Destination"]: value for value in info["Mounts"] if value["Type"] == "volume"}
        if (info["Config"]["User"] != "10001:10001" or not info["HostConfig"]["ReadonlyRootfs"]
                or set(mounts) != {"/data", "/run/insight/s3"}
                or mounts["/run/insight/s3"]["RW"] or not mounts["/data"]["RW"]
                or mounts["/data"]["Name"] != self.data_volume or mounts["/run/insight/s3"]["Name"] != self.config_volume
                or any(value.startswith(("AWS_", "WEED_")) for value in info["Config"]["Env"])):
            raise QualificationError("runtime_boundary_invalid")
        self.endpoint = "https://" + HOST + ":" + str(self.port)

    def private_configuration(self) -> None:
        script = '''set -eu
test "$(id -u)" = 10001
test "$(stat -c '%u:%a' /run/insight/s3)" = 10001:700
for name in ca.pem server.crt server.key grpc-ca.pem grpc-server.crt grpc-server.key s3.json security.toml; do
  test "$(stat -c '%u:%a:%h' "/run/insight/s3/$name")" = 10001:600:1
  test -r "/run/insight/s3/$name"
  test ! -L "/run/insight/s3/$name"
done
test "$(ls -A /run/insight/s3 | wc -l)" -eq 8
if touch /run/insight/s3/should-not-write 2>/dev/null; then exit 1; fi
'''
        command(["docker", "exec", self.current, "/bin/sh", "-ec", script])
        self.check("actual_uid_private_files_and_readonly_config")

    def sdk_references(self) -> dict[str, str]:
        """Publish this fixture's frozen SDK credential file; return only non-secret references.

        Intended for another owning physical qualification's try/finally lifecycle. Its additional
        objects are outside sdk('verify')'s exact baseline inventory, so that caller must verify its
        own object generations after controlled_recreate(). No credential environment is inherited.
        """
        if not self.current or not self.endpoint or not self.input:
            raise QualificationError("sdk_references_not_ready")
        root_metadata = self.root.lstat()
        if (not stat.S_ISDIR(root_metadata.st_mode) or root_metadata.st_uid != os.geteuid()
                or stat.S_IMODE(root_metadata.st_mode) != 0o700):
            raise QualificationError("sdk_references_directory_invalid")
        access, secret = self.input["access_key"], self.input["secret_key"]
        if not re.fullmatch(r"[0-9a-f]{32}", access) or not re.fullmatch(r"[0-9a-f]{64}", secret):
            raise QualificationError("sdk_references_credentials_invalid")
        expected = f"[default]\naws_access_key_id={access}\naws_secret_access_key={secret}\n".encode()
        destination = self.root / "aws-credentials"
        try:
            descriptor = os.open(destination, os.O_RDONLY | os.O_NOFOLLOW)
        except FileNotFoundError:
            if self.references_published:
                raise QualificationError("sdk_references_drift") from None
            write_private(destination, expected)
            descriptor = os.open(destination, os.O_RDONLY | os.O_NOFOLLOW)
        except OSError:
            raise QualificationError("sdk_references_drift") from None
        with os.fdopen(descriptor, "rb") as source:
            metadata = os.fstat(source.fileno())
            if (not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1
                    or metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) != 0o600
                    or metadata.st_size != len(expected) or source.read(len(expected) + 1) != expected):
                raise QualificationError("sdk_references_drift")
        self.references_published = True
        return {"endpoint": self.endpoint, "bucket": self.prefix,
                "ca_file": str(self.root / "tls/ca.pem"), "credentials_file": str(destination)}

    def tls_boundaries(self) -> None:
        self.phase = "tls_boundaries"
        context = ssl.create_default_context(cafile=str(self.root / "tls/ca.pem"))
        def exchange(port, ctx, host, payload):
            with socket.create_connection(("127.0.0.1", port), timeout=3) as tcp:
                with ctx.wrap_socket(tcp, server_hostname=host) as tls:
                    tls.sendall(payload)
                    return tls.recv(4096)
        request = b"GET / HTTP/1.1\r\nHost: localhost.localstack.cloud\r\nConnection: close\r\n\r\n"
        if not exchange(self.port, context, HOST, request).startswith(b"HTTP/1.1 403"):
            raise QualificationError("https_unsigned_boundary")
        for ca, host in [("wrong-ca.pem", HOST), ("ca.pem", "wrong.s3-qualification.invalid")]:
            try:
                exchange(self.port, ssl.create_default_context(cafile=str(self.root / "tls" / ca)), host, request)
            except ssl.SSLCertVerificationError:
                pass
            else:
                raise QualificationError("tls_verification_not_enforced")
        preface = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n" + bytes.fromhex("000000040000000000")
        for credential in (None, "ordinary-client", "grpc-client"):
            grpc_context = ssl.create_default_context(cafile=str(self.root / "tls/grpc-ca.pem"))
            grpc_context.set_alpn_protocols(["h2"])
            if credential:
                grpc_context.load_cert_chain(str(self.root / "tls" / (credential + ".crt")), str(self.root / "tls" / (credential + ".key")))
            # TLS 1.3 may defer a client-certificate alert until the first I/O after handshake.
            try:
                response = exchange(self.grpc_port, grpc_context, HOST, preface)
            except (ssl.SSLError, ConnectionResetError) as error:
                if credential == "grpc-client":
                    reasons = {"TLSV1_ALERT_UNKNOWN_CA": "unknown_ca", "SSLV3_ALERT_BAD_CERTIFICATE": "bad_certificate",
                               "TLSV1_ALERT_BAD_CERTIFICATE": "bad_certificate", "CERTIFICATE_VERIFY_FAILED": "certificate_verify",
                               "TLSV1_ALERT_INTERNAL_ERROR": "internal_error", "TLSV1_ALERT_PROTOCOL_VERSION": "protocol_version",
                               "TLSV13_ALERT_CERTIFICATE_REQUIRED": "certificate_required", "SSLV3_ALERT_HANDSHAKE_FAILURE": "handshake_failure",
                               "TLSV1_ALERT_NO_APPLICATION_PROTOCOL": "alpn"}
                    raise QualificationError("grpc_private_client_rejected_" + reasons.get(getattr(error, "reason", ""), "unknown")) from None
            else:
                if credential == "grpc-client":
                    if len(response) < 9 or response[3] != 4:
                        raise QualificationError("grpc_private_client_missing_http2_settings")
                elif response:
                    raise QualificationError("grpc_without_private_client_accepted")
        with socket.create_connection(("127.0.0.1", self.grpc_port), timeout=3) as tcp:
            tcp.sendall(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            try:
                response = tcp.recv(4096)
            except ConnectionResetError:
                response = b""
            if response and not response.startswith((b"HTTP/1.0 400", b"\x15\x03")):
                raise QualificationError("grpc_plaintext_accepted")
        self.check("https_ca_san_and_grpc_separate_ca_client_boundaries")

    def sdk(self, mode: str, *, wrong_ca=False, wrong_san=False) -> None:
        self.phase = "sdk_" + mode
        print(json.dumps({"event": "phase_started", "phase": self.phase}), flush=True)
        endpoint = self.endpoint.replace(HOST, "127.0.0.1") if wrong_san else self.endpoint
        path = self.root / ("input-" + uuid.uuid4().hex + ".json")
        write_private(path, json.dumps(self.input | {"endpoint": endpoint, "mode": mode}).encode())
        env = environment() | {"INSIGHT_S3_FIXTURE_INPUT": str(path), "SSL_CERT_FILE": str(self.root / "tls" / ("wrong-ca.pem" if wrong_ca else "ca.pem")),
                               "SSL_CERT_DIR": "/etc/ssl/certs", "AWS_EC2_METADATA_DISABLED": "true"}
        with tempfile.TemporaryFile() as output:
            result = subprocess.run([str(self.executable), TEST, "--ignored", "--exact", "--nocapture"],
                                    env=env, cwd=ROOT, stdin=subprocess.DEVNULL, stdout=output,
                                    stderr=subprocess.STDOUT, timeout=180, check=False)
            output.seek(0)
            text = output.read(65_537).decode(errors="replace")
        if result.returncode or "test result: ok. 1 passed; 0 failed; 0 ignored;" not in text:
            # Only our fixed Rust error codes survive. No SDK error formatting is used in the test.
            safe = re.search(r'S3 physical qualification failed with safe code: "([a-z_]{1,64})"', text)
            raise QualificationError("sdk_" + (safe.group(1) if safe and safe.group(1) in SAFE_SDK_CODES else "failed"))
        self.check("sdk_" + mode + ("_wrong_ca" if wrong_ca else "_wrong_san" if wrong_san else ""))

    def controlled_recreate(self) -> None:
        self.phase = "controlled_shutdown"
        old = self.inspect(self.current)
        command(["docker", "stop", "--time", "30", old["Id"]], 45)
        stopped = self.inspect(self.current)
        if stopped["State"]["Running"] or stopped["State"]["OOMKilled"] or stopped["State"]["ExitCode"] == 137:
            raise QualificationError("controlled_shutdown_failed")
        command(["docker", "rm", old["Id"]])
        self.containers.remove(self.current)
        self.start(2)
        if self.inspect(self.current)["Id"] == old["Id"]:
            raise QualificationError("container_not_recreated")
        self.check("controlled_stop_and_new_container_same_volumes")

    def safe_failure_diagnostics(self) -> dict:
        if not self.current:
            return {"server": "not_started"}
        metadata = self.inspect(self.current)
        result = subprocess.run(["docker", "logs", "--tail", "30", metadata["Id"]], cwd=ROOT,
                                env=environment(), stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=5, check=False)
        # Never retain raw provider logs. Only these literal categories and bounded process status leave memory.
        raw = result.stdout[:65_536].lower()
        categories = {"invalid_flag": b"flag provided but not defined", "permission_denied": b"permission denied",
                      "read_only_filesystem": b"read-only file system", "missing_file": b"no such file or directory",
                      "tls_configuration": b"tls min version parse failed", "address_in_use": b"address already in use"}
        return {"server_running": metadata["State"]["Running"], "server_exit_code": metadata["State"]["ExitCode"],
                "categories": sorted(name for name, token in categories.items() if token in raw),
                "published_ports_present": all(isinstance(metadata["NetworkSettings"]["Ports"].get(port), list)
                                               for port in ("8333/tcp", "18333/tcp"))}

    def close(self) -> None:
        # Names are fresh nonces, but destructive cleanup still requires each actual owning label.
        for kind, names in [("container", self.containers), ("network", [self.network]),
                            ("volume", [self.data_volume, self.config_volume])]:
            for name in reversed(names):
                result = invoke(["docker", kind, "inspect", name], check=False)
                if result.returncode:
                    # An inspect error is not proof of absence. Closed list by this exact name only.
                    listed = command(["docker", kind, "ls", "--filter", "name=" + name, "--format", "{{.Name}}" if kind != "container" else "{{.Names}}", *(["--all"] if kind == "container" else [])])
                    if name in listed.splitlines():
                        raise QualificationError("cleanup_inspection_failed")
                    continue
                metadata = json.loads(result.stdout)[0]
                if not owned(metadata, self.nonce, container=kind == "container"):
                    raise QualificationError("cleanup_identity_drift")
                args = ["docker", kind, "rm"] + (["--force"] if kind == "container" else []) + [metadata["Id"] if kind == "container" else name]
                command(args, 45)
                if invoke(["docker", kind, "inspect", name], check=False).returncode == 0:
                    raise QualificationError("cleanup_not_complete")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = {"schema_version": 1, "image": IMAGE, "source_commit": COMMIT,
              "scope": "s3_only_controlled_shutdown_container_recreate", "checks": [], "cleanup": False, "passed": False}
    failure = None
    with tempfile.TemporaryDirectory(prefix="insight-s3-qualification-") as temporary:
        root = Path(temporary).resolve()
        fixture = None
        try:
            fixture = Fixture(root)
            fixture.prepare()
            fixture.start(1)
            fixture.sdk("seed")
            fixture.private_configuration()
            fixture.tls_boundaries()
            fixture.sdk("tls-negative", wrong_ca=True)
            fixture.sdk("tls-negative", wrong_san=True)
            evidence_before = hashlib.sha256((root / "evidence.json").read_bytes()).hexdigest()
            fixture.sdk("verify")
            fixture.controlled_recreate()
            fixture.sdk("verify")
            fixture.private_configuration()
            fixture.tls_boundaries()
            if hashlib.sha256((root / "evidence.json").read_bytes()).hexdigest() != evidence_before:
                raise QualificationError("readonly_evidence_drift")
            fixture.check("readonly_evidence_unchanged_across_container_recreate")
        except (Exception, KeyboardInterrupt) as error:
            failure = str(error) if isinstance(error, QualificationError) else "qualification_unavailable"
            report["failure"] = failure
            report["phase"] = fixture.phase if fixture else "compile"
            if fixture:
                try:
                    report["diagnostics"] = fixture.safe_failure_diagnostics()
                except Exception:
                    report["diagnostics"] = {"server": "unknown"}
        finally:
            if fixture:
                report["checks"] = fixture.checks
                report["fixture_nonce"] = fixture.nonce
                try:
                    fixture.close()
                    report["cleanup"] = True
                except Exception:
                    failure = "cleanup_failed"
                    report["cleanup"] = False
                    report["failure"] = failure
            else:
                # Construction compiles the test before any Docker resource creation.
                report["cleanup"] = True
            required = {"immutable_image_and_private_shared_tls_material", "sdk_seed", "sdk_verify",
                        "actual_uid_private_files_and_readonly_config", "https_ca_san_and_grpc_separate_ca_client_boundaries",
                        "sdk_tls-negative_wrong_ca", "sdk_tls-negative_wrong_san",
                        "controlled_stop_and_new_container_same_volumes", "readonly_evidence_unchanged_across_container_recreate"}
            report["passed"] = failure is None and report["cleanup"] and required.issubset(report["checks"])
            write_private(args.report.resolve(), json.dumps(report, indent=2).encode() + b"\n")
    print(json.dumps(report), flush=True)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
