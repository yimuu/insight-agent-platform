#!/usr/bin/env python3
"""Start a unified installation's host binaries and Console in the foreground.

Dependency containers retain their data on exit. Use session while up is running to renew access.
Generate the declaration with platform-installation native-input NAME DIGEST --output DIR/output
--port-base PORT. Build the host binaries and Console before invoking this consumer.
"""
import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import platform
import re
import socket
import struct
import sys
import tempfile
import time
from urllib.parse import urlsplit

from native_runtime import (NativeFailure, ProcessGroup, absolute, checked_file, clean_environment,
                            decode_json, freeze, lock, private_directory, process_environment,
                            read_file, signals, verify_artifact)
from provider_lifecycle import (LifecycleFailure, composition_snapshot)
from public_trust import PublicTrustFailure, deliver as deliver_trust, ready_identity, remember_ready


def bootstrap_executable(path):
    """The owning executable is checked before it is allowed to inspect other host artifacts."""
    with checked_file(path, 536870912, executable=True) as (descriptor, _):
        header = os.read(descriptor, 32)
    machine = platform.machine()
    if len(header) != 32 or machine not in ("x86_64", "arm64", "aarch64"):
        raise NativeFailure("unsupported-host-binary")
    arm = machine in ("arm64", "aarch64")
    valid = False
    if sys.platform == "darwin":
        valid = (header[:4] == b"\xcf\xfa\xed\xfe"
                 and struct.unpack_from("<I", header, 4)[0] == (0x0100000C if arm else 0x01000007)
                 and struct.unpack_from("<I", header, 12)[0] == 2)
    elif sys.platform == "linux":
        valid = (header[:6] == b"\x7fELF\x02\x01" and struct.unpack_from("<H", header, 16)[0] in (2, 3)
                 and struct.unpack_from("<H", header, 18)[0] == (183 if arm else 62))
    if not valid:
        raise NativeFailure("unsupported-host-binary")


def probe_ports(input_document, dependency_states):
    ports = set()
    for process in input_document["network"]["processes"]:
        for field in ("listen_address", "observability_address"):
            address = process.get(field)
            if address:
                host, port = address.rsplit(":", 1)
                if host != "127.0.0.1":
                    raise NativeFailure("native-listen-is-not-loopback")
                ports.add(int(port))
    network = input_document["network"]
    ports.add(urlsplit(network["console_origin"]).port)
    providers = network["providers"]
    dependency_ports = {"postgres": network["database"]["port"], "nats": network["nats_port"],
                        "s3": urlsplit(providers["artifact"]).port,
                        "openbao": urlsplit(providers["openbao"]).port}
    for service, port in dependency_ports.items():
        state = dependency_states.get(service)
        if service == "openbao" and not (state and state.running):
            state = dependency_states.get("openbao-initialize")
        if not (state and state.running):
            ports.add(port)
    for port in ports:
        if type(port) is not int or not 1024 <= port <= 65535:
            raise NativeFailure("invalid-native-port")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            try:
                listener.bind(("127.0.0.1", port))
            except OSError:
                raise NativeFailure("native-port-already-in-use") from None


class NativeInstallation:
    def __init__(self, arguments, group):
        self.arguments, self.group = arguments, group
        self.directory = arguments.directory
        self.output, self.state = self.directory / "output", self.directory / "private"
        self.owner = arguments.binaries / "platform-installation"
        self.work = private_directory(self.directory / "command-home", create=True)
        self.environment = clean_environment(self.work)
        self.input_file = self.directory / "input.json"
        self.plan = None
        self.document = None
        self.composition = None
        self.serving_deadline = None
        # Docker is an operator tool. Its selected local context remains available; this
        # environment never reaches installer commands or serving children.
        self.docker_environment = {key: os.environ[key] for key in (
            "PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH",
        ) if key in os.environ}

    def artifact(self, path):
        item = next((entry for entry in self.plan["artifacts"] if entry["path"] == str(path)), None)
        if item is None:
            raise NativeFailure("unfrozen-executable")
        verify_artifact(item)

    def preflight(self):
        bootstrap_executable(self.owner)
        content = read_file(self.arguments.input, 262144)
        # Generate from the caller's input first. The Rust owner performs all schema and
        # cross-field checks without installation state or credentials.
        with tempfile.TemporaryDirectory(prefix=".preflight-", dir=self.directory) as temporary:
            staged = Path(temporary) / "input.json"
            with staged.open("xb") as stream:
                os.fchmod(stream.fileno(), 0o600)
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            command = [str(self.owner), "native-plan", "--input", str(staged),
                       "--output", str(self.output), "--binaries", str(self.arguments.binaries),
                       "--console-directory", str(self.arguments.console_directory), "--node", str(self.arguments.node)]
            generated = self.group.run(command, self.environment, capture=True, timeout=120)
        self.plan = decode_json(generated)
        self.document = decode_json(content)
        for item in self.plan["artifacts"]:
            verify_artifact(item)
        node = self.plan["console"]["node_file"]
        self.artifact(node)
        version = self.group.run([node, "--version"], self.environment, capture=True, timeout=10)
        match = re.fullmatch(rb"v(\d+)\.(\d+)\.(\d+)\n", version)
        if not match or not (24, 11, 1) <= tuple(map(int, match.groups())) < (25, 0, 0):
            raise NativeFailure("unsupported-node-version")
        wasm = [item["path"] for item in self.plan["artifacts"]
                if item["path"].endswith(".wasm") and Path(item["path"]).is_relative_to(self.plan["console"]["bundle_directory"])]
        # A present but invalid compiler WASM is a preflight failure, before any provider starts.
        script = "import {readFileSync} from 'node:fs'; for (const p of process.argv.slice(1)) await WebAssembly.compile(readFileSync(p));"
        self.group.run([node, "--input-type=module", "-e", script, *wasm], self.environment, timeout=30)
        with lock(self.directory, ".installation-lock"):
            # Detect an input writer racing the read-only owner before freezing either result.
            if read_file(self.arguments.input, 262144) != content:
                raise NativeFailure("input-changed-during-preflight")
            freeze(self.directory, "input.json", content)
            freeze(self.directory, "native-plan.json", generated)
            self.artifact(self.owner)
            dependencies = self.group.run([str(self.owner), "dependencies", "--input", str(self.input_file),
                "--output", str(self.output), "--uid", str(os.getuid()), "--gid", str(os.getgid())],
                self.environment, capture=True, timeout=30)
            self.composition = decode_json(dependencies)
            composition_file = freeze(self.directory, "dependencies.json", dependencies)
        self.compose = ["docker", "compose", "--file", str(composition_file),
                        "--project-name", self.composition["name"]]

    def docker(self, arguments):
        return self.group.run(arguments, self.docker_environment, capture=True, timeout=self.budget(180))

    def budget(self, maximum):
        if self.serving_deadline is not None:
            remaining = self.serving_deadline - time.monotonic()
            if remaining <= 0:
                raise NativeFailure("serving-startup-timeout")
            maximum = min(maximum, remaining)
        return maximum

    def snapshot(self):
        return composition_snapshot(self.composition, self.docker)

    def owner_command(self, operation, *, process=None):
        self.artifact(self.owner)
        arguments = [str(self.owner), operation, "--input", str(self.input_file)]
        if process is not None:
            if operation != "ready":
                raise NativeFailure("invalid-process-readiness-command")
            arguments += ["--process", process]
        environment = dict(self.environment)
        if operation != "ready":
            arguments += ["--state", str(self.state)]
        if operation in ("prepare", "provision", "verify"):
            arguments += ["--output", str(self.output)]
        if operation in ("provision", "verify"):
            arguments += ["--binaries", str(self.arguments.binaries)]
            for item in self.plan["artifacts"]:
                if item["executable"]:
                    verify_artifact(item)
            environment.update({"AWS_SHARED_CREDENTIALS_FILE": str(self.state / "s3-artifact-gateway-credentials"),
                                "AWS_PROFILE": "default", "AWS_CONFIG_FILE": "/dev/null", "AWS_EC2_METADATA_DISABLED": "true",
                                "SSL_CERT_FILE": str(self.state / "ca.pem"), "SSL_CERT_DIR": "/etc/ssl/certs"})
        with lock(self.directory, ".installation-lock"):
            result = self.group.run(arguments, environment, capture=True,
                                    timeout=self.budget(300 if operation in ("provision", "verify") else 200))
        if operation in ("provision", "verify"):
            remember_ready(self.directory, result, input_digest=self.plan["input_digest"])
        return result

    def start_dependency(self, service):
        self.snapshot()  # Recheck project ownership immediately before each mutation.
        self.docker([*self.compose, "up", "--detach", "--no-deps", service])

    def start(self):
        initial = self.snapshot()
        probe_ports(self.document, initial)
        self.prepare_directories()
        self.owner_command("prepare")
        for service in ("postgres", "nats", "s3"):
            self.start_dependency(service)
        self.docker([*self.compose, "up", "--wait", "--wait-timeout", "90", "--no-deps", "postgres"])
        self.start_dependency("openbao")
        self.owner_command("bootstrap")
        self.owner_command("provision")
        self.serving_deadline = time.monotonic() + 120
        try:
            self.start_roles()
        finally:
            self.serving_deadline = None
        self.deliver_public_trust()
        self.deliver_session()
        print("Native installation ready; Ctrl-C stops these services and retains dependency data.", flush=True)
        while True:
            self.group.pump(0.25)

    def start_roles(self):
        private_directory(self.output / "temporary", create=True)
        for process in self.plan["processes"]:
            self.artifact(process["executable_file"])
            private_directory(Path(process["temporary_directory"]), create=True)
            environment = process_environment(Path(process["environment_file"]), Path(process["temporary_directory"]))
            self.budget(120)
            self.group.start([process["executable_file"]], environment, required=True,
                             cwd=process["temporary_directory"])
            self.owner_command("ready", process=process["process"])
        console = self.plan["console"]
        for item in self.plan["artifacts"]:
            if not item["executable"]:
                verify_artifact(item)
        self.artifact(console["node_file"])
        home = private_directory(self.directory / "console-home", create=True)
        self.budget(120)
        self.group.start([console["node_file"], console["entrypoint_file"], "--config", console["configuration_file"]],
                         clean_environment(home), required=True, cwd=console["bundle_directory"])
        self.owner_command("ready")

    def prepare_directories(self):
        # Missing data directories of an existing installation are never replaced with empty
        # ones. The shared owner validates contents and freezes new-directory identity.
        fresh = not (self.state.exists() or self.state.is_symlink())
        for directory in self.plan["preparation_directories"]:
            private_directory(Path(directory), create=fresh)

    def deliver_public_trust(self):
        identity = ready_identity(self.directory, input_digest=self.plan["input_digest"])
        result = deliver_trust(self.directory, self.owner_command("public-trust"),
                               input_digest=self.plan["input_digest"], identity_digest=identity)
        print(json.dumps(result, separators=(",", ":")), flush=True)

    def deliver_session(self):
        result = decode_json(self.owner_command("session"))
        fields = {"schema_version", "input_digest", "identity_digest", "session_file", "tenant_id", "endpoint", "expires_at_unix_seconds"}
        if (not isinstance(result, dict) or set(result) != fields
                or type(result["schema_version"]) is not int or result["schema_version"] != 1
                or result.get("input_digest") != self.plan["input_digest"]
                or result.get("session_file") != str(self.state / "session-token")
                or result.get("endpoint") != self.document["network"]["console_origin"]
                or not isinstance(result["identity_digest"], str)
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", result["identity_digest"])
                or not isinstance(result["tenant_id"], str)
                or not re.fullmatch(r"ten_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", result["tenant_id"])
                or type(result["expires_at_unix_seconds"]) is not int
                or not 0 < result["expires_at_unix_seconds"] - time.time() <= 900):
            raise NativeFailure("invalid-session-delivery")
        token = read_file(Path(result["session_file"]), 16384, private=True)
        if not re.fullmatch(rb"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\n", token):
            raise NativeFailure("invalid-session-file")
        print(json.dumps({key: result[key] for key in sorted(fields)}, separators=(",", ":")), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("render", "up", "verify", "status", "session", "public-trust"))
    parser.add_argument("--input", type=absolute, required=True)
    parser.add_argument("--directory", type=absolute, required=True)
    parser.add_argument("--binaries", type=absolute, required=True)
    parser.add_argument("--console-directory", type=absolute, required=True)
    parser.add_argument("--node", type=absolute, required=True)
    arguments = parser.parse_args()
    private_directory(arguments.directory, create=True)
    group = ProcessGroup(arguments.directory / "logs")
    with signals(group), ExitStack() as lifecycle:
        try:
            if arguments.operation == "up":
                lifecycle.enter_context(lock(arguments.directory, ".supervisor-lock"))
            installation = NativeInstallation(arguments, group)
            if arguments.operation == "up":
                installation.preflight()
                installation.start()
            else:
                installation.preflight()
                if arguments.operation == "render":
                    print(arguments.directory / "native-plan.json")
                elif arguments.operation == "session":
                    installation.deliver_session()
                elif arguments.operation == "public-trust":
                    installation.deliver_public_trust()
                elif arguments.operation == "status":
                    installation.owner_command("ready")
                    print("All native installation readiness probes passed")
                else:
                    installation.owner_command("verify")
                    installation.owner_command("ready")
                    print("Native installation configuration and readiness verified")
        finally:
            group.close()


if __name__ == "__main__":
    try:
        main()
    except (NativeFailure, LifecycleFailure, PublicTrustFailure, OSError, ValueError, KeyError, TypeError) as error:
        # No exception message, command line, token, provider body or private config is displayed.
        raise SystemExit(f"Native installation failed ({type(error).__name__}); inspect private logs. Dependency data was retained.") from None
