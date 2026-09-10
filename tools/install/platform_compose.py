#!/usr/bin/env python3
"""Run the shared installation owner and direct Compose processes; never mount Docker in a Pod."""
import argparse
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
import time
import uuid

from provider_lifecycle import (LifecycleFailure, OwnerFailure, composition_snapshot, ensure_provider, owner_result)
from public_trust import PublicTrustFailure, deliver as deliver_trust, ready_identity, remember_ready


class InstallationFailure(RuntimeError):
    pass


def image(value):
    if not re.fullmatch(r"(?:[A-Za-z0-9._/:-]+@)?sha256:[0-9a-f]{64}", value):
        raise argparse.ArgumentTypeError("an immutable image digest is required")
    return value


def regular(path, maximum):
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > maximum or metadata.st_nlink != 1:
        raise InstallationFailure("installation file is not a bounded regular file")
    return metadata


def checked_directory(path):
    if not path.is_absolute() or ".." in path.parts:
        raise InstallationFailure("installation directory must be absolute")
    for ancestor in [path, *path.parents]:
        if ancestor.exists() and (ancestor.is_symlink() or not ancestor.is_dir()):
            raise InstallationFailure("installation directory has an unsafe ancestor")
    if not path.exists():
        path.mkdir(mode=0o700)
    metadata = path.stat()
    if stat.S_IMODE(metadata.st_mode) != 0o700 or metadata.st_uid != os.getuid():
        raise InstallationFailure("installation directory must be private and owned by the caller")


@contextmanager
def installation_lock(directory):
    checked_directory(directory)
    descriptor = os.open(directory / ".installation-lock",
                         os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        metadata = os.fstat(descriptor)
        if (not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1
                or metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o600):
            raise InstallationFailure("installation lock is not private")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise InstallationFailure("another installation command is active") from None
        yield
    finally:
        os.close(descriptor)


def command(arguments, *, capture=False, timeout=300):
    completed = subprocess.run(arguments, stdin=subprocess.DEVNULL,
                               stdout=subprocess.PIPE if capture else None,
                               stderr=subprocess.PIPE if capture else None,
                               check=False, timeout=timeout)
    if completed.returncode:
        raise InstallationFailure("installation owner command failed; inspect its safe process diagnostics")
    if capture and len(completed.stdout) > 1024 * 1024:
        raise InstallationFailure("installation owner output exceeds its bound")
    return completed.stdout if capture else None


def rendered(arguments):
    input_file = arguments.input
    if not input_file.is_absolute() or ".." in input_file.parts:
        raise InstallationFailure("installation input must be absolute")
    regular(input_file, 262144)
    checked_directory(arguments.directory)
    def produce(path):
        # The pure owner reads only one declaration as the invoking user's UID. No credentials,
        # network, Docker socket or installation state are available in this container.
        return command(["docker", "run", "--rm", "--network", "none", "--read-only",
                      "--user", f"{os.getuid()}:{os.getgid()}",
                      "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
                      "--mount", f"type=bind,source={path},target={path},readonly",
                      "--entrypoint", "/usr/local/bin/platform-installation", arguments.runtime_image,
                      "compose", "--input", str(path), "--runtime-image", arguments.runtime_image,
                      "--console-image", arguments.console_image], capture=True)
    with input_file.open("rb") as source:
        content = source.read(262145)
    if len(content) > 262144:
        raise InstallationFailure("installation input exceeds its bound")
    published = arguments.directory / "input.json"
    with tempfile.TemporaryDirectory(prefix=".input-", dir=arguments.directory) as temporary:
        staged = Path(temporary) / "input.json"
        with staged.open("xb") as file:
            os.fchmod(file.fileno(), 0o600)
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
        produce(staged)  # Strict owning validation succeeds before a public declaration is copied.
        if published.exists():
            metadata = regular(published, 262144)
            if published.read_bytes() != content or stat.S_IMODE(metadata.st_mode) != 0o444:
                raise InstallationFailure("installation input drift; no replacement is permitted")
        else:
            # Input is a credential-free declaration. The distinct private prepared identity,
            # keys and sessions always remain 0600 in their separate installation volume.
            os.chmod(staged, 0o444)
            # The caller holds the per-installation lock across rendering and effects.
            # Rename avoids a crash leaving a second hard link to the completed declaration.
            os.rename(staged, published)
            descriptor = os.open(arguments.directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    output = produce(published)
    document = json.loads(output)
    compose = arguments.directory / "compose.json"
    if compose.exists():
        regular(compose, 1024 * 1024)
        if compose.read_bytes() != output:
            raise InstallationFailure("generated composition drift; refusing to replace an existing installation")
    else:
        with compose.open("xb") as file:
            os.fchmod(file.fileno(), 0o600)
            file.write(output)
            file.flush()
            os.fsync(file.fileno())
        descriptor = os.open(arguments.directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return ["docker", "compose", "--file", str(compose), "--project-name", document["name"]], document



def retain_images(arguments, document):
    # Tags keep selected artifacts reachable in local containerd stores. Containers still use
    # the immutable references from the owner-generated composition, never these retention tags.
    for role, selected in (("runtime", arguments.runtime_image), ("console", arguments.console_image)):
        identity = command(["docker", "image", "inspect", selected, "--format", "{{.Id}}"], capture=True).strip()
        if not re.fullmatch(rb"sha256:[0-9a-f]{64}", identity):
            raise InstallationFailure("selected image identity is invalid")
        tag = "insight-installation-retained/" + document["name"] + ":" + role
        present = command(["docker", "image", "ls", "--no-trunc", "--filter", "reference=" + tag,
                           "--format", "{{.ID}}"], capture=True).strip()
        if present and present != identity:
            raise InstallationFailure("foreign image retention reference; refusing replacement")
        if not present:
            if arguments.operation != "up":
                raise InstallationFailure("installation image retention reference is missing")
            command(["docker", "tag", selected, tag])
        observed = command(["docker", "image", "inspect", tag, "--format", "{{.Id}}"], capture=True).strip()
        if observed != identity:
            raise InstallationFailure("image retention reference changed concurrently")


def session(compose, directory, document):
    nonce = uuid.uuid4().hex
    name = "insight-installation-session-" + nonce
    created = False
    try:
        created = True
        output = command([*compose, "run", "--no-deps", "--name", name,
                          "--label", "insight.installation.session-nonce=" + nonce,
                          "installation-session"], capture=True)
        result = json.loads(output)
        fields = {"schema_version", "input_digest", "identity_digest", "session_file", "tenant_id", "endpoint", "expires_at_unix_seconds"}
        expected_digest = document["volumes"]["installation-private"]["labels"]["insight.installation.input"]
        endpoint = json.loads((directory / "input.json").read_bytes())["network"]["console_origin"]
        expiry = result.get("expires_at_unix_seconds")
        if (set(result) != fields or type(result["schema_version"]) is not int or result["schema_version"] != 1
                or result["session_file"] != "/installation/private/session-token"
                or result["input_digest"] != expected_digest or result["endpoint"] != endpoint
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", result["identity_digest"])
                or not isinstance(result["tenant_id"], str) or len(result["tenant_id"]) > 128
                or type(expiry) is not int or not 0 < expiry - time.time() <= 900):
            raise InstallationFailure("session owner returned an invalid delivery envelope")
        with tempfile.TemporaryDirectory(prefix=".session-", dir=directory) as temporary:
            token = Path(temporary) / "session-token"
            command(["docker", "cp", name + ":/installation/private/session-token", str(token)], capture=True)
            metadata = regular(token, 16384)
            value = token.read_bytes()
            if stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_uid != os.getuid() or not re.fullmatch(rb"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\n", value):
                raise InstallationFailure("session material is not a private token file")
            destination = directory / "session-token"
            if destination.exists():
                metadata = regular(destination, 16384)
                if stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_uid != os.getuid():
                    raise InstallationFailure("existing session file is not private")
            descriptor = os.open(token, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            os.replace(token, destination)
            descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        result["session_file"] = str(destination)
        print(json.dumps(result, separators=(",", ":")))
    finally:
        if created:
            inspected = subprocess.run(["docker", "inspect", "--format", '{{index .Config.Labels "insight.installation.session-nonce"}}', name],
                                       stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                       check=False, timeout=30)
            if inspected.returncode == 0 and inspected.stdout.strip() == nonce.encode():
                subprocess.run(["docker", "rm", "--force", name], stdin=subprocess.DEVNULL,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False, timeout=30)


def public_trust(compose, directory, document):
    input_digest = document["services"]["openbao"]["labels"]["insight.installation.input"]
    identity = ready_identity(directory, input_digest=input_digest)
    service = document["services"].get("installation-public-trust", {})
    expected = {"image": document["services"]["installation-prepare"]["image"], "user": "0:0",
                "entrypoint": ["/usr/local/bin/platform-installation"],
                "command": ["public-trust", "--input", "/installation-input/input.json", "--state", "/installation/private"],
                "network_mode": "none", "read_only": True, "restart": "no", "cap_drop": ["ALL"],
                "security_opt": ["no-new-privileges:true"],
                "volumes": [{"type": "volume", "source": "installation-private", "target": "/installation", "read_only": True, "volume": {"nocopy": True}},
                            {"type": "bind", "source": str(directory/"input.json"), "target": "/installation-input/input.json", "read_only": True}]}
    if any(service.get(key) != value for key, value in expected.items()) or any(service.get(key) for key in ("environment", "env_file", "cap_add", "privileged", "volumes_from")):
        raise InstallationFailure("public trust owner service is not the declared read-only process")
    output = command([*compose, "run", "--rm", "--no-deps", "installation-public-trust"], capture=True, timeout=30)
    result = deliver_trust(directory, output, input_digest=input_digest, identity_digest=identity)
    print(json.dumps(result, separators=(",", ":")))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("render", "up", "verify", "session", "public-trust"))
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--runtime-image", type=image, required=True)
    parser.add_argument("--console-image", type=image, required=True)
    arguments = parser.parse_args()
    with installation_lock(arguments.directory):
        execute(arguments)


def execute(arguments):
    compose, document = rendered(arguments)
    if arguments.operation == "render":
        print(arguments.directory / "compose.json")
        return
    provider_deadline = None
    def provider_timeout(maximum):
        if provider_deadline is None:
            return maximum
        remaining = provider_deadline-time.monotonic()
        if remaining <= 0:
            raise InstallationFailure("provider operation timed out")
        return min(maximum, remaining)
    def inventory():
        return composition_snapshot(document, lambda args: command(args, capture=True, timeout=provider_timeout(15)))
    inventory()  # Check ownership before any project or image-retention mutation.
    if arguments.operation == "public-trust":
        public_trust(compose, arguments.directory, document)
        return
    retain_images(arguments, document)
    input_digest = document["services"]["openbao"]["labels"]["insight.installation.input"]
    dependencies = ("postgres", "nats", "s3", "openbao", "openbao-initialize")
    serving = [name for name in document["services"]
               if not name.startswith("installation-") and name not in dependencies]
    def owner(operation, field):
        inventory()
        result = subprocess.run([*compose, "run", "--rm", "--no-deps", "installation-" + operation],
                                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, check=False, timeout=provider_timeout(60))
        provider_timeout(1)
        return owner_result(result, input_digest, field)
    def start(name):
        inventory()
        command([*compose, "--profile", "initialize", "up", "--detach", "--no-deps", name], timeout=provider_timeout(60))
    def stop(name, identity):
        existing = inventory().get(name)
        if existing is None or existing.identity != identity:
            raise InstallationFailure("provider container identity changed before stop")
        command(["docker", "stop", "--time", "30", identity], capture=True, timeout=provider_timeout(45))
        stopped = json.loads(command(["docker", "inspect", "--format",
                            '{"identity":{{json .Id}},"running":{{json .State.Running}},"exit_code":{{json .State.ExitCode}}}',
                            identity], capture=True, timeout=provider_timeout(10)))
        provider_timeout(1)
        if stopped != {"identity": identity, "running": False, "exit_code": 0}:
            raise InstallationFailure("provider did not stop cleanly; serving remains disabled")
    if arguments.operation == "up":
        command([*compose, "run", "--rm", "--no-deps", "installation-prepare"])
        inventory()
        command([*compose, "up", "--detach", "--no-deps", "postgres", "nats", "s3"])
        command([*compose, "up", "--wait", "--no-deps", "postgres"])
        provider_deadline = time.monotonic()+180
        try:
            ensure_provider(input_digest=input_digest,
                            request_start=lambda: owner("provider-start", "mode"),
                            observe=lambda: owner("provider-observe", "phase"),
                            container=lambda name: inventory().get(name), start=start, stop=stop)
            provider_timeout(1)
        finally:
            provider_deadline = None
        proof = command([*compose, "run", "--rm", "--no-deps", "installation-provision"], capture=True)
        remember_ready(arguments.directory, proof, input_digest=input_digest)
        inventory()
        command([*compose, "up", "--detach", "--no-deps", *serving])
        command([*compose, "run", "--rm", "--no-deps", "installation-ready"])
    elif arguments.operation == "verify":
        current = inventory()
        if any(name not in current or not current[name].running
               for name in (*serving, "postgres", "nats", "s3", "openbao")):
            raise InstallationFailure("installation dependency or serving process is missing")
        proof = command([*compose, "run", "--rm", "--no-deps", "installation-verify"], capture=True)
        remember_ready(arguments.directory, proof, input_digest=input_digest)
        command([*compose, "run", "--rm", "--no-deps", "installation-ready"])
    if arguments.operation in ("up", "session"):
        if arguments.operation == "up":
            inventory()
            public_trust(compose, arguments.directory, document)
        session(compose, arguments.directory, document)


if __name__ == "__main__":
    try:
        main()
    except (InstallationFailure, LifecycleFailure, PublicTrustFailure, OSError, ValueError, subprocess.SubprocessError) as error:
        if isinstance(error, OwnerFailure):
            print('installation ' + error.code, file=sys.stderr)
        raise SystemExit(f"Installation failed ({type(error).__name__}); no existing resource was reset") from None
