"""Bounded evidence readers for disposable qualification fixtures; no startup orchestration."""
import json
import os
from pathlib import Path
import re
import stat
import tempfile
MAXIMUM = 1_048_576
class InstallationFailure(Exception):
    """Closed diagnostics without external output or credentials."""

def decode(data):
    def unique(items):
        result = {}
        for key, value in items:
            if key in result:
                raise InstallationFailure("duplicate JSON field")
            result[key] = value
        return result
    if len(data) > MAXIMUM:
        raise InstallationFailure("installation JSON exceeds its bound")
    try:
        def invalid_constant(_value):
            raise InstallationFailure("non-finite JSON number")
        result = json.loads(data, object_pairs_hook=unique, parse_constant=invalid_constant)
        def bounded(value, depth=0):
            if depth > 32:
                raise InstallationFailure("JSON nesting exceeds its bound")
            if isinstance(value, dict):
                if len(value) > 64:
                    raise InstallationFailure("JSON object exceeds its bound")
                for key, child in value.items():
                    bounded(key, depth+1)
                    bounded(child, depth+1)
            elif isinstance(value, list):
                if len(value) > 256:
                    raise InstallationFailure("JSON array exceeds its bound")
                for child in value: bounded(child, depth+1)
            elif isinstance(value, str) and len(value.encode()) > 65_536:
                raise InstallationFailure("JSON string exceeds its bound")
        bounded(result)
        return result
    except (ValueError, RecursionError) as error:
        raise InstallationFailure("invalid installation JSON") from error


def read_file(path, *, private=False, maximum=MAXIMUM):
    if not path.is_absolute() or ".." in path.parts:
        raise InstallationFailure("an absolute file path is required")
    for ancestor in path.parents:
        if not stat.S_ISDIR(ancestor.lstat().st_mode):
            raise InstallationFailure("file has an unsafe ancestor")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1 or metadata.st_size > maximum:
            raise InstallationFailure("file is not a bounded regular file")
        if private and (stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_uid != os.getuid()):
            raise InstallationFailure("file is not private")
        with os.fdopen(os.dup(descriptor), "rb") as file:
            result = file.read(maximum+1)
        if len(result) > maximum:
            raise InstallationFailure("file grew beyond its bound")
        return result
    finally:
        os.close(descriptor)


def persist(path, value, *, immutable=False):
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()+b"\n"
    persist_bytes(path, encoded, immutable=immutable)


def persist_bytes(path, encoded, *, immutable=False):
    if len(encoded) > MAXIMUM:
        raise InstallationFailure("installation state exceeds its bound")
    try:
        path.lstat()
        exists = True
    except FileNotFoundError:
        exists = False
    if exists:
        previous = read_file(path, private=True)
        if immutable:
            if previous != encoded:
                raise InstallationFailure("immutable installation input or topology changed")
            return
    with tempfile.NamedTemporaryFile(prefix=".installation-", dir=path.parent, delete=False) as file:
        temporary = Path(file.name)
        os.fchmod(file.fileno(), 0o600)
        file.write(encoded)
        file.flush()
        os.fsync(file.fileno())
    try:
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def validate_plan(plan):
    fields = {"schema_version", "namespace", "input", "input_digest", "runtime_image", "console_image", "dependencies", "dependency_commands", "dependency_stop_grace_seconds", "processes"}
    if set(plan) != fields or type(plan["schema_version"]) is not int or plan["schema_version"] != 1:
        raise InstallationFailure("unexpected shared installation plan")
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,38}[a-z0-9])?", plan["namespace"]):
        raise InstallationFailure("invalid installation namespace")
    if plan["input"]["name"] != plan["namespace"] or plan["input"]["network"]["topology"] != "kubernetes_local":
        raise InstallationFailure("shared plan topology differs")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", plan["input_digest"]):
        raise InstallationFailure("invalid installation input digest")
    if set(plan["dependencies"]) != {"postgres", "nats", "s3", "openbao"}:
        raise InstallationFailure("invalid dependency closure")
    for image in [plan["runtime_image"], plan["console_image"], *plan["dependencies"].values()]:
        if not re.fullmatch(r"[A-Za-z0-9._/:-]+@sha256:[0-9a-f]{64}", image):
            raise InstallationFailure("repository image digests are required")
    if set(plan["dependency_commands"]) != {"s3"} or not isinstance(plan["dependency_commands"]["s3"], list) or not 1 <= len(plan["dependency_commands"]["s3"]) <= 64 or any(not isinstance(arg, str) or not 1 <= len(arg) <= 512 or "\x00" in arg for arg in plan["dependency_commands"]["s3"]):
        raise InstallationFailure("invalid shared dependency command")
    if (not isinstance(plan["dependency_stop_grace_seconds"], dict) or set(plan["dependency_stop_grace_seconds"]) != {"s3"}
            or type(plan["dependency_stop_grace_seconds"]["s3"]) is not int or plan["dependency_stop_grace_seconds"]["s3"] != 45):
        raise InstallationFailure("invalid shared dependency shutdown grace")
    names = set()
    if not 1 <= len(plan["processes"]) <= 24:
        raise InstallationFailure("invalid process closure")
    for process in plan["processes"]:
        if set(process) != {"name", "binary", "uid", "port", "observability_port", "paths"} or process["uid"] != 10001:
            raise InstallationFailure("invalid physical process declaration")
        if not re.fullmatch(r"[a-z][a-z0-9-]{0,40}", process["name"]) or process["name"] in names or not re.fullmatch(r"platform-[a-z0-9-]+", process["binary"]):
            raise InstallationFailure("invalid process name or executable")
        names.add(process["name"])
        for port in [process["observability_port"], *([] if process["port"] is None else [process["port"]])]:
            if type(port) is not int or not 1 <= port <= 65_535:
                raise InstallationFailure("invalid process port")
        if process["paths"] != {"process": process["name"], "configuration_directory": "/run/insight/role/config", "credential_directory": "/run/insight/role/credentials", "temporary_directory": "/var/lib/insight"}:
            raise InstallationFailure("process paths differ from the shared container policy")
    return plan
