"""Deployment-only provider start permission and exact Compose ownership checks.

The Rust installer owns durable permission and provider identity. This adapter only waits on
read-only observations of one live container; it never interprets private installation state.
"""
from dataclasses import dataclass
import json
import re
import time


class LifecycleFailure(RuntimeError):
    pass


class OwnerFailure(LifecycleFailure):
    def __init__(self, code):
        self.code = code
        super().__init__("installation owner returned " + code)


@dataclass(frozen=True)
class ContainerState:
    identity: str
    running: bool


_ERRORS = frozenset(("InvalidInput", "InvalidEndpoint", "InvalidRoleClosure", "InvalidPath",
                     "UnsupportedTopology", "IdentityDrift", "ConfigurationDrift", "ForeignState",
                     "CredentialInvalid", "PrerequisiteUnavailable", "SchemaMismatch",
                     "ExternalOutcomeUnknown", "Conflict", "Incomplete"))
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
_CONTAINER = re.compile(r"[0-9a-f]{64}")


def checked_envelope(value, input_digest, field, identity_digest=None):
    fields = {"schema_version", "input_digest", "identity_digest", field}
    if (field not in ("mode", "phase") or not isinstance(value, dict) or set(value) != fields
            or type(value["schema_version"]) is not int or value["schema_version"] != 1
            or value["input_digest"] != input_digest
            or not _DIGEST.fullmatch(input_digest)
            or not isinstance(value["identity_digest"], str)
            or not _DIGEST.fullmatch(value["identity_digest"])
            or (identity_digest is not None and value["identity_digest"] != identity_digest)
            or (field == "mode" and value[field] not in ("initialize_once", "serve"))
            or (field == "phase" and value[field] != "provider_ready")):
        raise LifecycleFailure("invalid provider owner envelope")
    return value


def _unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate object field")
        value[key] = item
    return value


def _json(content):
    return json.loads(content, object_pairs_hook=_unique_object,
                      parse_constant=lambda _value: (_ for _ in ()).throw(ValueError("non-finite JSON")))


def owner_result(completed, input_digest, field):
    """Only fixed owning error lines are interpreted; raw diagnostics never escape this parser."""
    if len(completed.stdout) > 16384 or len(completed.stderr) > 65536:
        raise LifecycleFailure("provider owner response exceeds its bound")
    if completed.returncode:
        codes = [line.removeprefix(b"installation ").decode("ascii")
                 for line in completed.stderr.splitlines()
                 if line.startswith(b"installation ")
                 and line.removeprefix(b"installation ") in {code.encode() for code in _ERRORS}]
        if len(codes) == 1:
            raise OwnerFailure(codes[0])
        raise LifecycleFailure("provider owner failed without a closed diagnostic")
    try:
        value = _json(completed.stdout)
    except (ValueError, UnicodeError):
        raise LifecycleFailure("provider owner response is invalid") from None
    return checked_envelope(value, input_digest, field)


def ensure_provider(*, input_digest, request_start, observe, container, start, stop,
                    timeout=180, pause=time.sleep):
    """Acquire once, or resume only observation of the exact already-running initializer."""
    if not 0 < timeout <= 600:
        raise LifecycleFailure("invalid provider readiness bound")
    deadline = time.monotonic() + timeout
    init_name, serve_name = "openbao-initialize", "openbao"
    initial, serving = container(init_name), container(serve_name)
    if initial and initial.running and serving and serving.running:
        raise LifecycleFailure("both provider modes are running")
    identity = None
    try:
        permission = checked_envelope(request_start(), input_digest, "mode")
        identity = permission["identity_digest"]
        mode = permission["mode"]
    except OwnerFailure as error:
        if error.code != "ExternalOutcomeUnknown":
            raise
        if initial is None or not initial.running or serving is not None:
            raise LifecycleFailure("unknown initialization requires its original live container") from None
        mode = "observe_original"

    def wait_current(service, expected):
        if expected is None or not expected.running:
            raise LifecycleFailure("provider container is not running")
        while True:
            if time.monotonic() >= deadline:
                raise LifecycleFailure("provider readiness observation timed out")
            if container(service) != expected:
                raise LifecycleFailure("provider container changed during observation")
            try:
                value = checked_envelope(observe(), input_digest, "phase", identity)
            except OwnerFailure as error:
                if error.code not in ("Incomplete", "PrerequisiteUnavailable"):
                    raise
                if time.monotonic() >= deadline:
                    raise LifecycleFailure("provider readiness observation timed out") from None
                pause(min(1, max(0, deadline - time.monotonic())))
                continue
            if time.monotonic() >= deadline:
                raise LifecycleFailure("provider readiness observation timed out")
            if container(service) != expected:
                raise LifecycleFailure("provider container changed during observation")
            return value

    if mode == "initialize_once":
        if initial is not None or serving is not None:
            raise LifecycleFailure("first provider start found an existing container")
        start(init_name)
        initial = container(init_name)
    if mode in ("initialize_once", "observe_original") or (initial and initial.running):
        ready = wait_current(init_name, initial)
        identity = ready["identity_digest"]
        stop(init_name, initial.identity)
        stopped = container(init_name)
        if stopped is not None and (stopped.identity != initial.identity or stopped.running):
            raise LifecycleFailure("initializer did not stop with its original identity")
    elif mode != "serve":
        raise LifecycleFailure("provider mode is invalid")
    serving = container(serve_name)
    if serving is None or not serving.running:
        start(serve_name)
        serving = container(serve_name)
    return wait_current(serve_name, serving)


def composition_snapshot(document, run):
    """Read only narrowly selected Docker metadata and reject every foreign project container.

    `run(argv)` returns bounded stdout bytes and raises on failure. No inspect output is logged.
    Images are compared by actual local immutable image ID, including digest-pinned index inputs.
    """
    services = document["services"]
    project = document["name"]
    if not isinstance(services, dict) or not 1 <= len(services) <= 64:
        raise LifecycleFailure("invalid composition service closure")
    for service in services.values():
        digest = service.get("labels", {}).get("insight.installation.input")
        if not isinstance(digest, str) or not _DIGEST.fullmatch(digest):
            raise LifecycleFailure("composition is missing its installation identity")
    raw = run(["docker", "ps", "--all", "--no-trunc", "--filter",
               "label=com.docker.compose.project=" + project, "--format", "{{.ID}}"])
    ids = raw.decode("ascii").splitlines()
    if len(ids) > 128 or any(not _CONTAINER.fullmatch(item) for item in ids):
        raise LifecycleFailure("composition container inventory is invalid")
    if not ids:
        return {}
    expected = {}
    for service in services.values():
        selected = service["image"]
        if selected not in expected:
            actual = run(["docker", "image", "inspect", selected, "--format", "{{.Id}}"]).strip().decode("ascii")
            if not _DIGEST.fullmatch(actual):
                raise LifecycleFailure("composition image identity is invalid")
            expected[selected] = actual
    states = {}
    template = ('{"identity":{{json .Id}},"running":{{json .State.Running}},'
                '"image":{{json .Image}},"labels":{{json .Config.Labels}}}')
    for identifier in ids:
        value = _json(run(["docker", "inspect", "--format", template, identifier]))
        labels = value.get("labels") or {}
        name = labels.get("com.docker.compose.service")
        if (name not in services or labels.get("com.docker.compose.project") != project
                or labels.get("insight.installation.input") != services[name]["labels"]["insight.installation.input"]
                or value.get("identity") != identifier or type(value.get("running")) is not bool
                or value.get("image") != expected[services[name]["image"]]):
            raise LifecycleFailure("foreign composition container; no mutation is permitted")
        if labels.get("com.docker.compose.oneoff", "false").lower() == "true":
            continue
        if name in states:
            raise LifecycleFailure("multiple containers claim the same installation service")
        states[name] = ContainerState(identifier, value["running"])
    return states
