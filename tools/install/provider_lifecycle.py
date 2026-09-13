"""Read-only, exact Compose ownership snapshots for native supervision and qualification."""
from dataclasses import dataclass
import json
import re

class LifecycleFailure(RuntimeError):
    pass

@dataclass(frozen=True)
class ContainerState:
    identity: str
    running: bool

_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
_CONTAINER = re.compile(r"[0-9a-f]{64}")

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
