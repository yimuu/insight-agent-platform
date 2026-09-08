"""Owning contract for exact native image build records and index assembly."""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
import hashlib
import json
import os
from pathlib import Path
import re
import stat


MAX_INDEX_BYTES = 64 * 1024
MAX_RECORD_BYTES = 128 * 1024
MAX_CONSOLE_ARCHIVE_BYTES = 256 * 1024 * 1024
MAX_ATTESTATIONS_PER_PLATFORM = 4
PLATFORMS = frozenset({"linux/amd64", "linux/arm64"})
COMPONENTS = {"runtime": "platform-runtime", "sandbox_runner": "platform-sandbox-runner"}
DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
OCI_INDEX = "application/vnd.oci.image.index.v1+json"
OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def integer(value: object, minimum: int = 0) -> bool:
    return type(value) is int and minimum <= value <= 2**53 - 1


def annotations(value: object) -> None:
    require(isinstance(value, dict), "invalid annotations")
    for key, item in value.items():
        require(isinstance(key, str) and bool(key) and isinstance(item, str),
                "annotations must map string keys to string values")
        key.encode("utf-8")
        item.encode("utf-8")


def canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def strict_json(payload: bytes, limit: int) -> object:
    require(0 < len(payload) <= limit, "JSON byte bound exceeded")

    def pairs(items: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in items:
            require(key not in result, "duplicate JSON field")
            result[key] = value
        return result

    def constant(value: str) -> object:
        raise ValueError(f"non-finite JSON constant: {value}")

    return json.loads(payload.decode("utf-8"), object_pairs_hook=pairs,
                      parse_constant=constant)


def bounded_file(path: Path, limit: int) -> bytes:
    metadata = path.lstat()
    require(stat.S_ISREG(metadata.st_mode), "artifact must be a regular file")
    require(0 < metadata.st_size <= limit, "artifact byte bound exceeded")
    with path.open("rb") as source:
        payload = source.read(limit + 1)
    require(len(payload) == metadata.st_size, "artifact changed while reading")
    return payload


def closed(value: object, owner: type) -> dict[str, object]:
    require(isinstance(value, dict), "record must be an object")
    require(set(value) == {field.name for field in fields(owner)}, "record field closure mismatch")
    return value


@dataclass(frozen=True)
class BuildIdentity:
    repository: str
    git_commit: str
    release_tag: str
    run_id: int
    run_attempt: int

    def validate(self) -> None:
        require(isinstance(self.repository, str) and
                re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,99}/[a-z0-9][a-z0-9_.-]{0,99}", self.repository)
                is not None, "invalid repository")
        require(isinstance(self.git_commit, str) and
                re.fullmatch(r"[0-9a-f]{40}", self.git_commit) is not None, "invalid source revision")
        require(isinstance(self.release_tag, str) and len(self.release_tag) <= 48 and
                re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", self.release_tag) is not None,
                "invalid release tag")
        require(integer(self.run_id, 1) and integer(self.run_attempt, 1), "invalid workflow identity")


def index_descriptors(payload: bytes, expected_digest: str,
                      expected_platforms: frozenset[str]) -> dict[str, dict[str, object]]:
    require(0 < len(payload) <= MAX_INDEX_BYTES, "index byte bound exceeded")
    require(isinstance(expected_digest, str) and DIGEST.fullmatch(expected_digest) is not None,
            "invalid index digest")
    require(digest(payload) == expected_digest, "raw index digest mismatch")
    value = strict_json(payload, MAX_INDEX_BYTES)
    require(isinstance(value, dict) and type(value.get("schemaVersion")) is int and
            value.get("schemaVersion") == 2 and
            value.get("mediaType") == OCI_INDEX, "expected OCI image index")
    require(set(value) <= {"schemaVersion", "mediaType", "manifests", "annotations"},
            "unsupported index field")
    annotations(value.get("annotations", {}))
    descriptors = value.get("manifests")
    require(isinstance(descriptors, list) and
            len(expected_platforms) * 2 <= len(descriptors) <=
            len(expected_platforms) * (1 + MAX_ATTESTATIONS_PER_PLATFORM),
            "index descriptor count is outside bounds")
    runnable: dict[str, str] = {}
    proofs: list[tuple[str, str]] = []
    result: dict[str, dict[str, object]] = {}
    for descriptor in descriptors:
        require(isinstance(descriptor, dict) and descriptor.get("mediaType") == OCI_MANIFEST,
                "unsupported index descriptor")
        require(set(descriptor) <= {"mediaType", "digest", "size", "platform", "annotations"},
                "unsupported descriptor field")
        subject = descriptor.get("digest")
        require(isinstance(subject, str) and DIGEST.fullmatch(subject) is not None and
                subject not in result, "duplicate or invalid descriptor digest")
        require(integer(descriptor.get("size"), 1), "invalid manifest size")
        platform = descriptor.get("platform")
        require(isinstance(platform, dict) and set(platform) <= {"os", "architecture", "variant"},
                "unsupported descriptor platform")
        key = f"{platform.get('os')}/{platform.get('architecture')}"
        notes = descriptor.get("annotations", {})
        annotations(notes)
        if key in expected_platforms:
            require(key not in runnable, "duplicate runnable platform")
            require("variant" not in platform or
                    (key == "linux/arm64" and platform["variant"] == "v8"), "unsupported CPU variant")
            require("vnd.docker.reference.type" not in notes and
                    "vnd.docker.reference.digest" not in notes,
                    "runnable descriptor cannot impersonate an attestation")
            runnable[key] = subject
        else:
            require(key == "unknown/unknown" and "variant" not in platform and
                    notes.get("vnd.docker.reference.type") == "attestation-manifest",
                    "foreign or unclassified index descriptor")
            reference = notes.get("vnd.docker.reference.digest")
            require(isinstance(reference, str) and DIGEST.fullmatch(reference) is not None,
                    "invalid attestation reference")
            proofs.append((subject, reference))
        result[subject] = descriptor
    require(set(runnable) == expected_platforms, "runnable platform closure mismatch")
    for _, reference in proofs:
        require(reference in runnable.values(), "attestation refers to a foreign manifest")
    for child in runnable.values():
        require(1 <= sum(reference == child for _, reference in proofs) <=
                MAX_ATTESTATIONS_PER_PLATFORM, "missing or excessive attestation descriptors")
    return result


@dataclass(frozen=True)
class NativeBuildRecord:
    schema_version: int
    identity: BuildIdentity
    component: str
    platform: str
    subject: str
    index_digest: str
    started_epoch: int
    finished_epoch: int
    index_json: str

    @property
    def filename(self) -> str:
        return f"{self.component}-{self.platform.removeprefix('linux/')}.json"

    def validate(self, expected: BuildIdentity, now: int) -> dict[str, dict[str, object]]:
        self.identity.validate()
        expected.validate()
        require(type(self.schema_version) is int and self.schema_version == 1, "unsupported record version")
        require(self.identity == expected, "native build source or workflow identity mismatch")
        require(isinstance(self.component, str) and self.component in COMPONENTS and
                isinstance(self.platform, str) and self.platform in PLATFORMS, "unknown component or platform")
        require(self.subject == f"ghcr.io/{expected.repository}/{COMPONENTS[self.component]}",
                "native image subject mismatch")
        require(integer(now, 1) and integer(self.started_epoch, 1) and
                integer(self.finished_epoch, 1) and
                self.started_epoch <= self.finished_epoch <= now, "invalid build time ordering")
        require(isinstance(self.index_json, str), "index must preserve its exact UTF-8 JSON bytes")
        return index_descriptors(self.index_json.encode("utf-8"), self.index_digest,
                                 frozenset({self.platform}))

    def encode(self, expected: BuildIdentity, now: int) -> bytes:
        self.validate(expected, now)
        payload = canonical(asdict(self))
        require(len(payload) <= MAX_RECORD_BYTES, "native record byte bound exceeded")
        return payload

    @classmethod
    def decode(cls, payload: bytes, expected: BuildIdentity, now: int) -> NativeBuildRecord:
        value = closed(strict_json(payload, MAX_RECORD_BYTES), cls)
        identity = BuildIdentity(**closed(value["identity"], BuildIdentity))
        result = cls(**{**value, "identity": identity})
        result.validate(expected, now)
        return result


@dataclass(frozen=True)
class ConsoleBuildRecord:
    schema_version: int
    identity: BuildIdentity
    archive_sha256: str
    archive_bytes: int
    started_epoch: int
    finished_epoch: int

    @property
    def filename(self) -> str:
        return f"console-{self.identity.release_tag.removeprefix('v')}.tar.gz"

    def validate(self, expected: BuildIdentity, now: int) -> None:
        self.identity.validate()
        expected.validate()
        require(type(self.schema_version) is int and self.schema_version == 1,
                "unsupported Console record version")
        require(self.identity == expected, "Console source or workflow identity mismatch")
        require(isinstance(self.archive_sha256, str) and DIGEST.fullmatch(self.archive_sha256) is not None,
                "invalid Console archive digest")
        require(integer(self.archive_bytes, 1) and self.archive_bytes <= MAX_CONSOLE_ARCHIVE_BYTES,
                "Console archive byte bound exceeded")
        require(integer(now, 1) and integer(self.started_epoch, 1) and integer(self.finished_epoch, 1) and
                self.started_epoch <= self.finished_epoch <= now, "invalid Console build time ordering")

    def encode(self, expected: BuildIdentity, now: int) -> bytes:
        self.validate(expected, now)
        return canonical(asdict(self))

    @classmethod
    def decode(cls, payload: bytes, expected: BuildIdentity, now: int) -> ConsoleBuildRecord:
        value = closed(strict_json(payload, MAX_RECORD_BYTES), cls)
        result = cls(**{**value, "identity": BuildIdentity(**closed(value["identity"], BuildIdentity))})
        result.validate(expected, now)
        return result


def load_records(directory: Path, expected: BuildIdentity, now: int) -> list[NativeBuildRecord]:
    expected_names = {f"{component}-{platform.removeprefix('linux/')}.json"
                      for component in COMPONENTS for platform in PLATFORMS}
    paths = []
    with os.scandir(directory) as entries:
        for entry in entries:
            require(len(paths) < len(expected_names), "native artifact entry bound exceeded")
            paths.append(Path(entry.path))
    require({path.name for path in paths} == expected_names, "native artifact file closure mismatch")
    records = []
    for path in sorted(paths):
        record = NativeBuildRecord.decode(bounded_file(path, MAX_RECORD_BYTES), expected, now)
        require(path.name == record.filename, "native artifact filename differs from its content")
        records.append(record)
    for platform in PLATFORMS:
        runtime = next(r for r in records if r.platform == platform and r.component == "runtime")
        runner = next(r for r in records if r.platform == platform and r.component == "sandbox_runner")
        require(runtime.finished_epoch <= runner.started_epoch, "runner precedes its runtime cache build")
    return records


def verify_merged_index(records: list[NativeBuildRecord], expected: BuildIdentity, component: str,
                        payload: bytes, index_digest: str, ready_epoch: int) -> int:
    require(isinstance(component, str) and component in COMPONENTS, "unknown component")
    selected = [record for record in records if record.component == component]
    require(len(selected) == len(PLATFORMS) and {r.platform for r in selected} == PLATFORMS,
            "native merge input closure mismatch")
    original: dict[str, dict[str, object]] = {}
    for record in selected:
        descriptors = record.validate(expected, ready_epoch)
        require(not set(original).intersection(descriptors), "duplicate source manifest")
        original.update(descriptors)
    merged = index_descriptors(payload, index_digest, PLATFORMS)
    require(canonical(merged) == canonical(original),
            "merged index changed runnable or attestation descriptors")
    # The whole interval is charged, including upload, the other branch and job scheduling.
    return ready_epoch - min(record.started_epoch for record in selected)
