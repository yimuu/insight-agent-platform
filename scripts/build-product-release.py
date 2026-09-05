#!/usr/bin/env python3
"""Build the canonical, signed-subject release closure from already-built exact artifacts."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import stat
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TARGETS = (
    "aarch64-apple-darwin",
    "aarch64-unknown-linux-gnu",
    "x86_64-apple-darwin",
    "x86_64-unknown-linux-gnu",
)
REQUIRED_METADATA = (
    "build-provenance.intoto.jsonl",
    "cli.spdx.json",
    "console.spdx.json",
    "development-profile-v1.json",
    "release-performance.json",
    "runtime.spdx.json",
    "sandbox-runner.spdx.json",
)
DEVELOPMENT_QUALIFICATION_METADATA = "development-profile-performance.json"
PRODUCTIZATION_AGGREGATE_METADATA = "productization-10-of-10.json"
PRODUCTIZATION_SANDBOX_EVIDENCE_METADATA = "productization-opensandbox-evidence.json"
PRODUCTIZATION_SANDBOX_ENVIRONMENT_METADATA = "productization-sandbox-environment.json"
PRODUCTIZATION_RELEASE_CANDIDATE_METADATA = "productization-qualified-release-candidate.json"
PRODUCTIZATION_CHECKER = ROOT / "scripts/check-productization-scenario-reports.py"
BUNDLE_FIELDS = {
    "schema_version",
    "version",
    "git_commit",
    "created_at",
    "contract_digest",
    "profile_schema_digest",
    "development_profile_digest",
    "console",
    "cli",
    "images",
    "metadata",
}
RELEASE_CANDIDATE_FIELDS = {
    "release_bundle_digest",
    "runtime",
    "sandbox_runner",
    "console",
}
RELEASE_COMPONENT_FIELDS = {"subject", "index_digest", "platform", "platform_digest"}
DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")
TIME = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$")
SAFE_ASSET_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def strict_json_bytes(payload: bytes, label: str) -> object:
    def pairs(values: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in values:
            if key in result:
                raise ValueError(f"duplicate key {key!r} in {label}")
            result[key] = value
        return result
    return json.loads(payload, object_pairs_hook=pairs)


def sha256_bytes(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def payload_binding(payload: bytes) -> tuple[int, str]:
    return len(payload), sha256_bytes(payload)


def real_directory(path: Path, label: str) -> Path:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise ValueError(f"{label} is unavailable: {path}: {error}") from error
    if path.is_symlink() or not stat.S_ISDIR(metadata.st_mode):
        raise ValueError(f"{label} must be a real, non-symlink directory: {path}")
    return path.resolve()


def read_regular_bytes(path: Path, label: str) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ValueError(f"{label} must be a non-empty regular, non-symlink file: {path}") from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size == 0:
            raise ValueError(f"{label} must be a non-empty regular, non-symlink file: {path}")
        with os.fdopen(os.dup(descriptor), "rb") as source:
            payload = source.read()
        after = os.fstat(descriptor)
        stable_fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
        if any(getattr(before, field) != getattr(after, field) for field in stable_fields):
            raise ValueError(f"{label} changed while it was being read: {path}")
        if len(payload) != before.st_size:
            raise ValueError(f"{label} length changed while it was being read: {path}")
        return payload
    finally:
        os.close(descriptor)


def write_exclusive(path: Path, payload: bytes, label: str) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags, 0o644)
    except OSError as error:
        raise ValueError(f"{label} must be fresh and non-symlink: {path}") from error
    try:
        with os.fdopen(os.dup(descriptor), "wb") as destination:
            destination.write(payload)
            destination.flush()
            os.fsync(destination.fileno())
    finally:
        os.close(descriptor)


def artifact(
    root: Path,
    name: str,
    bound_payloads: dict[str, tuple[int, str]],
) -> dict[str, object]:
    payload = read_regular_bytes(root / name, f"required release artifact {name}")
    binding = payload_binding(payload)
    previous = bound_payloads.get(name)
    if previous is not None and binding != previous:
        raise ValueError(f"release artifact changed after it was bound: {name}")
    bound_payloads[name] = binding
    return {"path": name, "bytes": binding[0], "sha256": binding[1]}


def materialize_metadata(
    payload: bytes,
    root: Path,
    name: str,
    bound_payloads: dict[str, tuple[int, str]],
) -> str:
    write_exclusive(root / name, payload, f"release metadata destination {name}")
    bound_payloads[name] = payload_binding(payload)
    return name


def snapshot_report_directory(source: Path, destination: Path) -> dict[str, bytes]:
    source = real_directory(source, "productization report directory")
    destination.mkdir()
    payloads: dict[str, bytes] = {}
    for path in sorted(source.iterdir(), key=lambda item: item.name):
        if Path(path.name).name != path.name or not path.name.isascii() or path.suffix != ".json":
            raise ValueError(f"productization report directory contains a non-report entry: {path.name}")
        payload = read_regular_bytes(path, f"productization scenario report {path.name}")
        write_exclusive(destination / path.name, payload, f"scenario report snapshot {path.name}")
        payloads[path.name] = payload
    if not payloads:
        raise ValueError("productization report directory contains no reports")
    return payloads


def candidate_image_map(candidate: dict[str, object]) -> dict[str, dict[str, object]]:
    raw_images = candidate.get("images")
    if not isinstance(raw_images, list) or len(raw_images) != 3:
        raise ValueError("preliminary ReleaseBundle must contain exactly three images")
    result: dict[str, dict[str, object]] = {}
    for image in raw_images:
        if not isinstance(image, dict) or set(image) != {"name", "subject", "index_digest", "platforms"}:
            raise ValueError("preliminary ReleaseBundle contains a non-closed image")
        name = image.get("name")
        subject = image.get("subject")
        index_digest = image.get("index_digest")
        if (
            name not in {"runtime", "sandbox_runner", "console"}
            or name in result
            or not isinstance(subject, str)
            or not isinstance(index_digest, str)
            or not DIGEST.fullmatch(index_digest)
        ):
            raise ValueError("preliminary ReleaseBundle contains an invalid or duplicate image")
        raw_platforms = image.get("platforms")
        if not isinstance(raw_platforms, list) or len(raw_platforms) != 2:
            raise ValueError(f"preliminary ReleaseBundle image {name} is not multi-platform closed")
        platforms: dict[str, str] = {}
        for child in raw_platforms:
            if not isinstance(child, dict) or set(child) != {"platform", "digest"}:
                raise ValueError(f"preliminary ReleaseBundle image {name} has a non-closed child")
            platform = child.get("platform")
            digest = child.get("digest")
            if (
                platform not in {"linux/amd64", "linux/arm64"}
                or platform in platforms
                or not isinstance(digest, str)
                or not DIGEST.fullmatch(digest)
            ):
                raise ValueError(f"preliminary ReleaseBundle image {name} has an invalid child")
            platforms[platform] = digest
        if set(platforms) != {"linux/amd64", "linux/arm64"}:
            raise ValueError(f"preliminary ReleaseBundle image {name} misses a platform child")
        result[name] = {
            "subject": subject,
            "index_digest": index_digest,
            "platforms": platforms,
        }
    if set(result) != {"runtime", "sandbox_runner", "console"}:
        raise ValueError("preliminary ReleaseBundle image set is incomplete")
    return result


def validate_qualified_candidate(
    candidate_payload: bytes,
    evidence_payload: bytes,
    revision: str,
    version: str,
    current_images: dict[str, object],
) -> dict[str, object]:
    candidate = strict_json_bytes(candidate_payload, "preliminary signed ReleaseBundle")
    if candidate_payload != canonical(candidate):
        raise ValueError("preliminary signed ReleaseBundle must use canonical JSON bytes")
    if (
        not isinstance(candidate, dict)
        or set(candidate) != BUNDLE_FIELDS
        or candidate.get("schema_version") != 1
        or candidate.get("version") != version
        or candidate.get("git_commit") != revision
    ):
        raise ValueError("preliminary signed ReleaseBundle is not the same release revision")
    candidate_metadata = candidate.get("metadata")
    if (
        not isinstance(candidate_metadata, list)
        or [entry.get("path") if isinstance(entry, dict) else None for entry in candidate_metadata]
        != list(REQUIRED_METADATA)
    ):
        raise ValueError("preliminary signed ReleaseBundle must contain only base release metadata")

    evidence = strict_json_bytes(evidence_payload, "productization OpenSandbox evidence")
    release_candidate = evidence.get("release_candidate") if isinstance(evidence, dict) else None
    if not isinstance(release_candidate, dict) or set(release_candidate) != RELEASE_CANDIDATE_FIELDS:
        raise ValueError("productization evidence must bind a non-null signed release candidate")
    if release_candidate.get("release_bundle_digest") != sha256_bytes(candidate_payload):
        raise ValueError("productization evidence release_bundle_digest differs from the preliminary bundle")

    candidate_images = candidate_image_map(candidate)
    for name in ("runtime", "sandbox_runner", "console"):
        component = release_candidate.get(name)
        current = current_images.get(name)
        qualified = candidate_images[name]
        if not isinstance(component, dict) or set(component) != RELEASE_COMPONENT_FIELDS:
            raise ValueError(f"productization release_candidate.{name} is not closed")
        if not isinstance(current, dict):
            raise ValueError(f"current images input misses {name}")
        current_platforms = current.get("platforms")
        if not isinstance(current_platforms, dict):
            raise ValueError(f"current image {name} has no platform closure")
        expected = {
            "subject": qualified["subject"],
            "index_digest": qualified["index_digest"],
            "platform": "linux/amd64",
            "platform_digest": qualified["platforms"]["linux/amd64"],
        }
        current_identity = {
            "subject": current.get("subject"),
            "index_digest": current.get("index_digest"),
            "platform": "linux/amd64",
            "platform_digest": current_platforms.get("linux/amd64"),
        }
        if component != expected or current_identity != expected:
            raise ValueError(
                f"productization {name} identity differs across evidence, preliminary bundle, and images.json"
            )
    return candidate


def productization_metadata(
    root: Path,
    revision: str,
    version: str,
    current_images: dict[str, object],
    aggregate_path: Path,
    report_directory: Path,
    sandbox_evidence_path: Path,
    sandbox_environment_path: Path,
    release_candidate_path: Path,
    bound_payloads: dict[str, tuple[int, str]],
) -> tuple[list[str], dict[str, object]]:
    with tempfile.TemporaryDirectory(prefix="insight-productization-release-") as temporary:
        snapshot = Path(temporary)
        snapshot_reports = snapshot / "reports"
        report_payloads = snapshot_report_directory(report_directory, snapshot_reports)
        aggregate_payload = read_regular_bytes(aggregate_path, "productization aggregate")
        sandbox_evidence_payload = read_regular_bytes(
            sandbox_evidence_path, "productization OpenSandbox evidence"
        )
        sandbox_environment_payload = read_regular_bytes(
            sandbox_environment_path, "productization sandbox environment"
        )
        release_candidate_payload = read_regular_bytes(
            release_candidate_path, "preliminary signed ReleaseBundle"
        )
        snapshot_aggregate = snapshot / "supplied-aggregate.json"
        snapshot_evidence = snapshot / "sandbox-evidence.json"
        snapshot_environment = snapshot / "sandbox-environment.json"
        snapshot_candidate = snapshot / "preliminary-release-bundle.json"
        for destination, payload, label in (
            (snapshot_aggregate, aggregate_payload, "productization aggregate snapshot"),
            (snapshot_evidence, sandbox_evidence_payload, "OpenSandbox evidence snapshot"),
            (snapshot_environment, sandbox_environment_payload, "sandbox environment snapshot"),
            (snapshot_candidate, release_candidate_payload, "release candidate snapshot"),
        ):
            write_exclusive(destination, payload, label)

        recomputed_path = snapshot / PRODUCTIZATION_AGGREGATE_METADATA
        checked = subprocess.run(
            [
                sys.executable,
                str(PRODUCTIZATION_CHECKER),
                str(snapshot_reports),
                "--source-revision",
                revision,
                "--aggregate-output",
                str(recomputed_path),
                "--sandbox-evidence",
                str(snapshot_evidence),
                "--sandbox-environment",
                str(snapshot_environment),
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        if checked.returncode != 0:
            detail = checked.stderr.strip() or checked.stdout.strip()
            raise ValueError(f"productization qualification is invalid: {detail}")
        recomputed = read_regular_bytes(recomputed_path, "recomputed productization aggregate")

        aggregate = strict_json_bytes(aggregate_payload, "productization aggregate")
        if aggregate_payload != canonical(aggregate):
            raise ValueError("productization aggregate must use canonical JSON bytes")
        if aggregate_payload != recomputed:
            raise ValueError(
                "productization aggregate differs from the strict scenario qualification authority"
            )
        if not isinstance(aggregate, dict) or not isinstance(aggregate.get("reports"), list):
            raise ValueError("productization aggregate has no closed report list")

        report_assets: list[tuple[str, bytes]] = []
        for entry in aggregate["reports"]:
            if not isinstance(entry, dict) or not isinstance(entry.get("scenario_id"), str):
                raise ValueError("productization aggregate report entry is invalid")
            scenario_id = entry["scenario_id"]
            name = f"{scenario_id}.json"
            if Path(name).name != name or not name.isascii():
                raise ValueError(
                    f"productization scenario id is not a safe asset name: {scenario_id!r}"
                )
            payload = report_payloads.get(name)
            if payload is None or entry.get("report_digest") != sha256_bytes(payload):
                raise ValueError(
                    f"productization aggregate digest differs from snapshotted report: {name}"
                )
            report_assets.append((name, payload))

        candidate = validate_qualified_candidate(
            release_candidate_payload,
            sandbox_evidence_payload,
            revision,
            version,
            current_images,
        )
        metadata_assets = [
            (PRODUCTIZATION_AGGREGATE_METADATA, aggregate_payload),
            (PRODUCTIZATION_SANDBOX_EVIDENCE_METADATA, sandbox_evidence_payload),
            (PRODUCTIZATION_SANDBOX_ENVIRONMENT_METADATA, sandbox_environment_payload),
            (PRODUCTIZATION_RELEASE_CANDIDATE_METADATA, release_candidate_payload),
            *report_assets,
        ]
        names = [
            materialize_metadata(payload, root, name, bound_payloads)
            for name, payload in metadata_assets
        ]
        return names, candidate


def validate_cli_archive(
    root: Path,
    version: str,
    target: str,
    bound_payloads: dict[str, tuple[int, str]],
) -> tuple[dict[str, object], dict[str, object]]:
    archive_name = f"insight-{version}-{target}.tar.gz"
    binary_name = f"insight-{version}-{target}"
    archive_path = root / archive_name
    binary_path = root / binary_name
    archive_payload = read_regular_bytes(archive_path, f"CLI archive {archive_name}")
    binary_payload = read_regular_bytes(binary_path, f"CLI binary {binary_name}")
    bound_payloads[archive_name] = payload_binding(archive_payload)
    bound_payloads[binary_name] = payload_binding(binary_payload)
    archive = {
        "path": archive_name,
        "bytes": len(archive_payload),
        "sha256": sha256_bytes(archive_payload),
    }
    binary = {
        "path": binary_name,
        "bytes": len(binary_payload),
        "sha256": sha256_bytes(binary_payload),
    }
    with tarfile.open(fileobj=io.BytesIO(archive_payload), mode="r:gz") as package:
        members = package.getmembers()
        if [member.name for member in members] != ["insight", "LICENSE", "VERSION"]:
            raise ValueError(f"{archive_name} must contain only insight, LICENSE, VERSION in that order")
        if any(not member.isfile() or member.issym() or member.islnk() for member in members):
            raise ValueError(f"{archive_name} contains a non-regular member")
        executable = package.extractfile(members[0])
        if executable is None or executable.read() != binary_payload:
            raise ValueError(f"{archive_name} insight does not match its exact update binary")
        release_version = package.extractfile(members[2])
        if release_version is None or release_version.read() != f"{version}\n".encode():
            raise ValueError(f"{archive_name} VERSION does not match the release")
    return archive, binary


def validate_image(name: str, value: object) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != {"subject", "index_digest", "platforms"}:
        raise ValueError(f"image {name} is not a closed subject")
    subject = value["subject"]
    if not isinstance(subject, str) or ":latest" in subject or ":candidate-" in subject:
        raise ValueError(f"image {name} uses a mutable subject")
    if not isinstance(value["index_digest"], str) or not DIGEST.fullmatch(value["index_digest"]):
        raise ValueError(f"image {name} index digest is invalid")
    platforms = value["platforms"]
    if not isinstance(platforms, dict) or set(platforms) != {"linux/amd64", "linux/arm64"}:
        raise ValueError(f"image {name} must close amd64 and arm64")
    result = []
    for platform in sorted(platforms):
        digest = platforms[platform]
        if not isinstance(digest, str) or not DIGEST.fullmatch(digest):
            raise ValueError(f"image {name} {platform} digest is invalid")
        result.append({"platform": platform, "digest": digest})
    return {"name": name, "subject": subject, "index_digest": value["index_digest"], "platforms": result}


def repository_head() -> str:
    try:
        revision = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as error:
        raise ValueError(f"cannot resolve current repository HEAD: {error}") from error
    if not COMMIT.fullmatch(revision):
        raise ValueError("current repository HEAD is not an exact lowercase commit")
    return revision


def require_flat_regular_directory(root: Path) -> None:
    for path in root.iterdir():
        try:
            metadata = path.lstat()
        except OSError as error:
            raise ValueError(f"cannot inspect release artifact entry {path}: {error}") from error
        if not stat.S_ISREG(metadata.st_mode) or not SAFE_ASSET_NAME.fullmatch(path.name):
            raise ValueError(
                "release artifact directory may contain only regular, non-symlink files "
                f"with safe names: {path.name!r}"
            )


def verify_candidate_matches_bundle(
    candidate: dict[str, object],
    bundle: dict[str, object],
) -> None:
    for field in sorted(BUNDLE_FIELDS - {"metadata"}):
        if candidate.get(field) != bundle.get(field):
            raise ValueError(
                f"preliminary signed ReleaseBundle {field} differs from the qualified final bundle"
            )
    final_metadata = bundle.get("metadata")
    candidate_metadata = candidate.get("metadata")
    if (
        not isinstance(final_metadata, list)
        or not isinstance(candidate_metadata, list)
        or final_metadata[: len(candidate_metadata)] != candidate_metadata
    ):
        raise ValueError(
            "preliminary signed ReleaseBundle base metadata differs from the final bundle"
        )


def verify_bound_payloads(root: Path, expected: dict[str, tuple[int, str]]) -> None:
    for name, binding in expected.items():
        observed = read_regular_bytes(root / name, f"postcondition artifact {name}")
        if payload_binding(observed) != binding:
            raise ValueError(f"release artifact changed after it was bound: {name}")


def write_closed_checksums(
    root: Path,
    bound_payloads: dict[str, tuple[int, str]],
) -> None:
    checksum_path = root / "checksums.txt"
    captured: dict[str, tuple[int, str]] = {}
    for path in sorted(root.iterdir(), key=lambda item: item.name):
        try:
            metadata = path.lstat()
        except OSError as error:
            raise ValueError(f"cannot inspect checksum input {path}: {error}") from error
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"checksum input is not a regular, non-symlink file: {path.name}")
        payload = read_regular_bytes(path, f"checksum input {path.name}")
        binding = payload_binding(payload)
        expected = bound_payloads.get(path.name)
        if expected is not None and binding != expected:
            raise ValueError(f"checksummed artifact changed after it was bound: {path.name}")
        captured[path.name] = binding
    lines = "".join(
        f"{binding[1].removeprefix('sha256:')}  {name}\n"
        for name, binding in sorted(captured.items())
    ).encode("ascii")
    write_exclusive(checksum_path, lines, "release checksums")

    expected_names = set(captured) | {checksum_path.name}
    observed_names = {path.name for path in root.iterdir()}
    if observed_names != expected_names:
        raise ValueError("release artifact directory changed while checksums were generated")
    require_flat_regular_directory(root)
    for name, binding in captured.items():
        observed = read_regular_bytes(root / name, f"checksum postcondition {name}")
        if payload_binding(observed) != binding:
            raise ValueError(f"checksummed artifact changed after checksums were generated: {name}")
    if read_regular_bytes(checksum_path, "release checksums postcondition") != lines:
        raise ValueError("release checksums changed after they were generated")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--git-commit", required=True)
    parser.add_argument("--created-at", required=True)
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--images", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--include-development-qualification", action="store_true")
    parser.add_argument("--include-productization-qualification", action="store_true")
    parser.add_argument("--productization-aggregate", type=Path)
    parser.add_argument("--productization-report-directory", type=Path)
    parser.add_argument("--productization-sandbox-evidence", type=Path)
    parser.add_argument("--productization-sandbox-environment", type=Path)
    parser.add_argument("--productization-release-candidate-bundle", type=Path)
    args = parser.parse_args()
    if not VERSION.fullmatch(args.version):
        raise ValueError("version must be normalized major.minor.patch")
    if not COMMIT.fullmatch(args.git_commit):
        raise ValueError("git commit must be an exact lowercase 40-character SHA-1")
    if args.git_commit != repository_head():
        raise ValueError("git commit must equal the current repository HEAD")
    if not TIME.fullmatch(args.created_at):
        raise ValueError("created-at must be UTC with six fractional digits")
    root = real_directory(args.artifacts, "release artifact root")
    output = args.output
    if output.name != "release-bundle.json" or output.parent.resolve() != root:
        raise ValueError("output must be the canonical artifacts/release-bundle.json path")
    reserved_outputs = (
        output,
        root / "checksums.txt",
        root / "release-bundle.signature.json",
        root / "release-bundle.sigstore.json",
    )
    if any(os.path.lexists(path) for path in reserved_outputs):
        raise ValueError("release bundle, checksums, and signatures must be fresh and non-symlink")
    require_flat_regular_directory(root)

    bound_payloads: dict[str, tuple[int, str]] = {}
    images_payload = read_regular_bytes(args.images, "images input")
    images = strict_json_bytes(images_payload, "images input")
    if not isinstance(images, dict) or set(images) != {"console", "runtime", "sandbox_runner"}:
        raise ValueError("images input must close console, runtime, and sandbox_runner")
    validated_images = [validate_image(name, images[name]) for name in sorted(images)]
    images_path = args.images.resolve()
    if images_path.parent == root:
        bound_payloads[images_path.name] = payload_binding(images_payload)

    profile_source = ROOT / "release/development-profile-v1.json"
    profile_schema = ROOT / "release/development-profile-v1.schema.json"
    profile_source_payload = read_regular_bytes(profile_source, "repository development profile")
    profile_schema_payload = read_regular_bytes(profile_schema, "development profile schema")
    profile_value = strict_json_bytes(profile_source_payload, "repository development profile")
    canonical_profile = canonical(profile_value)
    profile_asset = root / "development-profile-v1.json"
    profile_asset_payload = read_regular_bytes(profile_asset, "release development profile")
    if profile_asset_payload != canonical_profile:
        raise ValueError("release development profile is not the canonical repository profile")
    bound_payloads[profile_asset.name] = payload_binding(profile_asset_payload)

    cli = []
    for target in TARGETS:
        archive, binary = validate_cli_archive(root, args.version, target, bound_payloads)
        cli.append({"target": target, "archive": archive, "binary": binary})
    metadata_names = list(REQUIRED_METADATA)
    if args.include_development_qualification:
        metadata_names.append(DEVELOPMENT_QUALIFICATION_METADATA)
    productization_inputs = (
        args.productization_aggregate,
        args.productization_report_directory,
        args.productization_sandbox_evidence,
        args.productization_sandbox_environment,
        args.productization_release_candidate_bundle,
    )
    preliminary_candidate: dict[str, object] | None = None
    if args.include_productization_qualification:
        if any(value is None for value in productization_inputs):
            raise ValueError(
                "productization qualification requires aggregate, report directory, "
                "sandbox evidence, sandbox environment, and preliminary release candidate inputs"
            )
        productization_names, preliminary_candidate = productization_metadata(
            root,
            args.git_commit,
            args.version,
            images,
            args.productization_aggregate,
            args.productization_report_directory,
            args.productization_sandbox_evidence,
            args.productization_sandbox_environment,
            args.productization_release_candidate_bundle,
            bound_payloads,
        )
        metadata_names.extend(productization_names)
    elif any(value is not None for value in productization_inputs):
        raise ValueError(
            "productization evidence inputs require --include-productization-qualification"
        )
    bundle = {
        "schema_version": 1,
        "version": args.version,
        "git_commit": args.git_commit,
        "created_at": args.created_at,
        "contract_digest": strict_json_bytes(
            read_regular_bytes(
                ROOT / "contracts/platform-v1/manifest.json", "platform contract manifest"
            ),
            "platform contract manifest",
        )["contract_digest"],
        "profile_schema_digest": sha256_bytes(profile_schema_payload),
        "development_profile_digest": sha256_bytes(canonical_profile),
        "console": artifact(root, f"console-{args.version}.tar.gz", bound_payloads),
        "cli": cli,
        "images": validated_images,
        "metadata": [artifact(root, name, bound_payloads) for name in metadata_names],
    }
    if preliminary_candidate is not None:
        verify_candidate_matches_bundle(preliminary_candidate, bundle)
    encoded = canonical(bundle)
    write_exclusive(output, encoded, "canonical ReleaseBundle output")
    bound_payloads[output.name] = payload_binding(encoded)
    verify_bound_payloads(root, bound_payloads)
    write_closed_checksums(root, bound_payloads)


if __name__ == "__main__":
    main()
