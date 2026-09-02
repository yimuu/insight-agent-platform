#!/usr/bin/env python3
"""Build the canonical, signed-subject release closure from already-built exact artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import tarfile
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
DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")
TIME = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$")


def canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def strict_json(path: Path) -> object:
    def pairs(values: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in values:
            if key in result:
                raise ValueError(f"duplicate key {key!r} in {path}")
            result[key] = value
        return result
    return json.loads(path.read_bytes(), object_pairs_hook=pairs)


def sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def artifact(root: Path, name: str) -> dict[str, object]:
    path = root / name
    if not path.is_file() or path.stat().st_size == 0:
        raise ValueError(f"required release artifact is missing or empty: {name}")
    return {"path": name, "bytes": path.stat().st_size, "sha256": sha256(path)}


def validate_cli_archive(root: Path, version: str, target: str) -> tuple[dict[str, object], dict[str, object]]:
    archive_name = f"insight-{version}-{target}.tar.gz"
    binary_name = f"insight-{version}-{target}"
    archive_path = root / archive_name
    binary_path = root / binary_name
    archive = artifact(root, archive_name)
    binary = artifact(root, binary_name)
    with tarfile.open(archive_path, "r:gz") as package:
        members = package.getmembers()
        if [member.name for member in members] != ["insight", "LICENSE", "VERSION"]:
            raise ValueError(f"{archive_name} must contain only insight, LICENSE, VERSION in that order")
        if any(not member.isfile() or member.issym() or member.islnk() for member in members):
            raise ValueError(f"{archive_name} contains a non-regular member")
        executable = package.extractfile(members[0])
        if executable is None or executable.read() != binary_path.read_bytes():
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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--git-commit", required=True)
    parser.add_argument("--created-at", required=True)
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--images", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--include-development-qualification", action="store_true")
    args = parser.parse_args()
    if not VERSION.fullmatch(args.version):
        raise ValueError("version must be normalized major.minor.patch")
    if not COMMIT.fullmatch(args.git_commit):
        raise ValueError("git commit must be an exact lowercase 40-character SHA-1")
    if not TIME.fullmatch(args.created_at):
        raise ValueError("created-at must be UTC with six fractional digits")
    root = args.artifacts.resolve()
    images = strict_json(args.images)
    if not isinstance(images, dict) or set(images) != {"console", "runtime", "sandbox_runner"}:
        raise ValueError("images input must close console, runtime, and sandbox_runner")

    profile_source = ROOT / "release/development-profile-v1.json"
    profile_schema = ROOT / "release/development-profile-v1.schema.json"
    profile_value = strict_json(profile_source)
    canonical_profile = canonical(profile_value)
    profile_asset = root / "development-profile-v1.json"
    if profile_asset.read_bytes() != canonical_profile:
        raise ValueError("release development profile is not the canonical repository profile")

    cli = []
    for target in TARGETS:
        archive, binary = validate_cli_archive(root, args.version, target)
        cli.append({"target": target, "archive": archive, "binary": binary})
    metadata_names = list(REQUIRED_METADATA)
    if args.include_development_qualification:
        metadata_names.append(DEVELOPMENT_QUALIFICATION_METADATA)
    bundle = {
        "schema_version": 1,
        "version": args.version,
        "git_commit": args.git_commit,
        "created_at": args.created_at,
        "contract_digest": strict_json(ROOT / "contracts/platform-v1/manifest.json")["contract_digest"],
        "profile_schema_digest": sha256(profile_schema),
        "development_profile_digest": "sha256:" + hashlib.sha256(canonical_profile).hexdigest(),
        "console": artifact(root, f"console-{args.version}.tar.gz"),
        "cli": cli,
        "images": [validate_image(name, images[name]) for name in sorted(images)],
        "metadata": [artifact(root, name) for name in metadata_names],
    }
    encoded = canonical(bundle)
    args.output.write_bytes(encoded)
    checksum_paths = sorted(
        path for path in root.iterdir()
        if path.is_file() and path.name not in {
            "checksums.txt", "release-bundle.signature.json", args.output.name
        }
    ) + [args.output]
    checksums = "".join(
        f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n" for path in checksum_paths
    )
    (root / "checksums.txt").write_text(checksums, encoding="ascii")


if __name__ == "__main__":
    main()
