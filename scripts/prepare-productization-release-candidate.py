#!/usr/bin/env python3
"""Validate and unpack the exact signed candidate used by the 10/10 journey."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import tarfile
from pathlib import Path, PurePosixPath


DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
HEX_DIGEST = re.compile(r"^[0-9a-f]{64}$")
REVISION = re.compile(r"^[0-9a-f]{40}$")
RELEASE_TAG = re.compile(r"^v([0-9]+\.[0-9]+\.[0-9]+)$")
REPOSITORY = re.compile(r"^[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)+$")
MAX_CONSOLE_MEMBERS = 4_096
MAX_CONSOLE_MEMBER_BYTES = 32 * 1024 * 1024
MAX_CONSOLE_TOTAL_BYTES = 256 * 1024 * 1024


def strict_json(path: Path) -> object:
    def pairs(values: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in values:
            if key in result:
                raise ValueError(f"duplicate key {key!r} in {path}")
            result[key] = value
        return result

    return json.loads(path.read_bytes(), object_pairs_hook=pairs)


def regular_file(path: Path, label: str) -> os.stat_result:
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or path.is_symlink() or metadata.st_size == 0:
        raise ValueError(f"{label} must be a non-empty regular, non-symlink file")
    return metadata


def digest_bytes(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def digest_file(path: Path) -> str:
    return digest_bytes(path.read_bytes())


def closed(value: object, keys: set[str], label: str) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError(f"{label} must contain exactly {sorted(keys)}")
    return value


def checked_artifact(root: Path, value: object, label: str) -> Path:
    artifact = closed(value, {"path", "bytes", "sha256"}, label)
    name = artifact["path"]
    if not isinstance(name, str) or Path(name).name != name or name in {".", ".."}:
        raise ValueError(f"{label} path is unsafe")
    path = root / name
    metadata = regular_file(path, label)
    if artifact["bytes"] != metadata.st_size or artifact["sha256"] != digest_file(path):
        raise ValueError(f"{label} bytes or digest differs from ReleaseBundle")
    return path


def validate_checksums(root: Path) -> None:
    checksum_path = root / "checksums.txt"
    regular_file(checksum_path, "checksums.txt")
    observed: dict[str, str] = {}
    for line in checksum_path.read_text(encoding="ascii").splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  ([A-Za-z0-9][A-Za-z0-9._-]*)", line)
        if match is None or match.group(2) in observed:
            raise ValueError("checksums.txt is malformed, unsafe, or contains a duplicate")
        observed[match.group(2)] = match.group(1)
    excluded = {
        "checksums.txt",
        "release-bundle.signature.json",
        "release-bundle.sigstore.json",
    }
    expected = {path.name for path in root.iterdir() if path.name not in excluded}
    if set(observed) != expected:
        raise ValueError("checksums.txt does not close the candidate artifact directory")
    for name, expected_digest in observed.items():
        path = root / name
        regular_file(path, f"checksummed artifact {name}")
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected_digest:
            raise ValueError(f"checksummed artifact {name} has drifted")


def validate_image(
    value: object,
    name: str,
    expected_subject: str,
    platform: str,
) -> tuple[dict[str, str], dict[str, str]]:
    image = closed(value, {"name", "subject", "index_digest", "platforms"}, f"image {name}")
    if image["name"] != name or image["subject"] != expected_subject:
        raise ValueError(f"image {name} does not use its exact release repository")
    index_digest = image["index_digest"]
    if not isinstance(index_digest, str) or DIGEST.fullmatch(index_digest) is None:
        raise ValueError(f"image {name} index digest is invalid")
    platforms = image["platforms"]
    if not isinstance(platforms, list) or len(platforms) != 2:
        raise ValueError(f"image {name} must close exactly two release platforms")
    mapped: dict[str, str] = {}
    for position, raw in enumerate(platforms):
        child = closed(raw, {"platform", "digest"}, f"image {name} platform {position}")
        child_platform = child["platform"]
        child_digest = child["digest"]
        if (
            not isinstance(child_platform, str)
            or child_platform in mapped
            or child_platform not in {"linux/amd64", "linux/arm64"}
            or not isinstance(child_digest, str)
            or DIGEST.fullmatch(child_digest) is None
        ):
            raise ValueError(f"image {name} has an invalid or duplicate platform child")
        mapped[child_platform] = child_digest
    if set(mapped) != {"linux/amd64", "linux/arm64"}:
        raise ValueError(f"image {name} platform closure is incomplete")
    return (
        {
            "subject": expected_subject,
            "index_digest": index_digest,
            "platform": platform,
            "platform_digest": mapped[platform],
        },
        mapped,
    )


def validate_images_json(
    path: Path,
    release_images: dict[str, dict[str, str]],
    release_platforms: dict[str, dict[str, str]],
) -> None:
    raw = closed(strict_json(path), {"runtime", "sandbox_runner", "console"}, "images.json")
    for name, expected in release_images.items():
        value = closed(raw[name], {"subject", "index_digest", "platforms"}, f"images.json {name}")
        if value["subject"] != expected["subject"] or value["index_digest"] != expected["index_digest"]:
            raise ValueError(f"images.json {name} differs from ReleaseBundle")
        platforms = value["platforms"]
        if not isinstance(platforms, dict) or set(platforms) != {"linux/amd64", "linux/arm64"}:
            raise ValueError(f"images.json {name} platform closure is invalid")
        if any(not isinstance(item, str) or DIGEST.fullmatch(item) is None for item in platforms.values()):
            raise ValueError(f"images.json {name} contains an invalid child digest")
        if platforms != release_platforms[name]:
            raise ValueError(f"images.json {name} platform children differ from ReleaseBundle")


def safe_member_name(raw: str) -> str:
    if "\\" in raw:
        raise ValueError("Console archive contains a non-portable path")
    value = PurePosixPath(raw)
    if value.is_absolute() or ".." in value.parts:
        raise ValueError("Console archive contains an escaping path")
    normalized = PurePosixPath(*[part for part in value.parts if part not in {"", "."}])
    return normalized.as_posix()


def extract_console(archive_path: Path, output: Path) -> None:
    if output.exists() or output.is_symlink():
        raise ValueError("Console output directory must not already exist")
    output.mkdir(parents=True)
    names: set[str] = set()
    total = 0
    try:
        with tarfile.open(archive_path, "r:gz") as archive:
            members = archive.getmembers()
            if len(members) > MAX_CONSOLE_MEMBERS:
                raise ValueError("Console archive contains too many members")
            for member in members:
                name = safe_member_name(member.name)
                if not name:
                    if not member.isdir():
                        raise ValueError("Console archive root member is not a directory")
                    continue
                if name in names:
                    raise ValueError("Console archive contains duplicate members")
                names.add(name)
                if not member.isdir() and not member.isfile():
                    raise ValueError("Console archive contains a non-regular member")
                destination = output.joinpath(*PurePosixPath(name).parts)
                destination.resolve().relative_to(output.resolve())
                if member.isdir():
                    destination.mkdir(parents=True, exist_ok=False)
                    continue
                if member.size < 0 or member.size > MAX_CONSOLE_MEMBER_BYTES:
                    raise ValueError("Console archive member exceeds the size limit")
                total += member.size
                if total > MAX_CONSOLE_TOTAL_BYTES:
                    raise ValueError("Console archive exceeds the expanded size limit")
                destination.parent.mkdir(parents=True, exist_ok=True)
                source = archive.extractfile(member)
                if source is None:
                    raise ValueError("Console archive regular member has no payload")
                payload = source.read(MAX_CONSOLE_MEMBER_BYTES + 1)
                if len(payload) != member.size:
                    raise ValueError("Console archive member length differs from its header")
                with destination.open("xb") as target:
                    target.write(payload)
        regular_file(output / "index.html", "Console index.html")
    except Exception:
        for path in sorted(output.rglob("*"), reverse=True):
            if path.is_dir() and not path.is_symlink():
                path.rmdir()
            else:
                path.unlink()
        output.rmdir()
        raise


def tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    files = sorted(path for path in root.rglob("*") if path.is_file())
    if any(path.is_symlink() for path in root.rglob("*")):
        raise ValueError(f"Console tree {root} contains a symbolic link")
    for path in files:
        relative = path.relative_to(root).as_posix().encode()
        payload = path.read_bytes()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return "sha256:" + digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--assets", required=True, type=Path)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--release-tag", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--platform", choices=("linux/amd64", "linux/arm64"), required=True)
    parser.add_argument("--console-output", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    tag_match = RELEASE_TAG.fullmatch(args.release_tag)
    if tag_match is None or REVISION.fullmatch(args.revision) is None:
        raise ValueError("release tag or revision is invalid")
    if REPOSITORY.fullmatch(args.repository) is None:
        raise ValueError("repository is invalid")
    version = tag_match.group(1)
    if not args.assets.is_dir() or args.assets.is_symlink() or args.output.exists() or args.output.is_symlink():
        raise ValueError("candidate directory must be real and output must be fresh")
    root = args.assets.resolve()
    for path in root.iterdir():
        regular_file(path, f"candidate artifact {path.name}")
    for required in (
        "checksums.txt",
        "images.json",
        "release-bundle.json",
        "release-bundle.signature.json",
        "release-bundle.sigstore.json",
    ):
        regular_file(root / required, required)
    validate_checksums(root)

    bundle_path = root / "release-bundle.json"
    bundle = closed(
        strict_json(bundle_path),
        {
            "schema_version", "version", "git_commit", "created_at", "contract_digest",
            "profile_schema_digest", "development_profile_digest", "console", "cli",
            "images", "metadata",
        },
        "ReleaseBundle",
    )
    if bundle["schema_version"] != 1 or bundle["version"] != version or bundle["git_commit"] != args.revision:
        raise ValueError("ReleaseBundle does not match the requested exact release")

    expected_subjects = {
        "runtime": f"ghcr.io/{args.repository}/platform-runtime",
        "sandbox_runner": f"ghcr.io/{args.repository}/platform-sandbox-runner",
        "console": f"ghcr.io/{args.repository}/platform-console",
    }
    raw_images = bundle["images"]
    if not isinstance(raw_images, list) or len(raw_images) != 3:
        raise ValueError("ReleaseBundle image closure is not exact")
    release_images: dict[str, dict[str, str]] = {}
    release_platforms: dict[str, dict[str, str]] = {}
    for value in raw_images:
        if not isinstance(value, dict) or not isinstance(value.get("name"), str):
            raise ValueError("ReleaseBundle contains an invalid image")
        name = value["name"]
        if name not in expected_subjects or name in release_images:
            raise ValueError("ReleaseBundle contains an unknown or duplicate image")
        identity, platforms = validate_image(value, name, expected_subjects[name], args.platform)
        release_images[name] = identity
        release_platforms[name] = platforms
    if set(release_images) != set(expected_subjects):
        raise ValueError("ReleaseBundle image closure is incomplete")
    validate_images_json(root / "images.json", release_images, release_platforms)

    cli = bundle["cli"]
    if not isinstance(cli, list):
        raise ValueError("ReleaseBundle CLI closure is invalid")
    target = {"linux/amd64": "x86_64-unknown-linux-gnu", "linux/arm64": "aarch64-unknown-linux-gnu"}[args.platform]
    matches = [value for value in cli if isinstance(value, dict) and value.get("target") == target]
    if len(matches) != 1:
        raise ValueError("ReleaseBundle does not contain exactly one host CLI")
    host_cli = closed(matches[0], {"target", "archive", "binary"}, "host CLI")
    binary_path = checked_artifact(root, host_cli["binary"], "host CLI binary")
    checked_artifact(root, host_cli["archive"], "host CLI archive")
    console_path = checked_artifact(root, bundle["console"], "Console archive")
    extract_console(console_path, args.console_output)

    closure = {
        "schema_version": 1,
        "kind": "insight.productization.release-candidate/v1",
        "source_revision": args.revision,
        "version": version,
        "release_bundle_digest": digest_file(bundle_path),
        "cli": {"path": str(binary_path), "sha256": digest_file(binary_path)},
        "console_assets": {
            "path": str(args.console_output.resolve()),
            "archive_sha256": digest_file(console_path),
            "tree_digest": tree_digest(args.console_output),
        },
        "images": release_images,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(json.dumps(closure, sort_keys=True, separators=(",", ":")).encode())


if __name__ == "__main__":
    main()
