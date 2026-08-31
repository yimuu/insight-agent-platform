#!/usr/bin/env python3
"""Reduce BuildKit OCI indexes to the exact child subjects used by ReleaseBundle."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


def strict_json(path: Path) -> object:
    def pairs(values: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in values:
            if key in result:
                raise ValueError(f"duplicate key {key!r} in {path}")
            result[key] = value
        return result
    return json.loads(path.read_bytes(), object_pairs_hook=pairs)


def image(name: str, subject: str, digest: str, index_path: Path) -> dict[str, object]:
    if not DIGEST.fullmatch(digest):
        raise ValueError(f"{name} index digest is invalid")
    if "@" in subject or ":latest" in subject or ":candidate-" in subject:
        raise ValueError(f"{name} subject must be one immutable release tag without a digest")
    index = strict_json(index_path)
    if not isinstance(index, dict) or not isinstance(index.get("manifests"), list):
        raise ValueError(f"{name} is not an OCI image index")
    platforms: dict[str, str] = {}
    for descriptor in index["manifests"]:
        if not isinstance(descriptor, dict) or not isinstance(descriptor.get("platform"), dict):
            continue
        platform = descriptor["platform"]
        key = f"{platform.get('os')}/{platform.get('architecture')}"
        if key not in {"linux/amd64", "linux/arm64"}:
            continue
        child = descriptor.get("digest")
        if key in platforms or not isinstance(child, str) or not DIGEST.fullmatch(child):
            raise ValueError(f"{name} contains a duplicate or invalid {key} descriptor")
        platforms[key] = child
    if set(platforms) != {"linux/amd64", "linux/arm64"}:
        raise ValueError(f"{name} index does not close linux/amd64 and linux/arm64")
    return {"subject": subject, "index_digest": digest, "platforms": platforms}


def main() -> None:
    parser = argparse.ArgumentParser()
    for name in ("runtime", "sandbox_guest", "console"):
        option = name.replace("_", "-")
        parser.add_argument(f"--{option}-subject", required=True)
        parser.add_argument(f"--{option}-digest", required=True)
        parser.add_argument(f"--{option}-index", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = {}
    for name in ("runtime", "sandbox_guest", "console"):
        result[name] = image(
            name,
            getattr(args, f"{name}_subject"),
            getattr(args, f"{name}_digest"),
            getattr(args, f"{name}_index"),
        )
    args.output.write_bytes(json.dumps(result, sort_keys=True, separators=(",", ":")).encode())


if __name__ == "__main__":
    main()
