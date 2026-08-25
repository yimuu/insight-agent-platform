#!/usr/bin/env python3
"""Create a canonical digest index for every file in a candidate artifact directory."""

import argparse
import hashlib
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("candidate_dir", type=Path)
    args = parser.parse_args()
    root = args.candidate_dir.resolve()
    excluded = {"release-bundle-manifest.json", "release-bundle-manifest.sigstore.json"}
    artifacts = []
    for path in sorted((item for item in root.rglob("*") if item.is_file()), key=lambda item: item.relative_to(root).as_posix()):
        relative = path.relative_to(root).as_posix()
        if relative in excluded:
            continue
        raw = path.read_bytes()
        artifacts.append({"bytes": len(raw), "path": relative, "sha256": hashlib.sha256(raw).hexdigest()})
    if not artifacts:
        raise ValueError("release bundle cannot be empty")
    manifest = {"schema_version": 1, "artifacts": artifacts}
    encoded = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    (root / "release-bundle-manifest.json").write_bytes(encoded)


if __name__ == "__main__":
    main()
