#!/usr/bin/env python3
"""Prove that the signed Console archive and released Console image contain one tree."""

from __future__ import annotations

import argparse
import hashlib
import json
import stat
from pathlib import Path


def tree(root: Path) -> tuple[str, dict[str, str]]:
    if not root.is_dir() or root.is_symlink():
        raise ValueError(f"Console root is not a real directory: {root}")
    framed = hashlib.sha256()
    files: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not (stat.S_ISDIR(metadata.st_mode) or stat.S_ISREG(metadata.st_mode)):
            raise ValueError(f"Console tree contains a non-regular entry: {path}")
        if not stat.S_ISREG(metadata.st_mode):
            continue
        relative = path.relative_to(root).as_posix()
        payload = path.read_bytes()
        files[relative] = "sha256:" + hashlib.sha256(payload).hexdigest()
        encoded = relative.encode()
        framed.update(len(encoded).to_bytes(8, "big"))
        framed.update(encoded)
        framed.update(len(payload).to_bytes(8, "big"))
        framed.update(payload)
    if "index.html" not in files:
        raise ValueError("Console tree has no index.html")
    return "sha256:" + framed.hexdigest(), files


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-closure", required=True, type=Path)
    parser.add_argument("--archive-directory", required=True, type=Path)
    parser.add_argument("--image-directory", required=True, type=Path)
    args = parser.parse_args()
    for path, label in (
        (args.candidate_closure, "candidate closure"),
        (args.archive_directory, "archive directory"),
        (args.image_directory, "image directory"),
    ):
        if path.is_symlink():
            raise ValueError(f"{label} must not be a symbolic link")
    closure = json.loads(args.candidate_closure.read_bytes())
    expected = closure.get("console_assets", {}).get("tree_digest")
    archive_digest, archive_files = tree(args.archive_directory.resolve())
    image_digest, image_files = tree(args.image_directory.resolve())
    if expected != archive_digest or archive_digest != image_digest or archive_files != image_files:
        raise ValueError("signed Console archive and exact released Console image trees differ")
    print(f"Exact candidate Console tree passed ({archive_digest}).")


if __name__ == "__main__":
    main()
