#!/usr/bin/env python3
"""Create and self-verify the detached Ed25519 signature consumed by `insight update`."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path


ED25519_SPKI_PREFIX = bytes.fromhex("302a300506032b6570032100")


def command(arguments: list[str]) -> bytes:
    result = subprocess.run(arguments, capture_output=True, check=False)
    if result.returncode != 0:
        detail = result.stderr.decode(errors="replace").strip()[:512]
        raise ValueError(f"release signing command failed: {detail}")
    return result.stdout


def decode_public_key(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    try:
        key = base64.urlsafe_b64decode(value + padding)
    except ValueError as error:
        raise ValueError("release public key is not base64url") from error
    if len(key) != 32:
        raise ValueError("release public key must be exactly 32 bytes")
    return key


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bundle", required=True, type=Path)
    parser.add_argument("--private-key", required=True, type=Path)
    parser.add_argument("--public-key-base64", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    bundle = args.bundle.read_bytes()
    expected_public = decode_public_key(args.public_key_base64)
    public_der = command([
        "openssl", "pkey", "-in", str(args.private_key), "-pubout", "-outform", "DER"
    ])
    if public_der != ED25519_SPKI_PREFIX + expected_public:
        raise ValueError("release private key does not match the compiled public trust root")
    with tempfile.TemporaryDirectory() as temporary:
        bundle_path = Path(temporary) / "release-bundle.json"
        signature_path = Path(temporary) / "signature.bin"
        public_path = Path(temporary) / "public.pem"
        bundle_path.write_bytes(bundle)
        command([
            "openssl", "pkeyutl", "-sign", "-rawin", "-inkey", str(args.private_key),
            "-in", str(bundle_path), "-out", str(signature_path),
        ])
        public_path.write_bytes(command([
            "openssl", "pkey", "-in", str(args.private_key), "-pubout"
        ]))
        command([
            "openssl", "pkeyutl", "-verify", "-rawin", "-pubin", "-inkey",
            str(public_path), "-sigfile", str(signature_path), "-in", str(bundle_path),
        ])
        signature = signature_path.read_bytes()
    value = {
        "algorithm": "ed25519",
        "key_id": "sha256:" + hashlib.sha256(expected_public).hexdigest(),
        "schema_version": 1,
        "signature": base64.urlsafe_b64encode(signature).decode().rstrip("="),
    }
    args.output.write_bytes(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())


if __name__ == "__main__":
    main()
