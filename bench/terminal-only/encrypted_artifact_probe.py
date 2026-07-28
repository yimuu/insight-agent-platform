#!/usr/bin/env python3
"""Fail-closed inspection of a tenant-encrypted Artifact's stored bytes."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


MAGIC = b"IAPTEA01"
MAX_KEY_VERSION_BYTES = 64
TENANT_DIGEST_BYTES = 32
NONCE_BYTES = 12
AUTHENTICATION_TAG_BYTES = 16
MIN_ENCRYPTED_BODY_BYTES = (
    TENANT_DIGEST_BYTES + NONCE_BYTES + AUTHENTICATION_TAG_BYTES
)
VALID_KEY_VERSION = re.compile(r"^[A-Za-z0-9._-]+$")


def inspect_stored_bytes(
    stored: bytes,
    *,
    tenant_id: str,
    marker: str,
    expected_key_version: str,
) -> dict[str, Any]:
    failures: list[str] = []
    key_version: str | None = None
    framing_complete = False
    if not stored.startswith(MAGIC):
        failures.append("stored Artifact does not start with IAPTEA01")
    elif len(stored) <= len(MAGIC):
        failures.append("stored Artifact has no key-version length")
    else:
        version_length = stored[len(MAGIC)]
        version_start = len(MAGIC) + 1
        version_end = version_start + version_length
        if (
            version_length == 0
            or version_length > MAX_KEY_VERSION_BYTES
            or version_end > len(stored)
        ):
            failures.append("stored Artifact has an invalid key-version length")
        else:
            try:
                key_version = stored[version_start:version_end].decode("ascii")
            except UnicodeDecodeError:
                failures.append("stored Artifact key version is not ASCII")
            if key_version is not None and not VALID_KEY_VERSION.fullmatch(
                key_version
            ):
                failures.append("stored Artifact key version is invalid")
            framing_complete = (
                len(stored) - version_end >= MIN_ENCRYPTED_BODY_BYTES
            )
            if not framing_complete:
                failures.append(
                    "stored Artifact is truncated before the tenant digest, "
                    "nonce, and authentication tag are complete"
                )
    if key_version != expected_key_version:
        failures.append(
            "stored Artifact key version does not match the active "
            f"qualification version: {key_version!r}"
        )

    tenant_bytes = tenant_id.encode("utf-8")
    marker_bytes = marker.encode("utf-8")
    tenant_plaintext_absent = tenant_bytes not in stored
    marker_plaintext_absent = marker_bytes not in stored
    if not tenant_plaintext_absent:
        failures.append("tenant ID occurs in stored Artifact plaintext")
    if not marker_plaintext_absent:
        failures.append("privacy marker occurs in stored Artifact plaintext")

    return {
        "passed": not failures,
        "failures": failures,
        "magic": MAGIC.decode("ascii") if stored.startswith(MAGIC) else None,
        "active_key_version": key_version,
        "stored_bytes": len(stored),
        "framing_complete": framing_complete,
        "tenant_id_plaintext_absent": tenant_plaintext_absent,
        "marker_plaintext_absent": marker_plaintext_absent,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--marker", required=True)
    parser.add_argument("--expected-key-version", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    report = inspect_stored_bytes(
        Path(args.input).read_bytes(),
        tenant_id=args.tenant_id,
        marker=args.marker,
        expected_key_version=args.expected_key_version,
    )
    Path(args.output).write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
