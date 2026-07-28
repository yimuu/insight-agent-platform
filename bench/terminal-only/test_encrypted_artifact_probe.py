#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


PROBE_PATH = Path(__file__).with_name("encrypted_artifact_probe.py")
SPEC = importlib.util.spec_from_file_location("encrypted_artifact_probe", PROBE_PATH)
assert SPEC is not None and SPEC.loader is not None
PROBE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROBE)


def encrypted_fixture(
    *,
    version: str = "qualification-v1",
    ciphertext: bytes = b"\x00\x01opaque",
) -> bytes:
    encoded_version = version.encode("ascii")
    return (
        PROBE.MAGIC
        + bytes([len(encoded_version)])
        + encoded_version
        + b"d" * 32
        + b"n" * 12
        + ciphertext
        + b"t" * 16
    )


class EncryptedArtifactProbeTests(unittest.TestCase):
    def test_encrypted_artifact_passes_without_plaintext(self) -> None:
        result = PROBE.inspect_stored_bytes(
            encrypted_fixture(),
            tenant_id="tenant-private",
            marker="marker-private",
            expected_key_version="qualification-v1",
        )
        self.assertTrue(result["passed"])
        self.assertEqual(result["magic"], "IAPTEA01")
        self.assertEqual(result["active_key_version"], "qualification-v1")
        self.assertTrue(result["framing_complete"])

    def test_exact_minimum_complete_framing_passes(self) -> None:
        result = PROBE.inspect_stored_bytes(
            encrypted_fixture(ciphertext=b""),
            tenant_id="tenant-private",
            marker="marker-private",
            expected_key_version="qualification-v1",
        )
        self.assertTrue(result["passed"])
        self.assertTrue(result["framing_complete"])

    def test_truncated_digest_nonce_or_authentication_tag_fails(self) -> None:
        version = b"qualification-v1"
        prefix = PROBE.MAGIC + bytes([len(version)]) + version
        incomplete_lengths = (
            0,
            PROBE.TENANT_DIGEST_BYTES - 1,
            PROBE.TENANT_DIGEST_BYTES,
            PROBE.TENANT_DIGEST_BYTES + PROBE.NONCE_BYTES - 1,
            PROBE.TENANT_DIGEST_BYTES + PROBE.NONCE_BYTES,
            PROBE.MIN_ENCRYPTED_BODY_BYTES - 1,
        )
        for body_length in incomplete_lengths:
            with self.subTest(body_length=body_length):
                result = PROBE.inspect_stored_bytes(
                    prefix + b"x" * body_length,
                    tenant_id="tenant-private",
                    marker="marker-private",
                    expected_key_version="qualification-v1",
                )
                self.assertFalse(result["passed"])
                self.assertFalse(result["framing_complete"])
                self.assertTrue(
                    any("truncated" in item for item in result["failures"])
                )

    def test_plaintext_or_wrong_magic_fails(self) -> None:
        result = PROBE.inspect_stored_bytes(
            b"PLAINTXT" + b"tenant-private marker-private",
            tenant_id="tenant-private",
            marker="marker-private",
            expected_key_version="qualification-v1",
        )
        self.assertFalse(result["passed"])
        self.assertTrue(any("IAPTEA01" in item for item in result["failures"]))
        self.assertTrue(any("tenant ID" in item for item in result["failures"]))
        self.assertTrue(any("privacy marker" in item for item in result["failures"]))

    def test_non_active_key_version_fails(self) -> None:
        result = PROBE.inspect_stored_bytes(
            encrypted_fixture(version="old-v1"),
            tenant_id="tenant-private",
            marker="marker-private",
            expected_key_version="qualification-v1",
        )
        self.assertFalse(result["passed"])
        self.assertTrue(any("active" in item for item in result["failures"]))


if __name__ == "__main__":
    unittest.main()
