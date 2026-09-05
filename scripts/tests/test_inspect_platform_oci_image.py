from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
INSPECTOR = REPOSITORY_ROOT / "scripts" / "inspect-platform-oci-image.py"
OCI_INDEX = "application/vnd.oci.image.index.v1+json"
OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_CONFIG = "application/vnd.oci.image.config.v1+json"
OCI_LAYER = "application/vnd.oci.image.layer.v1.tar+gzip"


def encoded_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def descriptor(payload: bytes, media_type: str) -> Dict[str, Any]:
    return {
        "digest": digest(payload),
        "mediaType": media_type,
        "size": len(payload),
    }


def write_layout(
    path: Path,
    *,
    architecture: str = "amd64",
    config_architecture: Optional[str] = None,
    config_os: str = "linux",
    descriptor_platform: Optional[Mapping[str, str]] = None,
    layout_marker: Optional[Mapping[str, Any]] = None,
    index_descriptor_media_type: str = OCI_MANIFEST,
    manifest_media_type: str = OCI_MANIFEST,
    config_descriptor_media_type: str = OCI_CONFIG,
    index_schema_version: Any = 2,
    manifest_schema_version: Any = 2,
    extra_index_descriptor: bool = False,
    duplicate_index_key: bool = False,
    duplicate_config_key: bool = False,
    corrupt_manifest_blob: bool = False,
    corrupt_config_blob: bool = False,
    corrupt_layer_blob: bool = False,
    manifest_size_delta: int = 0,
    config_size_delta: int = 0,
    add_symlink_member: bool = False,
) -> Tuple[str, str]:
    config_value = {
        "architecture": config_architecture or architecture,
        "config": {"Entrypoint": ["/bin/example"]},
        "os": config_os,
        "rootfs": {"diff_ids": [], "type": "layers"},
    }
    if duplicate_config_key:
        config_payload = (
            b'{"architecture":"'
            + (config_architecture or architecture).encode()
            + b'","architecture":"'
            + (config_architecture or architecture).encode()
            + b'","os":"'
            + config_os.encode()
            + b'"}'
        )
    else:
        config_payload = encoded_json(config_value)

    layer_payload = b"qualification-layer"
    config_descriptor = descriptor(config_payload, config_descriptor_media_type)
    config_descriptor["size"] += config_size_delta
    layer_descriptor = descriptor(layer_payload, OCI_LAYER)
    manifest_value = {
        "config": config_descriptor,
        "layers": [layer_descriptor],
        "mediaType": manifest_media_type,
        "schemaVersion": manifest_schema_version,
    }
    manifest_payload = encoded_json(manifest_value)
    manifest_descriptor = descriptor(manifest_payload, index_descriptor_media_type)
    manifest_descriptor["size"] += manifest_size_delta
    manifest_descriptor["annotations"] = {
        "org.opencontainers.image.ref.name": "qualification"
    }
    manifest_descriptor["platform"] = dict(
        descriptor_platform
        or {
            "architecture": architecture,
            "os": "linux",
        }
    )

    manifests = [manifest_descriptor]
    if extra_index_descriptor:
        second = dict(manifest_descriptor)
        second["annotations"] = {
            "org.opencontainers.image.ref.name": "qualification"
        }
        manifests.append(second)
    index_value = {
        "manifests": manifests,
        "mediaType": OCI_INDEX,
        "schemaVersion": index_schema_version,
    }
    if duplicate_index_key:
        index_payload = (
            b'{"schemaVersion":2,"schemaVersion":2,"mediaType":'
            + encoded_json(OCI_INDEX)
            + b',"manifests":'
            + encoded_json(manifests)
            + b"}"
        )
    else:
        index_payload = encoded_json(index_value)

    manifest_blob = manifest_payload
    config_blob = config_payload
    layer_blob = layer_payload
    if corrupt_manifest_blob:
        manifest_blob = bytes([manifest_blob[0] ^ 1]) + manifest_blob[1:]
    if corrupt_config_blob:
        config_blob = bytes([config_blob[0] ^ 1]) + config_blob[1:]
    if corrupt_layer_blob:
        layer_blob = bytes([layer_blob[0] ^ 1]) + layer_blob[1:]

    files = {
        "blobs/sha256/" + digest(config_payload).split(":", 1)[1]: config_blob,
        "blobs/sha256/" + digest(layer_payload).split(":", 1)[1]: layer_blob,
        "blobs/sha256/" + digest(manifest_payload).split(":", 1)[1]: manifest_blob,
        "index.json": index_payload,
        "oci-layout": encoded_json(
            layout_marker
            if layout_marker is not None
            else {"imageLayoutVersion": "1.0.0"}
        ),
    }
    with tarfile.open(path, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for directory in ("blobs", "blobs/sha256"):
            member = tarfile.TarInfo(directory)
            member.type = tarfile.DIRTYPE
            member.mode = 0o755
            archive.addfile(member)
        for name, payload in files.items():
            member = tarfile.TarInfo(name)
            member.mode = 0o444
            member.size = len(payload)
            archive.addfile(member, io.BytesIO(payload))
        if add_symlink_member:
            member = tarfile.TarInfo("unexpected-link")
            member.type = tarfile.SYMTYPE
            member.linkname = "index.json"
            archive.addfile(member)
    return str(manifest_descriptor["digest"]), str(config_descriptor["digest"])


class InspectPlatformOciImageTests(unittest.TestCase):
    def run_inspector(
        self,
        archive: Path,
        output: Path,
        *,
        platform: str = "linux/amd64",
        expected_manifest_digest: Optional[str] = None,
    ) -> subprocess.CompletedProcess[str]:
        command = [
            sys.executable,
            str(INSPECTOR),
            "--oci-archive",
            str(archive),
            "--platform",
            platform,
            "--output",
            str(output),
        ]
        if expected_manifest_digest is not None:
            command.extend(
                ["--expected-manifest-digest", expected_manifest_digest]
            )
        return subprocess.run(
            command,
            cwd=REPOSITORY_ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    def assert_rejected(
        self,
        layout_options: Mapping[str, Any],
        message: str,
        *,
        platform: str = "linux/amd64",
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            archive = root / "image.oci.tar"
            output = root / "identity.json"
            write_layout(archive, **layout_options)
            result = self.run_inspector(archive, output, platform=platform)
            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertIn(message, result.stderr)
            self.assertFalse(output.exists())

    def test_outputs_exact_canonical_identity_and_binds_expected_digest(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            archive = root / "image.oci.tar"
            output = root / "identity.json"
            manifest_digest, config_digest = write_layout(archive)
            result = self.run_inspector(
                archive,
                output,
                expected_manifest_digest=manifest_digest,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, "")
            self.assertEqual(
                output.read_bytes(),
                encoded_json(
                    {
                        "config_digest": config_digest,
                        "manifest_digest": manifest_digest,
                        "platform": "linux/amd64",
                    }
                ),
            )

            wrong_output = root / "wrong.json"
            result = self.run_inspector(
                archive,
                wrong_output,
                expected_manifest_digest="sha256:" + "0" * 64,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("platform manifest digest", result.stderr)
            self.assertFalse(wrong_output.exists())

    def test_accepts_arm64_only_when_descriptor_and_config_match(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            archive = root / "image.oci.tar"
            output = root / "identity.json"
            write_layout(archive, architecture="arm64")
            result = self.run_inspector(
                archive,
                output,
                platform="linux/arm64",
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(output.read_bytes())["platform"], "linux/arm64")

    def test_rejects_duplicate_json_keys(self) -> None:
        cases = (
            ({"duplicate_index_key": True}, "duplicate object key 'schemaVersion'"),
            ({"duplicate_config_key": True}, "duplicate object key 'architecture'"),
        )
        for options, message in cases:
            with self.subTest(options=options):
                self.assert_rejected(options, message)

    def test_rejects_extra_tagged_descriptors_and_non_image_descriptors(self) -> None:
        cases = (
            (
                {"extra_index_descriptor": True},
                "tags and additional descriptors are ambiguous",
            ),
            (
                {
                    "index_descriptor_media_type": (
                        "application/vnd.in-toto+json"
                    )
                },
                "expected 'application/vnd.oci.image.manifest.v1+json'",
            ),
            (
                {"index_descriptor_media_type": OCI_INDEX},
                "expected 'application/vnd.oci.image.manifest.v1+json'",
            ),
        )
        for options, message in cases:
            with self.subTest(options=options):
                self.assert_rejected(options, message)

    def test_rejects_descriptor_size_and_blob_hash_mismatches(self) -> None:
        cases = (
            ({"manifest_size_delta": 1}, "descriptor size"),
            ({"config_size_delta": 1}, "descriptor size"),
            ({"corrupt_manifest_blob": True}, "content digest"),
            ({"corrupt_config_blob": True}, "content digest"),
            ({"corrupt_layer_blob": True}, "content digest"),
        )
        for options, message in cases:
            with self.subTest(options=options):
                self.assert_rejected(options, message)

    def test_rejects_wrong_descriptor_or_config_platform(self) -> None:
        cases = (
            (
                {"descriptor_platform": {"architecture": "arm64", "os": "linux"}},
                "index manifest descriptor platform",
            ),
            (
                {
                    "descriptor_platform": {
                        "architecture": "amd64",
                        "os": "linux",
                        "variant": "v1",
                    }
                },
                "index manifest descriptor platform",
            ),
            ({"config_architecture": "arm64"}, "image config architecture"),
            ({"config_os": "windows"}, "image config os"),
        )
        for options, message in cases:
            with self.subTest(options=options):
                self.assert_rejected(options, message)

    def test_rejects_non_v1_layout_or_malformed_manifest_contract(self) -> None:
        cases = (
            (
                {"layout_marker": {"imageLayoutVersion": "1.1.0"}},
                "imageLayoutVersion 1.0.0",
            ),
            ({"index_schema_version": 2.0}, "schemaVersion must be the integer 2"),
            ({"manifest_schema_version": 1}, "schemaVersion must be the integer 2"),
            (
                {"manifest_media_type": OCI_INDEX},
                "platform manifest mediaType",
            ),
            (
                {"config_descriptor_media_type": OCI_MANIFEST},
                "image config descriptor.mediaType",
            ),
        )
        for options, message in cases:
            with self.subTest(options=options):
                self.assert_rejected(options, message)

    def test_rejects_symlink_archive_and_non_regular_tar_member(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            archive = root / "image.oci.tar"
            write_layout(archive)
            link = root / "linked.oci.tar"
            os.symlink(archive.name, link)
            result = self.run_inspector(link, root / "linked.json")
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("regular non-symlink file", result.stderr)

        self.assert_rejected(
            {"add_symlink_member": True},
            "is not a regular file or directory",
        )


if __name__ == "__main__":
    unittest.main()
