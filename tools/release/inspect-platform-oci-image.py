#!/usr/bin/env python3
"""Inspect one unambiguous single-platform OCI image archive.

The command deliberately accepts only the narrow OCI shape emitted by the
platform build.  It derives image identity from verified archive bytes, never
from a Docker image config ID or a tag lookup.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
import tarfile
from pathlib import Path
from typing import Any, BinaryIO, Dict, List, Mapping, NoReturn, Optional, Tuple


OCI_INDEX_MEDIA_TYPE = "application/vnd.oci.image.index.v1+json"
OCI_MANIFEST_MEDIA_TYPE = "application/vnd.oci.image.manifest.v1+json"
OCI_CONFIG_MEDIA_TYPE = "application/vnd.oci.image.config.v1+json"
OCI_LAYER_MEDIA_TYPES = {
    "application/vnd.oci.image.layer.v1.tar",
    "application/vnd.oci.image.layer.v1.tar+gzip",
    "application/vnd.oci.image.layer.v1.tar+zstd",
}

SUPPORTED_PLATFORMS = {"linux/amd64", "linux/arm64"}
DIGEST_PREFIX = "sha256:"
DIGEST_HEX_LENGTH = 64
MAX_JSON_BYTES = 16 * 1024 * 1024
MAX_ARCHIVE_MEMBERS = 100_000
MAX_PATH_BYTES = 4096
COPY_CHUNK_BYTES = 1024 * 1024


class InspectionError(ValueError):
    """The archive does not prove one exact platform image identity."""


def fail(message: str) -> NoReturn:
    raise InspectionError(message)


def validate_digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.startswith(DIGEST_PREFIX):
        fail(f"{label} must be a sha256 digest string")
    encoded = value[len(DIGEST_PREFIX) :]
    if len(encoded) != DIGEST_HEX_LENGTH or any(
        character not in "0123456789abcdef" for character in encoded
    ):
        fail(f"{label} must be canonical sha256:<64 lowercase hex>")
    return value


def reject_json_constant(value: str) -> NoReturn:
    fail(f"JSON contains non-standard numeric constant {value!r}")


def unique_json_object(pairs: List[Tuple[str, Any]]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            fail(f"JSON contains duplicate object key {key!r}")
        result[key] = value
    return result


def parse_json(payload: bytes, label: str) -> Dict[str, Any]:
    if len(payload) > MAX_JSON_BYTES:
        fail(f"{label} exceeds the {MAX_JSON_BYTES}-byte inspection limit")
    try:
        decoded = payload.decode("utf-8")
    except UnicodeDecodeError as error:
        fail(f"{label} is not UTF-8: {error}")
    try:
        value = json.loads(
            decoded,
            object_pairs_hook=unique_json_object,
            parse_constant=reject_json_constant,
        )
    except (json.JSONDecodeError, TypeError) as error:
        fail(f"{label} is not strict JSON: {error}")
    if not isinstance(value, dict):
        fail(f"{label} must contain a JSON object")
    return value


def validate_annotations(value: Any, label: str) -> None:
    if value is None:
        return
    if not isinstance(value, dict) or any(
        not isinstance(key, str) or not isinstance(item, str)
        for key, item in value.items()
    ):
        fail(f"{label} must map strings to strings")


def require_schema_version(value: Mapping[str, Any], label: str) -> None:
    version = value.get("schemaVersion")
    if type(version) is not int or version != 2:
        fail(f"{label} schemaVersion must be the integer 2")


def descriptor_fields(
    value: Any,
    label: str,
    *,
    expected_media_type: Optional[str] = None,
    require_platform: bool = False,
) -> Tuple[str, str, int]:
    if not isinstance(value, dict):
        fail(f"{label} must be an OCI descriptor object")

    allowed_keys = {"mediaType", "digest", "size", "annotations"}
    if require_platform:
        allowed_keys.add("platform")
    unknown_keys = set(value) - allowed_keys
    if unknown_keys:
        fail(f"{label} has unsupported fields {sorted(unknown_keys)!r}")

    media_type = value.get("mediaType")
    if not isinstance(media_type, str) or not media_type:
        fail(f"{label}.mediaType must be a non-empty string")
    if expected_media_type is not None and media_type != expected_media_type:
        fail(
            f"{label}.mediaType is {media_type!r}, expected "
            f"{expected_media_type!r}"
        )

    digest = validate_digest(value.get("digest"), f"{label}.digest")
    size = value.get("size")
    if isinstance(size, bool) or not isinstance(size, int) or size < 0:
        fail(f"{label}.size must be a non-negative integer")
    validate_annotations(value.get("annotations"), f"{label}.annotations")
    return media_type, digest, size


def canonical_member_path(name: str, is_directory: bool) -> str:
    if not isinstance(name, str) or not name:
        fail("OCI archive has a member with an empty path")
    try:
        encoded = name.encode("utf-8")
    except UnicodeEncodeError as error:
        fail(f"OCI archive member path is not UTF-8: {error}")
    if len(encoded) > MAX_PATH_BYTES:
        fail(f"OCI archive member path exceeds {MAX_PATH_BYTES} bytes")
    if name.startswith("/") or "\\" in name:
        fail(f"OCI archive member has a non-canonical path {name!r}")
    if any(ord(character) < 32 or ord(character) == 127 for character in name):
        fail(f"OCI archive member path contains a control character {name!r}")

    candidate = name[:-1] if is_directory and name.endswith("/") else name
    parts = candidate.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        fail(f"OCI archive member has a non-canonical path {name!r}")
    if not is_directory and name.endswith("/"):
        fail(f"OCI archive regular file has a directory path {name!r}")
    return candidate


class OciArchive:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.source: Optional[BinaryIO] = None
        self.archive: Optional[tarfile.TarFile] = None
        self.members: Dict[str, tarfile.TarInfo] = {}

    def __enter__(self) -> "OciArchive":
        try:
            path_metadata = self.path.lstat()
        except OSError as error:
            fail(f"cannot stat OCI archive {self.path}: {error}")
        if stat.S_ISLNK(path_metadata.st_mode) or not stat.S_ISREG(
            path_metadata.st_mode
        ):
            fail("OCI archive must be a regular non-symlink file")

        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(
            os, "O_NOFOLLOW", 0
        )
        try:
            descriptor = os.open(self.path, flags)
        except OSError as error:
            fail(
                f"OCI archive must be a readable regular non-symlink file: {error}"
            )
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                fail("OCI archive must be a regular non-symlink file")
            if (metadata.st_dev, metadata.st_ino) != (
                path_metadata.st_dev,
                path_metadata.st_ino,
            ):
                fail("OCI archive changed while it was being opened")
            self.source = os.fdopen(descriptor, "rb")
            descriptor = -1
            self.archive = tarfile.open(
                fileobj=self.source,
                mode="r:",
                errorlevel=2,
            )
            for index, member in enumerate(self.archive):
                if index >= MAX_ARCHIVE_MEMBERS:
                    fail(f"OCI archive exceeds {MAX_ARCHIVE_MEMBERS} members")
                if member.pax_headers:
                    fail(f"OCI archive member {member.name!r} uses PAX metadata")
                if not (member.isdir() or member.isreg()):
                    fail(
                        f"OCI archive member {member.name!r} is not a regular "
                        "file or directory"
                    )
                path = canonical_member_path(member.name, member.isdir())
                if path in self.members:
                    fail(f"OCI archive repeats member {path!r}")
                if member.isdir():
                    if path not in {"blobs", "blobs/sha256"}:
                        fail(f"OCI archive has unexpected directory {path!r}")
                elif not self._allowed_file(path):
                    fail(f"OCI archive has unexpected file {path!r}")
                self.members[path] = member
            if self.archive.pax_headers:
                fail("OCI archive uses global PAX metadata")
            self._validate_layout_marker()
            return self
        except (InspectionError, tarfile.TarError, OSError):
            self.__exit__(None, None, None)
            raise
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    def __exit__(self, *_: Any) -> None:
        if self.archive is not None:
            self.archive.close()
            self.archive = None
        if self.source is not None:
            self.source.close()
            self.source = None

    @staticmethod
    def _allowed_file(path: str) -> bool:
        if path in {"oci-layout", "index.json"}:
            return True
        if not path.startswith("blobs/sha256/"):
            return False
        encoded = path[len("blobs/sha256/") :]
        return len(encoded) == DIGEST_HEX_LENGTH and all(
            character in "0123456789abcdef" for character in encoded
        )

    def _read_member(self, path: str, maximum: Optional[int] = None) -> bytes:
        member = self.members.get(path)
        if member is None or not member.isreg():
            fail(f"OCI archive is missing regular file {path!r}")
        if maximum is not None and member.size > maximum:
            fail(f"OCI archive member {path!r} exceeds {maximum} bytes")
        assert self.archive is not None
        stream = self.archive.extractfile(member)
        if stream is None:
            fail(f"cannot read OCI archive member {path!r}")
        payload = stream.read()
        if len(payload) != member.size:
            fail(f"OCI archive member {path!r} is truncated")
        return payload

    def _validate_layout_marker(self) -> None:
        marker = parse_json(
            self._read_member("oci-layout", MAX_JSON_BYTES),
            "oci-layout",
        )
        if marker != {"imageLayoutVersion": "1.0.0"}:
            fail("oci-layout must declare exactly imageLayoutVersion 1.0.0")

    def verified_blob_bytes(
        self,
        descriptor: Mapping[str, Any],
        label: str,
        *,
        expected_media_type: Optional[str] = None,
        require_platform: bool = False,
        maximum: Optional[int] = None,
    ) -> bytes:
        _, digest, expected_size = descriptor_fields(
            descriptor,
            label,
            expected_media_type=expected_media_type,
            require_platform=require_platform,
        )
        if maximum is not None and expected_size > maximum:
            fail(f"{label} exceeds {maximum} bytes")
        path = "blobs/sha256/" + digest[len(DIGEST_PREFIX) :]
        member = self.members.get(path)
        if member is None or not member.isreg():
            fail(f"OCI archive is missing blob {digest}")
        if member.size != expected_size:
            fail(
                f"{label} descriptor size {expected_size} does not match "
                f"archive member size {member.size}"
            )
        payload = self._read_member(path, maximum)
        actual_digest = DIGEST_PREFIX + hashlib.sha256(payload).hexdigest()
        if actual_digest != digest:
            fail(f"{label} blob has content digest {actual_digest}, expected {digest}")
        return payload

    def verify_blob(
        self,
        descriptor: Mapping[str, Any],
        label: str,
    ) -> None:
        _, digest, expected_size = descriptor_fields(descriptor, label)
        path = "blobs/sha256/" + digest[len(DIGEST_PREFIX) :]
        member = self.members.get(path)
        if member is None or not member.isreg():
            fail(f"OCI archive is missing blob {digest}")
        if member.size != expected_size:
            fail(
                f"{label} descriptor size {expected_size} does not match "
                f"archive member size {member.size}"
            )
        assert self.archive is not None
        stream = self.archive.extractfile(member)
        if stream is None:
            fail(f"cannot read OCI blob {digest}")
        hasher = hashlib.sha256()
        copied = 0
        while True:
            chunk = stream.read(COPY_CHUNK_BYTES)
            if not chunk:
                break
            copied += len(chunk)
            hasher.update(chunk)
        if copied != expected_size:
            fail(f"OCI blob {digest} is truncated")
        actual_digest = DIGEST_PREFIX + hasher.hexdigest()
        if actual_digest != digest:
            fail(f"{label} blob has content digest {actual_digest}, expected {digest}")

    def inspect(
        self,
        platform: str,
        expected_manifest_digest: Optional[str],
    ) -> Dict[str, str]:
        expected_os, expected_architecture = platform.split("/", 1)
        index = parse_json(
            self._read_member("index.json", MAX_JSON_BYTES),
            "index.json",
        )
        if set(index) != {"schemaVersion", "mediaType", "manifests"}:
            fail(
                "index.json must contain exactly schemaVersion, mediaType, and "
                "manifests"
            )
        require_schema_version(index, "index.json")
        if index.get("mediaType") != OCI_INDEX_MEDIA_TYPE:
            fail(f"index.json mediaType must be {OCI_INDEX_MEDIA_TYPE!r}")
        manifests = index.get("manifests")
        if not isinstance(manifests, list) or len(manifests) != 1:
            fail(
                "index.json must contain exactly one direct platform image "
                "manifest; tags and additional descriptors are ambiguous"
            )

        manifest_descriptor = manifests[0]
        _, manifest_digest, _ = descriptor_fields(
            manifest_descriptor,
            "index manifest descriptor",
            expected_media_type=OCI_MANIFEST_MEDIA_TYPE,
            require_platform=True,
        )
        expected_platform = {
            "architecture": expected_architecture,
            "os": expected_os,
        }
        if manifest_descriptor.get("platform") != expected_platform:
            fail(
                "index manifest descriptor platform is "
                f"{manifest_descriptor.get('platform')!r}, expected "
                f"{expected_platform!r}"
            )
        if (
            expected_manifest_digest is not None
            and manifest_digest != expected_manifest_digest
        ):
            fail(
                f"platform manifest digest is {manifest_digest}, expected "
                f"{expected_manifest_digest}"
            )

        manifest_payload = self.verified_blob_bytes(
            manifest_descriptor,
            "platform manifest",
            expected_media_type=OCI_MANIFEST_MEDIA_TYPE,
            require_platform=True,
            maximum=MAX_JSON_BYTES,
        )
        manifest = parse_json(manifest_payload, "platform manifest")
        allowed_manifest_keys = {
            "schemaVersion",
            "mediaType",
            "config",
            "layers",
            "annotations",
        }
        unknown_manifest_keys = set(manifest) - allowed_manifest_keys
        if unknown_manifest_keys:
            fail(
                "platform manifest has unsupported fields "
                f"{sorted(unknown_manifest_keys)!r}"
            )
        require_schema_version(manifest, "platform manifest")
        if manifest.get("mediaType") != OCI_MANIFEST_MEDIA_TYPE:
            fail(f"platform manifest mediaType must be {OCI_MANIFEST_MEDIA_TYPE!r}")
        validate_annotations(
            manifest.get("annotations"),
            "platform manifest annotations",
        )

        config_descriptor = manifest.get("config")
        _, config_digest, _ = descriptor_fields(
            config_descriptor,
            "image config descriptor",
            expected_media_type=OCI_CONFIG_MEDIA_TYPE,
        )
        config_payload = self.verified_blob_bytes(
            config_descriptor,
            "image config",
            expected_media_type=OCI_CONFIG_MEDIA_TYPE,
            maximum=MAX_JSON_BYTES,
        )
        config = parse_json(config_payload, "image config")
        if config.get("os") != expected_os:
            fail(
                f"image config os is {config.get('os')!r}, expected "
                f"{expected_os!r}"
            )
        if config.get("architecture") != expected_architecture:
            fail(
                "image config architecture is "
                f"{config.get('architecture')!r}, expected "
                f"{expected_architecture!r}"
            )

        layers = manifest.get("layers")
        if not isinstance(layers, list):
            fail("platform manifest layers must be an array")
        verified_layer_digests = set()
        for index, layer_descriptor in enumerate(layers):
            media_type, layer_digest, _ = descriptor_fields(
                layer_descriptor,
                f"layer descriptor {index}",
            )
            if media_type not in OCI_LAYER_MEDIA_TYPES:
                fail(
                    f"layer descriptor {index}.mediaType {media_type!r} is not "
                    "a supported OCI image layer"
                )
            if layer_digest not in verified_layer_digests:
                self.verify_blob(layer_descriptor, f"layer {index}")
                verified_layer_digests.add(layer_digest)

        return {
            "manifest_digest": manifest_digest,
            "config_digest": config_digest,
            "platform": platform,
        }


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify and inspect one single-platform OCI image archive."
    )
    parser.add_argument("--oci-archive", required=True, type=Path)
    parser.add_argument("--platform", required=True, choices=sorted(SUPPORTED_PLATFORMS))
    parser.add_argument("--expected-manifest-digest")
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    arguments = parse_arguments()
    try:
        expected_digest = None
        if arguments.expected_manifest_digest is not None:
            expected_digest = validate_digest(
                arguments.expected_manifest_digest,
                "--expected-manifest-digest",
            )
        with OciArchive(arguments.oci_archive) as archive:
            result = archive.inspect(arguments.platform, expected_digest)
        encoded = json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
        arguments.output.write_bytes(encoded)
    except (InspectionError, OSError, tarfile.TarError) as error:
        print(f"platform OCI image inspection failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
