#!/usr/bin/env python3
"""Verify the clean-cut composition of a Sandbox Package OCI image.

This verifier intentionally understands only the OCI/tar encodings needed by the
platform release path.  Anything it cannot interpret without ambiguity is an
error, rather than evidence that an image is safe.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import stat
import sys
import tarfile
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Any,
    BinaryIO,
    Dict,
    Iterator,
    List,
    Mapping,
    NoReturn,
    Optional,
    Set,
    Tuple,
)


OCI_LAYOUT_MEDIA_TYPE = "application/vnd.oci.image.index.v1+json"
OCI_MANIFEST_MEDIA_TYPE = "application/vnd.oci.image.manifest.v1+json"
OCI_CONFIG_MEDIA_TYPE = "application/vnd.oci.image.config.v1+json"
OCI_LAYER_MEDIA_TYPES = {
    "application/vnd.oci.image.layer.v1.tar": "tar",
    "application/vnd.oci.image.layer.v1.tar+gzip": "gzip",
}

PACKAGE_PATH = "opt/insight/package"
# BuildKit may materialize these parent directories for COPY. They are accepted
# only as inert root:root 0755 directories, and may not change an existing node.
PACKAGE_STRUCTURAL_PARENTS = {"opt", "opt/insight"}
RUNNER_LAUNCHER_PATH = "usr/local/bin/platform-sandbox-runner"

# Linux vfs_cap_data revision 2, effective flag set, permitted bits 5, 6, and 7
# (CAP_KILL, CAP_SETGID, CAP_SETUID), with no inheritable capabilities.
EXPECTED_RUNNER_CAPABILITY = bytes.fromhex(
    "01000002e0000000000000000000000000000000"
)

DIGEST_PREFIX = "sha256:"
DIGEST_HEX_LENGTH = 64
MAX_JSON_BYTES = 16 * 1024 * 1024
MAX_COMPRESSED_LAYER_BYTES = 2 * 1024 * 1024 * 1024
MAX_LAYOUT_MEMBERS = 100_000
MAX_LAYER_MEMBERS = 1_000_000
MAX_PATH_BYTES = 4096
MAX_PAX_BYTES = 1024 * 1024
MAX_UNCOMPRESSED_LAYER_BYTES = 2 * 1024 * 1024 * 1024
COPY_CHUNK_BYTES = 1024 * 1024
TAR_BLOCK_BYTES = 512

# These POSIX PAX keys merely carry fields already exposed by TarInfo.  All
# vendor metadata is rejected except SCHILY.xattr.*, which is the xattr
# encoding written and consumed by the platform's Go/containerd toolchain.
SAFE_PAX_KEYS = {
    "path",
    "linkpath",
    "size",
    "uid",
    "gid",
    "uname",
    "gname",
    "mtime",
    "atime",
    "ctime",
    "charset",
    "hdrcharset",
    "comment",
}
SCHILY_XATTR_PREFIX = "SCHILY.xattr."


class VerificationError(ValueError):
    """The input does not prove the required image composition."""


def fail(message: str) -> NoReturn:
    raise VerificationError(message)


def validate_digest(value: Any, label: str) -> str:
    if not isinstance(value, str):
        fail(f"{label} must be a sha256 digest string")
    if not value.startswith(DIGEST_PREFIX):
        fail(f"{label} must use sha256")
    encoded = value[len(DIGEST_PREFIX) :]
    if len(encoded) != DIGEST_HEX_LENGTH or any(
        character not in "0123456789abcdef" for character in encoded
    ):
        fail(f"{label} must be canonical sha256:<64 lowercase hex>")
    return value


def _reject_json_constant(value: str) -> NoReturn:
    fail(f"JSON contains non-standard numeric constant {value!r}")


def _unique_json_object(pairs: List[Tuple[str, Any]]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            fail(f"JSON contains duplicate object key {key!r}")
        result[key] = value
    return result


def parse_json(payload: bytes, label: str) -> Dict[str, Any]:
    if len(payload) > MAX_JSON_BYTES:
        fail(f"{label} exceeds the {MAX_JSON_BYTES}-byte verifier limit")
    try:
        decoded = payload.decode("utf-8")
    except UnicodeDecodeError as error:
        fail(f"{label} is not UTF-8: {error}")
    try:
        value = json.loads(
            decoded,
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
    except (json.JSONDecodeError, TypeError) as error:
        fail(f"{label} is not strict JSON: {error}")
    if not isinstance(value, dict):
        fail(f"{label} must contain a JSON object")
    return value


def descriptor_fields(
    value: Any,
    label: str,
    expected_media_type: Optional[str] = None,
) -> Tuple[str, str, int]:
    if not isinstance(value, dict):
        fail(f"{label} must be an OCI descriptor object")
    if "data" in value or "urls" in value:
        fail(f"{label} must be backed only by the supplied OCI layout")
    media_type = value.get("mediaType")
    if not isinstance(media_type, str) or not media_type:
        fail(f"{label}.mediaType must be a non-empty string")
    if expected_media_type is not None and media_type != expected_media_type:
        fail(
            f"{label}.mediaType is {media_type!r}, expected {expected_media_type!r}"
        )
    digest = validate_digest(value.get("digest"), f"{label}.digest")
    size = value.get("size")
    if isinstance(size, bool) or not isinstance(size, int) or size < 0:
        fail(f"{label}.size must be a non-negative integer")
    annotations = value.get("annotations")
    if annotations is not None and (
        not isinstance(annotations, dict)
        or any(
            not isinstance(key, str) or not isinstance(item, str)
            for key, item in annotations.items()
        )
    ):
        fail(f"{label}.annotations must map strings to strings")
    return media_type, digest, size


def canonical_tar_path(name: str, label: str, is_directory: bool) -> str:
    if not isinstance(name, str) or not name:
        fail(f"{label} has an empty path")
    try:
        encoded = name.encode("utf-8")
    except UnicodeEncodeError as error:
        fail(f"{label} path is not UTF-8: {error}")
    if len(encoded) > MAX_PATH_BYTES:
        fail(f"{label} path exceeds {MAX_PATH_BYTES} bytes")
    if name.startswith("/"):
        fail(f"{label} uses an absolute path: {name!r}")
    if "\\" in name:
        fail(f"{label} uses a backslash path separator: {name!r}")
    if any(ord(character) < 32 or ord(character) == 127 for character in name):
        fail(f"{label} uses an ASCII control character: {name!r}")

    had_trailing_slash = name.endswith("/")
    parts: List[str] = []
    raw_parts = name.split("/")
    for index, part in enumerate(raw_parts):
        if part == "..":
            fail(f"{label} contains a '..' path component: {name!r}")
        if part == ".":
            continue
        if part == "":
            if index == len(raw_parts) - 1 and had_trailing_slash:
                continue
            fail(f"{label} contains an empty path component: {name!r}")
        parts.append(part)
    canonical = "/".join(parts)
    if not canonical and not is_directory:
        fail(f"{label} resolves to the layer root but is not a directory")
    if had_trailing_slash and not is_directory:
        fail(f"{label} has a directory suffix on a non-directory: {name!r}")
    return canonical


def pax_xattrs(headers: Mapping[str, str], label: str) -> Dict[str, bytes]:
    xattrs: Dict[str, bytes] = {}
    for key, value in headers.items():
        if not isinstance(key, str) or not isinstance(value, str):
            fail(f"{label} has a non-string PAX record")
        if key.startswith(SCHILY_XATTR_PREFIX):
            name = key[len(SCHILY_XATTR_PREFIX) :]
            if not name or any(
                ord(character) < 32 or ord(character) == 127 for character in name
            ):
                fail(f"{label} has an invalid SCHILY xattr name")
            if name in xattrs:
                fail(f"{label} repeats xattr {name!r}")
            xattrs[name] = value.encode("utf-8", "surrogateescape")
            continue
        if key not in SAFE_PAX_KEYS:
            fail(
                f"{label} uses unsupported PAX field {key!r}; only POSIX fields "
                "and raw SCHILY.xattr.* are interpreted, so this is rejected "
                "fail-closed"
            )
    return xattrs


def validate_mode(member: tarfile.TarInfo, label: str) -> int:
    mode = member.mode
    if isinstance(mode, bool) or not isinstance(mode, int) or mode < 0 or mode > 0o7777:
        fail(f"{label} has an invalid mode {mode!r}")
    if member.uid < 0 or member.gid < 0:
        fail(f"{label} has a negative uid or gid")
    return mode


class OciLayout:
    def __init__(self, path: Path, label: str) -> None:
        self.path = path
        self.label = label
        self.archive: Optional[tarfile.TarFile] = None
        self.members: Dict[str, tarfile.TarInfo] = {}

    def __enter__(self) -> "OciLayout":
        try:
            metadata = self.path.lstat()
        except OSError as error:
            fail(f"cannot stat {self.label} OCI archive {self.path}: {error}")
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            fail(f"{self.label} OCI archive must be a regular non-symlink file")
        try:
            with self.path.open("rb") as raw_archive:
                raw_entries = scan_raw_tar(
                    raw_archive,
                    f"{self.label} OCI archive",
                    allow_local_pax=False,
                    maximum_members=MAX_LAYOUT_MEMBERS,
                )
            raw_paths = {entry.path for entry in raw_entries}
            self.archive = tarfile.open(self.path, mode="r:", errorlevel=2)
            for index, member in enumerate(self.archive):
                if index >= MAX_LAYOUT_MEMBERS:
                    fail(
                        f"{self.label} OCI archive exceeds {MAX_LAYOUT_MEMBERS} members"
                    )
                path = canonical_tar_path(
                    member.name,
                    f"{self.label} OCI archive member",
                    member.isdir(),
                )
                validate_mode(member, f"{self.label} OCI archive member {path!r}")
                if member.mode & (stat.S_ISUID | stat.S_ISGID):
                    fail(f"{self.label} OCI archive member {path!r} has SUID/SGID")
                if pax_xattrs(
                    member.pax_headers,
                    f"{self.label} OCI archive member {path!r}",
                ):
                    fail(f"{self.label} OCI archive member {path!r} has xattrs")
                if not (member.isdir() or member.isreg()):
                    fail(
                        f"{self.label} OCI archive member {path!r} is not a regular "
                        "file or directory"
                    )
                if path in self.members:
                    fail(f"{self.label} OCI archive repeats member {path!r}")
                if not self._allowed_layout_path(path, member.isdir()):
                    fail(f"{self.label} OCI archive has unexpected member {path!r}")
                self.members[path] = member
            if set(self.members) != raw_paths:
                fail(
                    f"{self.label} OCI archive has divergent raw and parsed tar views"
                )
            if self.archive.pax_headers:
                fail(f"{self.label} OCI archive uses global PAX headers")
            self._validate_layout_marker()
            return self
        except VerificationError:
            self.__exit__(None, None, None)
            raise
        except (tarfile.TarError, OSError) as error:
            self.__exit__(None, None, None)
            fail(f"cannot read {self.label} OCI archive: {error}")

    def __exit__(self, *_: Any) -> None:
        if self.archive is not None:
            self.archive.close()
            self.archive = None

    @staticmethod
    def _allowed_layout_path(path: str, is_directory: bool) -> bool:
        if is_directory:
            return path in {"", "blobs", "blobs/sha256"}
        if path in {"oci-layout", "index.json"}:
            return True
        if not path.startswith("blobs/sha256/"):
            return False
        encoded = path[len("blobs/sha256/") :]
        return len(encoded) == DIGEST_HEX_LENGTH and all(
            character in "0123456789abcdef" for character in encoded
        )

    def _read_member(self, name: str, maximum: Optional[int] = None) -> bytes:
        member = self.members.get(name)
        if member is None or not member.isreg():
            fail(f"{self.label} OCI archive is missing regular file {name!r}")
        if maximum is not None and member.size > maximum:
            fail(f"{self.label} OCI member {name!r} exceeds {maximum} bytes")
        assert self.archive is not None
        stream = self.archive.extractfile(member)
        if stream is None:
            fail(f"cannot read {self.label} OCI member {name!r}")
        payload = stream.read()
        if len(payload) != member.size:
            fail(f"{self.label} OCI member {name!r} is truncated")
        return payload

    def _validate_layout_marker(self) -> None:
        marker = parse_json(
            self._read_member("oci-layout", MAX_JSON_BYTES),
            f"{self.label} oci-layout",
        )
        if marker != {"imageLayoutVersion": "1.0.0"}:
            fail(f"{self.label} oci-layout must declare exactly version 1.0.0")

    def image_descriptor(self, expected_digest: Optional[str] = None) -> Dict[str, Any]:
        index = parse_json(
            self._read_member("index.json", MAX_JSON_BYTES),
            f"{self.label} index.json",
        )
        if index.get("schemaVersion") != 2:
            fail(f"{self.label} index.json schemaVersion must be 2")
        media_type = index.get("mediaType")
        if media_type is not None and media_type != OCI_LAYOUT_MEDIA_TYPE:
            fail(f"{self.label} index.json has non-OCI mediaType {media_type!r}")
        manifests = index.get("manifests")
        if not isinstance(manifests, list) or len(manifests) != 1:
            fail(f"{self.label} index.json must select exactly one image manifest")
        descriptor = manifests[0]
        _, digest, _ = descriptor_fields(
            descriptor,
            f"{self.label} index manifest",
            OCI_MANIFEST_MEDIA_TYPE,
        )
        if expected_digest is not None and digest != expected_digest:
            fail(
                f"{self.label} platform manifest digest is {digest}, expected "
                f"{expected_digest}"
            )
        return descriptor

    @contextmanager
    def verified_blob(
        self, descriptor: Mapping[str, Any], label: str
    ) -> Iterator[BinaryIO]:
        _, digest, expected_size = descriptor_fields(descriptor, label)
        name = "blobs/sha256/" + digest[len(DIGEST_PREFIX) :]
        member = self.members.get(name)
        if member is None or not member.isreg():
            fail(f"{self.label} OCI archive is missing blob {digest}")
        if member.size != expected_size:
            fail(
                f"{label} descriptor size {expected_size} does not match archive "
                f"member size {member.size}"
            )
        assert self.archive is not None
        source = self.archive.extractfile(member)
        if source is None:
            fail(f"cannot read {self.label} blob {digest}")
        payload = tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024)
        hasher = hashlib.sha256()
        copied = 0
        try:
            while True:
                chunk = source.read(COPY_CHUNK_BYTES)
                if not chunk:
                    break
                copied += len(chunk)
                hasher.update(chunk)
                payload.write(chunk)
            actual_digest = DIGEST_PREFIX + hasher.hexdigest()
            if copied != expected_size:
                fail(f"{self.label} blob {digest} is truncated")
            if actual_digest != digest:
                fail(f"{self.label} blob {digest} has content digest {actual_digest}")
            payload.seek(0)
            yield payload
        finally:
            payload.close()

    def verified_json_blob(
        self,
        descriptor: Mapping[str, Any],
        label: str,
        expected_media_type: str,
    ) -> Dict[str, Any]:
        _, _, size = descriptor_fields(descriptor, label, expected_media_type)
        if size > MAX_JSON_BYTES:
            fail(f"{label} descriptor exceeds {MAX_JSON_BYTES} bytes")
        with self.verified_blob(descriptor, label) as payload:
            content = payload.read(MAX_JSON_BYTES + 1)
        return parse_json(content, label)


@dataclass(frozen=True)
class LayerEntry:
    path: str
    kind: str
    mode: int
    uid: int
    gid: int
    size: int
    linkname: Optional[str]
    xattrs: Mapping[str, bytes]


@dataclass
class FsNode:
    kind: str
    mode: int
    uid: int
    gid: int
    xattrs: Dict[str, bytes]


def parse_tar_number(field: bytes, label: str) -> int:
    if field and field[0] & 0x80:
        fail(f"{label} uses unsupported base-256 tar numeric encoding")
    stripped = field.strip(b" \x00")
    if not stripped:
        return 0
    if any(character not in b"01234567" for character in stripped):
        fail(f"{label} is not a canonical octal tar number")
    return int(stripped, 8)


def parse_tar_text(field: bytes, label: str) -> str:
    terminator = field.find(b"\x00")
    if terminator >= 0:
        payload = field[:terminator]
        if any(character != 0 for character in field[terminator:]):
            fail(f"{label} contains data after its NUL terminator")
    else:
        payload = field
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError as error:
        fail(f"{label} is not UTF-8: {error}")


def parse_pax_records(payload: bytes, label: str) -> Dict[str, bytes]:
    if len(payload) > MAX_PAX_BYTES:
        fail(f"{label} exceeds {MAX_PAX_BYTES} bytes")
    records: Dict[str, bytes] = {}
    offset = 0
    while offset < len(payload):
        separator = payload.find(b" ", offset)
        if separator < 0:
            fail(f"{label} has an unterminated PAX record length")
        encoded_length = payload[offset:separator]
        if (
            not encoded_length
            or encoded_length.startswith(b"0")
            or not encoded_length.isdigit()
        ):
            fail(f"{label} has a non-canonical PAX record length")
        length = int(encoded_length)
        end = offset + length
        if end > len(payload) or end <= separator + 2:
            fail(f"{label} has an out-of-bounds PAX record")
        record = payload[separator + 1 : end]
        if not record.endswith(b"\n"):
            fail(f"{label} has a PAX record without a newline")
        assignment = record[:-1]
        equals = assignment.find(b"=")
        if equals <= 0:
            fail(f"{label} has a malformed PAX assignment")
        try:
            key = assignment[:equals].decode("utf-8")
        except UnicodeDecodeError as error:
            fail(f"{label} has a non-UTF-8 PAX key: {error}")
        if key in records:
            fail(f"{label} repeats PAX key {key!r}")
        records[key] = assignment[equals + 1 :]
        offset = end
    return records


def pax_entry_fields(
    records: Mapping[str, bytes],
    raw_path: str,
    raw_linkname: str,
    raw_size: int,
    raw_uid: int,
    raw_gid: int,
    label: str,
) -> Tuple[str, str, Dict[str, bytes]]:
    xattrs: Dict[str, bytes] = {}
    path = raw_path
    linkname = raw_linkname
    for key, value in records.items():
        if key.startswith(SCHILY_XATTR_PREFIX):
            name = key[len(SCHILY_XATTR_PREFIX) :]
            if not name or any(
                ord(character) < 32 or ord(character) == 127 for character in name
            ):
                fail(f"{label} has an invalid SCHILY xattr name")
            xattrs[name] = value
            continue
        if key not in SAFE_PAX_KEYS:
            fail(
                f"{label} uses unsupported PAX field {key!r}; only POSIX fields "
                "and raw SCHILY.xattr.* are interpreted, so this is rejected "
                "fail-closed"
            )
        if key in {"path", "linkpath"}:
            try:
                decoded = value.decode("utf-8")
            except UnicodeDecodeError as error:
                fail(f"{label} PAX {key} is not UTF-8: {error}")
            if key == "path":
                path = decoded
            else:
                linkname = decoded
        elif key in {"size", "uid", "gid"}:
            try:
                decoded_number = value.decode("ascii")
            except UnicodeDecodeError as error:
                fail(f"{label} PAX {key} is not ASCII: {error}")
            if not decoded_number.isdigit():
                fail(f"{label} PAX {key} must be an unsigned decimal integer")
            expected = {"size": raw_size, "uid": raw_uid, "gid": raw_gid}[key]
            if int(decoded_number) != expected:
                fail(
                    f"{label} PAX {key} disagrees with the raw tar header; "
                    "ambiguous overrides are rejected fail-closed"
                )
        elif key in {"uname", "gname", "charset", "hdrcharset", "comment"}:
            try:
                value.decode("utf-8")
            except UnicodeDecodeError as error:
                fail(f"{label} PAX {key} is not UTF-8: {error}")
        else:
            try:
                float(value.decode("ascii"))
            except (UnicodeDecodeError, ValueError) as error:
                fail(f"{label} PAX {key} is not a numeric timestamp: {error}")
    return path, linkname, xattrs


def scan_raw_tar(
    stream: BinaryIO,
    label: str,
    *,
    allow_local_pax: bool,
    maximum_members: int,
) -> List[LayerEntry]:
    stream.seek(0, 2)
    total_size = stream.tell()
    stream.seek(0)
    entries: List[LayerEntry] = []
    seen_paths: Set[str] = set()
    pending_pax: Optional[Dict[str, bytes]] = None

    while stream.tell() < total_size:
        header_offset = stream.tell()
        header = stream.read(TAR_BLOCK_BYTES)
        if len(header) != TAR_BLOCK_BYTES:
            fail(f"{label} has a truncated tar header at byte {header_offset}")
        if not any(header):
            second = stream.read(TAR_BLOCK_BYTES)
            if len(second) != TAR_BLOCK_BYTES or any(second):
                fail(f"{label} does not end with two zero tar blocks")
            while True:
                trailing = stream.read(COPY_CHUNK_BYTES)
                if not trailing:
                    break
                if any(trailing):
                    fail(f"{label} has non-zero data after its tar end marker")
            if pending_pax is not None:
                fail(f"{label} ends with an unapplied local PAX header")
            return entries

        expected_checksum = parse_tar_number(
            header[148:156], f"{label} header checksum at byte {header_offset}"
        )
        actual_checksum = sum(header[:148]) + 8 * ord(" ") + sum(header[156:])
        if expected_checksum != actual_checksum:
            fail(
                f"{label} has an invalid tar checksum at byte {header_offset}"
            )
        magic = header[257:263]
        version = header[263:265]
        if not (
            (magic == b"ustar\x00" and version == b"00")
            or (magic == b"ustar " and version in {b" \x00", b"00"})
        ):
            fail(f"{label} uses an unsupported non-POSIX tar header")

        name = parse_tar_text(header[0:100], f"{label} raw name")
        # The old GNU header reuses the POSIX prefix bytes for GNU-only fields.
        # Long GNU names are rejected below, so its effective prefix is empty.
        prefix = (
            ""
            if magic == b"ustar "
            else parse_tar_text(header[345:500], f"{label} raw prefix")
        )
        raw_path = f"{prefix}/{name}" if prefix else name
        raw_linkname = parse_tar_text(header[157:257], f"{label} raw linkname")
        mode = parse_tar_number(header[100:108], f"{label} mode")
        uid = parse_tar_number(header[108:116], f"{label} uid")
        gid = parse_tar_number(header[116:124], f"{label} gid")
        size = parse_tar_number(header[124:136], f"{label} size")
        typeflag = header[156:157]
        if mode > 0o7777:
            fail(f"{label} member {raw_path!r} has an invalid mode {mode:#o}")
        canonical_tar_path(
            raw_path,
            f"{label} raw header at byte {header_offset}",
            typeflag == b"5",
        )

        data_offset = stream.tell()
        padded_size = ((size + TAR_BLOCK_BYTES - 1) // TAR_BLOCK_BYTES) * TAR_BLOCK_BYTES
        data_end = data_offset + size
        padded_end = data_offset + padded_size
        if data_end > total_size or padded_end > total_size:
            fail(f"{label} member {raw_path!r} has truncated payload data")

        if typeflag in {b"x", b"g", b"X", b"L", b"K", b"S"}:
            if typeflag != b"x" or not allow_local_pax:
                rendered = typeflag.decode("ascii", "backslashreplace")
                fail(
                    f"{label} uses unsupported tar metadata type {rendered!r}; "
                    "global PAX, GNU long name/link, Solaris, and sparse metadata "
                    "are rejected fail-closed"
                )
            if pending_pax is not None:
                fail(f"{label} stacks multiple local PAX headers")
            if size > MAX_PAX_BYTES:
                fail(f"{label} local PAX header exceeds {MAX_PAX_BYTES} bytes")
            pax_payload = stream.read(size)
            if len(pax_payload) != size:
                fail(f"{label} has a truncated local PAX header")
            pending_pax = parse_pax_records(pax_payload, f"{label} local PAX header")
            stream.seek(data_end)
        else:
            if len(entries) >= maximum_members:
                fail(f"{label} exceeds {maximum_members} filesystem members")
            records = pending_pax or {}
            pending_pax = None
            effective_path, effective_linkname, xattrs = pax_entry_fields(
                records,
                raw_path,
                raw_linkname,
                size,
                uid,
                gid,
                f"{label} member {raw_path!r}",
            )
            kind_by_type = {
                b"\x00": "file",
                b"0": "file",
                b"1": "hardlink",
                b"2": "symlink",
                b"3": "character-device",
                b"4": "block-device",
                b"5": "directory",
                b"6": "fifo",
            }
            kind = kind_by_type.get(typeflag)
            if kind is None:
                rendered = typeflag.decode("ascii", "backslashreplace")
                fail(
                    f"{label} member {effective_path!r} has unsupported tar type "
                    f"{rendered!r}"
                )
            raw_canonical = canonical_tar_path(
                raw_path,
                f"{label} raw member",
                kind == "directory",
            )
            path = canonical_tar_path(
                effective_path,
                f"{label} effective member",
                kind == "directory",
            )
            if raw_canonical != path and "path" not in records:
                fail(f"{label} has divergent raw and effective paths")
            raw_whiteout = raw_canonical.rsplit("/", 1)[-1].startswith(".wh.")
            effective_whiteout = path.rsplit("/", 1)[-1].startswith(".wh.")
            if raw_whiteout != effective_whiteout:
                fail(
                    f"{label} PAX path changes whiteout interpretation for "
                    f"{raw_path!r}"
                )
            if path in seen_paths:
                fail(f"{label} repeats canonical path {path!r}")
            seen_paths.add(path)
            linkname: Optional[str] = None
            if kind == "hardlink":
                linkname = canonical_tar_path(
                    effective_linkname,
                    f"{label} hardlink target for {path!r}",
                    False,
                )
            elif kind == "symlink":
                try:
                    effective_linkname.encode("utf-8")
                except UnicodeEncodeError as error:
                    fail(f"{label} symlink {path!r} target is not UTF-8: {error}")
                if any(
                    ord(character) < 32 or ord(character) == 127
                    for character in effective_linkname
                ):
                    fail(f"{label} symlink {path!r} has a control-character target")
                linkname = effective_linkname
            if kind != "file" and size != 0:
                fail(f"{label} non-regular member {path!r} has payload data")
            entries.append(
                LayerEntry(
                    path=path,
                    kind=kind,
                    mode=mode,
                    uid=uid,
                    gid=gid,
                    size=size,
                    linkname=linkname,
                    xattrs=xattrs,
                )
            )
            stream.seek(data_end)

        padding = stream.read(padded_end - data_end)
        if len(padding) != padded_end - data_end or any(padding):
            fail(f"{label} member {raw_path!r} has non-zero or truncated padding")
    fail(f"{label} has no two-block tar end marker")


def read_layer(
    payload: BinaryIO,
    media_type: str,
    label: str,
) -> Tuple[List[LayerEntry], str]:
    encoding = OCI_LAYER_MEDIA_TYPES.get(media_type)
    if encoding is None:
        fail(
            f"{label} uses unsupported layer mediaType {media_type!r}; only "
            "uncompressed OCI tar and OCI tar+gzip are accepted fail-closed"
        )
    payload.seek(0)
    gzip_stream: Optional[gzip.GzipFile] = None
    source: BinaryIO = payload
    if encoding == "gzip":
        gzip_stream = gzip.GzipFile(fileobj=payload, mode="rb")
        source = gzip_stream
    uncompressed = tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024)
    hasher = hashlib.sha256()
    copied = 0
    try:
        while True:
            chunk = source.read(COPY_CHUNK_BYTES)
            if not chunk:
                break
            copied += len(chunk)
            if copied > MAX_UNCOMPRESSED_LAYER_BYTES:
                fail(
                    f"{label} exceeds the {MAX_UNCOMPRESSED_LAYER_BYTES}-byte "
                    "uncompressed verifier limit"
                )
            hasher.update(chunk)
            uncompressed.write(chunk)
        if gzip_stream is not None:
            gzip_stream.close()
            if payload.tell() != payload.seek(0, 2):
                fail(f"{label} gzip decoder did not consume the complete blob")
        uncompressed.seek(0)
        entries = scan_raw_tar(
            uncompressed,
            label,
            allow_local_pax=True,
            maximum_members=MAX_LAYER_MEMBERS,
        )
    except (gzip.BadGzipFile, EOFError, OSError) as error:
        fail(f"cannot decompress or parse {label}: {error}")
    finally:
        uncompressed.close()
    return entries, DIGEST_PREFIX + hasher.hexdigest()


def is_whiteout(entry: LayerEntry) -> bool:
    return entry.path.rsplit("/", 1)[-1].startswith(".wh.")


def remove_tree(rootfs: Dict[str, FsNode], path: str) -> None:
    prefix = path + "/"
    for candidate in list(rootfs):
        if candidate == path or candidate.startswith(prefix):
            del rootfs[candidate]


def validate_whiteout(entry: LayerEntry, label: str) -> Tuple[str, bool]:
    basename = entry.path.rsplit("/", 1)[-1]
    parent = entry.path.rsplit("/", 1)[0] if "/" in entry.path else ""
    if entry.kind != "file" or entry.size != 0 or entry.xattrs:
        fail(f"{label} whiteout {entry.path!r} must be an empty regular file")
    if entry.mode & (stat.S_ISUID | stat.S_ISGID):
        fail(f"{label} whiteout {entry.path!r} has SUID/SGID")
    if basename == ".wh..wh..opq":
        return parent, True
    target_name = basename[len(".wh.") :]
    if (
        not target_name
        or target_name in {".", ".."}
        or target_name.startswith(".wh.")
    ):
        fail(f"{label} has invalid whiteout {entry.path!r}")
    target = f"{parent}/{target_name}" if parent else target_name
    return target, False


def parent_paths(path: str) -> Iterator[str]:
    parts = path.split("/")
    for length in range(1, len(parts)):
        yield "/".join(parts[:length])


def apply_layer(
    rootfs: Dict[str, FsNode],
    entries: List[LayerEntry],
    label: str,
    package_addition: bool,
) -> None:
    ordinary: List[LayerEntry] = []
    for entry in entries:
        if is_whiteout(entry):
            if package_addition:
                fail(f"{label} adds forbidden whiteout {entry.path!r}")
            target, opaque = validate_whiteout(entry, label)
            if opaque:
                prefix = target + "/" if target else ""
                for candidate in list(rootfs):
                    if candidate.startswith(prefix) and candidate != target:
                        del rootfs[candidate]
            else:
                remove_tree(rootfs, target)
            continue
        ordinary.append(entry)

    for entry in ordinary:
        if package_addition:
            structural_parent = entry.path in PACKAGE_STRUCTURAL_PARENTS
            package_payload = (
                entry.path == PACKAGE_PATH
                or entry.path.startswith(PACKAGE_PATH + "/")
            )
            if not (structural_parent or package_payload):
                fail(
                    f"{label} adds {entry.path!r} outside /{PACKAGE_PATH}"
                )
            if structural_parent and entry.kind != "directory":
                fail(
                    f"{label} may represent structural parent /{entry.path} only "
                    "as a directory"
                )
            if structural_parent and (
                entry.mode != 0o755 or entry.uid != 0 or entry.gid != 0
            ):
                fail(
                    f"{label} structural parent /{entry.path} must remain "
                    "root:root mode 0755"
                )
            if entry.kind not in {"file", "directory"}:
                fail(
                    f"{label} adds forbidden {entry.kind} entry {entry.path!r}; "
                    "only regular files and directories are allowed"
                )
            if entry.xattrs:
                names = ", ".join(sorted(entry.xattrs))
                fail(f"{label} adds forbidden xattr(s) on {entry.path!r}: {names}")
            if entry.mode & (stat.S_ISUID | stat.S_ISGID):
                fail(f"{label} adds SUID/SGID entry {entry.path!r}")
            existing_parent = rootfs.get(entry.path) if structural_parent else None
            if existing_parent is not None and (
                existing_parent.kind != "directory"
                or existing_parent.mode != entry.mode
                or existing_parent.uid != entry.uid
                or existing_parent.gid != entry.gid
                or existing_parent.xattrs
            ):
                fail(
                    f"{label} changes existing structural parent /{entry.path}"
                )

        for parent in parent_paths(entry.path):
            node = rootfs.get(parent)
            if node is not None and node.kind != "directory":
                fail(
                    f"{label} traverses non-directory parent {parent!r} while "
                    f"adding {entry.path!r}"
                )

        if entry.kind == "hardlink":
            assert entry.linkname is not None
            target = rootfs.get(entry.linkname)
            if target is None or target.kind != "file":
                fail(
                    f"{label} hardlink {entry.path!r} has unavailable regular-file "
                    f"target {entry.linkname!r}"
                )
            if target.xattrs or target.mode & (stat.S_ISUID | stat.S_ISGID):
                fail(
                    f"{label} hardlink {entry.path!r} aliases security-sensitive "
                    f"target {entry.linkname!r}"
                )
            if entry.xattrs or entry.mode & (stat.S_ISUID | stat.S_ISGID):
                fail(f"{label} hardlink {entry.path!r} has security-sensitive metadata")
            remove_tree(rootfs, entry.path)
            rootfs[entry.path] = target
            target.mode = entry.mode
            target.uid = entry.uid
            target.gid = entry.gid
            continue

        existing = rootfs.get(entry.path)
        if entry.kind == "directory" and existing is not None and existing.kind == "directory":
            existing.mode = entry.mode
            existing.uid = entry.uid
            existing.gid = entry.gid
            existing.xattrs = dict(entry.xattrs)
            continue
        if entry.kind == "directory" and existing is None and any(
            candidate.startswith(entry.path + "/") for candidate in rootfs
        ):
            # The unpacker may already have materialized an implied parent for
            # an earlier child in this layer. Applying its explicit metadata
            # must not remove those children.
            rootfs[entry.path] = FsNode(
                kind=entry.kind,
                mode=entry.mode,
                uid=entry.uid,
                gid=entry.gid,
                xattrs=dict(entry.xattrs),
            )
            continue
        remove_tree(rootfs, entry.path)
        rootfs[entry.path] = FsNode(
            kind=entry.kind,
            mode=entry.mode,
            uid=entry.uid,
            gid=entry.gid,
            xattrs=dict(entry.xattrs),
        )


@dataclass(frozen=True)
class ImageMetadata:
    manifest_digest: str
    manifest: Mapping[str, Any]
    config: Mapping[str, Any]
    layers: List[Mapping[str, Any]]
    diff_ids: List[str]


def load_image_metadata(
    layout: OciLayout,
    expected_manifest_digest: Optional[str],
) -> ImageMetadata:
    manifest_descriptor = layout.image_descriptor(expected_manifest_digest)
    _, manifest_digest, _ = descriptor_fields(
        manifest_descriptor,
        f"{layout.label} manifest descriptor",
        OCI_MANIFEST_MEDIA_TYPE,
    )
    manifest = layout.verified_json_blob(
        manifest_descriptor,
        f"{layout.label} manifest",
        OCI_MANIFEST_MEDIA_TYPE,
    )
    if manifest.get("schemaVersion") != 2:
        fail(f"{layout.label} manifest schemaVersion must be 2")
    if manifest.get("mediaType") not in (None, OCI_MANIFEST_MEDIA_TYPE):
        fail(f"{layout.label} manifest has a non-OCI mediaType")
    config_descriptor = manifest.get("config")
    descriptor_fields(
        config_descriptor,
        f"{layout.label} config descriptor",
        OCI_CONFIG_MEDIA_TYPE,
    )
    config = layout.verified_json_blob(
        config_descriptor,
        f"{layout.label} config",
        OCI_CONFIG_MEDIA_TYPE,
    )
    layers = manifest.get("layers")
    if not isinstance(layers, list) or not layers:
        fail(f"{layout.label} manifest layers must be a non-empty array")
    for index, descriptor in enumerate(layers):
        media_type, _, _ = descriptor_fields(
            descriptor,
            f"{layout.label} layer descriptor {index}",
        )
        if media_type not in OCI_LAYER_MEDIA_TYPES:
            fail(
                f"{layout.label} layer descriptor {index} uses unsupported "
                f"mediaType {media_type!r}; zstd and non-OCI encodings are rejected "
                "fail-closed"
            )

    rootfs = config.get("rootfs")
    if not isinstance(rootfs, dict):
        fail(f"{layout.label} config rootfs must be an object")
    if rootfs.get("type") != "layers":
        fail(f"{layout.label} config rootfs.type must be 'layers'")
    diff_ids = rootfs.get("diff_ids")
    if not isinstance(diff_ids, list) or len(diff_ids) != len(layers):
        fail(f"{layout.label} config rootfs.diff_ids must match manifest layers")
    normalized_diff_ids = [
        validate_digest(value, f"{layout.label} config diff_id {index}")
        for index, value in enumerate(diff_ids)
    ]
    if not isinstance(config.get("architecture"), str) or not config.get("architecture"):
        fail(f"{layout.label} config architecture must be a non-empty string")
    if not isinstance(config.get("os"), str) or not config.get("os"):
        fail(f"{layout.label} config os must be a non-empty string")
    if not isinstance(config.get("config"), dict):
        fail(f"{layout.label} config.config must be an object")
    return ImageMetadata(
        manifest_digest=manifest_digest,
        manifest=manifest,
        config=config,
        layers=layers,
        diff_ids=normalized_diff_ids,
    )


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def runtime_config(config: Mapping[str, Any]) -> bytes:
    # The complete config object is compared, not a cherry-picked field list.
    # This also compares the presence of fields and fails closed for unknown
    # top-level runtime extensions.  Only build identity/history and rootfs are
    # allowed to differ as a consequence of appending package layers.
    ignored_build_fields = {"created", "author", "rootfs", "history"}
    return canonical_json(
        {
            key: value
            for key, value in config.items()
            if key not in ignored_build_fields
        }
    )


def verify_layer_blob(
    layout: OciLayout,
    descriptor: Mapping[str, Any],
    label: str,
) -> Tuple[List[LayerEntry], str]:
    media_type, _, size = descriptor_fields(descriptor, label)
    if size > MAX_COMPRESSED_LAYER_BYTES:
        fail(f"{label} exceeds the compressed layer verifier limit")
    with layout.verified_blob(descriptor, label) as payload:
        return read_layer(payload, media_type, label)


def verify_package_image(
    runner_archive: Path,
    package_archive: Path,
    expected_runner_manifest_digest: str,
    expected_package_manifest_digest: str,
) -> str:
    expected_runner = validate_digest(
        expected_runner_manifest_digest,
        "expected runner platform manifest digest",
    )
    expected_package = validate_digest(
        expected_package_manifest_digest,
        "expected package platform manifest digest",
    )
    rootfs: Dict[str, FsNode] = {}
    with OciLayout(runner_archive, "runner") as runner_layout, OciLayout(
        package_archive, "package"
    ) as package_layout:
        runner = load_image_metadata(runner_layout, expected_runner)
        package = load_image_metadata(package_layout, expected_package)

        if len(package.layers) <= len(runner.layers):
            fail("package layers do not have runner layers as a strict prefix")
        if canonical_json(package.layers[: len(runner.layers)]) != canonical_json(
            runner.layers
        ):
            fail("package layers do not exactly preserve the runner layer prefix")
        if runtime_config(package.config) != runtime_config(runner.config):
            fail("package OCI runtime config differs from the runner runtime config")
        if package.diff_ids[: len(runner.diff_ids)] != runner.diff_ids:
            fail("package config diff_ids do not preserve the runner strict prefix")

        for index, descriptor in enumerate(runner.layers):
            entries, actual_diff_id = verify_layer_blob(
                runner_layout,
                descriptor,
                f"runner layer {index}",
            )
            if actual_diff_id != runner.diff_ids[index]:
                fail(
                    f"runner layer {index} DiffID is {actual_diff_id}, expected "
                    f"{runner.diff_ids[index]}"
                )
            apply_layer(rootfs, entries, f"runner layer {index}", False)

            # A digest-equal prefix must also be physically complete in the
            # package layout; importing it must not depend on the runner tar.
            with package_layout.verified_blob(
                package.layers[index],
                f"package prefix layer {index}",
            ):
                pass

        for index in range(len(runner.layers), len(package.layers)):
            descriptor = package.layers[index]
            entries, actual_diff_id = verify_layer_blob(
                package_layout,
                descriptor,
                f"package added layer {index - len(runner.layers)}",
            )
            if actual_diff_id != package.diff_ids[index]:
                fail(
                    f"package added layer {index - len(runner.layers)} DiffID is "
                    f"{actual_diff_id}, expected {package.diff_ids[index]}"
                )
            if not entries:
                fail(f"package added layer {index - len(runner.layers)} is empty")
            apply_layer(
                rootfs,
                entries,
                f"package added layer {index - len(runner.layers)}",
                True,
            )

    capability_paths = sorted(
        path for path, node in rootfs.items() if "security.capability" in node.xattrs
    )
    if capability_paths != [RUNNER_LAUNCHER_PATH]:
        rendered = ", ".join("/" + path for path in capability_paths) or "none"
        fail(
            "final rootfs must have security.capability only on "
            f"/{RUNNER_LAUNCHER_PATH}; found {rendered}"
        )
    launcher = rootfs.get(RUNNER_LAUNCHER_PATH)
    if launcher is None or launcher.kind != "file":
        fail(f"final rootfs launcher /{RUNNER_LAUNCHER_PATH} is not a regular file")
    if launcher.xattrs.get("security.capability") != EXPECTED_RUNNER_CAPABILITY:
        actual = launcher.xattrs.get("security.capability", b"").hex() or "missing"
        fail(
            f"final rootfs launcher capability is {actual}, expected "
            f"{EXPECTED_RUNNER_CAPABILITY.hex()}"
        )
    sensitive_modes = sorted(
        "/" + path
        for path, node in rootfs.items()
        if node.mode & (stat.S_ISUID | stat.S_ISGID)
    )
    if sensitive_modes:
        fail(
            "final rootfs contains SUID/SGID entries: " + ", ".join(sensitive_modes)
        )
    return package.manifest_digest


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verify that a Sandbox Package OCI archive is a strict, inert "
            "filesystem extension of the exact runner platform manifest."
        ),
        epilog=(
            "The verifier accepts uncompressed or gzip OCI layers and raw "
            "SCHILY.xattr.* PAX xattrs. Other compression, vendor PAX, or xattr "
            "encodings are rejected fail-closed."
        ),
    )
    parser.add_argument("--runner-oci-archive", required=True, type=Path)
    parser.add_argument("--package-oci-archive", required=True, type=Path)
    parser.add_argument("--runner-platform-manifest-digest", required=True)
    parser.add_argument("--package-platform-manifest-digest", required=True)
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    try:
        package_digest = verify_package_image(
            args.runner_oci_archive,
            args.package_oci_archive,
            args.runner_platform_manifest_digest,
            args.package_platform_manifest_digest,
        )
    except VerificationError as error:
        print(f"Sandbox Package OCI verification failed: {error}", file=sys.stderr)
        return 1
    print(
        "Sandbox Package OCI composition passed "
        f"(runner {args.runner_platform_manifest_digest}, package {package_digest}; "
        "xattrs interpreted only as raw SCHILY.xattr.*)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
