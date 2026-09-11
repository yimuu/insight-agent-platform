"""Deliver one installation's public CA; never change operating-system or browser trust."""
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import ssl
import stat
import tempfile

MAX_RESPONSE_BYTES = 24 * 1024
MAX_CERTIFICATE_BYTES = 16 * 1024
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")


class PublicTrustFailure(RuntimeError):
    pass


def decode(data):
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise PublicTrustFailure("public-trust-duplicate-field")
            value[key] = item
        return value
    def invalid(_):
        raise PublicTrustFailure("public-trust-invalid-json")
    if not isinstance(data, bytes) or not 0 < len(data) <= MAX_RESPONSE_BYTES:
        raise PublicTrustFailure("public-trust-response-bound")
    try:
        return json.loads(data, object_pairs_hook=pairs, parse_constant=invalid)
    except (ValueError, UnicodeError, RecursionError):
        raise PublicTrustFailure("public-trust-invalid-json") from None


def certificate(data, *, input_digest, identity_digest):
    value = decode(data)
    fields = {"schema_version", "input_digest", "identity_digest", "certificate_pem", "certificate_sha256"}
    if (not isinstance(value, dict) or set(value) != fields
            or type(value["schema_version"]) is not int or value["schema_version"] != 1
            or not all(isinstance(item, str) and DIGEST.fullmatch(item)
                       for item in (input_digest, identity_digest, value["certificate_sha256"]))
            or value["input_digest"] != input_digest or value["identity_digest"] != identity_digest
            or not isinstance(value["certificate_pem"], str)):
        raise PublicTrustFailure("public-trust-identity-or-envelope-differs")
    try:
        pem = value["certificate_pem"].encode("ascii")
    except UnicodeError:
        raise PublicTrustFailure("public-trust-certificate-invalid") from None
    match = re.fullmatch(rb"-----BEGIN CERTIFICATE-----\n([A-Za-z0-9+/=\n]+)-----END CERTIFICATE-----\n", pem)
    if not 0 < len(pem) <= MAX_CERTIFICATE_BYTES or match is None:
        raise PublicTrustFailure("public-trust-certificate-invalid")
    try:
        der = base64.b64decode(match[1].replace(b"\n", b""), validate=True)
        if not der:
            raise ValueError()
        # Parse the single public certificate locally. This context is never used
        # for a connection, and does not install trust into any global store.
        ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT).load_verify_locations(cadata=value["certificate_pem"])
    except (ValueError, ssl.SSLError):
        raise PublicTrustFailure("public-trust-certificate-invalid") from None
    file_digest = "sha256:" + hashlib.sha256(pem).hexdigest()
    if value["certificate_sha256"] != file_digest:
        raise PublicTrustFailure("public-trust-certificate-digest-differs")
    return pem, file_digest


def _directory(path):
    if not path.is_absolute() or ".." in path.parts:
        raise PublicTrustFailure("public-trust-output-directory-invalid")
    for item in (path, *path.parents):
        metadata = item.lstat()
        if not stat.S_ISDIR(metadata.st_mode):
            raise PublicTrustFailure("public-trust-output-ancestor-invalid")
    metadata = path.stat()
    if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) != 0o700:
        raise PublicTrustFailure("public-trust-output-directory-not-private")


def read_private(path, maximum=MAX_RESPONSE_BYTES):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        metadata = os.fstat(descriptor)
        if (not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1
                or metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) != 0o600
                or not 0 < metadata.st_size <= maximum):
            raise PublicTrustFailure("public-trust-existing-file-invalid")
        data = os.read(descriptor, maximum + 1)
        if len(data) != metadata.st_size or os.read(descriptor, 1):
            raise PublicTrustFailure("public-trust-existing-file-changed")
        return data
    finally:
        os.close(descriptor)


def _publish(directory, filename, data, maximum):
    if filename not in {"public-ca.pem", "ready-owner-proof.json"}:
        raise PublicTrustFailure("public-trust-output-name-invalid")
    directory = Path(directory)
    _directory(directory)
    destination = directory / filename
    lock = os.open(directory / ".public-trust-lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        metadata = os.fstat(lock)
        if (not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1
                or metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) != 0o600):
            raise PublicTrustFailure("public-trust-lock-invalid")
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if os.path.lexists(destination):
            if read_private(destination, maximum) != data:
                raise PublicTrustFailure("public-trust-existing-file-differs")
        else:
            # A task-private temporary name prevents partial final certificates.
            # A crash leaves evidence in that private directory, never a partial CA.
            with tempfile.TemporaryDirectory(prefix=".public-trust-", dir=directory) as temporary:
                staged = Path(temporary) / filename
                descriptor = os.open(staged, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
                if os.path.lexists(destination):
                    raise PublicTrustFailure("public-trust-output-changed")
                # Atomic no-replace publication: a concurrently created foreign
                # destination is retained. A crash before unlink leaves nlink=2,
                # which replay rejects instead of repairing unknown state.
                os.link(staged, destination, follow_symlinks=False)
                staged.unlink()
                descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
    finally:
        os.close(lock)
    return destination


def _ready(data, input_digest):
    value = decode(data)
    if (not isinstance(value, dict) or set(value) != {"schema_version", "phase", "input_digest", "identity_digest"}
            or type(value["schema_version"]) is not int or value["schema_version"] != 1
            or value["phase"] != "ready" or value["input_digest"] != input_digest
            or not isinstance(input_digest, str) or not DIGEST.fullmatch(input_digest)
            or not isinstance(value["identity_digest"], str) or not DIGEST.fullmatch(value["identity_digest"])):
        raise PublicTrustFailure("public-trust-ready-owner-proof-invalid")
    return value


def remember_ready(directory, data, *, input_digest):
    """Retain actual provision/verify output, never infer Ready from trust export."""
    value = _ready(data, input_digest)
    _publish(directory, "ready-owner-proof.json", json.dumps(value, sort_keys=True, separators=(",", ":")).encode()+b"\n", 1024)


def ready_identity(directory, *, input_digest):
    _directory(Path(directory))
    value = _ready(read_private(Path(directory)/"ready-owner-proof.json", 1024), input_digest)
    return value["identity_digest"]


def deliver(directory, data, *, input_digest, identity_digest):
    pem, file_digest = certificate(data, input_digest=input_digest, identity_digest=identity_digest)
    destination = _publish(directory, "public-ca.pem", pem, MAX_CERTIFICATE_BYTES)
    return {"schema_version": 1, "input_digest": input_digest, "identity_digest": identity_digest,
            "certificate_file": str(destination), "certificate_sha256": file_digest}
