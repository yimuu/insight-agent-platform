#!/usr/bin/env python3
"""Build and validate the inert OpenSandbox candidate used by the Kind L4 probe.

The probe creates a real, uniquely keyed Armed runner, but deliberately has no
activation operation.  Its signing seed exists only long enough to authenticate
the two v2 state reads around the Server/Controller restart.
"""

from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import stat
import sys
import time
from typing import Optional
import uuid


DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
HEX_32_RE = re.compile(r"[0-9a-f]{64}")
BOUNDED_ID_RE = re.compile(r"[A-Za-z0-9_-]{1,128}")
OCI_DIGEST_URI_RE = re.compile(r"[^\x00-\x1f\x7f]{1,512}@sha256:[0-9a-f]{64}")

RUNNER_CONFIG_ENV = "INSIGHT_SANDBOX_RUNNER_CONFIG"
RUNNER_CONFIG_DIGEST_ENV = "INSIGHT_SANDBOX_RUNNER_CONFIG_DIGEST"
EXECD_ACCESS_TOKEN_ENV = "EXECD_ACCESS_TOKEN"

_Q = 2**255 - 19
_L = 2**252 + 27742317777372353535851937790883648493
_D = (-121665 * pow(121666, _Q - 2, _Q)) % _Q
_I = pow(2, (_Q - 1) // 4, _Q)


class DuplicateKey(ValueError):
    pass


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateKey(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def canonical_json(value: object) -> bytes:
    # Probe values are ASCII strings and integers, so this is byte-identical to
    # the owning serde_jcs representation without carrying another dependency.
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def canonical_digest(value: object) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(value)).hexdigest()


def _require_digest(value: str, label: str) -> str:
    if DIGEST_RE.fullmatch(value) is None:
        raise ValueError(f"{label} is not a canonical SHA-256 digest")
    return value


def _encode_metadata_digest(value: str) -> str:
    raw = bytes.fromhex(_require_digest(value, "metadata digest")[7:])
    return "v1-" + base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _recover_x(y: int) -> int:
    xx = (y * y - 1) * pow(_D * y * y + 1, _Q - 2, _Q) % _Q
    x = pow(xx, (_Q + 3) // 8, _Q)
    if (x * x - xx) % _Q != 0:
        x = x * _I % _Q
    if x & 1:
        x = _Q - x
    return x


_BASE_Y = 4 * pow(5, _Q - 2, _Q) % _Q
_BASE = (_recover_x(_BASE_Y), _BASE_Y)


def _point_add(left: tuple[int, int], right: tuple[int, int]) -> tuple[int, int]:
    x1, y1 = left
    x2, y2 = right
    product = _D * x1 * x2 * y1 * y2
    x3 = (x1 * y2 + x2 * y1) * pow(1 + product, _Q - 2, _Q) % _Q
    y3 = (y1 * y2 + x1 * x2) * pow(1 - product, _Q - 2, _Q) % _Q
    return x3, y3


def _scalar_multiply(point: tuple[int, int], scalar: int) -> tuple[int, int]:
    result = (0, 1)
    addend = point
    while scalar:
        if scalar & 1:
            result = _point_add(result, addend)
        addend = _point_add(addend, addend)
        scalar >>= 1
    return result


def _encode_point(point: tuple[int, int]) -> bytes:
    x, y = point
    encoded = y | ((x & 1) << 255)
    return encoded.to_bytes(32, "little")


def ed25519_public_key(seed: bytes) -> bytes:
    if len(seed) != 32:
        raise ValueError("Ed25519 seed must be exactly 32 bytes")
    expanded = bytearray(hashlib.sha512(seed).digest())
    expanded[0] &= 248
    expanded[31] &= 63
    expanded[31] |= 64
    scalar = int.from_bytes(expanded[:32], "little")
    return _encode_point(_scalar_multiply(_BASE, scalar))


def ed25519_sign(seed: bytes, message: bytes) -> bytes:
    if len(seed) != 32:
        raise ValueError("Ed25519 seed must be exactly 32 bytes")
    expanded = bytearray(hashlib.sha512(seed).digest())
    expanded[0] &= 248
    expanded[31] &= 63
    expanded[31] |= 64
    scalar = int.from_bytes(expanded[:32], "little")
    prefix = bytes(expanded[32:])
    public_key = _encode_point(_scalar_multiply(_BASE, scalar))
    nonce = int.from_bytes(hashlib.sha512(prefix + message).digest(), "little") % _L
    encoded_nonce = _encode_point(_scalar_multiply(_BASE, nonce))
    challenge = int.from_bytes(
        hashlib.sha512(encoded_nonce + public_key + message).digest(), "little"
    ) % _L
    response = (nonce + challenge * scalar) % _L
    return encoded_nonce + response.to_bytes(32, "little")


def _resource_id(prefix: str, timestamp_milliseconds: int) -> str:
    random_bits = secrets.randbits(74)
    random_a = random_bits >> 62
    random_b = random_bits & ((1 << 62) - 1)
    value = (
        ((timestamp_milliseconds & ((1 << 48) - 1)) << 80)
        | (7 << 76)
        | (random_a << 64)
        | (2 << 62)
        | random_b
    )
    return f"{prefix}_{uuid.UUID(int=value)}"


def build_create_request(
    *,
    image_uri: str,
    runtime_contract_digest: str,
    profile_deployment_digest: str,
    signing_seed: bytes,
    execd_access_token: str,
    timestamp_milliseconds: int,
) -> dict[str, object]:
    if OCI_DIGEST_URI_RE.fullmatch(image_uri) is None:
        raise ValueError("image URI must be an immutable OCI digest reference")
    runtime_contract_digest = _require_digest(
        runtime_contract_digest, "runtime contract digest"
    )
    profile_deployment_digest = _require_digest(
        profile_deployment_digest, "profile deployment digest"
    )
    if HEX_32_RE.fullmatch(execd_access_token) is None:
        raise ValueError("execd access token must be 256-bit lowercase hex")

    tenant_id = _resource_id("ten", timestamp_milliseconds)
    invocation_id = _resource_id("inv", timestamp_milliseconds)
    job_id = _resource_id("job", timestamp_milliseconds)
    package_version_id = _resource_id("sprev", timestamp_milliseconds)
    runtime_version_id = _resource_id("srrev", timestamp_milliseconds)
    profile_deployment_id = _resource_id("sxdep", timestamp_milliseconds)
    input_value_id = _resource_id("val", timestamp_milliseconds)
    output_value_id = _resource_id("val", timestamp_milliseconds)
    while output_value_id == input_value_id:
        output_value_id = _resource_id("val", timestamp_milliseconds)

    input_value = {"probe": "l4-controller-persistence-only"}
    input_digest = canonical_digest(input_value)
    marker_digest = lambda marker: canonical_digest(
        {"domain": "insight.sandbox.readiness-probe/v1", "marker": marker}
    )
    limits = {
        "maximum_input_bytes": 1024,
        "maximum_output_bytes": 1024,
        "cpu_millicores": 250,
        "memory_mebibytes": 128,
        "pids": 8,
        "ephemeral_storage_bytes": 67_108_864,
        "wall_milliseconds": 60_000,
        "cleanup_milliseconds": 10_000,
    }
    provisioning_limits = {
        "maximum_candidates": 1,
        "candidate_page_items": 4,
        "candidate_quiescence_milliseconds": 100,
        "provisioning_timeout_milliseconds": 30_000,
        "orphan_page_items": 20,
        "runner_header_bytes": 8_192,
        "diagnostic_bytes": 8_192,
    }
    deadline_milliseconds = timestamp_milliseconds + 300_000
    deadline_seconds, fractional_milliseconds = divmod(deadline_milliseconds, 1_000)
    deadline = datetime.fromtimestamp(deadline_seconds, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    if fractional_milliseconds:
        deadline += f".{fractional_milliseconds:03d}"
    deadline += "Z"
    semantic_request = {
        "schema_version": 1,
        "tenant_id": tenant_id,
        "invocation_id": invocation_id,
        "job_id": job_id,
        "package_version_id": package_version_id,
        "image_uri": image_uri,
        "runtime_version_id": runtime_version_id,
        "runtime_contract_digest": runtime_contract_digest,
        "sandbox_profile_deployment_id": profile_deployment_id,
        "profile_deployment_digest": profile_deployment_digest,
        "runner_argv": ["/usr/local/bin/platform-sandbox-runner"],
        "package_argv": ["/opt/insight/package"],
        "input_value_id": input_value_id,
        "output_value_id": output_value_id,
        "classification": "internal",
        "input_schema_digest": marker_digest("input-schema"),
        "input_digest": input_digest,
        "output_schema_digest": marker_digest("output-schema"),
        "network_mode": "disabled",
        "limits": limits,
        "provisioning_limits": provisioning_limits,
        "deadline_at": deadline,
    }
    execution_request_digest = canonical_digest(semantic_request)
    provisioning_token = {
        "schema_version": 1,
        "tenant_id": tenant_id,
        "job_id": job_id,
        "physical_attempt": 1,
        "execution_request_digest": execution_request_digest,
    }
    provisioning_token_digest = canonical_digest(
        {"domain": "insight.sandbox.provision/v1", "token": provisioning_token}
    )
    runner_config = {
        "schema_version": 1,
        "execution_request_digest": execution_request_digest,
        "input_schema_digest": semantic_request["input_schema_digest"],
        "input_digest": input_digest,
        "output_schema_digest": semantic_request["output_schema_digest"],
        "activation_verifying_key": ed25519_public_key(signing_seed).hex(),
        "package_uid": 65_533,
        "package_argv": ["/opt/insight/package"],
        "maximum_input_bytes": limits["maximum_input_bytes"],
        "maximum_output_bytes": limits["maximum_output_bytes"],
        "maximum_diagnostic_bytes": provisioning_limits["diagnostic_bytes"],
        "maximum_processes": limits["pids"],
        "wall_milliseconds": limits["wall_milliseconds"],
    }
    runner_config_json = canonical_json(runner_config).decode("utf-8")
    runner_config_digest = "sha256:" + hashlib.sha256(
        runner_config_json.encode("utf-8")
    ).hexdigest()
    metadata = {
        "platform.insight.dev/schema": "v1",
        "insight.platform/sandbox-template": "armed-runner-v2",
        "platform.insight.dev/tenant": tenant_id,
        "platform.insight.dev/purpose": "readiness",
        "platform.insight.dev/job": job_id,
        "platform.insight.dev/attempt": "1",
        "platform.insight.dev/create": "1",
        "platform.insight.dev/provision": _encode_metadata_digest(
            provisioning_token_digest
        ),
        "platform.insight.dev/request": _encode_metadata_digest(
            execution_request_digest
        ),
        "platform.insight.dev/runtime": _encode_metadata_digest(
            runtime_contract_digest
        ),
        "platform.insight.dev/profile": _encode_metadata_digest(
            profile_deployment_digest
        ),
        "platform.insight.dev/network": "disabled",
    }
    return {
        "image": {"uri": image_uri},
        "timeout": 300,
        "resourceLimits": {
            "cpu": "250m",
            "memory": "128Mi",
            "ephemeral-storage": "67108864",
        },
        "resourceRequests": {
            "cpu": "250m",
            "memory": "128Mi",
            "ephemeral-storage": "67108864",
        },
        "env": {
            EXECD_ACCESS_TOKEN_ENV: execd_access_token,
            RUNNER_CONFIG_ENV: runner_config_json,
            RUNNER_CONFIG_DIGEST_ENV: runner_config_digest,
        },
        "metadata": metadata,
        "entrypoint": ["/usr/local/bin/platform-sandbox-runner"],
        "secureAccess": False,
    }


def runner_state_proof(signing_seed: bytes, sandbox_id: str, request_digest: str) -> str:
    if BOUNDED_ID_RE.fullmatch(sandbox_id) is None:
        raise ValueError("sandbox ID is outside the runner protocol")
    request_digest = _require_digest(request_digest, "execution request digest")
    preimage = canonical_json(
        {
            "domain": "insight.sandbox.runner-http-request/v1",
            "sandbox_id": sandbox_id,
            "execution_request_digest": request_digest,
            "method": "GET",
            "path": "/v2/state",
            "body_digest": "sha256:" + hashlib.sha256(b"").hexdigest(),
        }
    )
    return "v1." + ed25519_sign(signing_seed, preimage).hex()


def validate_armed_state(
    state: object,
    sandbox_id: str,
    request_digest: str,
    expected_boot_id: Optional[str] = None,
) -> str:
    if BOUNDED_ID_RE.fullmatch(sandbox_id) is None:
        raise ValueError("sandbox ID is outside the runner protocol")
    request_digest = _require_digest(request_digest, "execution request digest")
    expected_keys = {
        "magic",
        "schema_version",
        "sandbox_id",
        "boot_id",
        "execution_request_digest",
        "phase",
        "frame_digest",
    }
    if not isinstance(state, dict) or set(state) != expected_keys:
        raise ValueError("runner state frame is not closed")
    boot_id = state.get("boot_id")
    if (
        state.get("magic") != "insight.sandbox.runner/v1"
        or type(state.get("schema_version")) is not int
        or state.get("schema_version") != 1
        or state.get("sandbox_id") != sandbox_id
        or state.get("execution_request_digest") != request_digest
        or state.get("phase") != "armed"
        or not isinstance(boot_id, str)
        or BOUNDED_ID_RE.fullmatch(boot_id) is None
        or (expected_boot_id is not None and boot_id != expected_boot_id)
    ):
        raise ValueError("runner is not the expected inert Armed candidate")
    frame_without_digest = dict(state)
    observed_digest = frame_without_digest.pop("frame_digest")
    if observed_digest != canonical_digest(frame_without_digest):
        raise ValueError("runner state frame digest is invalid")
    return boot_id


def _write_exclusive(path: Path, payload: bytes, mode: int) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
    except BaseException:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        raise


def _read_signing_seed(path: Path) -> bytes:
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o077:
        raise ValueError("signing seed must be a private regular file")
    raw = path.read_text(encoding="ascii").strip()
    if HEX_32_RE.fullmatch(raw) is None:
        raise ValueError("signing seed is not 256-bit lowercase hex")
    return bytes.fromhex(raw)


def _create(args: argparse.Namespace) -> None:
    seed = secrets.token_bytes(32)
    request = build_create_request(
        image_uri=args.image_uri,
        runtime_contract_digest=args.runtime_contract_digest,
        profile_deployment_digest=args.profile_deployment_digest,
        signing_seed=seed,
        execd_access_token=secrets.token_hex(32),
        timestamp_milliseconds=int(time.time() * 1000),
    )
    _write_exclusive(Path(args.signing_seed_output), seed.hex().encode("ascii") + b"\n", 0o600)
    _write_exclusive(Path(args.output), canonical_json(request) + b"\n", 0o600)


def _state_proof(args: argparse.Namespace) -> None:
    print(
        runner_state_proof(
            _read_signing_seed(Path(args.signing_seed)),
            args.sandbox_id,
            args.execution_request_digest,
        )
    )


def _validate_state(args: argparse.Namespace) -> None:
    raw = Path(args.state).read_bytes()
    if not raw or len(raw) > 65_536:
        raise ValueError("runner state response has an invalid size")
    state = json.loads(raw, object_pairs_hook=_strict_object)
    print(
        validate_armed_state(
            state,
            args.sandbox_id,
            args.execution_request_digest,
            args.expected_boot_id,
        )
    )


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)

    create = commands.add_parser("create")
    create.add_argument("--image-uri", required=True)
    create.add_argument("--runtime-contract-digest", required=True)
    create.add_argument("--profile-deployment-digest", required=True)
    create.add_argument("--signing-seed-output", required=True)
    create.add_argument("--output", required=True)
    create.set_defaults(handler=_create)

    proof = commands.add_parser("state-proof")
    proof.add_argument("--signing-seed", required=True)
    proof.add_argument("--sandbox-id", required=True)
    proof.add_argument("--execution-request-digest", required=True)
    proof.set_defaults(handler=_state_proof)

    validate = commands.add_parser("validate-armed-state")
    validate.add_argument("--state", required=True)
    validate.add_argument("--sandbox-id", required=True)
    validate.add_argument("--execution-request-digest", required=True)
    validate.add_argument("--expected-boot-id")
    validate.set_defaults(handler=_validate_state)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    try:
        args = parse_args(argv)
        args.handler(args)
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as error:
        print(f"OpenSandbox L4 probe input rejected: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
