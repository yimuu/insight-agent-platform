#!/usr/bin/env python3
"""Package and verify offline recovery declarations; never restore or authorize external effects."""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys

SCRIPT = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("artifact_signature", SCRIPT / "sign-product-release.py")
signatures = importlib.util.module_from_spec(spec)
spec.loader.exec_module(signatures)
MANIFEST_LIMIT = 2_097_152
REPORT_LIMIT = 1_048_576
ENVELOPE_LIMIT = 2_097_152


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def strict_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("recovery JSON contains duplicate properties")
        result[key] = value
    return result


def read(path, limit):
    info = path.lstat()
    if path.is_symlink() or not path.is_file() or info.st_size > limit:
        raise ValueError("recovery file must be bounded and physical")
    with path.open("rb") as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise ValueError("recovery file exceeded bound while reading")
    return raw


def document(path, limit):
    return json.loads(read(path, limit), object_pairs_hook=strict_object,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")))


def digest(raw):
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def validate(tool, directory, now):
    result = subprocess.run([str(tool), "validate-recovery-set", str(directory / "manifest.json"), str(directory / "verification-report.json"), now], capture_output=True, check=False)
    if result.returncode:
        raise ValueError("owning recovery validation failed: " + result.stderr.decode(errors="replace")[-1024:])
    if len(result.stdout) > ENVELOPE_LIMIT:
        raise ValueError("owning recovery validation output exceeded bound")
    return json.loads(result.stdout)


def inventory(directory, contract):
    expected = {"manifest.json", "verification-report.json"}
    expected.update("evidence/" + value.removeprefix("sha256:") for value in contract["evidence_digests"])
    actual = set()
    for item in directory.iterdir():
        if item.name in {"recovery-set.json", "recovery-set.signature.json"}:
            continue
        if item.is_symlink():
            raise ValueError("recovery set forbids symbolic links")
        if item.name == "evidence" and item.is_dir():
            actual.update("evidence/" + child.name for child in item.iterdir())
        elif item.is_file():
            actual.add(item.name)
        else:
            raise ValueError("recovery set contains an unexpected entry")
    if actual != expected:
        raise ValueError("recovery set contains missing or extra evidence")
    files = []
    total = 0
    for name in sorted(expected):
        bound = MANIFEST_LIMIT if name == "manifest.json" else REPORT_LIMIT if name == "verification-report.json" else contract["evidence_max_bytes"]
        raw = read(directory / name, bound)
        total += len(raw)
        if total > contract["set_max_bytes"]:
            raise ValueError("recovery set exceeds total size bound")
        identity = digest(raw)
        if name.startswith("evidence/") and identity != "sha256:" + name.split("/")[1]:
            raise ValueError("recovery evidence bytes do not match identity")
        files.append({"path": name, "byte_length": len(raw), "digest": identity})
    return files


def envelope(directory, contract):
    return {"schema_version": 1, "kind": "insight.platform/recovery-set/v1",
            "recovery_tool_build_digest": contract["recovery_tool_build_digest"],
            "manifest_digest": contract["manifest_digest"], "files": inventory(directory, contract)}


def validate_envelope(tool, path):
    subprocess.run([str(tool), "validate-recovery-envelope", str(path)], check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    create = subparsers.add_parser("create")
    create.add_argument("--manifest", required=True, type=Path)
    create.add_argument("--report", required=True, type=Path)
    create.add_argument("--evidence-directory", required=True, type=Path)
    create.add_argument("--private-key", required=True, type=Path)
    create.add_argument("--output", required=True, type=Path)
    verify = subparsers.add_parser("verify")
    verify.add_argument("--set", required=True, type=Path)
    for command in (create, verify):
        command.add_argument("--validator", required=True, type=Path)
        command.add_argument("--trusted-public-key-base64", required=True)
    args = parser.parse_args()
    now = datetime.now(timezone.utc).isoformat()
    if args.command == "create":
        # The exact source inputs are validated before any evidence is copied or signed.
        manifest = read(args.manifest, MANIFEST_LIMIT)
        report = read(args.report, REPORT_LIMIT)
        if args.output.exists() or args.output.is_symlink():
            raise ValueError("recovery output must be a new directory")
        args.output.mkdir(mode=0o700)
        try:
            (args.output / "manifest.json").write_bytes(manifest)
            (args.output / "verification-report.json").write_bytes(report)
            contract = validate(args.validator, args.output, now)
            evidence = args.output / "evidence"
            evidence.mkdir()
            if args.evidence_directory.is_symlink() or not args.evidence_directory.is_dir():
                raise ValueError("evidence source must be a physical directory")
            expected = {value.removeprefix("sha256:") for value in contract["evidence_digests"]}
            if {item.name for item in args.evidence_directory.iterdir()} != expected:
                raise ValueError("evidence source contains missing or extra files")
            total = len(manifest) + len(report)
            for name in sorted(expected):
                raw = read(args.evidence_directory / name, contract["evidence_max_bytes"])
                total += len(raw)
                if total > contract["set_max_bytes"]:
                    raise ValueError("recovery evidence exceeds total size bound")
                if digest(raw) != "sha256:" + name:
                    raise ValueError("recovery evidence identity mismatch")
                (evidence / name).write_bytes(raw)
            raw = canonical(envelope(args.output, contract))
            (args.output / "recovery-set.json").write_bytes(raw)
            validate_envelope(args.validator, args.output / "recovery-set.json")
            signature = signatures.create_signature(raw, args.private_key, args.trusted_public_key_base64)
            signatures.verify_signature(raw, signature, args.trusted_public_key_base64)
            (args.output / "recovery-set.json").write_bytes(raw)
            (args.output / "recovery-set.signature.json").write_bytes(canonical(signature))
        except Exception:
            shutil.rmtree(args.output)
            raise
    else:
        if args.set.is_symlink() or not args.set.is_dir():
            raise ValueError("recovery set must be a physical directory")
        raw = read(args.set / "recovery-set.json", ENVELOPE_LIMIT)
        value = document(args.set / "recovery-set.json", ENVELOPE_LIMIT)
        if canonical(value) != raw:
            raise ValueError("recovery set envelope must be canonical JSON")
        signature = document(args.set / "recovery-set.signature.json", 4096)
        signatures.verify_signature(raw, signature, args.trusted_public_key_base64)
        validate_envelope(args.validator, args.set / "recovery-set.json")
        contract = validate(args.validator, args.set, now)
        if envelope(args.set, contract) != value:
            raise ValueError("recovery signed closure mismatch")
    print(json.dumps({"recovery_set_digest": digest(raw), "declaration_consistency_verified": True, "external_state_verified_by_tool": False,
                      "quarantined_effect_count": contract["quarantined_effect_count"]}, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError, TypeError, subprocess.CalledProcessError) as error:
        print(f"recovery set rejected: {error}", file=sys.stderr)
        sys.exit(1)
