import base64
import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "Cargo.toml").is_file())
SCRIPT = ROOT / "tools/release/platform-recovery.py"


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def digest(value):
    return "sha256:" + hashlib.sha256(value if isinstance(value, bytes) else canonical(value)).hexdigest()


class RecoverySetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        subprocess.run(["cargo", "build", "--locked", "--quiet", "-p", "insight-platform-contract-tooling", "--bin", "platform-qualification"], cwd=ROOT, check=True)
        metadata = json.loads(subprocess.check_output(
            ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"], cwd=ROOT))
        cls.tool = Path(metadata["target_directory"]) / "debug/platform-qualification"

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.private = self.directory / "key.pem"
        subprocess.run(["openssl", "genpkey", "-algorithm", "ED25519", "-out", str(self.private)], check=True, capture_output=True)
        public = subprocess.check_output(["openssl", "pkey", "-in", str(self.private), "-pubout", "-outform", "DER"])[-32:]
        self.public = base64.urlsafe_b64encode(public).decode().rstrip("=")
        config = self.directory / "config.json"
        config.write_text("{}")
        programs = json.loads(subprocess.check_output([str(self.tool), "print-worker-execution-capabilities", "platform-orchestration-worker", str(config)]))
        now = datetime.now(timezone.utc).replace(microsecond=0)
        self.time = lambda seconds: (now + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.proof = b"dedicated external recovery attestation fixture; no real provider restore\n"
        self.proof_digest = digest(self.proof)
        self.key = {"kind": "kms", "encryption_domain_id": "enc_0198f1c5-0787-75e1-a9e8-d95ca0f39009", "key_version_identity_digest": digest(b"key-version")}
        self.object = {"artifact": {"artifact_id": "art_0198f1c5-0787-75e1-a9e8-d95ca0f39008", "content_digest": digest(b"content"), "byte_length": 7, "media_type": "application/json", "classification": "internal", "display_name": None}, "storage_binding_digest": digest(b"binding"), "object_generation_digest": digest(b"generation"), "required_key_digest": digest(self.key)}
        self.effect = digest(b"stable-effect")
        self.manifest = {"schema_version": 1, "created_at": self.time(-30), "valid_until": self.time(3600), "database": {"instance_identity_digest": digest(b"pg-instance"), "timeline": 1, "wal_lsn": "0/16B6C50", "snapshot_digest": digest(b"backup"), "schema_snapshot_digest": digest(b"current-schema"), "captured_at": self.time(-60), "reference_inventory_digest": self.proof_digest}, "objects": [self.object], "required_keys": [self.key], "potentially_affected_effects": [self.effect], "program_capabilities": programs, "runner_build_digest": digest(b"runner"), "package_set_digest": self.proof_digest, "release_digest": digest(b"release"), "recovery_tool_build_digest": digest(self.tool.read_bytes())}
        phases = ["old_writers_isolated", "old_identities_revoked", "old_sessions_terminated", "credentials_rotated", "restricted_environment_installed", "database_structure_verified", "owner_and_quota_verified", "cleanup_obligations_verified", "reference_inventory_complete", "secret_validity_rechecked", "consumer_watermarks_reconciled"]
        self.report = {"schema_version": 1, "manifest_digest": digest(self.manifest), "verified_at": self.time(-10), "object_observations": [{"subject_digest": digest(self.object), "evidence_digest": self.proof_digest}], "key_observations": [{"subject_digest": digest(self.key), "evidence_digest": self.proof_digest}], "backup_holds": [{"subject_digest": identity, "protected_from": self.time(-120), "expires_at": self.time(7200), "evidence_digest": self.proof_digest} for identity in [digest(self.object), digest(self.key)]], "phases": [{"phase": phase, "verified": True, "evidence_digest": self.proof_digest} for phase in phases], "effects": [{"effect_identity_digest": self.effect, "disposition": {"state": "verified_completed", "evidence_digest": self.proof_digest}}], "unrecovered_effects": []}
        self.evidence = self.directory / "evidence"
        self.evidence.mkdir()
        (self.evidence / self.proof_digest.removeprefix("sha256:")).write_bytes(self.proof)
        self.output = self.directory / "set"

    def create(self, manifest=None, report=None):
        (self.directory / "manifest.json").write_bytes(canonical(manifest or self.manifest))
        (self.directory / "report.json").write_bytes(canonical(report or self.report))
        return subprocess.run(["python3", str(SCRIPT), "create", "--manifest", str(self.directory / "manifest.json"), "--report", str(self.directory / "report.json"), "--evidence-directory", str(self.evidence), "--private-key", str(self.private), "--output", str(self.output), "--validator", str(self.tool), "--trusted-public-key-base64=" + self.public], cwd=ROOT, capture_output=True)

    def verify(self, public=None):
        return subprocess.run(["python3", str(SCRIPT), "verify", "--set", str(self.output), "--validator", str(self.tool), "--trusted-public-key-base64=" + (public or self.public)], cwd=ROOT, capture_output=True)

    def test_signed_complete_set_and_quarantined_unknown_are_verifiable(self):
        self.report["effects"][0]["disposition"] = {"state": "quarantined", "evidence_digest": self.proof_digest, "isolation_evidence_digest": self.proof_digest}
        self.report["unrecovered_effects"] = [self.effect]
        created = self.create()
        self.assertEqual(created.returncode, 0, created.stderr.decode())
        verified = self.verify()
        self.assertEqual(verified.returncode, 0, verified.stderr.decode())
        outcome = json.loads(verified.stdout)
        self.assertEqual(outcome["quarantined_effect_count"], 1)
        self.assertFalse(outcome["external_state_verified_by_tool"])

    def test_tampered_manifest_report_evidence_or_signature_is_rejected(self):
        result = self.create()
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        for relative in ["manifest.json", "verification-report.json", "evidence/" + self.proof_digest.removeprefix("sha256:"), "recovery-set.signature.json"]:
            with self.subTest(relative=relative):
                target = self.output / relative
                raw = target.read_bytes()
                target.write_bytes(raw + b" ")
                if relative.endswith("signature.json"):
                    value = json.loads(raw)
                    value["signature"] = "A" * 86
                    target.write_bytes(canonical(value))
                self.assertNotEqual(self.verify().returncode, 0)
                target.write_bytes(raw)
        self.assertNotEqual(self.verify(base64.urlsafe_b64encode(b"x" * 32).decode().rstrip("=")).returncode, 0)
        signature_path = self.output / "recovery-set.signature.json"
        signature = json.loads(signature_path.read_bytes())
        signature["schema_version"] = True
        signature_path.write_bytes(canonical(signature))
        self.assertNotEqual(self.verify().returncode, 0)

    def test_missing_asset_key_hold_phase_and_unisolated_effect_reject(self):
        for field in ["object_observations", "key_observations", "backup_holds", "phases", "effects"]:
            with self.subTest(field=field):
                report = copy.deepcopy(self.report)
                report[field] = []
                rejected = self.create(report=report)
                self.assertNotEqual(rejected.returncode, 0, field)
                self.assertFalse(self.output.exists())
        report = copy.deepcopy(self.report)
        report["effects"][0]["disposition"]["state"] = "unresolved"
        self.assertNotEqual(self.create(report=report).returncode, 0)
        report = copy.deepcopy(self.report)
        report["backup_holds"][0]["expires_at"] = self.time(-1)
        self.assertNotEqual(self.create(report=report).returncode, 0)
        report = copy.deepcopy(self.report)
        report["phases"][0]["verified"] = False
        self.assertNotEqual(self.create(report=report).returncode, 0)

    def test_current_versions_duplicates_bounds_and_exact_evidence_closure_reject(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["program_capabilities"]["capabilities"][0]["ir_abi_version"] = 5
        report = copy.deepcopy(self.report)
        report["manifest_digest"] = digest(manifest)
        self.assertNotEqual(self.create(manifest, report).returncode, 0)
        report = copy.deepcopy(self.report)
        report["object_observations"] *= 2
        self.assertNotEqual(self.create(report=report).returncode, 0)
        evidence = next(self.evidence.iterdir())
        evidence.unlink()
        self.assertNotEqual(self.create().returncode, 0)
        with evidence.open("wb") as stream:
            stream.truncate(4_194_305)
        self.assertNotEqual(self.create().returncode, 0)


    def test_secret_version_must_be_exact_pinned_and_unknown_fields_reject(self):
        policy = {"kind": "pinned", "opaque_version_identity_digest": digest(b"provider-secret-version")}
        secret = {"kind": "secret", "binding": {
            "secret_binding_id": "sbd_0198f1c5-0787-75e1-a9e8-d95ca0f39010",
            "binding_generation": 2, "provider_id": "spr_0198f1c5-0787-75e1-a9e8-d95ca0f39011",
            "purpose": "model.invoke", "resolution_policy": policy, "resolution_policy_digest": digest(policy)}}
        self.manifest["required_keys"].append(secret)
        self.report["manifest_digest"] = digest(self.manifest)
        self.report["key_observations"].append({"subject_digest": digest(secret), "evidence_digest": self.proof_digest})
        self.report["backup_holds"].append({"subject_digest": digest(secret), "protected_from": self.time(-120), "expires_at": self.time(7200), "evidence_digest": self.proof_digest})
        accepted = self.create()
        self.assertEqual(accepted.returncode, 0, accepted.stderr.decode())
        self.assertEqual(self.verify().returncode, 0)
        for mutation in ["unknown", "rotation", "version", "missing_key", "null_key", "wrong_key_kind"]:
            with self.subTest(mutation=mutation):
                manifest = copy.deepcopy(self.manifest)
                if mutation == "unknown":
                    manifest["secret_value"] = "must never be accepted"
                elif mutation == "version":
                    manifest["schema_version"] = 2
                elif mutation == "missing_key":
                    del manifest["objects"][0]["required_key_digest"]
                elif mutation == "null_key":
                    manifest["objects"][0]["required_key_digest"] = None
                elif mutation == "wrong_key_kind":
                    manifest["objects"][0]["required_key_digest"] = digest(secret)
                else:
                    binding = manifest["required_keys"][-1]["binding"]
                    binding["resolution_policy"] = {"kind": "follow_provider_rotation", "rotation_policy_revision_id": "prev_0198f1c5-0787-75e1-a9e8-d95ca0f39012"}
                    binding["resolution_policy_digest"] = digest(binding["resolution_policy"])
                report = copy.deepcopy(self.report)
                report["manifest_digest"] = digest(manifest)
                (self.directory / "invalid-manifest.json").write_bytes(canonical(manifest))
                (self.directory / "invalid-report.json").write_bytes(canonical(report))
                result = subprocess.run([str(self.tool), "validate-recovery-set", str(self.directory / "invalid-manifest.json"), str(self.directory / "invalid-report.json"), self.time(0)], capture_output=True)
                self.assertNotEqual(result.returncode, 0)

    def test_envelope_parser_accepts_8194_files_and_rejects_8195(self):
        files = [{"path": name, "byte_length": 1, "digest": digest(name.encode())}
                 for name in ["manifest.json", "verification-report.json"]]
        for ordinal in range(8192):
            identity = digest(str(ordinal).encode())
            files.append({"path": "evidence/" + identity.removeprefix("sha256:"),
                          "byte_length": 1, "digest": identity})
        envelope = {"schema_version": 1, "kind": "insight.platform/recovery-set/v1",
                    "recovery_tool_build_digest": digest(self.tool.read_bytes()),
                    "manifest_digest": digest(self.manifest), "files": files}
        source = self.directory / "extreme-envelope.json"
        source.write_bytes(canonical(envelope))
        accepted = subprocess.run([str(self.tool), "validate-recovery-envelope", str(source)], capture_output=True)
        self.assertEqual(accepted.returncode, 0, accepted.stderr.decode())
        identity = digest(b"one-too-many")
        files.append({"path": "evidence/" + identity.removeprefix("sha256:"), "byte_length": 1, "digest": identity})
        source.write_bytes(canonical(envelope))
        rejected = subprocess.run([str(self.tool), "validate-recovery-envelope", str(source)], capture_output=True)
        self.assertNotEqual(rejected.returncode, 0)


if __name__ == "__main__":
    unittest.main()
