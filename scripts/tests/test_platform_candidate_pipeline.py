import hashlib
import json
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64


class CandidatePipelineTests(unittest.TestCase):
    def build(self, output):
        environment = Path(output).parent / (Path(output).name + "-environment")
        environment.mkdir(exist_ok=True)
        (environment / "closure.yaml").write_text("imagePolicy: exact-digest\n")
        subprocess.run([
            "python3", "scripts/build-platform-production-candidate.py",
            "--runtime-image-digest", DIGEST_A,
            "--sandbox-guest-image-digest", DIGEST_B,
            "--git-commit", "sha1:" + "c" * 40,
            "--created-at", "2026-08-26T12:00:00.000000Z",
            "--environment-closure", str(environment),
            "--output-dir", str(output),
        ], cwd=ROOT, check=True)

    def test_candidate_is_deterministic_and_closes_images_and_guest(self):
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            self.build(first)
            self.build(second)
            first_path = Path(first)
            second_path = Path(second)
            self.assertEqual(
                (first_path / "candidate-manifest.json").read_bytes(),
                (second_path / "candidate-manifest.json").read_bytes(),
            )
            candidate = json.loads((first_path / "candidate-manifest.json").read_bytes())
            self.assertEqual(16, len(candidate["component_images"]))
            self.assertEqual({DIGEST_A}, set(candidate["component_images"].values()))
            guest = json.loads(
                (first_path / "worker-manifests/sandbox-executor.gvisor.json").read_bytes()
            )
            self.assertEqual(DIGEST_B, guest["adapter_runtime_digest"])
            self.assertEqual(8, len(candidate["worker_manifests"]))

    def test_invalid_mutable_subject_is_rejected(self):
        with tempfile.TemporaryDirectory() as output:
            result = subprocess.run([
                "python3", "scripts/build-platform-production-candidate.py",
                "--runtime-image-digest", "latest",
                "--sandbox-guest-image-digest", DIGEST_B,
                "--git-commit", "sha1:" + "c" * 40,
                "--created-at", "2026-08-26T12:00:00.000000Z",
                "--environment-closure", output,
                "--output-dir", output,
            ], cwd=ROOT, capture_output=True, text=True)
            self.assertNotEqual(0, result.returncode)

    def test_gitops_environment_validator_accepts_exact_closed_input_and_rejects_drift(self):
        profile = ROOT / "contracts/platform-v1/qualification/production-release-profile.json"
        value = json.loads(profile.read_bytes())
        canonical = json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
        manifest = {
            "schema_version": 1,
            "environment_name": "production",
            "environment_class": "production",
            "application_repository": "yimuu/insight-agent-platform",
            "application_commit": "c" * 40,
            "qualification_profile_digest": "sha256:" + hashlib.sha256(canonical).hexdigest(),
            "deployment": {
                "requires_multi_node": True,
                "requires_runsc": True,
                "requires_validating_admission_policy_v1": True,
                "wasi_node_selector": {
                    "insight.platform.node-restriction.kubernetes.io/sandbox-wasi": "true"
                },
                "gvisor_node_selector": {
                    "insight.platform.node-restriction.kubernetes.io/sandbox-gvisor": "true"
                },
                "attestor_node_selector": {
                    "insight.platform.node-restriction.kubernetes.io/sandbox-attestor": "true"
                },
                "runtime_class": "runsc",
            },
            "dependencies": {
                "postgresql": "dedicated", "nats": "tls-core", "object_storage": "versioned-s3",
                "key_management": "kms", "secret_management": "external", "telemetry": "prometheus",
            },
            "secret_policy": {
                "plaintext_in_git": False,
                "kubeconfig_in_git": False,
                "references_only": True,
            },
        }
        with tempfile.TemporaryDirectory() as directory:
            closure = Path(directory)
            path = closure / "environment.json"
            path.write_text(json.dumps(manifest))
            command = [
                "python3", "scripts/validate-platform-gitops-environment.py",
                "--closure", str(closure),
                "--application-repository", "yimuu/insight-agent-platform",
                "--application-commit", "c" * 40,
                "--qualification-profile", str(profile),
            ]
            subprocess.run(command, cwd=ROOT, check=True)
            manifest["application_commit"] = "d" * 40
            path.write_text(json.dumps(manifest))
            self.assertNotEqual(0, subprocess.run(command, cwd=ROOT, capture_output=True).returncode)

    def test_release_bundle_indexes_nested_artifacts_and_excludes_itself(self):
        with tempfile.TemporaryDirectory() as output:
            root = Path(output)
            (root / "nested").mkdir()
            (root / "candidate-manifest.json").write_text("candidate\n")
            (root / "nested/evidence.txt").write_text("evidence\n")
            subprocess.run([
                "python3", "scripts/build-platform-release-bundle.py", output,
            ], cwd=ROOT, check=True)
            manifest = json.loads((root / "release-bundle-manifest.json").read_bytes())
            self.assertEqual(
                ["candidate-manifest.json", "nested/evidence.txt"],
                [artifact["path"] for artifact in manifest["artifacts"]],
            )


if __name__ == "__main__":
    unittest.main()
