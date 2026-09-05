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
            "--sandbox-runner-image-digest", DIGEST_B,
            "--git-commit", "sha1:" + "c" * 40,
            "--created-at", "2026-08-26T12:00:00.000000Z",
            "--environment-closure", str(environment),
            "--output-dir", str(output),
        ], cwd=ROOT, check=True)

    def test_candidate_is_deterministic_and_closes_images_and_runner(self):
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
            self.assertEqual(DIGEST_B, candidate["sandbox_runner_image_digest"])
            self.assertEqual(
                {
                    DIGEST_A,
                    "sha256:ae8dfbb277f40a39ff01ef35e5e1c10675acfe0fa9db15259b8f323e5efab778",
                    "sha256:a9a5f73c1785ebd955336ffa313973a35c1a1b662cb7afc4ea82d92021b3532a",
                },
                set(candidate["component_images"].values()),
            )
            dispatcher = json.loads(
                (first_path / "worker-manifests/sandbox-dispatcher.json").read_bytes()
            )
            self.assertEqual(DIGEST_A, dispatcher["adapter_runtime_digest"])
            self.assertEqual(7, len(candidate["worker_manifests"]))

    def test_invalid_mutable_subject_is_rejected(self):
        with tempfile.TemporaryDirectory() as output:
            result = subprocess.run([
                "python3", "scripts/build-platform-production-candidate.py",
                "--runtime-image-digest", "latest",
                "--sandbox-runner-image-digest", DIGEST_B,
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
            "schema_version": 2,
            "environment_name": "production",
            "environment_class": "production",
            "application_repository": "yimuu/insight-agent-platform",
            "application_commit": "c" * 40,
            "qualification_profile_digest": "sha256:" + hashlib.sha256(canonical).hexdigest(),
            "deployment": {
                "requires_multi_node": True,
                "requires_opensandbox_kubernetes": True,
                "requires_validating_admission_policy_v1": True,
                "container_runtime": "containerd-runc",
                "sandbox_control_namespace": "platform-sandbox",
                "sandbox_workload_namespace": "platform-sandbox-workloads",
                "opensandbox_source_commit": "c39b814f36ded4c61d5ac6f9332ee4dfbab86c00",
                "opensandbox_server_image_digest": "sha256:ae8dfbb277f40a39ff01ef35e5e1c10675acfe0fa9db15259b8f323e5efab778",
                "opensandbox_controller_image_digest": "sha256:a9a5f73c1785ebd955336ffa313973a35c1a1b662cb7afc4ea82d92021b3532a",
                "opensandbox_execd_image_digest": "sha256:6cf7dba2f21f0b536e100563d841ac58a9f31c2b0a081b7ac76796a24d6f47e2",
                "batchsandbox_crd_digest": "sha256:6a56fbec00a33acf30a4a9c3418172ad6ac1eba34d081881e6b5dd941cfa59d4",
                "kubernetes_provider_template_digest": "sha256:be829c7a936867d7aff62bf76d5e897b75c65628563ad2d354f4ccb36b30cc4c",
                "sandbox_network_policy_digest": "sha256:2bc456ef5f8427de8b142de9347d030fec638078dd11df111bc05ef85110e66e",
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
