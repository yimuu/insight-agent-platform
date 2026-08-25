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
            self.assertEqual(15, len(candidate["component_images"]))
            self.assertEqual({DIGEST_A}, set(candidate["component_images"].values()))
            guest = json.loads(
                (first_path / "worker-manifests/sandbox-executor.gvisor.json").read_bytes()
            )
            self.assertEqual(DIGEST_B, guest["adapter_runtime_digest"])
            self.assertEqual(7, len(candidate["worker_manifests"]))

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
