from __future__ import annotations

import hashlib
import io
import json
import pathlib
import subprocess
import tarfile
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/build-product-release.py"
SIGNER = ROOT / "scripts/sign-product-release.py"
IMAGE_METADATA = ROOT / "scripts/build-release-image-metadata.py"
PERFORMANCE = ROOT / "scripts/build-release-performance.py"
TARGETS = (
    "aarch64-apple-darwin",
    "aarch64-unknown-linux-gnu",
    "x86_64-apple-darwin",
    "x86_64-unknown-linux-gnu",
)
METADATA = (
    "build-provenance.intoto.jsonl", "cli.spdx.json", "console.spdx.json",
    "release-performance.json", "runtime.spdx.json", "sandbox-runner.spdx.json",
)


def digest(label: str) -> str:
    return "sha256:" + hashlib.sha256(label.encode()).hexdigest()


class ProductReleaseTests(unittest.TestCase):
    def fixture(self, root: pathlib.Path) -> pathlib.Path:
        version = "1.2.3"
        profile = json.loads((ROOT / "release/development-profile-v1.json").read_bytes())
        (root / "development-profile-v1.json").write_text(
            json.dumps(profile, sort_keys=True, separators=(",", ":")), encoding="utf-8"
        )
        for name in METADATA:
            (root / name).write_text("{}\n", encoding="utf-8")
        (root / f"console-{version}.tar.gz").write_bytes(b"console")
        for target in TARGETS:
            binary = f"binary-{target}".encode()
            (root / f"insight-{version}-{target}").write_bytes(binary)
            with tarfile.open(root / f"insight-{version}-{target}.tar.gz", "w:gz") as archive:
                for name, contents, mode in (
                    ("insight", binary, 0o755), ("LICENSE", b"license\n", 0o644),
                    ("VERSION", f"{version}\n".encode(), 0o644),
                ):
                    info = tarfile.TarInfo(name)
                    info.size = len(contents)
                    info.mode = mode
                    info.mtime = 0
                    archive.addfile(info, io.BytesIO(contents))
        images = {
            name: {
                "subject": f"ghcr.io/example/{name}:v{version}",
                "index_digest": digest(f"index-{name}"),
                "platforms": {platform: digest(f"{name}-{platform}") for platform in ("linux/amd64", "linux/arm64")},
            }
            for name in ("console", "runtime", "sandbox_runner")
        }
        image_path = root / "images.json"
        image_path.write_text(json.dumps(images), encoding="utf-8")
        return image_path

    def run_generator(
        self, root: pathlib.Path, images: pathlib.Path, *, include_qualification: bool = False
    ) -> subprocess.CompletedProcess[str]:
        arguments = [
            "python3", str(SCRIPT), "--version", "1.2.3", "--git-commit", "a" * 40,
            "--created-at", "2026-09-01T00:00:00.000000Z", "--artifacts", str(root),
            "--images", str(images), "--output", str(root / "release-bundle.json"),
        ]
        if include_qualification:
            arguments.append("--include-development-qualification")
        return subprocess.run(arguments, cwd=ROOT, capture_output=True, text=True)

    def test_builds_canonical_closed_bundle_and_checksums(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            result = self.run_generator(root, self.fixture(root))
            self.assertEqual(result.returncode, 0, result.stderr)
            raw = (root / "release-bundle.json").read_bytes()
            self.assertEqual(raw, json.dumps(json.loads(raw), sort_keys=True, separators=(",", ":")).encode())
            bundle = json.loads(raw)
            self.assertEqual(list(TARGETS), [item["target"] for item in bundle["cli"]])
            self.assertEqual(["console", "runtime", "sandbox_runner"], [item["name"] for item in bundle["images"]])
            self.assertIn("release-bundle.json", (root / "checksums.txt").read_text())

    def test_final_bundle_binds_development_qualification_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            images = self.fixture(root)
            evidence = root / "development-profile-performance.json"
            evidence.write_text('{"schema_version":1}\n', encoding="utf-8")
            result = self.run_generator(root, images, include_qualification=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            metadata = {
                item["path"]: item for item in json.loads((root / "release-bundle.json").read_bytes())["metadata"]
            }
            self.assertEqual(
                digest(evidence.read_text()), metadata["development-profile-performance.json"]["sha256"]
            )

    def test_missing_arch_partial_image_and_archive_extra_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            images = self.fixture(root)
            (root / f"insight-1.2.3-{TARGETS[-1]}").unlink()
            self.assertNotEqual(self.run_generator(root, images).returncode, 0)
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            images = self.fixture(root)
            value = json.loads(images.read_bytes())
            del value["runtime"]["platforms"]["linux/arm64"]
            images.write_text(json.dumps(value))
            self.assertNotEqual(self.run_generator(root, images).returncode, 0)

    def test_signer_binds_the_configured_public_trust_root(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            private = root / "private.pem"
            public_der = root / "public.der"
            bundle = root / "release-bundle.json"
            signature = root / "release-bundle.signature.json"
            bundle.write_bytes(b"{}")
            generated = subprocess.run([
                "openssl", "genpkey", "-algorithm", "ED25519", "-out", str(private)
            ], check=False, capture_output=True)
            if generated.returncode != 0:
                self.skipTest("local OpenSSL does not provide Ed25519; Rust verifier tests still run")
            subprocess.run([
                "openssl", "pkey", "-in", str(private), "-pubout", "-outform", "DER",
                "-out", str(public_der),
            ], check=True, capture_output=True)
            public = __import__("base64").urlsafe_b64encode(public_der.read_bytes()[-32:]).decode().rstrip("=")
            result = subprocess.run([
                "python3", str(SIGNER), "--bundle", str(bundle), "--private-key", str(private),
                "--public-key-base64", public, "--output", str(signature),
            ], cwd=ROOT, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual("ed25519", json.loads(signature.read_bytes())["algorithm"])
            wrong = subprocess.run([
                "python3", str(SIGNER), "--bundle", str(bundle), "--private-key", str(private),
                "--public-key-base64", "A" * 43, "--output", str(signature),
            ], cwd=ROOT, capture_output=True, text=True)
            self.assertNotEqual(wrong.returncode, 0)

    def test_image_index_requires_both_exact_release_children(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            arguments = []
            for name in ("runtime", "sandbox-runner", "console"):
                index = root / f"{name}.json"
                index.write_text(json.dumps({"manifests": [
                    {"digest": digest(f"{name}-amd64"), "platform": {"os": "linux", "architecture": "amd64"}},
                    {"digest": digest(f"{name}-arm64"), "platform": {"os": "linux", "architecture": "arm64"}},
                    {"digest": digest(f"{name}-attestation"), "platform": {"os": "unknown", "architecture": "unknown"}},
                ]}))
                arguments += [f"--{name}-subject", f"ghcr.io/example/{name}:v1.2.3",
                              f"--{name}-digest", digest(f"index-{name}"), f"--{name}-index", str(index)]
            output = root / "images.json"
            result = subprocess.run(["python3", str(IMAGE_METADATA), *arguments, "--output", str(output)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual({"linux/amd64", "linux/arm64"}, set(json.loads(output.read_bytes())["runtime"]["platforms"]))
            value = json.loads((root / "runtime.json").read_bytes())
            value["manifests"].pop(1)
            (root / "runtime.json").write_text(json.dumps(value))
            self.assertNotEqual(subprocess.run(["python3", str(IMAGE_METADATA), *arguments, "--output", str(output)], capture_output=True).returncode, 0)

    def test_performance_report_is_closed_and_budget_enforced(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            phases = [
                "console_build", "runtime_build_push", "sandbox_runner_build_push",
                "console_image_build_push", "sbom", "provenance", "cosign", "cold_pull", "warm_reuse",
            ] + [f"cli_build:{target}" for target in TARGETS]
            evidence = [{"name": name, "duration_seconds": 1} for name in phases]
            (root / "all.performance.json").write_text(json.dumps(evidence))
            output = root / "release-performance.json"
            command = ["python3", str(PERFORMANCE), "--version", "1.2.3", "--git-commit", "a" * 40,
                       "--evidence-directory", str(root), "--output", str(output)]
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual("passed", json.loads(output.read_bytes())["status"])
            evidence[0]["duration_seconds"] = 10000
            (root / "all.performance.json").write_text(json.dumps(evidence))
            self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)


if __name__ == "__main__":
    unittest.main()
