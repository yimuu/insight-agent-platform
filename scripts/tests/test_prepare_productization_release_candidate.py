from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "prepare-productization-release-candidate.py"
DIGESTS = {
    name: "sha256:" + character * 64
    for name, character in (("index", "a"), ("amd64", "b"), ("arm64", "c"))
}


class PrepareProductizationReleaseCandidateTests(unittest.TestCase):
    def fixture(self, root: Path, *, unsafe_console: bool = False) -> Path:
        assets = root / "assets"
        assets.mkdir()
        binary = assets / "insight-1.2.3-x86_64-unknown-linux-gnu"
        binary.write_bytes(b"candidate-cli")
        archive = assets / "insight-1.2.3-x86_64-unknown-linux-gnu.tar.gz"
        archive.write_bytes(b"candidate-cli-archive")
        console = assets / "console-1.2.3.tar.gz"
        with tarfile.open(console, "w:gz") as package:
            payload = b"<!doctype html><title>candidate</title>"
            member = tarfile.TarInfo("../index.html" if unsafe_console else "index.html")
            member.size = len(payload)
            package.addfile(member, io.BytesIO(payload))

        images = {}
        bundle_images = []
        for name, suffix in (
            ("runtime", "platform-runtime"),
            ("sandbox_runner", "platform-sandbox-runner"),
            ("console", "platform-console"),
        ):
            subject = f"ghcr.io/example/repo/{suffix}"
            platforms = {"linux/amd64": DIGESTS["amd64"], "linux/arm64": DIGESTS["arm64"]}
            images[name] = {"subject": subject, "index_digest": DIGESTS["index"], "platforms": platforms}
            bundle_images.append({
                "name": name,
                "subject": subject,
                "index_digest": DIGESTS["index"],
                "platforms": [
                    {"platform": platform, "digest": digest}
                    for platform, digest in sorted(platforms.items())
                ],
            })
        (assets / "images.json").write_text(json.dumps(images, sort_keys=True, separators=(",", ":")))

        def artifact(path: Path) -> dict[str, object]:
            payload = path.read_bytes()
            return {
                "path": path.name,
                "bytes": len(payload),
                "sha256": "sha256:" + hashlib.sha256(payload).hexdigest(),
            }

        bundle = {
            "schema_version": 1,
            "version": "1.2.3",
            "git_commit": "d" * 40,
            "created_at": "2026-01-01T00:00:00.000000Z",
            "contract_digest": "sha256:" + "e" * 64,
            "profile_schema_digest": "sha256:" + "f" * 64,
            "development_profile_digest": "sha256:" + "0" * 64,
            "console": artifact(console),
            "cli": [{"target": "x86_64-unknown-linux-gnu", "archive": artifact(archive), "binary": artifact(binary)}],
            "images": bundle_images,
            "metadata": [],
        }
        (assets / "release-bundle.json").write_text(json.dumps(bundle, sort_keys=True, separators=(",", ":")))
        (assets / "release-bundle.signature.json").write_text("{}")
        (assets / "release-bundle.sigstore.json").write_text("{}")
        covered = sorted(
            path for path in assets.iterdir()
            if path.name not in {"checksums.txt", "release-bundle.signature.json", "release-bundle.sigstore.json"}
        )
        (assets / "checksums.txt").write_text("".join(
            f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n" for path in covered
        ))
        return assets

    def run_script(self, root: Path, assets: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                "python3", str(SCRIPT), "--assets", str(assets),
                "--repository", "example/repo", "--release-tag", "v1.2.3",
                "--revision", "d" * 40, "--platform", "linux/amd64",
                "--console-output", str(root / "console"), "--output", str(root / "closure.json"),
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_closes_candidate_and_extracts_regular_console(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result = self.run_script(root, self.fixture(root))
            self.assertEqual(result.returncode, 0, result.stderr)
            closure = json.loads((root / "closure.json").read_bytes())
            self.assertEqual("linux/amd64", closure["images"]["runtime"]["platform"])
            self.assertEqual(DIGESTS["amd64"], closure["images"]["runtime"]["platform_digest"])
            self.assertTrue((root / "console" / "index.html").is_file())

    def test_rejects_console_archive_path_escape(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result = self.run_script(root, self.fixture(root, unsafe_console=True))
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("escaping path", result.stderr)
            self.assertFalse((root / "closure.json").exists())

    def test_rejects_drift_in_non_host_image_child(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            assets = self.fixture(root)
            images_path = assets / "images.json"
            images = json.loads(images_path.read_bytes())
            images["runtime"]["platforms"]["linux/arm64"] = "sha256:" + "9" * 64
            images_path.write_text(json.dumps(images, sort_keys=True, separators=(",", ":")))
            checksum_path = assets / "checksums.txt"
            lines = [
                f"{hashlib.sha256(images_path.read_bytes()).hexdigest()}  images.json\n"
                if line.endswith("  images.json\n") else line
                for line in checksum_path.read_text().splitlines(keepends=True)
            ]
            checksum_path.write_text("".join(lines))
            result = self.run_script(root, assets)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("platform children differ", result.stderr)
            self.assertFalse((root / "closure.json").exists())

    def test_rejects_symlink_candidate_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            assets = self.fixture(root)
            linked = root / "linked-assets"
            linked.symlink_to(assets, target_is_directory=True)
            result = self.run_script(root, linked)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("candidate directory must be real", result.stderr)


if __name__ == "__main__":
    unittest.main()
