from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest


ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "Cargo.toml").is_file())
SCRIPT = ROOT / "tools/release/prepare-productization-release-candidate.py"
SPEC = importlib.util.spec_from_file_location("prepare_candidate", SCRIPT)
CANDIDATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CANDIDATE)
DIGESTS = {
    name: "sha256:" + character * 64
    for name, character in (("index", "a"), ("amd64", "b"), ("arm64", "c"))
}


class PrepareProductizationReleaseCandidateTests(unittest.TestCase):
    def test_release_tar_command_round_trips_root_and_nested_directories(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "dist"
            (source / "assets").mkdir(parents=True)
            (source / "index.html").write_bytes(b"<title>actual tar producer</title>")
            (source / "assets/app.js").write_bytes(b"export const ready = true")
            archive = root / "console.tar.gz"
            subprocess.run(
                ["tar", "-czf", str(archive), "-C", str(source), "."],
                # macOS bsdtar otherwise adds AppleDouble files absent from the Linux producer.
                env={**os.environ, "COPYFILE_DISABLE": "1"},
                check=True, capture_output=True,
            )
            output = root / "unpacked"
            CANDIDATE.extract_console(archive, output)
            self.assertEqual(CANDIDATE.tree_digest(source), CANDIDATE.tree_digest(output))

    def test_console_root_and_normalized_paths_remain_closed(self) -> None:
        cases = [
            [("./", tarfile.DIRTYPE), (".", tarfile.DIRTYPE)],
            [("./", tarfile.REGTYPE)],
            [(".", tarfile.SYMTYPE)],
            [("./index.html", tarfile.REGTYPE), ("index.html", tarfile.REGTYPE)],
            [("./../index.html", tarfile.REGTYPE)],
            [("./assets/link", tarfile.LNKTYPE)],
        ]
        for entries in cases:
            with self.subTest(entries=entries), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                archive = root / "console.tar.gz"
                with tarfile.open(archive, "w:gz") as package:
                    for name, kind in entries:
                        member = tarfile.TarInfo(name)
                        member.type = kind
                        member.linkname = "../outside" if kind in (tarfile.SYMTYPE, tarfile.LNKTYPE) else ""
                        member.size = 1 if kind == tarfile.REGTYPE else 0
                        package.addfile(member, io.BytesIO(b"x") if member.size else None)
                output = root / "unpacked"
                with self.assertRaises(ValueError):
                    CANDIDATE.extract_console(archive, output)
                self.assertFalse(output.exists())

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
