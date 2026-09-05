from __future__ import annotations

import gzip
import hashlib
import io
import json
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
VERIFIER = REPOSITORY_ROOT / "scripts" / "verify-platform-sandbox-package-image.py"
OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_CONFIG = "application/vnd.oci.image.config.v1+json"
OCI_LAYER = "application/vnd.oci.image.layer.v1.tar"
OCI_LAYER_GZIP = "application/vnd.oci.image.layer.v1.tar+gzip"
RUNNER_PATH = "usr/local/bin/platform-sandbox-runner"
PACKAGE_PATH = "opt/insight/package"
EXPECTED_CAPABILITY = bytes.fromhex(
    "01000002e0000000000000000000000000000000"
)


def encoded_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def descriptor(payload: bytes, media_type: str) -> Dict[str, Any]:
    return {"mediaType": media_type, "digest": digest(payload), "size": len(payload)}


def tar_entry(
    name: str,
    *,
    kind: str = "file",
    data: bytes = b"",
    mode: int = 0o555,
    linkname: str = "",
    xattrs: Optional[Mapping[str, bytes]] = None,
    pax: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    return {
        "name": name,
        "kind": kind,
        "data": data,
        "mode": mode,
        "linkname": linkname,
        "xattrs": dict(xattrs or {}),
        "pax": dict(pax or {}),
    }


def make_layer(
    entries: Sequence[Mapping[str, Any]],
    *,
    compressed: bool = False,
    global_pax: Optional[Mapping[str, str]] = None,
) -> Tuple[bytes, str, str]:
    raw = io.BytesIO()
    with tarfile.open(
        fileobj=raw,
        mode="w",
        format=tarfile.PAX_FORMAT,
        pax_headers=dict(global_pax or {}),
    ) as archive:
        for entry in entries:
            member = tarfile.TarInfo(str(entry["name"]))
            member.mode = int(entry["mode"])
            member.uid = 0
            member.gid = 0
            kind = entry["kind"]
            content = bytes(entry["data"])
            if kind == "file":
                member.type = tarfile.REGTYPE
                member.size = len(content)
            elif kind == "directory":
                member.type = tarfile.DIRTYPE
                member.size = 0
            elif kind == "symlink":
                member.type = tarfile.SYMTYPE
                member.linkname = str(entry["linkname"])
                member.size = 0
            elif kind == "hardlink":
                member.type = tarfile.LNKTYPE
                member.linkname = str(entry["linkname"])
                member.size = 0
            elif kind == "character-device":
                member.type = tarfile.CHRTYPE
                member.devmajor = 1
                member.devminor = 3
                member.size = 0
            elif kind == "fifo":
                member.type = tarfile.FIFOTYPE
                member.size = 0
            else:
                raise AssertionError(f"unsupported fixture kind {kind}")
            member.pax_headers = dict(entry["pax"])
            for name, value in dict(entry["xattrs"]).items():
                member.pax_headers["SCHILY.xattr." + name] = value.decode(
                    "utf-8", "surrogateescape"
                )
            archive.addfile(member, io.BytesIO(content) if content else None)
    uncompressed = raw.getvalue()
    if compressed:
        payload = gzip.compress(uncompressed, mtime=0)
        media_type = OCI_LAYER_GZIP
    else:
        payload = uncompressed
        media_type = OCI_LAYER
    return payload, media_type, digest(uncompressed)


def runtime_config() -> Dict[str, Any]:
    return {
        "User": "65532:65532",
        "Env": ["PATH=/usr/local/bin:/usr/bin:/bin"],
        "Entrypoint": ["/usr/local/bin/platform-sandbox-runner"],
        "Cmd": None,
        "WorkingDir": "/",
    }


def write_layout(
    path: Path,
    layers: Sequence[Tuple[bytes, str, str]],
    *,
    runtime: Optional[Mapping[str, Any]] = None,
    corrupt_blob_digest: Optional[str] = None,
    archive_format: int = tarfile.PAX_FORMAT,
) -> str:
    config = encoded_json(
        {
            "architecture": "amd64",
            "os": "linux",
            "config": dict(runtime if runtime is not None else runtime_config()),
            "rootfs": {"type": "layers", "diff_ids": [item[2] for item in layers]},
            "history": [],
        }
    )
    layer_descriptors = [descriptor(item[0], item[1]) for item in layers]
    manifest = encoded_json(
        {
            "schemaVersion": 2,
            "mediaType": OCI_MANIFEST,
            "config": descriptor(config, OCI_CONFIG),
            "layers": layer_descriptors,
        }
    )
    manifest_descriptor = descriptor(manifest, OCI_MANIFEST)
    index = encoded_json(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [manifest_descriptor],
        }
    )
    files: Dict[str, bytes] = {
        "oci-layout": encoded_json({"imageLayoutVersion": "1.0.0"}),
        "index.json": index,
        "blobs/sha256/" + digest(config).split(":", 1)[1]: config,
        "blobs/sha256/" + digest(manifest).split(":", 1)[1]: manifest,
    }
    for payload, _, _ in layers:
        blob_digest = digest(payload)
        files["blobs/sha256/" + blob_digest.split(":", 1)[1]] = payload
    if corrupt_blob_digest is not None:
        files["blobs/sha256/" + corrupt_blob_digest.split(":", 1)[1]] += b"corrupt"
    with tarfile.open(path, mode="w", format=archive_format) as archive:
        for name, payload in files.items():
            member = tarfile.TarInfo(name)
            member.mode = 0o644
            member.size = len(payload)
            archive.addfile(member, io.BytesIO(payload))
    return str(manifest_descriptor["digest"])


class ImageFixture:
    def __init__(
        self,
        directory: Path,
        *,
        runner_entries: Optional[Sequence[Mapping[str, Any]]] = None,
        added_layers: Optional[Sequence[Sequence[Mapping[str, Any]]]] = None,
        package_runtime: Optional[Mapping[str, Any]] = None,
        compressed: bool = False,
        archive_format: int = tarfile.PAX_FORMAT,
    ) -> None:
        if runner_entries is None:
            runner_entries = [
                tar_entry(
                    RUNNER_PATH,
                    data=b"launcher",
                    xattrs={"security.capability": EXPECTED_CAPABILITY},
                ),
                tar_entry("usr/local/libexec/platform-sandbox-runner-core", data=b"core"),
            ]
        if added_layers is None:
            added_layers = [[tar_entry(PACKAGE_PATH, data=b"package", mode=0o555)]]
        self.runner_layer = make_layer(runner_entries, compressed=compressed)
        self.added_layers = [make_layer(layer, compressed=compressed) for layer in added_layers]
        self.runner_archive = directory / "runner.oci.tar"
        self.package_archive = directory / "package.oci.tar"
        self.runner_digest = write_layout(
            self.runner_archive,
            [self.runner_layer],
            archive_format=archive_format,
        )
        self.package_digest = write_layout(
            self.package_archive,
            [self.runner_layer, *self.added_layers],
            runtime=package_runtime,
            archive_format=archive_format,
        )


class VerifyPlatformSandboxPackageImageTests(unittest.TestCase):
    def run_verifier(
        self,
        fixture: ImageFixture,
        expected_runner_digest: Optional[str] = None,
        expected_package_digest: Optional[str] = None,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                str(VERIFIER),
                "--runner-oci-archive",
                str(fixture.runner_archive),
                "--package-oci-archive",
                str(fixture.package_archive),
                "--runner-platform-manifest-digest",
                expected_runner_digest or fixture.runner_digest,
                "--package-platform-manifest-digest",
                expected_package_digest or fixture.package_digest,
            ],
            cwd=REPOSITORY_ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    def assert_rejected(self, fixture: ImageFixture, message: str) -> None:
        result = self.run_verifier(fixture)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(message, result.stderr)

    def test_accepts_exact_uncompressed_and_gzip_composition(self) -> None:
        for compressed in (False, True):
            with self.subTest(compressed=compressed), tempfile.TemporaryDirectory() as temp:
                fixture = ImageFixture(Path(temp), compressed=compressed)
                result = self.run_verifier(fixture)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("Sandbox Package OCI composition passed", result.stdout)
                self.assertIn("raw SCHILY.xattr.*", result.stdout)

        with tempfile.TemporaryDirectory() as temp:
            fixture = ImageFixture(
                Path(temp),
                added_layers=[
                    [
                        tar_entry("opt/", kind="directory", mode=0o755),
                        tar_entry("opt/insight/", kind="directory", mode=0o755),
                        tar_entry(PACKAGE_PATH + "/", kind="directory", mode=0o555),
                        tar_entry(PACKAGE_PATH + "/bin", data=b"package", mode=0o555),
                    ]
                ],
            )
            result = self.run_verifier(fixture)
            self.assertEqual(result.returncode, 0, result.stderr)

        with tempfile.TemporaryDirectory() as temp:
            fixture = ImageFixture(Path(temp), archive_format=tarfile.GNU_FORMAT)
            result = self.run_verifier(fixture)
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_rejects_wrong_expected_runner_manifest_digest(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = ImageFixture(Path(temp))
            result = self.run_verifier(
                fixture, expected_runner_digest="sha256:" + "0" * 64
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("platform manifest digest", result.stderr)

    def test_rejects_wrong_expected_package_manifest_digest(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = ImageFixture(Path(temp))
            result = self.run_verifier(
                fixture, expected_package_digest="sha256:" + "0" * 64
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("package platform manifest digest", result.stderr)

    def test_rejects_non_strict_or_changed_runner_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            fixture = ImageFixture(directory)
            fixture.package_digest = write_layout(
                fixture.package_archive, [fixture.runner_layer]
            )
            self.assert_rejected(fixture, "strict prefix")

        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            fixture = ImageFixture(directory)
            changed_runner = make_layer(
                [
                    tar_entry(
                        RUNNER_PATH,
                        data=b"different",
                        xattrs={"security.capability": EXPECTED_CAPABILITY},
                    )
                ]
            )
            fixture.package_digest = write_layout(
                fixture.package_archive,
                [changed_runner, *fixture.added_layers],
            )
            self.assert_rejected(fixture, "runner layer prefix")

    def test_rejects_added_paths_outside_prefix_or_unsafe_paths(self) -> None:
        cases = {
            "outside": tar_entry("etc/injected", data=b"x"),
            "absolute": tar_entry("/opt/insight/package", data=b"x"),
            "traversal": tar_entry("opt/insight/package/../escape", data=b"x"),
            "unsafe-parent": tar_entry("opt/", kind="directory", mode=0o777),
        }
        for name, entry in cases.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temp:
                fixture = ImageFixture(Path(temp), added_layers=[[entry]])
                self.assert_rejected(fixture, "package added layer")

    def test_rejects_whiteout_link_device_and_fifo_additions(self) -> None:
        cases = {
            "whiteout": tar_entry("opt/insight/package/.wh.payload"),
            "symlink": tar_entry(
                "opt/insight/package/link", kind="symlink", linkname="/etc/passwd"
            ),
            "hardlink": tar_entry(
                "opt/insight/package/link",
                kind="hardlink",
                linkname=RUNNER_PATH,
            ),
            "device": tar_entry("opt/insight/package/dev", kind="character-device"),
            "fifo": tar_entry("opt/insight/package/fifo", kind="fifo"),
        }
        for name, entry in cases.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temp:
                fixture = ImageFixture(Path(temp), added_layers=[[entry]])
                self.assert_rejected(fixture, "package added layer")

    def test_rejects_added_suid_sgid_or_xattr(self) -> None:
        cases = {
            "suid": tar_entry(PACKAGE_PATH, data=b"x", mode=0o4555),
            "sgid": tar_entry(PACKAGE_PATH, data=b"x", mode=0o2555),
            "xattr": tar_entry(
                PACKAGE_PATH,
                data=b"x",
                xattrs={"user.example": b"present"},
            ),
        }
        for name, entry in cases.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temp:
                fixture = ImageFixture(Path(temp), added_layers=[[entry]])
                self.assert_rejected(fixture, "package added layer")

    def test_rejects_any_runtime_config_change(self) -> None:
        changed = runtime_config()
        changed["Env"] = [*changed["Env"], "INJECTED=true"]
        with tempfile.TemporaryDirectory() as temp:
            fixture = ImageFixture(Path(temp), package_runtime=changed)
            self.assert_rejected(fixture, "runtime config differs")

    def test_rejects_wrong_missing_or_additional_file_capabilities(self) -> None:
        cases: List[Tuple[str, Sequence[Mapping[str, Any]], str]] = [
            (
                "wrong",
                [
                    tar_entry(
                        RUNNER_PATH,
                        data=b"launcher",
                        xattrs={"security.capability": b"wrong"},
                    )
                ],
                "launcher capability",
            ),
            (
                "missing",
                [tar_entry(RUNNER_PATH, data=b"launcher")],
                "security.capability only",
            ),
            (
                "additional",
                [
                    tar_entry(
                        RUNNER_PATH,
                        data=b"launcher",
                        xattrs={"security.capability": EXPECTED_CAPABILITY},
                    ),
                    tar_entry(
                        "usr/bin/extra",
                        data=b"extra",
                        xattrs={"security.capability": EXPECTED_CAPABILITY},
                    ),
                ],
                "security.capability only",
            ),
        ]
        for name, entries, message in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temp:
                fixture = ImageFixture(Path(temp), runner_entries=entries)
                self.assert_rejected(fixture, message)

    def test_rejects_final_runner_suid_or_sgid(self) -> None:
        for mode in (0o4555, 0o2555):
            entries = [
                tar_entry(
                    RUNNER_PATH,
                    data=b"launcher",
                    xattrs={"security.capability": EXPECTED_CAPABILITY},
                ),
                tar_entry("usr/bin/unsafe", data=b"unsafe", mode=mode),
            ]
            with self.subTest(mode=oct(mode)), tempfile.TemporaryDirectory() as temp:
                fixture = ImageFixture(Path(temp), runner_entries=entries)
                self.assert_rejected(fixture, "final rootfs contains SUID/SGID")

    def test_rejects_unsupported_pax_xattr_encoding_fail_closed(self) -> None:
        entry = tar_entry(
            PACKAGE_PATH,
            data=b"x",
            pax={"LIBARCHIVE.xattr.user.example": "cHJlc2VudA=="},
        )
        with tempfile.TemporaryDirectory() as temp:
            fixture = ImageFixture(Path(temp), added_layers=[[entry]])
            self.assert_rejected(fixture, "rejected fail-closed")

    def test_rejects_global_pax_duplicate_paths_and_nonzero_tar_tail(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            fixture = ImageFixture(directory)
            global_layer = make_layer(
                [tar_entry(PACKAGE_PATH, data=b"x")],
                global_pax={"comment": "ambiguous"},
            )
            fixture.added_layers = [global_layer]
            fixture.package_digest = write_layout(
                fixture.package_archive, [fixture.runner_layer, global_layer]
            )
            self.assert_rejected(fixture, "metadata type 'g'")

        with tempfile.TemporaryDirectory() as temp:
            fixture = ImageFixture(
                Path(temp),
                added_layers=[
                    [
                        tar_entry(PACKAGE_PATH, data=b"first"),
                        tar_entry("./" + PACKAGE_PATH, data=b"second"),
                    ]
                ],
            )
            self.assert_rejected(fixture, "repeats canonical path")

        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            fixture = ImageFixture(directory)
            payload, media_type, _ = fixture.added_layers[0]
            tailed = payload + b"not-zero"
            tailed_layer = (tailed, media_type, digest(tailed))
            fixture.added_layers = [tailed_layer]
            fixture.package_digest = write_layout(
                fixture.package_archive, [fixture.runner_layer, tailed_layer]
            )
            self.assert_rejected(fixture, "non-zero data after its tar end marker")

    def test_rejects_corrupt_content_addressed_blob(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            fixture = ImageFixture(directory)
            layer_digest = digest(fixture.added_layers[0][0])
            fixture.package_digest = write_layout(
                fixture.package_archive,
                [fixture.runner_layer, *fixture.added_layers],
                corrupt_blob_digest=layer_digest,
            )
            self.assert_rejected(fixture, "descriptor size")


if __name__ == "__main__":
    unittest.main()
