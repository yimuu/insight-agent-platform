from __future__ import annotations

from dataclasses import asdict, replace
import argparse
import hashlib
import importlib.util
import io
import json
import os
import re
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest
from unittest import mock


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Cargo.toml").is_file())
SPEC = importlib.util.spec_from_file_location("native_image_contract", ROOT / "tools/release/native_image_contract.py")
CONTRACT = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = CONTRACT
SPEC.loader.exec_module(CONTRACT)
ASSEMBLY_SPEC = importlib.util.spec_from_file_location("native_assembly", ROOT / "tools/release/assemble-native-release-images.py")
ASSEMBLY = importlib.util.module_from_spec(ASSEMBLY_SPEC)
ASSEMBLY_SPEC.loader.exec_module(ASSEMBLY)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def sha(payload):
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def image_index(component, architecture):
    child = sha(f"{component}-{architecture}-image".encode())
    proof = sha(f"{component}-{architecture}-proof".encode())
    return {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [
            {"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": child,
             "size": 1234, "platform": {"os": "linux", "architecture": architecture}},
            {"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": proof,
             "size": 456, "platform": {"os": "unknown", "architecture": "unknown"},
             "annotations": {"vnd.docker.reference.type": "attestation-manifest",
                             "vnd.docker.reference.digest": child}},
        ],
    }


class NativeReleaseImageTests(unittest.TestCase):
    def setUp(self):
        self.identity = CONTRACT.BuildIdentity("owner/project", "a" * 40, "v1.2.3", 123, 2)

    def record(self, component="runtime", architecture="amd64", **changes):
        raw = encoded(image_index(component, architecture))
        suffix = "runtime" if component == "runtime" else "sandbox-runner"
        start, finish = (100, 130) if component == "runtime" else (150, 160)
        value = CONTRACT.NativeBuildRecord(
            1, self.identity, component, f"linux/{architecture}",
            f"ghcr.io/owner/project/platform-{suffix}", sha(raw), start, finish, raw.decode(),
        )
        return replace(value, **changes)

    def records(self):
        return [self.record(component, arch) for component in ("runtime", "sandbox_runner")
                for arch in ("amd64", "arm64")]

    def write_records(self, root, records=None):
        for record in records or self.records():
            (root / record.filename).write_bytes(record.encode(self.identity, 200))

    def test_exact_record_collection_round_trips(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.write_records(root)
            observed = CONTRACT.load_records(root, self.identity, 200)
            self.assertEqual(set(observed), set(self.records()))

    def test_foreign_commit_run_attempt_repo_and_tag_are_rejected(self):
        for field, value in (("repository", "other/project"), ("git_commit", "b" * 40),
                             ("release_tag", "v1.2.4"), ("run_id", 124), ("run_attempt", 1)):
            with self.subTest(field=field):
                record = self.record(identity=replace(self.identity, **{field: value}))
                with self.assertRaisesRegex(ValueError, "identity mismatch"):
                    record.validate(self.identity, 200)

    def test_record_schema_and_identity_are_closed_and_typed(self):
        raw = asdict(self.record())
        mutations = [
            {**raw, "extra": 1}, {key: value for key, value in raw.items() if key != "index_json"},
            {**raw, "schema_version": True}, {**raw, "schema_version": 2},
            {**raw, "component": "console"}, {**raw, "platform": "linux/386"},
            {**raw, "subject": "ghcr.io/other/project/platform-runtime"},
            {**raw, "identity": {**raw["identity"], "run_id": True}},
            {**raw, "identity": {**raw["identity"], "extra": 1}},
            {**raw, "started_epoch": True}, {**raw, "started_epoch": 1.5},
            {**raw, "started_epoch": -1}, {**raw, "started_epoch": 131},
            {**raw, "finished_epoch": 201},
        ]
        for value in mutations:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    CONTRACT.NativeBuildRecord.decode(encoded(value), self.identity, 200)
        for payload in (b'{"schema_version":1,"schema_version":1}',
                        b'{"schema_version":NaN}', b" " * (128 * 1024 + 1)):
            with self.assertRaises(ValueError):
                CONTRACT.NativeBuildRecord.decode(payload, self.identity, 200)

    def test_exact_index_bytes_are_hashed_before_semantic_validation(self):
        record = self.record()
        with self.assertRaisesRegex(ValueError, "raw index digest"):
            replace(record, index_json=record.index_json + "\n").validate(self.identity, 200)
        oversized = " " * (64 * 1024 + 1)
        with self.assertRaisesRegex(ValueError, "byte bound"):
            replace(record, index_json=oversized, index_digest=sha(oversized.encode())).validate(self.identity, 200)

    def test_foreign_platform_missing_and_misdirected_attestations_fail(self):
        for case in ("platform", "duplicate", "no-proof", "foreign-proof", "unknown", "variant", "size"):
            with self.subTest(case=case):
                value = image_index("runtime", "amd64")
                children = value["manifests"]
                if case == "platform": children[0]["platform"]["architecture"] = "arm64"
                if case == "duplicate": children.append(children[0])
                if case == "no-proof": children.pop()
                if case == "foreign-proof": children[1]["annotations"]["vnd.docker.reference.digest"] = "sha256:" + "f" * 64
                if case == "unknown": children[1]["annotations"]["vnd.docker.reference.type"] = "other"
                if case == "variant": children[0]["platform"]["variant"] = "v3"
                if case == "size": children[0]["size"] = True
                raw = encoded(value)
                with self.assertRaises(ValueError):
                    self.record(index_json=raw.decode(), index_digest=sha(raw)).validate(self.identity, 200)

    def test_non_string_and_invalid_unicode_annotations_are_rejected(self):
        for item in (True, 1, 1.0, None, [], {}, "\ud800"):
            with self.subTest(item=item):
                value = image_index("runtime", "amd64")
                value["manifests"][1]["annotations"]["example.org/source"] = item
                raw = encoded(value)
                with self.assertRaises(ValueError):
                    self.record(index_json=raw.decode(), index_digest=sha(raw)).validate(self.identity, 200)
        value = image_index("runtime", "amd64")
        value["manifests"][1]["unexpected"] = True
        raw = encoded(value)
        with self.assertRaisesRegex(ValueError, "descriptor field"):
            self.record(index_json=raw.decode(), index_digest=sha(raw)).validate(self.identity, 200)

    def test_input_bounds_apply_before_hashing_and_unbounded_directory_reads(self):
        with mock.patch.object(CONTRACT, "digest", side_effect=AssertionError("must not hash")):
            with self.assertRaisesRegex(ValueError, "byte bound"):
                CONTRACT.index_descriptors(b" " * (64 * 1024 + 1), "sha256:" + "a" * 64,
                                           frozenset({"linux/amd64"}))
        visited = []

        def entries():
            for number in range(6):
                visited.append(number)
                if number == 5:
                    raise AssertionError("must stop at the fifth entry")
                yield type("Entry", (), {"path": f"entry-{number}"})()

        with mock.patch.object(CONTRACT.os, "scandir") as scan:
            scan.return_value.__enter__.return_value = entries()
            with self.assertRaisesRegex(ValueError, "entry bound"):
                CONTRACT.load_records(Path("unused"), self.identity, 200)
        self.assertEqual(visited, [0, 1, 2, 3, 4])

    def test_collection_rejects_missing_extra_misnamed_and_symlinked_artifacts(self):
        for case in ("missing", "extra", "misnamed", "symlink"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                self.write_records(root)
                victim = root / "runtime-amd64.json"
                if case == "missing": victim.unlink()
                if case == "extra": (root / "old.json").write_text("{}")
                if case == "misnamed": victim.write_bytes((root / "runtime-arm64.json").read_bytes())
                if case == "symlink":
                    victim.unlink()
                    victim.symlink_to(root / "runtime-arm64.json")
                with self.assertRaises(ValueError):
                    CONTRACT.load_records(root, self.identity, 200)

    def test_runner_time_must_follow_its_runtime_build(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            records = self.records()
            records[-1] = replace(records[-1], started_epoch=120)
            self.write_records(root, records)
            with self.assertRaisesRegex(ValueError, "runner precedes"):
                CONTRACT.load_records(root, self.identity, 200)

    def test_console_record_binds_archive_identity_and_real_build_times(self):
        record = CONTRACT.ConsoleBuildRecord(1, self.identity, sha(b"archive"), 7, 100, 120)
        self.assertEqual(record.filename, "console-1.2.3.tar.gz")
        self.assertEqual(CONTRACT.ConsoleBuildRecord.decode(record.encode(self.identity, 200),
                                                            self.identity, 200), record)
        for changes in ({"identity": replace(self.identity, run_attempt=1)},
                        {"identity": replace(self.identity, git_commit="b" * 40)},
                        {"schema_version": True}, {"archive_bytes": True},
                        {"archive_bytes": 256 * 1024 * 1024 + 1},
                        {"archive_sha256": "unknown"}, {"started_epoch": 121},
                        {"finished_epoch": 201}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(record, **changes).validate(self.identity, 200)

    def test_merge_preserves_proof_descriptors_and_charges_all_waiting(self):
        records = [self.record(started_epoch=100, finished_epoch=130),
                   self.record(architecture="arm64", started_epoch=120, finished_epoch=190)]
        value = image_index("runtime", "amd64")
        value["manifests"].extend(image_index("runtime", "arm64")["manifests"])
        raw = encoded(value)
        elapsed = CONTRACT.verify_merged_index(records, self.identity, "runtime", raw, sha(raw), 250)
        # Longest individual build is 70 s; actual elapsed includes all 80 s of waiting.
        self.assertEqual(elapsed, 150)
        for case in ("lost-proof", "changed-proof", "changed-image", "different-size"):
            with self.subTest(case=case):
                changed = json.loads(raw)
                if case == "lost-proof": changed["manifests"].pop()
                if case == "changed-proof": changed["manifests"][1]["digest"] = "sha256:" + "f" * 64
                if case == "changed-image": changed["manifests"][0]["digest"] = "sha256:" + "e" * 64
                if case == "different-size": changed["manifests"][1]["size"] += 1
                payload = encoded(changed)
                with self.assertRaises(ValueError):
                    CONTRACT.verify_merged_index(records, self.identity, "runtime", payload, sha(payload), 250)
        with self.assertRaises(ValueError):
            CONTRACT.verify_merged_index(records, self.identity, "runtime", raw, sha(raw), 180)

    def test_assembly_validates_all_inputs_before_registry_mutation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.write_records(root)
            (root / "sandbox_runner-arm64.json").unlink()
            args = argparse.Namespace(input_directory=root, output_directory=root / "out", timing_directory=root)
            with mock.patch.object(ASSEMBLY, "run_bounded", side_effect=AssertionError("must not mutate")):
                with self.assertRaises(ValueError):
                    ASSEMBLY.merge_native(args, self.identity)
            self.assertFalse(args.output_directory.exists())

    def test_assembly_uses_only_exact_digests_and_records_verified_wall_time(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source.mkdir()
            self.write_records(source)
            args = argparse.Namespace(input_directory=source, output_directory=root / "out", timing_directory=root)
            calls = []
            merged = {}
            for component in ("runtime", "sandbox_runner"):
                value = image_index(component, "amd64")
                value["manifests"].extend(image_index(component, "arm64")["manifests"])
                merged[component] = encoded(value)

            def registry(argv):
                calls.append(argv)
                component = "sandbox_runner" if any("platform-sandbox-runner" in a for a in argv) else "runtime"
                raw = merged[component]
                if argv[3] == "create":
                    expected_sources = {f"{r.subject}@{r.index_digest}" for r in self.records() if r.component == component}
                    self.assertEqual(set(argv[-2:]), expected_sources)
                    Path(argv[argv.index("--metadata-file") + 1]).write_bytes(encoded({
                        "containerimage.descriptor": {"mediaType": "application/vnd.oci.image.index.v1+json",
                                                       "digest": sha(raw), "size": len(raw)},
                    }))
                    return b""
                self.assertEqual(argv[-1].split("@", 1)[1], sha(raw))
                return raw

            with mock.patch.object(ASSEMBLY, "run_bounded", side_effect=registry), mock.patch.object(ASSEMBLY.time, "time", return_value=250):
                ASSEMBLY.merge_native(args, self.identity)
            self.assertEqual(len(calls), 4)
            self.assertEqual((root / "runtime-start").read_text(), "100\n")
            self.assertEqual((root / "runner-start").read_text(), "150\n")
            self.assertEqual((root / "runtime-finish").read_text(), "250\n")
            self.assertEqual((root / "runner-finish").read_text(), "250\n")
            self.assertEqual((args.output_directory / "runtime-index.json").read_bytes(), merged["runtime"])

    def test_native_producer_reads_the_current_exact_registry_subject(self):
        with tempfile.TemporaryDirectory() as temporary:
            raw = encoded(image_index("runtime", "amd64"))
            args = argparse.Namespace(component="runtime", platform="linux/amd64", digest=sha(raw),
                                      started_epoch=100, finished_epoch=130, output_directory=Path(temporary))
            with mock.patch.object(ASSEMBLY, "run_bounded", return_value=raw) as command:
                ASSEMBLY.record_native(args, self.identity)
            self.assertEqual(command.call_args.args[0][-1], f"ghcr.io/owner/project/platform-runtime@{sha(raw)}")
            record = CONTRACT.NativeBuildRecord.decode((args.output_directory / "runtime-amd64.json").read_bytes(), self.identity, 200)
            self.assertEqual(record.index_json.encode(), raw)

    def test_console_copy_and_extraction_consume_the_bound_archive(self):
        for case in ("valid", "changed", "symlink", "old-attempt", "extra"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                source = root / "source"
                source.mkdir()
                archive = source / "console-1.2.3.tar.gz"
                with tarfile.open(archive, "w:gz") as output:
                    member = tarfile.TarInfo("index.html")
                    member.size = 5
                    if case == "symlink":
                        member.type = tarfile.SYMTYPE
                        member.linkname = "../outside"
                        output.addfile(member)
                    else:
                        output.addfile(member, io.BytesIO(b"hello"))
                size, digest = ASSEMBLY.file_binding(archive)
                identity = replace(self.identity, run_attempt=1) if case == "old-attempt" else self.identity
                record = CONTRACT.ConsoleBuildRecord(1, identity, digest, size, 100, 120)
                (source / "console-build.json").write_bytes(record.encode(identity, 200))
                if case == "changed": archive.write_bytes(b"different")
                if case == "extra": (source / "extra").write_text("old")
                args = argparse.Namespace(input_directory=source, output_directory=root / "dist",
                                          assets_directory=root / "assets", timing_directory=root)
                if case == "valid":
                    ASSEMBLY.prepare_console(args, self.identity)
                    self.assertEqual((args.output_directory / "index.html").read_bytes(), b"hello")
                    self.assertEqual(ASSEMBLY.file_binding(args.assets_directory / archive.name), (size, digest))
                    self.assertEqual((root / "console-build.time").read_text(), "100 120\n")
                else:
                    with self.assertRaises(ValueError):
                        ASSEMBLY.prepare_console(args, self.identity)
                    self.assertFalse(args.output_directory.exists())

    def test_process_output_and_elapsed_time_are_bounded(self):
        self.assertEqual(ASSEMBLY.run_bounded([sys.executable, "-c", "print('ready')"], timeout=5), b"ready\n")
        for script, message in (("import time; time.sleep(10)", "timed out"),
                                ("import sys; sys.stdout.buffer.write(b'x'*65537)", "output exceeded")):
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                ASSEMBLY.run_bounded([sys.executable, "-c", script], timeout=0.5)

    def test_cli_sigterm_reaps_its_owned_command(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            marker = root / "command.pid"
            docker = root / "docker"
            docker.write_text(f"#!{sys.executable}\nimport os, signal, time\n"
                              "from pathlib import Path\n"
                              "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
                              f"marker = Path({str(marker)!r})\n"
                              "pending = marker.with_suffix('.pending')\n"
                              "pending.write_text(str(os.getpid()))\n"
                              "pending.replace(marker)\n"
                              "time.sleep(30)\n")
            docker.chmod(0o700)
            environment = {**os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"],
                           "GITHUB_REF_TYPE": "tag", "GITHUB_REPOSITORY": "owner/project",
                           "GITHUB_SHA": "a" * 40, "GITHUB_REF_NAME": "v1.2.3",
                           "GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "2"}
            argv = [sys.executable, str(ROOT / "tools/release/assemble-native-release-images.py"),
                    "record-native", "--component", "runtime", "--platform", "linux/amd64",
                    "--digest", "sha256:" + "a" * 64, "--started-epoch", "100",
                    "--finished-epoch", "130", "--output-directory", str(root / "out")]
            owned_pid = None
            with subprocess.Popen(argv, env=environment, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE) as supervisor:
                try:
                    deadline = time.monotonic() + 5
                    while not marker.exists() and time.monotonic() < deadline:
                        self.assertIsNone(supervisor.poll(), "CLI exited before starting its command")
                        time.sleep(0.01)
                    self.assertTrue(marker.exists(), "command did not start within its bound")
                    owned_pid = int(marker.read_text())
                    supervisor.send_signal(signal.SIGTERM)
                    _, error = supervisor.communicate(timeout=5)
                    self.assertEqual(supervisor.returncode, 128 + signal.SIGTERM, error)
                    with self.assertRaises(ProcessLookupError):
                        os.kill(owned_pid, 0)
                    owned_pid = None
                    self.assertFalse((root / "out").exists())
                finally:
                    if supervisor.poll() is None:
                        supervisor.kill()
                        supervisor.wait(timeout=5)
                    if owned_pid is not None:
                        try:
                            os.killpg(owned_pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass

    def test_pipeline_rejects_native_identity_and_budget_bypasses(self):
        workflow = (ROOT / ".github/workflows/product-release.yml").read_text()
        cases = {
            "valid": workflow,
            "qemu": workflow.replace("  native-images:\n", "  native-images:\n    # docker/setup-qemu-action@\n"),
            "wrong-host": workflow.replace("runner: ubuntu-24.04-arm\n    steps:", "runner: ubuntu-24.04\n    steps:"),
            "missing-host-check": workflow.replace('          test "$(uname -m)" = "${{ matrix.machine }}"\n', ""),
            "missing-record": workflow.replace("assemble-native-release-images.py record-native", "echo ignored-record", 1),
            "foreign-attempt": workflow.replace("name: native-images-${{ github.sha }}-${{ github.run_id }}-${{ github.run_attempt }}-${{ matrix.arch }}", "name: native-images-old-${{ matrix.arch }}"),
            "missing-merge": workflow.replace("assemble-native-release-images.py merge-native", "echo ignored-merge"),
            "conditional-merge": workflow.replace("      - name: Merge and verify the exact native image subjects\n", "      - name: Merge and verify the exact native image subjects\n        if: false\n"),
            "lost-proof": workflow.replace("          sbom: true", "          sbom: false", 1),
            "lost-waiting": workflow.replace('"runtime_build_push", "duration_seconds": elapsed("runtime")', '"runtime_build_push", "duration_seconds": 0'),
            "runtime-budget": workflow,
            "console-run": workflow,
            "console-floating-base": workflow,
            "console-root": workflow,
            "console-arbitrary-files": workflow,
            "console-entrypoint": workflow,
        }
        files = ["Cargo.toml", "tools/checks/check-product-release.py", "tools/release/build-product-release.py",
                 "tools/development/build-development-profile-performance.py", "apps/console/scripts/build-agent-compiler.mjs",
                 "crates/authoring/platform-agent-compiler-wasm/Cargo.toml", ".github/workflows/ci.yml",
                 "deploy/images/console.Dockerfile", "deploy/release/performance-budgets-v1.json"]
        for name, changed in cases.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                for relative in files:
                    destination = root / relative
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(ROOT / relative, destination)
                (root / ".github/workflows/product-release.yml").write_text(changed)
                if name == "runtime-budget":
                    path = root / "deploy/release/performance-budgets-v1.json"
                    path.write_text(path.read_text().replace('"runtime_build_push": 3600', '"runtime_build_push": 3601'))
                if name == "console-run":
                    path = root / "deploy/images/console.Dockerfile"
                    path.write_text(path.read_text() + "RUN true\n")
                if name in {"console-floating-base", "console-root", "console-arbitrary-files", "console-entrypoint"}:
                    path = root / "deploy/images/console.Dockerfile"
                    dockerfile = path.read_text()
                    if name == "console-floating-base":
                        dockerfile = re.sub(r"@sha256:[0-9a-f]{64}", "", dockerfile)
                    elif name == "console-root":
                        dockerfile = dockerfile.replace("USER 1000:1000", "USER 0:0")
                    elif name == "console-arbitrary-files":
                        dockerfile = dockerfile.replace("COPY server/config.mjs server/gateway-server.mjs server/main.mjs server/process.mjs", "COPY server/")
                    else:
                        dockerfile = dockerfile.replace('/console/server/main.mjs"]', '/console/server/native.mjs"]')
                    path.write_text(dockerfile)
                result = subprocess.run([sys.executable, str(root / "tools/checks/check-product-release.py")],
                                        capture_output=True, timeout=5)
                self.assertEqual(result.returncode == 0, name == "valid", result.stderr)


if __name__ == "__main__":
    unittest.main()
