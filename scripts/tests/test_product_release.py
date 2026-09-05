from __future__ import annotations

import base64
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
PRODUCTIZATION_CHECKER = ROOT / "scripts/check-productization-scenario-reports.py"
PRODUCTIZATION_MANIFEST = json.loads(
    (ROOT / "examples/productization/scenarios.json").read_text(encoding="utf-8")
)
REVISION = subprocess.run(
    ["git", "rev-parse", "HEAD"],
    cwd=ROOT,
    check=True,
    capture_output=True,
    text=True,
).stdout.strip()
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
BASE_BUNDLE_METADATA = (
    "build-provenance.intoto.jsonl",
    "cli.spdx.json",
    "console.spdx.json",
    "development-profile-v1.json",
    "release-performance.json",
    "runtime.spdx.json",
    "sandbox-runner.spdx.json",
)
QUALIFIED_CANDIDATE_METADATA = "productization-qualified-release-candidate.json"


def digest(label: str) -> str:
    return "sha256:" + hashlib.sha256(label.encode()).hexdigest()


def canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def file_digest(path: pathlib.Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def framed_tree_digest(root: pathlib.Path) -> str:
    tree = hashlib.sha256()
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix().encode("utf-8")
        payload = path.read_bytes()
        tree.update(len(relative).to_bytes(8, "big"))
        tree.update(relative)
        tree.update(len(payload).to_bytes(8, "big"))
        tree.update(payload)
    return f"sha256:{tree.hexdigest()}"


class ProductReleaseTests(unittest.TestCase):
    def assert_checksum_closure(self, root: pathlib.Path) -> None:
        lines = (root / "checksums.txt").read_text(encoding="ascii").splitlines()
        observed: dict[str, str] = {}
        for line in lines:
            checksum, separator, name = line.partition("  ")
            self.assertEqual(separator, "  ")
            self.assertRegex(checksum, r"^[0-9a-f]{64}$")
            self.assertNotIn(name, observed)
            observed[name] = checksum
        expected_names = {
            path.name for path in root.iterdir() if path.name != "checksums.txt"
        }
        self.assertEqual(expected_names, set(observed))
        for name, checksum in observed.items():
            self.assertEqual(hashlib.sha256((root / name).read_bytes()).hexdigest(), checksum)

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
        image_subjects = {
            "runtime": "ghcr.io/example/insight/platform-runtime",
            "sandbox_runner": "ghcr.io/example/insight/platform-sandbox-runner",
            "console": "ghcr.io/example/insight/platform-console",
        }
        images = {
            name: {
                "subject": image_subjects[name],
                "index_digest": digest(f"index-{name}"),
                "platforms": {platform: digest(f"{name}-{platform}") for platform in ("linux/amd64", "linux/arm64")},
            }
            for name in ("console", "runtime", "sandbox_runner")
        }
        image_path = root / "images.json"
        image_path.write_text(json.dumps(images), encoding="utf-8")
        return image_path

    def run_generator(
        self,
        root: pathlib.Path,
        images: pathlib.Path,
        *,
        include_qualification: bool = False,
        include_productization: bool = False,
        productization_inputs: tuple[
            pathlib.Path,
            pathlib.Path,
            pathlib.Path,
            pathlib.Path,
            pathlib.Path,
        ]
        | None = None,
        output: pathlib.Path | None = None,
        revision: str = REVISION,
    ) -> subprocess.CompletedProcess[str]:
        output = output or root / "release-bundle.json"
        arguments = [
            "python3", str(SCRIPT), "--version", "1.2.3", "--git-commit", revision,
            "--created-at", "2026-09-01T00:00:00.000000Z", "--artifacts", str(root),
            "--images", str(images), "--output", str(output),
        ]
        if include_qualification:
            arguments.append("--include-development-qualification")
        if include_productization:
            arguments.append("--include-productization-qualification")
        if productization_inputs is not None:
            aggregate, reports, sandbox_evidence, sandbox_environment, candidate = (
                productization_inputs
            )
            arguments.extend(
                [
                    "--productization-aggregate",
                    str(aggregate),
                    "--productization-report-directory",
                    str(reports),
                    "--productization-sandbox-evidence",
                    str(sandbox_evidence),
                    "--productization-sandbox-environment",
                    str(sandbox_environment),
                    "--productization-release-candidate-bundle",
                    str(candidate),
                ]
            )
        return subprocess.run(arguments, cwd=ROOT, capture_output=True, text=True)

    def productization_fixture(
        self,
        root: pathlib.Path,
        images: pathlib.Path,
        *,
        bind_candidate: bool = True,
    ) -> tuple[pathlib.Path, pathlib.Path, pathlib.Path, pathlib.Path, pathlib.Path]:
        qualification = root.parent / "qualification"
        reports = qualification / "reports"
        reports.mkdir(parents=True)
        preliminary_result = self.run_generator(root, images)
        self.assertEqual(preliminary_result.returncode, 0, preliminary_result.stderr)
        candidate = qualification / "preliminary-release-bundle.json"
        candidate.write_bytes((root / "release-bundle.json").read_bytes())
        (root / "release-bundle.json").unlink()
        (root / "checksums.txt").unlink()
        candidate_bundle = json.loads(candidate.read_bytes())
        candidate_images = {}
        for image in candidate_bundle["images"]:
            platforms = {child["platform"]: child["digest"] for child in image["platforms"]}
            candidate_images[image["name"]] = {
                "subject": image["subject"],
                "index_digest": image["index_digest"],
                "platform": "linux/amd64",
                "platform_digest": platforms["linux/amd64"],
            }
        runtime = candidate_images["runtime"]
        runner = candidate_images["sandbox_runner"]
        platform_digest = (
            runtime["platform_digest"] if bind_candidate else digest("source-platform-image")
        )
        platform_repository = runtime["subject"] if bind_candidate else "insight-agent-platform"
        runner_digest = (
            runner["platform_digest"]
            if bind_candidate
            else digest("source-sandbox-runner-image")
        )
        runner_repository = (
            runner["subject"] if bind_candidate else "insight-platform-sandbox-runner"
        )
        sandbox_environment = qualification / "environment.json"
        environment = {
            "schema_version": 2,
            "kind": "insight.platform/kind-local-mechanics/v2",
            "production": False,
            "git_commit": REVISION,
            "platform_image_digest": platform_digest,
            "platform_image_repository": platform_repository,
            "sandbox_runner_image_digest": runner_digest,
            "sandbox_runner_image_repository": runner_repository,
            "deployment_config_digest": digest("deployment-config"),
            "generated_at": "2026-09-01T00:00:00.000000Z",
            "cluster_name": "productization-test",
            "kubeconfig": "/tmp/productization-test-kubeconfig",
        }
        if bind_candidate:
            environment["platform_image_identity"] = {
                "kind": "signed_release_candidate",
                "repository": environment["platform_image_repository"],
                "reference": f'{runtime["subject"]}@{runtime["platform_digest"]}',
                "config_digest": digest("runtime-config"),
                "index_digest": runtime["index_digest"],
                "platform": "linux/amd64",
                "platform_digest": runtime["platform_digest"],
            }
            environment["sandbox_runner_image_identity"] = {
                "kind": "signed_release_candidate",
                "repository": runner_repository,
                "reference": f'{runner_repository}@{runner["platform_digest"]}',
                "config_digest": digest("runner-config"),
                "index_digest": runner["index_digest"],
                "platform": "linux/amd64",
                "platform_digest": runner["platform_digest"],
            }
        else:
            environment["platform_image_identity"] = {
                "kind": "source_oci_manifest",
                "repository": environment["platform_image_repository"],
                "reference": f"insight-agent-platform@{platform_digest}",
                "config_digest": digest("source-runtime-config"),
                "index_digest": None,
                "platform": "linux/amd64",
                "platform_digest": platform_digest,
            }
            environment["sandbox_runner_image_identity"] = {
                "kind": "source_oci_manifest",
                "repository": runner_repository,
                "reference": f"{runner_repository}@{runner_digest}",
                "config_digest": digest("source-runner-config"),
                "index_digest": None,
                "platform": "linux/amd64",
                "platform_digest": runner_digest,
            }
        sandbox_environment.write_bytes(canonical(environment))
        sandbox_evidence = qualification / "productization-opensandbox-evidence.json"
        qualification_run_id = digest("qualification-run")
        evidence = {
            "schema_version": 1,
            "report_kind": "insight.productization.opensandbox-qualification/v1",
            "source_revision": REVISION,
            "qualification_run_id": qualification_run_id,
            "started_at": "2026-09-01T00:00:00Z",
            "finished_at": "2026-09-01T00:00:01Z",
            "environment": {
                "os": "linux",
                "architecture": "x86_64",
                "fresh_cluster": True,
                "cluster_name": environment["cluster_name"],
            },
            "runtime_contract_digest": digest("runtime-contract"),
            "package_image": f"insight-agent-platform@{digest('sandbox-package')}",
            "platform_image_digest": environment["platform_image_digest"],
            "sandbox_chart_digest": framed_tree_digest(
                ROOT / "deploy/helm/insight-platform-sandbox"
            ),
            "bootstrap_environment_digest": file_digest(sandbox_environment),
            "qualifier": "scripts/qualify-platform-sandbox-l3.sh",
            "release_candidate": (
                {
                    "release_bundle_digest": file_digest(candidate),
                    **candidate_images,
                }
                if bind_candidate
                else None
            ),
            "checks": [
                {"id": check_id, "status": "passed"}
                for check_id in (
                    "opensandbox_lifecycle",
                    "current_runtime_contract",
                    "direct_and_disabled_network",
                    "package_process_isolation",
                    "deadline_limit_enforced",
                    "dispatcher_recovery",
                )
            ],
            "status": "passed",
        }
        sandbox_evidence.write_bytes(canonical(evidence))
        profile_digest = digest("actual-profile")
        sandbox_evidence_digest = file_digest(sandbox_evidence)
        for scenario in PRODUCTIZATION_MANIFEST["scenarios"]:
            check = lambda check_id: {
                "id": check_id,
                "status": "passed",
                "evidence": f"closed evidence for {check_id}",
            }
            report = {
                "schema_version": 1,
                "report_kind": "insight.productization.scenario-report/v1",
                "scenario_id": scenario["id"],
                "contract_profile": "insight.platform/v1",
                "profile": scenario["profile"],
                "qualification_run_id": qualification_run_id,
                "actual_profile": "all",
                "profile_digest": profile_digest,
                "evidence_inputs": (
                    {"opensandbox_qualification": sandbox_evidence_digest}
                    if scenario["id"] == "sandbox-and-remote-framework-capability"
                    else {}
                ),
                "automation_layer": scenario["automation_layer"],
                "source_revision": REVISION,
                "environment": {
                    "os": "linux",
                    "architecture": "x86_64",
                    "fresh_profile": True,
                },
                "started_at": "2026-09-01T00:00:00Z",
                "finished_at": "2026-09-01T00:00:01Z",
                "status": "passed",
                "entrypoints": [check(item) for item in scenario["entrypoints"]],
                "assertions": [check(item) for item in scenario["assertions"]],
                "failure_probes": [check(item) for item in scenario["failure_probes"]],
            }
            (reports / f"{scenario['id']}.json").write_bytes(canonical(report))
        aggregate = qualification / "productization-10-of-10.json"
        generated = subprocess.run(
            [
                "python3",
                str(PRODUCTIZATION_CHECKER),
                str(reports),
                "--source-revision",
                REVISION,
                "--aggregate-output",
                str(aggregate),
                "--sandbox-evidence",
                str(sandbox_evidence),
                "--sandbox-environment",
                str(sandbox_environment),
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        self.assertEqual(generated.returncode, 0, generated.stderr)
        return aggregate, reports, sandbox_evidence, sandbox_environment, candidate

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
            self.assert_checksum_closure(root)

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

    def test_final_bundle_binds_development_and_productization_qualification(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary) / "artifacts"
            root.mkdir()
            images = self.fixture(root)
            productization = self.productization_fixture(root, images)
            development = root / "development-profile-performance.json"
            development.write_text('{"schema_version":1}\n', encoding="utf-8")
            result = self.run_generator(
                root,
                images,
                include_qualification=True,
                include_productization=True,
                productization_inputs=productization,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            metadata = {
                item["path"]: item
                for item in json.loads((root / "release-bundle.json").read_bytes())["metadata"]
            }
            report_names = [
                f"{scenario['id']}.json" for scenario in PRODUCTIZATION_MANIFEST["scenarios"]
            ]
            expected_order = [
                *BASE_BUNDLE_METADATA,
                "development-profile-performance.json",
                "productization-10-of-10.json",
                "productization-opensandbox-evidence.json",
                "productization-sandbox-environment.json",
                QUALIFIED_CANDIDATE_METADATA,
                *report_names,
            ]
            bundle = json.loads((root / "release-bundle.json").read_bytes())
            self.assertEqual(expected_order, [item["path"] for item in bundle["metadata"]])
            self.assertEqual(len(expected_order), len(metadata))
            for name in expected_order:
                self.assertEqual(file_digest(root / name), metadata[name]["sha256"])
            self.assertEqual(
                productization[4].read_bytes(),
                (root / QUALIFIED_CANDIDATE_METADATA).read_bytes(),
            )
            raw_evidence = json.loads(
                (root / "productization-opensandbox-evidence.json").read_bytes()
            )
            self.assertEqual(
                raw_evidence["release_candidate"]["release_bundle_digest"],
                metadata[QUALIFIED_CANDIDATE_METADATA]["sha256"],
            )
            self.assert_checksum_closure(root)

    def test_productization_evidence_requires_explicit_complete_opt_in(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary) / "artifacts"
            root.mkdir()
            images = self.fixture(root)
            productization = self.productization_fixture(root, images)
            without_opt_in = self.run_generator(
                root, images, productization_inputs=productization
            )
            self.assertNotEqual(without_opt_in.returncode, 0)
            self.assertIn("require --include-productization-qualification", without_opt_in.stderr)
            missing_inputs = self.run_generator(root, images, include_productization=True)
            self.assertNotEqual(missing_inputs.returncode, 0)
            self.assertIn("requires aggregate, report directory", missing_inputs.stderr)

    def test_productization_requires_the_exact_non_null_preliminary_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary) / "artifacts"
            root.mkdir()
            images = self.fixture(root)
            productization = self.productization_fixture(
                root, images, bind_candidate=False
            )
            result = self.run_generator(
                root,
                images,
                include_productization=True,
                productization_inputs=productization,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("non-null signed release candidate", result.stderr)

        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary) / "artifacts"
            root.mkdir()
            images = self.fixture(root)
            productization = self.productization_fixture(root, images)
            candidate = productization[4]
            value = json.loads(candidate.read_bytes())
            value["created_at"] = "2026-09-01T00:00:00.000001Z"
            candidate.write_bytes(canonical(value))
            result = self.run_generator(
                root,
                images,
                include_productization=True,
                productization_inputs=productization,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("release_bundle_digest differs", result.stderr)

    def test_every_qualified_image_must_match_the_current_release_images(self) -> None:
        mutations = (
            ("runtime", "subject"),
            ("sandbox_runner", "index_digest"),
            ("console", "platform_digest"),
        )
        for image_name, field in mutations:
            with self.subTest(image=image_name, field=field), tempfile.TemporaryDirectory() as temporary:
                root = pathlib.Path(temporary) / "artifacts"
                root.mkdir()
                images = self.fixture(root)
                productization = self.productization_fixture(root, images)
                value = json.loads(images.read_bytes())
                if field == "subject":
                    value[image_name]["subject"] = (
                        "ghcr.io/different/insight/platform-runtime"
                    )
                elif field == "index_digest":
                    value[image_name][field] = digest(f"wrong-{image_name}-index")
                else:
                    value[image_name]["platforms"]["linux/amd64"] = digest(
                        f"wrong-{image_name}-amd64"
                    )
                images.write_bytes(canonical(value))
                result = self.run_generator(
                    root,
                    images,
                    include_productization=True,
                    productization_inputs=productization,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(
                    f"productization {image_name} identity differs across evidence",
                    result.stderr,
                )

    def test_release_revision_must_equal_repository_head(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            images = self.fixture(root)
            result = self.run_generator(root, images, revision="b" * 40)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("must equal the current repository HEAD", result.stderr)

    def test_productization_aggregate_must_be_canonical_closed_passed_and_exact_revision(self) -> None:
        mutations = (
            ("noncanonical", lambda value: json.dumps(value, indent=2).encode()),
            ("not-closed", lambda value: canonical({**value, "unexpected": True})),
            ("not-passed", lambda value: canonical({**value, "status": "failed"})),
        )
        for label, mutate in mutations:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as temporary:
                root = pathlib.Path(temporary) / "artifacts"
                root.mkdir()
                images = self.fixture(root)
                productization = self.productization_fixture(root, images)
                aggregate = productization[0]
                aggregate.write_bytes(mutate(json.loads(aggregate.read_bytes())))
                result = self.run_generator(
                    root,
                    images,
                    include_productization=True,
                    productization_inputs=productization,
                )
                self.assertNotEqual(result.returncode, 0)
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary) / "artifacts"
            root.mkdir()
            images = self.fixture(root)
            productization = self.productization_fixture(root, images)
            aggregate = productization[0]
            value = json.loads(aggregate.read_bytes())
            value["source_revision"] = "b" * 40
            aggregate.write_bytes(canonical(value))
            result = self.run_generator(
                root,
                images,
                include_productization=True,
                productization_inputs=productization,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("differs from the strict scenario qualification authority", result.stderr)

    def test_productization_report_set_and_actual_file_digests_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary) / "artifacts"
            root.mkdir()
            images = self.fixture(root)
            productization = self.productization_fixture(root, images)
            missing = productization[1] / "deterministic-first-run.json"
            missing.unlink()
            result = self.run_generator(
                root,
                images,
                include_productization=True,
                productization_inputs=productization,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("exactly the ten canonical", result.stderr)
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary) / "artifacts"
            root.mkdir()
            images = self.fixture(root)
            productization = self.productization_fixture(root, images)
            report = productization[1] / "deterministic-first-run.json"
            report.write_text(json.dumps(json.loads(report.read_bytes()), indent=2), encoding="utf-8")
            result = self.run_generator(
                root,
                images,
                include_productization=True,
                productization_inputs=productization,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("canonical JSON bytes", result.stderr)

    def test_productization_raw_sandbox_inputs_are_bound(self) -> None:
        for input_index, expected_error in (
            (2, "OpenSandbox evidence summary"),
            (3, "bootstrap_environment_digest"),
        ):
            with self.subTest(input_index=input_index), tempfile.TemporaryDirectory() as temporary:
                root = pathlib.Path(temporary) / "artifacts"
                root.mkdir()
                images = self.fixture(root)
                productization = self.productization_fixture(root, images)
                path = productization[input_index]
                if input_index == 2:
                    value = json.loads(path.read_bytes())
                    value["status"] = "failed"
                    path.write_bytes(canonical(value))
                else:
                    path.write_bytes(path.read_bytes() + b"\n")
                result = self.run_generator(
                    root,
                    images,
                    include_productization=True,
                    productization_inputs=productization,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(expected_error, result.stderr)

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
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            images = self.fixture(root)
            target = TARGETS[0]
            binary = (root / f"insight-1.2.3-{target}").read_bytes()
            with tarfile.open(root / f"insight-1.2.3-{target}.tar.gz", "w:gz") as archive:
                for name, contents, mode in (
                    ("insight", binary, 0o755),
                    ("LICENSE", b"license\n", 0o644),
                    ("VERSION", b"1.2.3\n", 0o644),
                    ("EXTRA", b"not part of the release contract\n", 0o644),
                ):
                    info = tarfile.TarInfo(name)
                    info.size = len(contents)
                    info.mode = mode
                    info.mtime = 0
                    archive.addfile(info, io.BytesIO(contents))
            result = self.run_generator(root, images)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("must contain only insight, LICENSE, VERSION", result.stderr)

    def test_release_outputs_are_fresh_canonical_non_symlink_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            images = self.fixture(root)
            (root / "release-bundle.json").write_text("existing", encoding="utf-8")
            result = self.run_generator(root, images)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("must be fresh and non-symlink", result.stderr)

        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            images = self.fixture(root)
            result = self.run_generator(root, images, output=root / "cli.spdx.json")
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("canonical artifacts/release-bundle.json", result.stderr)

        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            images = self.fixture(root)
            outside = root.parent / f"{root.name}-outside-bundle"
            (root / "release-bundle.json").symlink_to(outside)
            result = self.run_generator(root, images)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(outside.exists())

        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            images = self.fixture(root)
            outside = root.parent / f"{root.name}-outside-checksums"
            (root / "checksums.txt").symlink_to(outside)
            result = self.run_generator(root, images)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(outside.exists())

        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            images = self.fixture(root)
            (root / "checksums.txt").write_text("stale\n", encoding="ascii")
            result = self.run_generator(root, images)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("must be fresh and non-symlink", result.stderr)

    def test_artifact_root_and_entries_reject_links_and_non_regular_files(self) -> None:
        for entry_kind in ("directory", "broken-link", "artifact-link", "unsafe-name"):
            with self.subTest(entry=entry_kind), tempfile.TemporaryDirectory() as temporary:
                root = pathlib.Path(temporary) / "artifacts"
                root.mkdir()
                images = self.fixture(root)
                if entry_kind == "directory":
                    (root / "unbound").mkdir()
                elif entry_kind == "broken-link":
                    (root / "unbound").symlink_to(root / "missing-target")
                else:
                    if entry_kind == "artifact-link":
                        outside = root.parent / "outside-metadata.json"
                        outside.write_text("{}\n", encoding="utf-8")
                        (root / "cli.spdx.json").unlink()
                        (root / "cli.spdx.json").symlink_to(outside)
                    else:
                        (root / "unsafe\nchecksum-name").write_text("payload", encoding="utf-8")
                result = self.run_generator(root, images)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("only regular, non-symlink files", result.stderr)

        with tempfile.TemporaryDirectory() as temporary:
            base = pathlib.Path(temporary)
            real_root = base / "real-artifacts"
            real_root.mkdir()
            images = self.fixture(real_root)
            linked_root = base / "linked-artifacts"
            linked_root.symlink_to(real_root, target_is_directory=True)
            result = self.run_generator(linked_root, images)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("real, non-symlink directory", result.stderr)

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
            signature_document = json.loads(signature.read_bytes())
            self.assertEqual("ed25519", signature_document["algorithm"])
            self.assertEqual(1, signature_document["schema_version"])
            expected_key_id = "sha256:" + hashlib.sha256(public_der.read_bytes()[-32:]).hexdigest()
            self.assertEqual(expected_key_id, signature_document["key_id"])
            encoded_signature = signature_document["signature"]
            decoded_signature = base64.urlsafe_b64decode(
                encoded_signature + "=" * (-len(encoded_signature) % 4)
            )
            self.assertEqual(64, len(decoded_signature))
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
