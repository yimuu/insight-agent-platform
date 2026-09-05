import json
import hashlib
from pathlib import Path
import subprocess
import tempfile
import unittest


def framed_tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix().encode("utf-8")
        payload = path.read_bytes()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return f"sha256:{digest.hexdigest()}"


ROOT = Path(__file__).resolve().parents[2]
CHECKER = ROOT / "scripts/check-productization-scenario-reports.py"
MANIFEST = json.loads(
    (ROOT / "examples/productization/scenarios.json").read_text(encoding="utf-8")
)
FIXTURE_DIRECTORY = ROOT / "tests/fixtures/productization-reports/incomplete"
REVISION = "0" * 40
QUALIFICATION_RUN_ID = "sha256:" + "1" * 64
PROFILE_DIGEST = "sha256:" + "2" * 64
PLATFORM_IMAGE_DIGEST = "sha256:" + "3" * 64
RUNTIME_CONTRACT_DIGEST = "sha256:" + "4" * 64
SANDBOX_RUNNER_IMAGE_DIGEST = "sha256:" + "8" * 64
SANDBOX_CHART_DIGEST = framed_tree_digest(
    ROOT / "deploy/helm/insight-platform-sandbox"
)
DEPLOYMENT_CONFIG_DIGEST = "sha256:" + "6" * 64
PACKAGE_IMAGE = "insight-agent-platform@sha256:" + "7" * 64


def canonical_bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()


def digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def run_checker(directory: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "python3",
            str(CHECKER),
            str(directory),
            *arguments,
            "--source-revision",
            REVISION,
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


class ProductizationScenarioReportTests(unittest.TestCase):
    def write_sandbox_evidence(self, directory: Path) -> tuple[Path, Path, str]:
        environment = {
            "schema_version": 2,
            "kind": "insight.platform/kind-local-mechanics/v2",
            "production": False,
            "git_commit": REVISION,
            "platform_image_digest": PLATFORM_IMAGE_DIGEST,
            "platform_image_repository": "insight-agent-platform",
            "platform_image_identity": {
                "kind": "source_oci_manifest",
                "repository": "insight-agent-platform",
                "reference": f"insight-agent-platform@{PLATFORM_IMAGE_DIGEST}",
                "config_digest": "sha256:" + "5" * 64,
                "index_digest": None,
                "platform": "linux/amd64",
                "platform_digest": PLATFORM_IMAGE_DIGEST,
            },
            "sandbox_runner_image_digest": SANDBOX_RUNNER_IMAGE_DIGEST,
            "sandbox_runner_image_repository": "insight-platform-sandbox-runner",
            "sandbox_runner_image_identity": {
                "kind": "source_oci_manifest",
                "repository": "insight-platform-sandbox-runner",
                "reference": (
                    "insight-platform-sandbox-runner@"
                    f"{SANDBOX_RUNNER_IMAGE_DIGEST}"
                ),
                "config_digest": "sha256:" + "9" * 64,
                "index_digest": None,
                "platform": "linux/amd64",
                "platform_digest": SANDBOX_RUNNER_IMAGE_DIGEST,
            },
            "deployment_config_digest": DEPLOYMENT_CONFIG_DIGEST,
            "generated_at": "2026-09-05T00:00:00Z",
            "cluster_name": "productization-fixture",
            "kubeconfig": "/tmp/productization-fixture-kubeconfig",
        }
        environment_payload = json.dumps(environment, indent=2).encode()
        environment_path = directory / "environment.json"
        environment_path.write_bytes(environment_payload)
        evidence = {
            "schema_version": 1,
            "report_kind": "insight.productization.opensandbox-qualification/v1",
            "source_revision": REVISION,
            "qualification_run_id": QUALIFICATION_RUN_ID,
            "started_at": "2026-09-05T00:00:00Z",
            "finished_at": "2026-09-05T00:00:01Z",
            "environment": {
                "os": "linux",
                "architecture": "x86_64",
                "fresh_cluster": True,
                "cluster_name": "productization-fixture",
            },
            "runtime_contract_digest": RUNTIME_CONTRACT_DIGEST,
            "package_image": PACKAGE_IMAGE,
            "platform_image_digest": PLATFORM_IMAGE_DIGEST,
            "sandbox_chart_digest": SANDBOX_CHART_DIGEST,
            "bootstrap_environment_digest": digest(environment_payload),
            "qualifier": "scripts/qualify-platform-sandbox-l3.sh",
            "release_candidate": None,
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
        evidence_payload = canonical_bytes(evidence)
        evidence_path = directory / "productization-opensandbox-evidence.json"
        evidence_path.write_bytes(evidence_payload)
        return evidence_path, environment_path, digest(evidence_payload)

    def write_complete_reports(self, directory: Path, sandbox_digest: str) -> None:
        for scenario in MANIFEST["scenarios"]:
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
                "qualification_run_id": QUALIFICATION_RUN_ID,
                "actual_profile": "all",
                "profile_digest": PROFILE_DIGEST,
                "evidence_inputs": (
                    {"opensandbox_qualification": sandbox_digest}
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
                "started_at": "2026-09-05T00:00:00Z",
                "finished_at": "2026-09-05T00:00:01Z",
                "status": "passed",
                "entrypoints": [check(item) for item in scenario["entrypoints"]],
                "assertions": [check(item) for item in scenario["assertions"]],
                "failure_probes": [check(item) for item in scenario["failure_probes"]],
            }
            (directory / f"{scenario['id']}.json").write_bytes(canonical_bytes(report))

    def run_with_mutated_sandbox_environment(
        self,
        root: Path,
        mutate_environment,
        mutate_evidence=lambda _evidence: None,
    ) -> subprocess.CompletedProcess[str]:
        reports = root / "reports"
        reports.mkdir()
        evidence_path, environment_path, _ = self.write_sandbox_evidence(root)
        environment = json.loads(environment_path.read_bytes())
        mutate_environment(environment)
        environment_payload = json.dumps(environment, indent=2).encode()
        environment_path.write_bytes(environment_payload)
        evidence = json.loads(evidence_path.read_bytes())
        evidence["bootstrap_environment_digest"] = digest(environment_payload)
        mutate_evidence(evidence)
        evidence_payload = canonical_bytes(evidence)
        evidence_path.write_bytes(evidence_payload)
        self.write_complete_reports(reports, digest(evidence_payload))
        return run_checker(
            reports,
            "--sandbox-evidence",
            str(evidence_path),
            "--sandbox-environment",
            str(environment_path),
        )

    def test_allow_incomplete_accepts_a_closed_partial_report(self) -> None:
        result = run_checker(FIXTURE_DIRECTORY, "--allow-incomplete")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("complete_gate=false", result.stdout)

    def test_complete_gate_rejects_a_partial_report_set(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            evidence, environment, _ = self.write_sandbox_evidence(Path(directory))
            result = run_checker(
                FIXTURE_DIRECTORY,
                "--sandbox-evidence",
                str(evidence),
                "--sandbox-environment",
                str(environment),
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("exactly the ten canonical", result.stderr)

    def test_passed_status_cannot_hide_a_required_not_run_check(self) -> None:
        source = FIXTURE_DIRECTORY / "deterministic-first-run.json"
        report = json.loads(source.read_text(encoding="utf-8"))
        report["status"] = "passed"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / source.name
            path.write_text(json.dumps(report), encoding="utf-8")
            result = run_checker(Path(directory), "--allow-incomplete")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("status=passed must exactly match", result.stderr)

    def test_duplicate_report_key_is_rejected(self) -> None:
        source = FIXTURE_DIRECTORY / "deterministic-first-run.json"
        payload = source.read_text(encoding="utf-8").replace(
            '  "schema_version": 1,',
            '  "schema_version": 1,\n  "schema_version": 1,',
            1,
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / source.name
            path.write_text(payload, encoding="utf-8")
            result = run_checker(Path(directory), "--allow-incomplete")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("duplicate object key", result.stderr)

    def test_strict_gate_writes_exact_revision_ten_of_ten_aggregate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = root / "reports"
            reports.mkdir()
            evidence, environment, sandbox_digest = self.write_sandbox_evidence(root)
            self.write_complete_reports(reports, sandbox_digest)
            aggregate = root / "aggregate.json"
            result = run_checker(
                reports,
                "--aggregate-output",
                str(aggregate),
                "--sandbox-evidence",
                str(evidence),
                "--sandbox-environment",
                str(environment),
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            evidence = json.loads(aggregate.read_text(encoding="utf-8"))
        self.assertEqual(evidence["report_kind"], "insight.productization.scenario-aggregate/v1")
        self.assertEqual(evidence["source_revision"], REVISION)
        self.assertEqual(evidence["scenario_count"], 10)
        self.assertEqual(evidence["status"], "passed")
        self.assertEqual(evidence["qualification_run_id"], QUALIFICATION_RUN_ID)
        self.assertEqual(evidence["actual_profile"], "all")
        self.assertEqual(evidence["profile_digest"], PROFILE_DIGEST)
        self.assertEqual(
            [item["scenario_id"] for item in evidence["reports"]],
            [item["id"] for item in MANIFEST["scenarios"]],
        )
        self.assertTrue(
            all(item["report_digest"].startswith("sha256:") for item in evidence["reports"])
        )

    def test_partial_gate_cannot_write_an_aggregate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            aggregate = Path(directory) / "aggregate.json"
            result = run_checker(
                FIXTURE_DIRECTORY,
                "--allow-incomplete",
                "--aggregate-output",
                str(aggregate),
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("only valid for the strict complete gate", result.stderr)
            self.assertFalse(aggregate.exists())

    def test_strict_gate_requires_raw_sandbox_evidence_pair(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = root / "reports"
            reports.mkdir()
            _, _, sandbox_digest = self.write_sandbox_evidence(root)
            self.write_complete_reports(reports, sandbox_digest)
            result = run_checker(reports)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("strict 10/10 gate requires", result.stderr)

    def test_strict_gate_rejects_reports_from_multiple_runs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = root / "reports"
            reports.mkdir()
            evidence, environment, sandbox_digest = self.write_sandbox_evidence(root)
            self.write_complete_reports(reports, sandbox_digest)
            path = reports / "deterministic-first-run.json"
            report = json.loads(path.read_bytes())
            report["qualification_run_id"] = "sha256:" + "9" * 64
            path.write_bytes(canonical_bytes(report))
            result = run_checker(
                reports,
                "--sandbox-evidence",
                str(evidence),
                "--sandbox-environment",
                str(environment),
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("one qualification_run_id", result.stderr)

    def test_strict_gate_rejects_raw_qualification_run_id_from_another_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = root / "reports"
            reports.mkdir()
            evidence_path, environment, _ = self.write_sandbox_evidence(root)
            evidence = json.loads(evidence_path.read_bytes())
            evidence["qualification_run_id"] = "sha256:" + "8" * 64
            evidence_payload = canonical_bytes(evidence)
            evidence_path.write_bytes(evidence_payload)
            self.write_complete_reports(reports, digest(evidence_payload))
            result = run_checker(
                reports,
                "--sandbox-evidence",
                str(evidence_path),
                "--sandbox-environment",
                str(environment),
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("qualification_run_id differs", result.stderr)

    def test_strict_gate_rejects_report_window_that_does_not_cover_raw_l3(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = root / "reports"
            reports.mkdir()
            evidence_path, environment, _ = self.write_sandbox_evidence(root)
            evidence = json.loads(evidence_path.read_bytes())
            evidence["finished_at"] = "2026-09-05T00:00:02Z"
            evidence_payload = canonical_bytes(evidence)
            evidence_path.write_bytes(evidence_payload)
            self.write_complete_reports(reports, digest(evidence_payload))
            result = run_checker(
                reports,
                "--sandbox-evidence",
                str(evidence_path),
                "--sandbox-environment",
                str(environment),
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("finished_at does not cover", result.stderr)

    def test_strict_gate_rejects_report_start_after_raw_l3_start(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = root / "reports"
            reports.mkdir()
            evidence_path, environment_path, _ = self.write_sandbox_evidence(root)
            environment = json.loads(environment_path.read_bytes())
            environment["generated_at"] = "2026-09-04T23:59:58Z"
            environment_payload = json.dumps(environment, indent=2).encode()
            environment_path.write_bytes(environment_payload)
            evidence = json.loads(evidence_path.read_bytes())
            evidence["started_at"] = "2026-09-04T23:59:59Z"
            evidence["bootstrap_environment_digest"] = digest(environment_payload)
            evidence_payload = canonical_bytes(evidence)
            evidence_path.write_bytes(evidence_payload)
            self.write_complete_reports(reports, digest(evidence_payload))
            result = run_checker(
                reports,
                "--sandbox-evidence",
                str(evidence_path),
                "--sandbox-environment",
                str(environment_path),
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("started_at does not cover", result.stderr)

    def test_strict_gate_rejects_raw_l3_outside_bootstrap_window(self) -> None:
        cases = (
            ("2026-09-05T00:00:01Z", "before its bootstrap environment exists"),
            ("2026-09-04T20:59:59Z", "outside the three-hour bootstrap window"),
        )
        for generated_at, expected_error in cases:
            with self.subTest(generated_at=generated_at), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                reports = root / "reports"
                reports.mkdir()
                evidence_path, environment_path, _ = self.write_sandbox_evidence(root)
                environment = json.loads(environment_path.read_bytes())
                environment["generated_at"] = generated_at
                environment_payload = json.dumps(environment, indent=2).encode()
                environment_path.write_bytes(environment_payload)
                evidence = json.loads(evidence_path.read_bytes())
                evidence["bootstrap_environment_digest"] = digest(environment_payload)
                evidence_payload = canonical_bytes(evidence)
                evidence_path.write_bytes(evidence_payload)
                self.write_complete_reports(reports, digest(evidence_payload))
                result = run_checker(
                    reports,
                    "--sandbox-evidence",
                    str(evidence_path),
                    "--sandbox-environment",
                    str(environment_path),
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(expected_error, result.stderr)

    def test_strict_gate_rejects_wrong_checked_chart_digest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = root / "reports"
            reports.mkdir()
            evidence_path, environment, _ = self.write_sandbox_evidence(root)
            evidence = json.loads(evidence_path.read_bytes())
            evidence["sandbox_chart_digest"] = "sha256:" + "5" * 64
            evidence_payload = canonical_bytes(evidence)
            evidence_path.write_bytes(evidence_payload)
            self.write_complete_reports(reports, digest(evidence_payload))
            result = run_checker(
                reports,
                "--sandbox-evidence",
                str(evidence_path),
                "--sandbox-environment",
                str(environment),
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("current checked chart", result.stderr)

    def test_strict_gate_rejects_symlinked_report(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = root / "reports"
            reports.mkdir()
            evidence, environment, sandbox_digest = self.write_sandbox_evidence(root)
            self.write_complete_reports(reports, sandbox_digest)
            report = reports / "deterministic-first-run.json"
            target = root / "linked-report.json"
            target.write_bytes(report.read_bytes())
            report.unlink()
            report.symlink_to(target)
            result = run_checker(
                reports,
                "--sandbox-evidence",
                str(evidence),
                "--sandbox-environment",
                str(environment),
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("non-symlink", result.stderr)

    def test_strict_gate_rejects_symlinked_report_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = root / "reports"
            reports.mkdir()
            evidence, environment, sandbox_digest = self.write_sandbox_evidence(root)
            self.write_complete_reports(reports, sandbox_digest)
            linked_reports = root / "linked-reports"
            linked_reports.symlink_to(reports, target_is_directory=True)
            result = run_checker(
                linked_reports,
                "--sandbox-evidence",
                str(evidence),
                "--sandbox-environment",
                str(environment),
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("real directory", result.stderr)

    def test_strict_gate_rejects_broken_report_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = root / "reports"
            reports.mkdir()
            evidence, environment, sandbox_digest = self.write_sandbox_evidence(root)
            self.write_complete_reports(reports, sandbox_digest)
            report = reports / "deterministic-first-run.json"
            report.unlink()
            report.symlink_to(root / "missing-report.json")
            result = run_checker(
                reports,
                "--sandbox-evidence",
                str(evidence),
                "--sandbox-environment",
                str(environment),
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("non-symlink", result.stderr)

    def test_strict_gate_rejects_nested_or_extra_entries(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = root / "reports"
            reports.mkdir()
            evidence, environment, sandbox_digest = self.write_sandbox_evidence(root)
            self.write_complete_reports(reports, sandbox_digest)
            nested = reports / "nested"
            nested.mkdir()
            (nested / "extra.json").write_text("{}", encoding="utf-8")
            result = run_checker(
                reports,
                "--sandbox-evidence",
                str(evidence),
                "--sandbox-environment",
                str(environment),
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("exactly the ten canonical", result.stderr)

    def test_strict_gate_rejects_an_extra_top_level_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = root / "reports"
            reports.mkdir()
            evidence, environment, sandbox_digest = self.write_sandbox_evidence(root)
            self.write_complete_reports(reports, sandbox_digest)
            (reports / "notes.txt").write_text("not evidence", encoding="utf-8")
            result = run_checker(
                reports,
                "--sandbox-evidence",
                str(evidence),
                "--sandbox-environment",
                str(environment),
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("exactly the ten canonical", result.stderr)

    def test_strict_gate_rejects_non_all_actual_profile(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = root / "reports"
            reports.mkdir()
            evidence, environment, sandbox_digest = self.write_sandbox_evidence(root)
            self.write_complete_reports(reports, sandbox_digest)
            path = reports / "deterministic-first-run.json"
            report = json.loads(path.read_bytes())
            report["actual_profile"] = "starter"
            path.write_bytes(canonical_bytes(report))
            result = run_checker(
                reports,
                "--sandbox-evidence",
                str(evidence),
                "--sandbox-environment",
                str(environment),
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("actual_profile=all", result.stderr)

    def test_strict_gate_rejects_noncanonical_raw_sandbox_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = root / "reports"
            reports.mkdir()
            evidence, environment, sandbox_digest = self.write_sandbox_evidence(root)
            self.write_complete_reports(reports, sandbox_digest)
            evidence.write_bytes(json.dumps(json.loads(evidence.read_bytes()), indent=2).encode())
            result = run_checker(
                reports,
                "--sandbox-evidence",
                str(evidence),
                "--sandbox-environment",
                str(environment),
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("canonical JSON bytes", result.stderr)

    def test_strict_gate_rejects_noncanonical_scenario_report(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = root / "reports"
            reports.mkdir()
            evidence, environment, sandbox_digest = self.write_sandbox_evidence(root)
            self.write_complete_reports(reports, sandbox_digest)
            path = reports / "deterministic-first-run.json"
            path.write_bytes(json.dumps(json.loads(path.read_bytes()), indent=2).encode())
            result = run_checker(
                reports,
                "--sandbox-evidence",
                str(evidence),
                "--sandbox-environment",
                str(environment),
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("qualification reports must use canonical JSON bytes", result.stderr)

    def test_strict_gate_rejects_environment_bytes_not_bound_by_raw_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = root / "reports"
            reports.mkdir()
            evidence, environment, sandbox_digest = self.write_sandbox_evidence(root)
            self.write_complete_reports(reports, sandbox_digest)
            environment.write_bytes(environment.read_bytes() + b"\n")
            result = run_checker(
                reports,
                "--sandbox-evidence",
                str(evidence),
                "--sandbox-environment",
                str(environment),
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("environment file bytes", result.stderr)

    def test_strict_gate_accepts_closed_signed_candidate_image_binding(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = root / "reports"
            reports.mkdir()
            evidence_path, environment_path, _ = self.write_sandbox_evidence(root)
            environment = json.loads(environment_path.read_bytes())
            components = {
                name: {
                    "subject": f"ghcr.io/example/insight/{suffix}",
                    "index_digest": "sha256:" + index_digit * 64,
                    "platform": "linux/amd64",
                    "platform_digest": "sha256:" + platform_digit * 64,
                }
                for name, suffix, index_digit, platform_digit in (
                    ("runtime", "platform-runtime", "8", "9"),
                    ("sandbox_runner", "platform-sandbox-runner", "a", "b"),
                    ("console", "platform-console", "c", "d"),
                )
            }
            runtime = components["runtime"]
            environment["platform_image_digest"] = runtime["platform_digest"]
            environment["platform_image_repository"] = runtime["subject"]
            environment["platform_image_identity"] = {
                "kind": "signed_release_candidate",
                "repository": runtime["subject"],
                "reference": f'{runtime["subject"]}@{runtime["platform_digest"]}',
                "config_digest": "sha256:" + "e" * 64,
                "index_digest": runtime["index_digest"],
                "platform": runtime["platform"],
                "platform_digest": runtime["platform_digest"],
            }
            runner = components["sandbox_runner"]
            environment["sandbox_runner_image_digest"] = runner["platform_digest"]
            environment["sandbox_runner_image_repository"] = runner["subject"]
            environment["sandbox_runner_image_identity"] = {
                "kind": "signed_release_candidate",
                "repository": runner["subject"],
                "reference": f'{runner["subject"]}@{runner["platform_digest"]}',
                "config_digest": "sha256:" + "0" * 64,
                "index_digest": runner["index_digest"],
                "platform": runner["platform"],
                "platform_digest": runner["platform_digest"],
            }
            environment_payload = json.dumps(environment, indent=2).encode()
            environment_path.write_bytes(environment_payload)
            evidence = json.loads(evidence_path.read_bytes())
            evidence["platform_image_digest"] = runtime["platform_digest"]
            evidence["bootstrap_environment_digest"] = digest(environment_payload)
            evidence["release_candidate"] = {
                "release_bundle_digest": "sha256:" + "f" * 64,
                **components,
            }
            evidence_payload = canonical_bytes(evidence)
            evidence_path.write_bytes(evidence_payload)
            self.write_complete_reports(reports, digest(evidence_payload))
            result = run_checker(
                reports,
                "--sandbox-evidence",
                str(evidence_path),
                "--sandbox-environment",
                str(environment_path),
            )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_strict_gate_rejects_legacy_local_image_config_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_with_mutated_sandbox_environment(
                Path(directory),
                lambda environment: environment["platform_image_identity"].update(
                    {"kind": "local_image_config"}
                ),
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("runtime image identity kind is invalid", result.stderr)

    def test_strict_gate_rejects_inexact_source_oci_manifest_identity(self) -> None:
        cases = (
            ({"index_digest": "sha256:" + "8" * 64}, "source OCI runtime image identity"),
            ({"platform_digest": "sha256:" + "8" * 64}, "source OCI runtime image identity"),
            ({"reference": "insight-agent-platform:mutable"}, "source OCI runtime image identity"),
            ({"config_digest": "not-a-digest"}, "runtime config digest"),
            ({"platform": "linux/s390x"}, "runtime image architecture is unsupported"),
        )
        for update, expected_error in cases:
            with self.subTest(update=update), tempfile.TemporaryDirectory() as directory:
                result = self.run_with_mutated_sandbox_environment(
                    Path(directory),
                    lambda environment, update=update: environment[
                        "platform_image_identity"
                    ].update(update),
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(expected_error, result.stderr)

    def test_strict_gate_rejects_inexact_source_sandbox_runner_identity(self) -> None:
        cases = (
            (
                {"index_digest": "sha256:" + "a" * 64},
                "source OCI Sandbox runner image identity",
            ),
            (
                {"platform_digest": "sha256:" + "a" * 64},
                "source OCI Sandbox runner image identity",
            ),
            (
                {"reference": "insight-platform-sandbox-runner:mutable"},
                "source OCI Sandbox runner image identity",
            ),
            ({"config_digest": "not-a-digest"}, "Sandbox runner config digest"),
            (
                {"platform": "linux/s390x"},
                "Sandbox runner image architecture is unsupported",
            ),
        )
        for update, expected_error in cases:
            with self.subTest(update=update), tempfile.TemporaryDirectory() as directory:
                result = self.run_with_mutated_sandbox_environment(
                    Path(directory),
                    lambda environment, update=update: environment[
                        "sandbox_runner_image_identity"
                    ].update(update),
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(expected_error, result.stderr)

    def test_strict_gate_rejects_candidate_sandbox_runner_identity_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = root / "reports"
            reports.mkdir()
            evidence_path, environment_path, _ = self.write_sandbox_evidence(root)
            environment = json.loads(environment_path.read_bytes())
            components = {
                name: {
                    "subject": f"ghcr.io/example/insight/{suffix}",
                    "index_digest": "sha256:" + index_digit * 64,
                    "platform": "linux/amd64",
                    "platform_digest": "sha256:" + platform_digit * 64,
                }
                for name, suffix, index_digit, platform_digit in (
                    ("runtime", "platform-runtime", "8", "9"),
                    ("sandbox_runner", "platform-sandbox-runner", "a", "b"),
                    ("console", "platform-console", "c", "d"),
                )
            }
            runtime = components["runtime"]
            environment["platform_image_digest"] = runtime["platform_digest"]
            environment["platform_image_repository"] = runtime["subject"]
            environment["platform_image_identity"] = {
                "kind": "signed_release_candidate",
                "repository": runtime["subject"],
                "reference": f'{runtime["subject"]}@{runtime["platform_digest"]}',
                "config_digest": "sha256:" + "e" * 64,
                "index_digest": runtime["index_digest"],
                "platform": runtime["platform"],
                "platform_digest": runtime["platform_digest"],
            }
            runner = components["sandbox_runner"]
            environment["sandbox_runner_image_digest"] = runner["platform_digest"]
            environment["sandbox_runner_image_repository"] = runner["subject"]
            environment["sandbox_runner_image_identity"] = {
                "kind": "signed_release_candidate",
                "repository": runner["subject"],
                "reference": f'{runner["subject"]}@{runner["platform_digest"]}',
                "config_digest": "sha256:" + "0" * 64,
                "index_digest": runner["index_digest"],
                "platform": runner["platform"],
                "platform_digest": runner["platform_digest"],
            }
            environment_payload = json.dumps(environment, indent=2).encode()
            environment_path.write_bytes(environment_payload)
            evidence = json.loads(evidence_path.read_bytes())
            evidence["platform_image_digest"] = runtime["platform_digest"]
            evidence["bootstrap_environment_digest"] = digest(environment_payload)
            evidence["release_candidate"] = {
                "release_bundle_digest": "sha256:" + "f" * 64,
                **components,
            }
            evidence["release_candidate"]["sandbox_runner"]["index_digest"] = (
                "sha256:" + "1" * 64
            )
            evidence_payload = canonical_bytes(evidence)
            evidence_path.write_bytes(evidence_payload)
            self.write_complete_reports(reports, digest(evidence_payload))
            result = run_checker(
                reports,
                "--sandbox-evidence",
                str(evidence_path),
                "--sandbox-environment",
                str(environment_path),
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "bootstrap Sandbox runner image differs from the signed release candidate",
            result.stderr,
        )

    def test_strict_gate_rejects_release_candidate_bound_to_source_manifest(self) -> None:
        components = {
            name: {
                "subject": f"ghcr.io/example/insight/{suffix}",
                "index_digest": "sha256:" + index_digit * 64,
                "platform": "linux/amd64",
                "platform_digest": "sha256:" + platform_digit * 64,
            }
            for name, suffix, index_digit, platform_digit in (
                ("runtime", "platform-runtime", "8", "9"),
                ("sandbox_runner", "platform-sandbox-runner", "a", "b"),
                ("console", "platform-console", "c", "d"),
            )
        }
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_with_mutated_sandbox_environment(
                Path(directory),
                lambda _environment: None,
                lambda evidence: evidence.update(
                    {
                        "release_candidate": {
                            "release_bundle_digest": "sha256:" + "f" * 64,
                            **components,
                        }
                    }
                ),
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("cannot bind a source OCI image identity", result.stderr)

    def test_strict_gate_rejects_unbound_sandbox_report_digest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = root / "reports"
            reports.mkdir()
            evidence, environment, sandbox_digest = self.write_sandbox_evidence(root)
            self.write_complete_reports(reports, sandbox_digest)
            path = reports / "sandbox-and-remote-framework-capability.json"
            report = json.loads(path.read_bytes())
            report["evidence_inputs"]["opensandbox_qualification"] = "sha256:" + "0" * 64
            path.write_bytes(canonical_bytes(report))
            result = run_checker(
                reports,
                "--sandbox-evidence",
                str(evidence),
                "--sandbox-environment",
                str(environment),
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("differs from the supplied raw evidence", result.stderr)

    def test_strict_gate_rejects_multiple_runtime_profile_digests(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = root / "reports"
            reports.mkdir()
            evidence, environment, sandbox_digest = self.write_sandbox_evidence(root)
            self.write_complete_reports(reports, sandbox_digest)
            path = reports / "deterministic-first-run.json"
            report = json.loads(path.read_bytes())
            report["profile_digest"] = "sha256:" + "9" * 64
            path.write_bytes(canonical_bytes(report))
            result = run_checker(
                reports,
                "--sandbox-evidence",
                str(evidence),
                "--sandbox-environment",
                str(environment),
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("one exact runtime profile digest", result.stderr)


if __name__ == "__main__":
    unittest.main()
