from pathlib import Path
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[2]
WRAPPER = ROOT / "scripts/run-productization-sandbox-qualification.sh"
QUALIFIER = ROOT / "scripts/qualify-platform-sandbox-l3.sh"


class ProductizationSandboxQualificationTests(unittest.TestCase):
    def test_help_names_all_authoritative_inputs(self) -> None:
        result = subprocess.run(
            ["bash", str(WRAPPER), "--help"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--environment", result.stdout)
        self.assertIn("--source-revision", result.stdout)
        self.assertIn("--output", result.stdout)

    def test_missing_environment_fails_before_qualification(self) -> None:
        result = subprocess.run(
            ["bash", str(WRAPPER)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("--environment, --source-revision, and --output are required", result.stderr)
        self.assertNotIn("Compiling", result.stderr)

    def test_l3_cargo_invocations_are_locked(self) -> None:
        source = QUALIFIER.read_text(encoding="utf-8")
        self.assertNotIn("cargo test -p ", source)
        self.assertNotIn("cargo run -p ", source)
        self.assertIn("cargo test --locked -p insight-platform-opensandbox-client", source)
        self.assertIn("cargo test --locked -p insight-platform-qualification-tests", source)
        self.assertIn("cargo run --locked -p insight-platform-postgres", source)

    def test_l3_crash_injection_is_bound_to_its_exact_abort_point(self) -> None:
        source = QUALIFIER.read_text(encoding="utf-8")
        marker = 'PLATFORM_OPENSANDBOX_L3_ABORT_MARKER="$abort_marker"'
        exact_status = 'if [[ "$abort_status" -ne 101 ]]'
        exact_signal = "(signal: 6, SIGABRT: process abort signal)"
        terminal_phase = "PLATFORM_OPENSANDBOX_L3_CONTROL_PHASE=cancel-terminal"
        self.assertIn(marker, source)
        self.assertIn(exact_status, source)
        self.assertIn(exact_signal, source)
        self.assertIn("opensandbox-abort-marker/v1", source)
        self.assertLess(source.index(marker), source.index(exact_status))
        self.assertLess(source.index(exact_status), source.index(exact_signal))
        self.assertLess(source.index(exact_signal), source.index(terminal_phase))

    def test_raw_evidence_uses_rust_target_names(self) -> None:
        source = WRAPPER.read_text(encoding="utf-8")
        self.assertIn('"Darwin": "macos"', source)
        self.assertIn('"arm64": "aarch64"', source)
        self.assertIn('"amd64": "x86_64"', source)
        self.assertIn('"os": rust_os', source)
        self.assertIn('"architecture": rust_arch', source)

    def test_raw_evidence_generates_a_unique_run_and_l3_time_window(self) -> None:
        source = WRAPPER.read_text(encoding="utf-8")
        self.assertIn("secrets.token_bytes(32)", source)
        self.assertIn('"qualification_run_id": qualification_run_id', source)
        self.assertIn('"started_at": started_at', source)
        self.assertIn('"finished_at": finished_at', source)
        self.assertIn("age = started_at - generated_at", source)
        self.assertIn("age.total_seconds() > 3 * 60 * 60", source)
        qualifier = '"$root/scripts/qualify-platform-sandbox-l3.sh"'
        self.assertLess(source.index("started_at=$(python3"), source.index(qualifier))
        self.assertLess(source.index(qualifier), source.index("finished_at=$(python3"))

    def test_bootstrap_consumer_requires_v2_exact_runtime_and_runner_identities(self) -> None:
        source = WRAPPER.read_text(encoding="utf-8")
        self.assertIn('value["schema_version"] != 2', source)
        self.assertIn('"insight.platform/kind-local-mechanics/v2"', source)
        self.assertIn('"sandbox_runner_image_digest"', source)
        self.assertIn('"sandbox_runner_image_repository"', source)
        self.assertIn('"sandbox_runner_image_identity"', source)
        self.assertIn('identity["kind"] == "source_oci_manifest"', source)
        self.assertIn("identity[\"index_digest\"] is not None", source)
        self.assertIn(
            'identity["platform_digest"] != value[digest_field]',
            source,
        )
        self.assertIn('runner = images["sandbox_runner"]', source)
        self.assertIn(
            'runner_identity = environment["sandbox_runner_image_identity"]',
            source,
        )
        self.assertIn(
            'runner_identity["kind"] != platform_identity["kind"]',
            source,
        )
        self.assertNotIn("local_image_config", source)


if __name__ == "__main__":
    unittest.main()
