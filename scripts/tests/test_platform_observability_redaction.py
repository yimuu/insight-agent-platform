import importlib.util
from pathlib import Path
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "check_platform_observability_redaction",
    ROOT / "scripts/check-platform-observability-redaction.py",
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class PlatformObservabilityRedactionTests(unittest.TestCase):
    def check_source(self, source: str) -> list[str]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            crate = root / "crates/example/src"
            crate.mkdir(parents=True)
            (crate / "lib.rs").write_text(source, encoding="utf-8")
            return MODULE.check(root)

    def test_production_after_cfg_test_is_still_scanned(self) -> None:
        failures = self.check_source(
            """
#[cfg(test)]
fn helper() {}

fn production(failure: impl std::fmt::Display) {
    tracing::error!(error = %failure, "operation failed");
}
"""
        )
        self.assertEqual(
            failures,
            ["crates/example/src/lib.rs:6 raw generic error in tracing field"],
        )

    def test_high_cardinality_field_is_rejected(self) -> None:
        failures = self.check_source(
            'fn production() { tracing::info!(tenant_id = "tenant", "request"); }\n'
        )
        self.assertEqual(len(failures), 1)
        self.assertIn("forbidden tracing fields ['tenant_id']", failures[0])

    def test_safe_classification_fields_are_accepted(self) -> None:
        failures = self.check_source(
            'fn production() { tracing::warn!(retryable = true, "dependency unavailable"); }\n'
        )
        self.assertEqual(failures, [])


if __name__ == "__main__":
    unittest.main()
