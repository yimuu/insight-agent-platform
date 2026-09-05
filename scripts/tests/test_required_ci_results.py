import importlib.util
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "check_required_ci_results", ROOT / "scripts/check-required-ci-results.py"
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def selections(**overrides: bool) -> dict[str, bool]:
    selected = {
        "runtime": False,
        "cli": False,
        "console": False,
        "policy": False,
    }
    selected.update(overrides)
    return selected


class RequiredCiResultsTests(unittest.TestCase):
    def test_runtime_selection_requires_both_full_lanes_and_skips_cli(self) -> None:
        selected = selections(runtime=True, cli=True, policy=True)
        expected = MODULE.expected_results(selected)
        self.assertEqual(expected["lint"], "success")
        self.assertEqual(expected["test"], "success")
        self.assertEqual(expected["cli"], "skipped")
        self.assertEqual(expected["policy"], "success")
        self.assertEqual(MODULE.validate_results(selected, expected), [])

    def test_cli_only_selection_requires_cli_success(self) -> None:
        selected = selections(cli=True)
        expected = MODULE.expected_results(selected)
        self.assertEqual(expected["cli"], "success")
        self.assertEqual(expected["lint"], "skipped")
        self.assertEqual(MODULE.validate_results(selected, expected), [])

    def test_selected_lane_cannot_be_skipped(self) -> None:
        selected = selections(console=True)
        actual = MODULE.expected_results(selected)
        actual["console"] = "skipped"
        self.assertEqual(
            MODULE.validate_results(selected, actual),
            ["console lane expected success but was 'skipped'"],
        )

    def test_unselected_lane_cannot_run_instead_of_skipping(self) -> None:
        selected = selections()
        actual = MODULE.expected_results(selected)
        actual["policy"] = "success"
        self.assertEqual(
            MODULE.validate_results(selected, actual),
            ["policy lane expected skipped but was 'success'"],
        )

    def test_failure_and_cancelled_results_are_rejected(self) -> None:
        selected = selections(runtime=True)
        actual = MODULE.expected_results(selected)
        actual["lint"] = "failure"
        actual["test"] = "cancelled"
        self.assertEqual(
            MODULE.validate_results(selected, actual),
            [
                "lint lane expected success but was 'failure'",
                "test lane expected success but was 'cancelled'",
            ],
        )

    def test_productization_is_always_required_and_cannot_skip(self) -> None:
        actual = MODULE.expected_results(selections())
        self.assertEqual(actual["productization"], "success")
        actual["productization"] = "skipped"
        self.assertEqual(
            MODULE.validate_results(selections(), actual),
            ["productization lane expected success but was 'skipped'"],
        )


if __name__ == "__main__":
    unittest.main()
