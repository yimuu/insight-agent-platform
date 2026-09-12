#!/usr/bin/env python3
"""Check ADR-0017's exact test-only allowances against a real Cargo resolve graph.

Usage: python3 tools/tests/test_crate_boundary_live_fixtures.py METADATA_JSON
"""
import contextlib
import copy
import importlib.util
import io
import json
from pathlib import Path
import sys
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("crate_boundaries", ROOT / "tools/checks/check-crate-boundaries.py")
validator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(validator)


class LiveFixtureBoundaryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not hasattr(cls, "metadata"):
            cls.metadata = json.loads(subprocess.check_output(
                ["cargo", "metadata", "--locked", "--all-features", "--format-version", "1"],
                cwd=ROOT, timeout=60,
            ))

    def check_graph(self, metadata):
        output = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            status = validator.check(metadata, ROOT / "tools/baselines/crate-boundary-third-party-features.tsv", ROOT)
        return status, output.getvalue()

    def test_reviewed_real_graph_passes(self):
        status, output = self.check_graph(self.metadata)
        self.assertEqual(status, 0, output)

    def test_test_only_dependencies_cannot_enter_the_shipped_graph(self):
        packages = {p["id"]: p["name"] for p in self.metadata["packages"]}
        for name in ["insight-platform-api", "insight-platform-gateway", "insight-platform-model-worker", "axum"]:
            for kinds in [[None], ["build"], ["dev", None], ["dev", "build"]]:
                with self.subTest(dependency=name, kinds=kinds):
                    mutated = copy.deepcopy(self.metadata)
                    owner = next(n for n in mutated["resolve"]["nodes"] if packages[n["id"]] == "insight-platform-postgres")
                    edge = next(d for d in owner["deps"] if packages[d["pkg"]] == name)
                    self.assertEqual(validator.dependency_kinds(edge), ["dev"])
                    edge["dep_kinds"] = [{"kind": k, "target": None} for k in kinds]
                    status, output = self.check_graph(mutated)
                    self.assertEqual(status, 1)
                    self.assertIn("platform_postgres: forbidden", output)
                    self.assertIn(name + "@", output)

    def test_gateway_model_allowance_does_not_permit_storage_to_depend_on_serving(self):
        self.assertIn("models_domain", validator.ALLOWED_INTERNAL["public_gateway"])
        for role in ["public_gateway", "platform_api", "model_worker"]:
            self.assertNotIn(role, validator.ALLOWED_INTERNAL["platform_postgres"])
        self.assertIn("axum", validator.FORBIDDEN_DIRECT["platform_postgres"])


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    LiveFixtureBoundaryTests.metadata = json.loads(Path(sys.argv[1]).read_text())
    unittest.main(argv=[sys.argv[0]])
