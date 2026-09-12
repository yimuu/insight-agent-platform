#!/usr/bin/env python3
"""Regression checks for the independent validator's exact reviewed contract boundaries."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "platform_v1_validator", Path(__file__).resolve().parents[1] / "checks/check-platform-v1-contracts.py"
)
validator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(validator)


class ReviewedContractBoundaryTests(unittest.TestCase):
    def test_current_reviewed_contracts_pass(self):
        errors = []
        validator.check_foundation_surfaces(errors)
        validator.check_machine_registry_contracts(errors)
        self.assertEqual(errors, [])

    def test_path_allowlist_still_rejects_additions_and_missing_execution_read(self):
        original = Path.read_text
        for remove_execution in (False, True):
            with self.subTest(remove_execution=remove_execution):
                def read(path, *args, **kwargs):
                    content = original(path, *args, **kwargs)
                    if path == validator.CONTRACT_ROOT / "openapi.yaml":
                        if remove_execution:
                            content = content.replace(
                                "  /runs/{run_id}/executions/{source_kind}/{source_id}:\n", ""
                            )
                        else:
                            content += "\n  /unreviewed-route:\n"
                    return content
                errors = []
                with patch.object(Path, "read_text", read):
                    validator.check_foundation_surfaces(errors)
                self.assertIn(
                    "OpenAPI exposes a path outside the reviewed implementing slice", errors
                )

    def test_provider_closure_requires_endpoint_and_rejects_unknown_binding(self):
        original = validator.load
        for remove_endpoint in (False, True):
            with self.subTest(remove_endpoint=remove_endpoint):
                def load(path):
                    value = original(path)
                    if path.name == "deployment-closure.schema.json":
                        bindings = value["$defs"]["ModelProviderDeploymentClosure"]["properties"]["bindings"]
                        if remove_endpoint:
                            bindings["required"].remove("endpoint")
                            del bindings["properties"]["endpoint"]
                        else:
                            bindings["required"].append("unreviewed_binding")
                            bindings["properties"]["unreviewed_binding"] = {"type": "string"}
                    return value
                errors = []
                with patch.object(validator, "load", load):
                    validator.check_machine_registry_contracts(errors)
                self.assertIn("ModelProviderDeploymentClosure.bindings differs from its Rust owner fields", errors)
                self.assertIn("ModelProviderDeploymentClosure.bindings exposes unknown or missing fields", errors)


if __name__ == "__main__":
    unittest.main()
