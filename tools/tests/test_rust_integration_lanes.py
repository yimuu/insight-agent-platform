import importlib.util
from pathlib import Path
import unittest

ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "Cargo.toml").is_file())
SPEC = importlib.util.spec_from_file_location("integration_lanes", ROOT / "tools/ci/run-rust-integration-tests.py")
LANES = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LANES)

class IntegrationLaneTests(unittest.TestCase):
    def fixture(self):
        packages = [{"id": name, "name": name, "targets": [{"kind": ["test"], "name": target}]} for name, target in sorted(LANES.EXTERNAL_TARGETS)]
        packages.append({"id": "new-owner", "name": "new-owner", "targets": [{"kind": ["test"], "name": "new-durable-fixture"}]})
        return {"workspace_members": [package["id"] for package in packages], "packages": packages}

    def test_new_integration_targets_are_executed_by_default(self):
        commands = LANES.integration_commands(self.fixture())
        self.assertEqual(1, len(commands))
        self.assertEqual(["-p", "new-owner", "--test", "new-durable-fixture"], commands[0][-4:])

    def test_stale_external_exclusion_fails_closed(self):
        metadata = self.fixture()
        metadata["packages"].pop(0)
        with self.assertRaises(ValueError):
            LANES.integration_commands(metadata)
