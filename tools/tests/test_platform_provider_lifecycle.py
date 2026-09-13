"""Read-only Compose identity and ownership snapshots; no services or credentials."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("provider_lifecycle_under_test", ROOT / "tools/install/provider_lifecycle.py")
LIFE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = LIFE
SPEC.loader.exec_module(LIFE)
INPUT = "sha256:" + "a" * 64
IDENTITY = "sha256:" + "b" * 64
INIT_ID, SERVE_ID, FOREIGN_ID = "1" * 64, "2" * 64, "3" * 64


class CompositionSnapshotTests(unittest.TestCase):
    def fixture(self):
        image = "fixture@sha256:" + "d" * 64
        image_id = "sha256:" + "e" * 64
        document = {"name": "own-project", "services": {"openbao": {"image": image, "labels": {"insight.installation.input": INPUT}}}}
        metadata = {"identity": SERVE_ID, "running": True, "image": image_id, "labels": {
            "com.docker.compose.project": "own-project", "com.docker.compose.service": "openbao", "insight.installation.input": INPUT}}
        return document, metadata, image_id

    def snapshot(self, document, rows, image_id):
        calls = []
        def run(arguments):
            calls.append(arguments)
            if arguments[:3] == ["docker", "image", "inspect"]:
                return (image_id + "\n").encode()
            if arguments[:2] == ["docker", "ps"]:
                return ("\n".join(row["identity"] for row in rows) + "\n").encode() if rows else b""
            if arguments[:2] == ["docker", "inspect"]:
                return json.dumps(next(row for row in rows if row["identity"] == arguments[-1])).encode()
            raise AssertionError("unexpected non-readonly Docker command")
        result = LIFE.composition_snapshot(document, run)
        return result, calls

    def test_exact_owner_and_image_snapshot_uses_only_readonly_commands(self):
        document, row, image_id = self.fixture()
        result, calls = self.snapshot(document, [row], image_id)
        self.assertEqual(result, {"openbao": LIFE.ContainerState(SERVE_ID, True)})
        self.assertTrue(all(command[:2] in (["docker", "image"], ["docker", "ps"], ["docker", "inspect"]) for command in calls))

    def test_same_name_different_input_project_or_image_is_foreign(self):
        for field in ("input", "project", "service", "image", "running"):
            with self.subTest(field=field):
                document, row, image_id = self.fixture()
                if field == "input": row["labels"]["insight.installation.input"] = "sha256:" + "f" * 64
                elif field == "project": row["labels"]["com.docker.compose.project"] = "foreign"
                elif field == "service": row["labels"]["com.docker.compose.service"] = "unexpected-service"
                elif field == "image": row["image"] = "sha256:" + "f" * 64
                else: row["running"] = "true"
                with self.assertRaisesRegex(LIFE.LifecycleFailure, "foreign composition"):
                    self.snapshot(document, [row], image_id)

    def test_duplicate_service_claims_and_unknown_image_identity_are_rejected(self):
        document, row, image_id = self.fixture()
        duplicate = copy.deepcopy(row)
        duplicate["identity"] = FOREIGN_ID
        with self.assertRaisesRegex(LIFE.LifecycleFailure, "multiple containers"):
            self.snapshot(document, [row, duplicate], image_id)
        with self.assertRaisesRegex(LIFE.LifecycleFailure, "image identity"):
            self.snapshot(document, [row], "mutable-image")

    def test_oneoff_is_excluded_only_after_foreign_image_and_input_checks(self):
        document, row, image_id = self.fixture()
        row["labels"]["com.docker.compose.oneoff"] = "true"
        self.assertEqual(self.snapshot(document, [row], image_id)[0], {})
        row["image"] = "sha256:" + "f" * 64
        with self.assertRaises(LIFE.LifecycleFailure):
            self.snapshot(document, [row], image_id)


if __name__ == "__main__":
    unittest.main()
