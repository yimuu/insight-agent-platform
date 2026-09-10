"""Independent static IAM fixture boundaries; physical behavior is tested by the owning SDK."""
import importlib.util
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("s3_iam_qualification", ROOT / "tools/qualification/qualify-platform-installation-s3-iam.py")
HARNESS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(HARNESS)


class StaticIamFixtureTests(unittest.TestCase):
    def test_read_diagnostics_drop_all_raw_values_and_bound_count(self):
        line = "IAM_READ object=main operation=head class=not_found"
        self.assertEqual(HARNESS.read_diagnostics(line), [{"object": "main", "operation": "head", "class": "not_found"}])
        self.assertEqual(HARNESS.read_diagnostics(line + " credential=do-not-emit\nIAM_READ object=secret operation=get class=unknown"), [])
        self.assertEqual(len(HARNESS.read_diagnostics((line + "\n") * 100)), 8)

    def setUp(self):
        credentials = {role: {"accessKey": str(index) * 32, "secretKey": str(index) * 64}
                       for index, role in enumerate(HARNESS.ROLES)}
        self.configuration = HARNESS.configuration("isolated-bucket", credentials)
        self.policies = {item["name"]: json.loads(item["content"])["Statement"] for item in self.configuration["policies"]}

    def test_attached_policies_replace_legacy_actions_without_admin_identity(self):
        for identity in self.configuration["identities"]:
            self.assertEqual(identity["actions"], [])
            self.assertEqual(identity["policyNames"], [identity["name"]])
        self.assertNotIn("Admin", json.dumps(self.configuration))

    def test_initializer_has_no_object_access_and_runtime_cannot_change_bucket(self):
        self.assertTrue(all(item["Resource"] == ["arn:aws:s3:::isolated-bucket"] for item in self.policies["initializer"]))
        for role, statements in self.policies.items():
            if role == "initializer":
                continue
            for item in statements:
                if item["Resource"] == ["arn:aws:s3:::isolated-bucket"]:
                    self.assertIn(item["Action"], [["s3:ListBucket"], ["s3:GetBucketVersioning"]])
            self.assertNotIn("s3:*", json.dumps(statements))

    def test_method_conditions_close_implicit_multipart_delete_and_list(self):
        for role in ("artifact-gateway", "artifact-data"):
            write = next(item for item in self.policies[role] if item["Action"] == ["s3:PutObject"])
            self.assertEqual(write["Condition"], {"StringEquals": {"s3:RequestMethod": "PUT"}})
        for statements in self.policies.values():
            listing = next(item for item in statements if item["Action"] == ["s3:ListBucket"])
            self.assertEqual(listing["Condition"], {"StringEquals": {"s3:RequestMethod": "HEAD"}})

    def test_readonly_and_delete_identities_never_get_put_or_legacy_write(self):
        reader_actions = [action for item in self.policies["qualification-reader"] for action in item["Action"]]
        self.assertFalse(any("Put" in action or "Delete" in action for action in reader_actions))
        deletion = [item for item in self.policies["artifact-maintenance"] if "s3:DeleteObjectVersion" in item["Action"]]
        self.assertEqual(len(deletion), 1)
        self.assertEqual(deletion[0]["Resource"], ["arn:aws:s3:::isolated-bucket/v1/*"])
        self.assertEqual(deletion[0]["Condition"], {"StringEquals": {"s3:RequestMethod": "DELETE"}})
        self.assertNotIn("s3:DeleteObject\"", json.dumps(self.policies["artifact-maintenance"]))


if __name__ == "__main__":
    unittest.main()
