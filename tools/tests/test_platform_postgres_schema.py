"""Independent schema checks accept current authorities and reject weakened boundaries."""
import contextlib
import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "postgres_schema_check", ROOT / "tools/checks/check-platform-postgres-schema.py"
)
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


class CurrentSchemaChecks(unittest.TestCase):
    def validate(self, mutate=lambda sql: sql, contract_mutation=lambda value: None):
        with tempfile.TemporaryDirectory() as temporary:
            crate = Path(temporary)
            sql = mutate(CHECK.SCHEMA_PATH.read_text())
            contract = CHECK.load_json(CHECK.CONTRACT_PATH)
            digest = CHECK.prefixed_sha256(sql.encode())
            contract["schema_snapshot"] = {"path": "schema.sql", "digest": digest}
            contract["schema_snapshot_digest"] = digest
            contract_mutation(contract)
            (crate / "schema.sql").write_text(sql)
            (crate / "schema-contract.json").write_text(json.dumps(contract))
            (crate / "schema-inventory.json").write_bytes(
                (CHECK.CRATE / "schema-inventory.json").read_bytes()
            )
            errors = io.StringIO()
            with patch.multiple(CHECK, CRATE=crate, SCHEMA_PATH=crate / "schema.sql",
                                CONTRACT_PATH=crate / "schema-contract.json"), \
                    contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(errors):
                result = CHECK.main()
            return result, errors.getvalue()

    def test_current_schema_includes_installation_identity_and_tenant_conversations(self):
        result, errors = self.validate()
        self.assertEqual(result, 0, errors)

    def test_old_version_cannot_be_blessed_by_matching_snapshot_hash(self):
        result, errors = self.validate(contract_mutation=lambda c: c.update(schema_contract_version=15))
        self.assertEqual(result, 1)
        self.assertIn("version must be 17", errors)

    def test_identity_and_retention_guards_survive_updated_hashes(self):
        changes = [
            ("CREATE TABLE insight_platform.conversations (\n tenant_id text NOT NULL,",
             "CREATE TABLE insight_platform.conversations (\n", "lacks tenant_id"),
            ("password_hash bytea NOT NULL", "password_plaintext bytea NOT NULL", "credential/session columns"),
            ("DEFAULT true CHECK (singleton)", "DEFAULT true", "required identity/retention constraint"),
            ("AND expires_at <= created_at + interval '8 hours'", "", "required identity/retention constraint"),
            ("FOREIGN KEY (tenant_id, run_id) REFERENCES insight_platform.runs(tenant_id, run_id)",
             "CHECK (true)", "conversation_turns lacks required identity/retention constraint"),
        ]
        for old, new, expected in changes:
            with self.subTest(boundary=expected, mutation=old):
                def mutate(sql):
                    self.assertIn(old, sql)
                    return new.join(sql.rsplit(old, 1))
                result, errors = self.validate(mutate)
                self.assertEqual(result, 1)
                self.assertIn(expected, errors)


if __name__ == "__main__":
    unittest.main()
