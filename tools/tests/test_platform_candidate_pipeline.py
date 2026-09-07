import hashlib
import json
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "Cargo.toml").is_file())
DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64


class CandidatePipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        subprocess.run(["cargo", "build", "--locked", "--quiet", "-p", "insight-platform-contract-tooling", "--bin", "platform-qualification"], cwd=ROOT, check=True)
        metadata = json.loads(subprocess.check_output(
            ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"], cwd=ROOT))
        cls.tool = Path(metadata["target_directory"]) / "debug/platform-qualification"
        cls.executables = json.loads(subprocess.check_output([str(cls.tool), "print-worker-executables"], cwd=ROOT))
        cls.protocols = json.loads(subprocess.check_output([str(cls.tool), "print-builtin-worker-protocols"], cwd=ROOT))

    def worker_fixture(self, environment):
        manifests = environment / "worker-manifests"
        configs = environment / "worker-configs"
        binaries = environment / "worker-binaries"
        for directory in [manifests, configs, binaries]:
            directory.mkdir(exist_ok=True)
        for worker in self.executables:
            binary = worker["binary"]
            raw = ("dedicated deployment test executable bytes: " + binary + "\n").encode()
            (binaries / binary).write_bytes(raw)
            config = {
                "validator_digest": DIGEST_B,
                "native_catalog": {"adapter_contract_digest": DIGEST_A, "installed_adapter_digest": DIGEST_B},
                "sources": [{"binding": {"adapter_contract_digest": DIGEST_A}}],
                "scan_worker": {"scanner_contract_digest": self.protocols["integrity_scanner_contract_digest"]},
                "worker": {},
                "installed_adapters": ([{"qualified_name": "anthropic.messages/2023-06-01", "worker_manifest_digest": DIGEST_B,
                                         "adapter_contract_digest": DIGEST_A}] if binary == "platform-model-worker"
                                       else [dict(self.protocols["native_capability"])]),
            }
            config.update(self.protocols["remote_context"])
            for kind in ["http", "grpc", "mcp"]:
                config[f"installed_{kind}_codecs"] = [dict(self.protocols["codecs"][kind], descriptor_digest=DIGEST_B)]
            config_path = configs / (binary + ".json")
            config_path.write_text(json.dumps(config))
            catalog = json.loads(subprocess.check_output([str(self.tool), "print-worker-execution-capabilities", binary, str(config_path)], cwd=ROOT))
            manifest = {"manifest_version": 2, "worker_role": worker["worker_role"], "work_class": worker["work_class"],
                        "adapter_runtime_digest": DIGEST_A, "worker_build_digest": "sha256:" + hashlib.sha256(raw).hexdigest(),
                        "execution_capabilities": catalog, "protocol_version": 1, "max_concurrency": 4, "critical_control_reserved_slots": 1}
            (manifests / (binary + ".json")).write_text(json.dumps(manifest))
            target = config
            parts = worker["manifest_pointer"].strip("/").split("/")
            for part in parts[:-1]:
                target = target.setdefault(part, {})
            target[parts[-1]] = manifest
            config_path.write_text(json.dumps(config))
        history_bytes = b"dedicated history executable fixture bytes\n"
        (binaries / "platform-history-maintenance").write_bytes(history_bytes)
        (environment / "history-maintenance.json").write_text(json.dumps({
            "schema_version":1, "component_role":"history_maintenance", "executable_digest":"sha256:" + hashlib.sha256(history_bytes).hexdigest(),
            "observability_listen_address":"127.0.0.1:19099", "database_max_connections":2,"database_acquire_timeout_milliseconds":1000,
            "poll_interval_milliseconds":1000,"maximum_runs":16,"maximum_events_per_run":32,
            "retention_policy":{"schema_version":2,"public_event_minimum_seconds":86400,"audit_event_minimum_seconds":86400,"receipt_minimum_seconds":86400,"published_outbox_minimum_seconds":86400,"cleanup_minimum_seconds":86400},
        }))
        (binaries / "platform-schema").write_bytes(b"dedicated schema executable fixture bytes\n")
        return manifests, configs, binaries

    def build(self, output):
        environment = Path(output).parent / (Path(output).name + "-environment")
        environment.mkdir(exist_ok=True)
        (environment / "closure.yaml").write_text("imagePolicy: exact-digest\n")
        manifests, configs, binaries = self.worker_fixture(environment)
        subprocess.run([
            "python3", "tools/release/build-platform-production-candidate.py",
            "--runtime-image-digest", DIGEST_A,
            "--sandbox-runner-image-digest", DIGEST_B,
            "--git-commit", "sha1:" + "c" * 40,
            "--created-at", "2026-08-26T12:00:00.000000Z",
            "--environment-closure", str(environment),
            "--history-maintenance-config", str(environment / "history-maintenance.json"),
            "--worker-manifests", str(manifests), "--worker-configs", str(configs), "--worker-binaries", str(binaries),
            "--output-dir", str(output),
        ], cwd=ROOT, check=True)

    def test_candidate_is_deterministic_and_closes_images_and_runner(self):
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            self.build(first)
            self.build(second)
            first_path = Path(first)
            second_path = Path(second)
            self.assertEqual(
                (first_path / "candidate-manifest.json").read_bytes(),
                (second_path / "candidate-manifest.json").read_bytes(),
            )
            candidate = json.loads((first_path / "candidate-manifest.json").read_bytes())
            self.assertEqual(18, len(candidate["component_images"]))
            self.assertIn("outbox_worker", candidate["component_images"])
            self.assertEqual(DIGEST_B, candidate["sandbox_runner_image_digest"])
            self.assertEqual(
                {
                    DIGEST_A,
                    "sha256:ae8dfbb277f40a39ff01ef35e5e1c10675acfe0fa9db15259b8f323e5efab778",
                    "sha256:a9a5f73c1785ebd955336ffa313973a35c1a1b662cb7afc4ea82d92021b3532a",
                },
                set(candidate["component_images"].values()),
            )
            dispatcher = json.loads(
                (first_path / "worker-manifests/platform-sandbox-dispatcher.json").read_bytes()
            )
            self.assertEqual(DIGEST_A, dispatcher["adapter_runtime_digest"])
            self.assertEqual(15, len(candidate["worker_manifests"]))
            self.assertNotEqual(DIGEST_A, dispatcher["worker_build_digest"])
            self.assertTrue((first_path / "worker-executable-evidence.json").is_file())
            self.assertFalse((first_path / "migration-baseline.sql").exists())
            evidence = json.loads((first_path / "schema-executable-evidence.json").read_bytes())
            self.assertEqual("sha256:" + hashlib.sha256((first_path / "platform-schema").read_bytes()).hexdigest(), evidence["runner_build_digest"])
            self.assertEqual("sha256:" + hashlib.sha256((first_path / "schema/schema.sql").read_bytes()).hexdigest(), evidence["schema_snapshot_digest"])
            self.assertEqual({"schema.sql", "schema-contract.json", "schema-inventory.json"}, {entry.name for entry in (first_path / "schema").iterdir()})

    def test_worker_verification_rejects_changed_binary_and_invented_capability(self):
        with tempfile.TemporaryDirectory() as directory:
            manifests, configs, binaries = self.worker_fixture(Path(directory))
            command = [str(self.tool), "validate-worker-deployment", str(manifests), str(configs), str(binaries), DIGEST_A]
            subprocess.run(command, cwd=ROOT, check=True, capture_output=True)
            binary = binaries / "platform-orchestration-worker"
            original = binary.read_bytes()
            binary.write_bytes(original + b"tampered")
            self.assertNotEqual(0, subprocess.run(command, cwd=ROOT, capture_output=True).returncode)
            binary.write_bytes(original)
            path = manifests / "platform-orchestration-worker.json"
            manifest = json.loads(path.read_bytes())
            manifest["execution_capabilities"]["capabilities"][0]["program_semantic_identity"] = DIGEST_B
            path.write_text(json.dumps(manifest))
            config_path = configs / path.name
            config = json.loads(config_path.read_bytes())
            config["worker_manifest"] = manifest
            config_path.write_text(json.dumps(config))
            self.assertNotEqual(0, subprocess.run(command, cwd=ROOT, capture_output=True).returncode)

    def test_history_evidence_rejects_binary_policy_role_and_budget_drift(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, _, binaries = self.worker_fixture(root)
            config = root / "history-maintenance.json"
            binary = binaries / "platform-history-maintenance"
            original_config = config.read_bytes()
            command = [str(self.tool), "validate-history-maintenance-deployment", str(config), str(binary), DIGEST_A]
            evidence = subprocess.check_output(command, cwd=ROOT)
            evidence_path = root / "history-evidence.json"
            evidence_path.write_bytes(evidence)
            verify = [str(self.tool), "verify-history-maintenance-deployment", str(config), str(binary), str(evidence_path)]
            subprocess.run(verify, check=True, cwd=ROOT)
            original_binary = binary.read_bytes()
            binary.write_bytes(original_binary + b"tamper")
            self.assertNotEqual(0, subprocess.run(command, cwd=ROOT, capture_output=True).returncode)
            binary.write_bytes(original_binary)
            changed = json.loads(original_config)
            changed["retention_policy"]["public_event_minimum_seconds"] += 1
            config.write_text(json.dumps(changed))
            self.assertNotEqual(0, subprocess.run(verify, cwd=ROOT, capture_output=True).returncode)
            for key, value in (("component_role", "artifact_maintenance"), ("maximum_runs", 129), ("maximum_events_per_run", 1001), ("poll_interval_milliseconds", 0)):
                changed = json.loads(original_config)
                changed[key] = value
                config.write_text(json.dumps(changed))
                self.assertNotEqual(0, subprocess.run(command, cwd=ROOT, capture_output=True).returncode, key)
            config.write_bytes(original_config + b" " * 65536)
            self.assertNotEqual(0, subprocess.run(command, cwd=ROOT, capture_output=True).returncode, "runtime and release tool must share the bounded config reader")


    def test_schema_evidence_rejects_changed_or_missing_release_bytes(self):
        with tempfile.TemporaryDirectory() as output:
            self.build(output)
            root = Path(output)
            command = [str(self.tool), "verify-schema-executable-evidence", str(root / "schema"), str(root / "platform-schema"), str(root / "schema-executable-evidence.json")]
            subprocess.run(command, check=True, cwd=ROOT)
            snapshot = root / "schema/schema.sql"
            original = snapshot.read_bytes()
            snapshot.write_bytes(original + b"\n-- altered release bytes\n")
            self.assertNotEqual(0, subprocess.run(command, capture_output=True, cwd=ROOT).returncode)
            snapshot.write_bytes(original)
            (root / "platform-schema").write_bytes(b"replacement runner")
            self.assertNotEqual(0, subprocess.run(command, capture_output=True, cwd=ROOT).returncode)
            snapshot.unlink()
            self.assertNotEqual(0, subprocess.run(command, capture_output=True, cwd=ROOT).returncode)

    def test_invalid_mutable_subject_is_rejected(self):
        with tempfile.TemporaryDirectory() as output:
            result = subprocess.run([
                "python3", "tools/release/build-platform-production-candidate.py",
                "--runtime-image-digest", "latest",
                "--sandbox-runner-image-digest", DIGEST_B,
                "--git-commit", "sha1:" + "c" * 40,
                "--created-at", "2026-08-26T12:00:00.000000Z",
                "--environment-closure", output,
                "--history-maintenance-config", output,
                "--worker-manifests", output, "--worker-configs", output, "--worker-binaries", output,
                "--output-dir", output,
            ], cwd=ROOT, capture_output=True, text=True)
            self.assertNotEqual(0, result.returncode)

    def test_gitops_environment_validator_accepts_exact_closed_input_and_rejects_drift(self):
        profile = ROOT / "contracts/platform-v1/qualification/production-release-profile.json"
        value = json.loads(profile.read_bytes())
        canonical = json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
        manifest = {
            "schema_version": 2,
            "environment_name": "production",
            "environment_class": "production",
            "application_repository": "yimuu/insight-agent-platform",
            "application_commit": "c" * 40,
            "qualification_profile_digest": "sha256:" + hashlib.sha256(canonical).hexdigest(),
            "deployment": {
                "requires_multi_node": True,
                "requires_opensandbox_kubernetes": True,
                "requires_validating_admission_policy_v1": True,
                "container_runtime": "containerd-runc",
                "sandbox_control_namespace": "platform-sandbox",
                "sandbox_workload_namespace": "platform-sandbox-workloads",
                "opensandbox_source_commit": "c39b814f36ded4c61d5ac6f9332ee4dfbab86c00",
                "opensandbox_server_image_digest": "sha256:ae8dfbb277f40a39ff01ef35e5e1c10675acfe0fa9db15259b8f323e5efab778",
                "opensandbox_controller_image_digest": "sha256:a9a5f73c1785ebd955336ffa313973a35c1a1b662cb7afc4ea82d92021b3532a",
                "opensandbox_execd_image_digest": "sha256:6cf7dba2f21f0b536e100563d841ac58a9f31c2b0a081b7ac76796a24d6f47e2",
                "batchsandbox_crd_digest": "sha256:6a56fbec00a33acf30a4a9c3418172ad6ac1eba34d081881e6b5dd941cfa59d4",
                "kubernetes_provider_template_digest": "sha256:be829c7a936867d7aff62bf76d5e897b75c65628563ad2d354f4ccb36b30cc4c",
                "sandbox_network_policy_digest": "sha256:2bc456ef5f8427de8b142de9347d030fec638078dd11df111bc05ef85110e66e",
            },
            "dependencies": {
                "postgresql": "dedicated", "nats": "tls-core", "object_storage": "versioned-s3",
                "key_management": "kms", "secret_management": "external", "telemetry": "prometheus",
            },
            "secret_policy": {
                "plaintext_in_git": False,
                "kubeconfig_in_git": False,
                "references_only": True,
            },
        }
        with tempfile.TemporaryDirectory() as directory:
            closure = Path(directory)
            path = closure / "environment.json"
            path.write_text(json.dumps(manifest))
            command = [
                "python3", "tools/checks/validate-platform-gitops-environment.py",
                "--closure", str(closure),
                "--application-repository", "yimuu/insight-agent-platform",
                "--application-commit", "c" * 40,
                "--qualification-profile", str(profile),
            ]
            subprocess.run(command, cwd=ROOT, check=True)
            manifest["application_commit"] = "d" * 40
            path.write_text(json.dumps(manifest))
            self.assertNotEqual(0, subprocess.run(command, cwd=ROOT, capture_output=True).returncode)

    def test_release_bundle_indexes_nested_artifacts_and_excludes_itself(self):
        with tempfile.TemporaryDirectory() as output:
            root = Path(output)
            (root / "nested").mkdir()
            (root / "candidate-manifest.json").write_text("candidate\n")
            (root / "nested/evidence.txt").write_text("evidence\n")
            subprocess.run([
                "python3", "tools/release/build-platform-release-bundle.py", output,
            ], cwd=ROOT, check=True)
            manifest = json.loads((root / "release-bundle-manifest.json").read_bytes())
            self.assertEqual(
                ["candidate-manifest.json", "nested/evidence.txt"],
                [artifact["path"] for artifact in manifest["artifacts"]],
            )


if __name__ == "__main__":
    unittest.main()
