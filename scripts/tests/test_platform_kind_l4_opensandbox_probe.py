import base64
import importlib.util
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest
import uuid


ROOT = Path(__file__).parents[2]
PROBE = ROOT / "deploy/kind/probes/opensandbox-l4-probe.py"
L4 = ROOT / "scripts/verify-platform-kind-l4.sh"
SPEC = importlib.util.spec_from_file_location("opensandbox_l4_probe", PROBE)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class OpenSandboxL4ProbeTests(unittest.TestCase):
    def test_ed25519_matches_rfc8032_and_the_owning_rust_proof_vector(self):
        rfc_seed = bytes.fromhex(
            "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"
        )
        self.assertEqual(
            "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
            MODULE.ed25519_public_key(rfc_seed).hex(),
        )
        self.assertEqual(
            "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e06522490155"
            "5fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b",
            MODULE.ed25519_sign(rfc_seed, b"").hex(),
        )
        self.assertEqual(
            "v1.f0b61d9e192dde01e1aa67fb59e3ea9c3b4171c881778efddca7609ab5fe9fb9"
            "82cd3559e488ae60044c76a8e1b96d6cb3186be7e28baeb6b2b347de760fc100",
            MODULE.runner_state_proof(
                bytes.fromhex("11" * 32), "sandbox-one", "sha256:" + "a" * 64
            ),
        )

    def test_create_request_is_the_closed_inert_candidate_contract(self):
        image = "registry.invalid/insight/package@sha256:" + "a" * 64
        runtime = "sha256:" + "b" * 64
        profile = "sha256:" + "c" * 64
        seed = bytes.fromhex("11" * 32)
        token = "22" * 32
        request = MODULE.build_create_request(
            image_uri=image,
            runtime_contract_digest=runtime,
            profile_deployment_digest=profile,
            signing_seed=seed,
            execd_access_token=token,
            timestamp_milliseconds=1_800_000_000_000,
        )

        self.assertEqual(
            {
                "image",
                "timeout",
                "resourceLimits",
                "resourceRequests",
                "env",
                "metadata",
                "entrypoint",
                "secureAccess",
            },
            set(request),
        )
        self.assertEqual({"uri": image}, request["image"])
        self.assertFalse(request["secureAccess"])
        self.assertEqual(["/usr/local/bin/platform-sandbox-runner"], request["entrypoint"])
        self.assertEqual(
            {
                "EXECD_ACCESS_TOKEN",
                "INSIGHT_SANDBOX_RUNNER_CONFIG",
                "INSIGHT_SANDBOX_RUNNER_CONFIG_DIGEST",
            },
            set(request["env"]),
        )
        self.assertEqual(token, request["env"]["EXECD_ACCESS_TOKEN"])

        raw_config = request["env"]["INSIGHT_SANDBOX_RUNNER_CONFIG"]
        config = json.loads(raw_config)
        self.assertEqual(MODULE.canonical_json(config).decode(), raw_config)
        self.assertEqual(
            "sha256:" + MODULE.hashlib.sha256(raw_config.encode()).hexdigest(),
            request["env"]["INSIGHT_SANDBOX_RUNNER_CONFIG_DIGEST"],
        )
        self.assertEqual(
            {
                "schema_version",
                "execution_request_digest",
                "input_schema_digest",
                "input_digest",
                "output_schema_digest",
                "activation_verifying_key",
                "package_uid",
                "package_argv",
                "maximum_input_bytes",
                "maximum_output_bytes",
                "maximum_diagnostic_bytes",
                "maximum_processes",
                "wall_milliseconds",
            },
            set(config),
        )
        self.assertNotIn("activation_" + "token_digest", config)
        self.assertEqual(MODULE.ed25519_public_key(seed).hex(), config["activation_verifying_key"])
        self.assertEqual(65_533, config["package_uid"])
        self.assertEqual(["/opt/insight/package"], config["package_argv"])

        metadata = request["metadata"]
        self.assertEqual(12, len(metadata))
        self.assertEqual("readiness", metadata["platform.insight.dev/purpose"])
        self.assertEqual("disabled", metadata["platform.insight.dev/network"])
        self.assertEqual("armed-runner-v2", metadata["insight.platform/sandbox-template"])
        self.assertEqual(
            config["execution_request_digest"][7:],
            self.decode_metadata_digest(metadata["platform.insight.dev/request"]).hex(),
        )
        self.assertEqual(
            runtime[7:],
            self.decode_metadata_digest(metadata["platform.insight.dev/runtime"]).hex(),
        )
        self.assertEqual(
            profile[7:],
            self.decode_metadata_digest(metadata["platform.insight.dev/profile"]).hex(),
        )
        for key, prefix in (
            ("platform.insight.dev/tenant", "ten"),
            ("platform.insight.dev/job", "job"),
        ):
            observed_prefix, raw_uuid = metadata[key].split("_", 1)
            self.assertEqual(prefix, observed_prefix)
            self.assertEqual(7, uuid.UUID(raw_uuid).version)

    def test_cli_keeps_random_signing_seed_private_and_emits_bound_proof(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            seed = directory / "seed"
            request = directory / "request.json"
            self.run_probe(
                "create",
                "--image-uri",
                "registry.invalid/package@sha256:" + "d" * 64,
                "--runtime-contract-digest",
                "sha256:" + "e" * 64,
                "--profile-deployment-digest",
                "sha256:" + "f" * 64,
                "--signing-seed-output",
                str(seed),
                "--output",
                str(request),
            )
            self.assertEqual(0o600, stat.S_IMODE(seed.stat().st_mode))
            self.assertEqual(0o600, stat.S_IMODE(request.stat().st_mode))
            document = json.loads(request.read_bytes())
            config = json.loads(document["env"]["INSIGHT_SANDBOX_RUNNER_CONFIG"])
            proof = self.run_probe(
                "state-proof",
                "--signing-seed",
                str(seed),
                "--sandbox-id",
                "sandbox-probe",
                "--execution-request-digest",
                config["execution_request_digest"],
            ).stdout.strip()
            self.assertRegex(proof, r"^v1\.[0-9a-f]{128}$")
            self.assertNotEqual(
                proof,
                MODULE.runner_state_proof(
                    bytes.fromhex("11" * 32),
                    "sandbox-probe",
                    config["execution_request_digest"],
                ),
            )

            os.chmod(seed, 0o644)
            rejected = self.run_probe(
                "state-proof",
                "--signing-seed",
                str(seed),
                "--sandbox-id",
                "sandbox-probe",
                "--execution-request-digest",
                config["execution_request_digest"],
                check=False,
            )
            self.assertEqual(2, rejected.returncode)
            self.assertIn("private regular file", rejected.stderr)

    def test_state_validation_requires_closed_armed_frame_and_stable_boot(self):
        state = {
            "magic": "insight.sandbox.runner/v1",
            "schema_version": 1,
            "sandbox_id": "sandbox-probe",
            "boot_id": "boot-probe",
            "execution_request_digest": "sha256:" + "a" * 64,
            "phase": "armed",
        }
        state["frame_digest"] = MODULE.canonical_digest(state)
        self.assertEqual(
            "boot-probe",
            MODULE.validate_armed_state(
                state, "sandbox-probe", "sha256:" + "a" * 64, "boot-probe"
            ),
        )
        changed = dict(state, boot_id="other-boot")
        changed["frame_digest"] = MODULE.canonical_digest(
            {key: value for key, value in changed.items() if key != "frame_digest"}
        )
        with self.assertRaisesRegex(ValueError, "expected inert Armed"):
            MODULE.validate_armed_state(
                changed, "sandbox-probe", "sha256:" + "a" * 64, "boot-probe"
            )
        with self.assertRaisesRegex(ValueError, "not closed"):
            MODULE.validate_armed_state(
                dict(state, future=True), "sandbox-probe", "sha256:" + "a" * 64
            )
        boolean_schema = dict(state, schema_version=True)
        boolean_schema["frame_digest"] = MODULE.canonical_digest(
            {key: value for key, value in boolean_schema.items() if key != "frame_digest"}
        )
        with self.assertRaisesRegex(ValueError, "expected inert Armed"):
            MODULE.validate_armed_state(
                boolean_schema, "sandbox-probe", "sha256:" + "a" * 64
            )

    def test_l4_script_has_no_static_or_activation_path(self):
        source = L4.read_text()
        retired_request = ROOT / "deploy/kind/probes" / ("opensandbox-" + "smoke-request.json")
        self.assertFalse(retired_request.exists())
        self.assertIn("opensandbox-l4-probe.py", source)
        self.assertIn("/proxy/18080/v2/state", source)
        self.assertNotIn("/v2/" + "activate", source)
        self.assertNotIn("/v2/" + "result", source)
        self.assertNotIn("activation_" + "token_digest", source)
        self.assertIn("INSIGHT_KIND_SANDBOX_PACKAGE_REPOSITORY:-}", source)
        self.assertIn("INSIGHT_KIND_SANDBOX_PACKAGE_DIGEST:-}", source)
        self.assertIn("[a-z0-9._/-]+$", source)
        self.assertIn("after_batchsandbox_uid", source)
        self.assertIn("expected_boot_id", source)
        self.assertIn("opensandbox_probe_provisioning_label", source)
        self.assertIn("no activation was sent", source)
        self.assertIn('.env.EXECD_ACCESS_TOKEN = "[redacted]"', source)

    @staticmethod
    def decode_metadata_digest(value):
        raw = value.removeprefix("v1-")
        return base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))

    @staticmethod
    def run_probe(*arguments, check=True):
        environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        return subprocess.run(
            [sys.executable, str(PROBE), *arguments],
            check=check,
            text=True,
            capture_output=True,
            env=environment,
        )


if __name__ == "__main__":
    unittest.main()
