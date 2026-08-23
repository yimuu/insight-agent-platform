import importlib.util
import pathlib
import unittest


SCRIPT = pathlib.Path(__file__).parents[1] / "qualify-platform-gvisor-admission.py"
SPEC = importlib.util.spec_from_file_location("gvisor_admission", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def source_pod():
    return {
        "metadata": {
            "name": "insight-gv-" + "a" * 32,
            "labels": {
                "app.kubernetes.io/name": "insight-platform-sandbox-guest",
                "app.kubernetes.io/component": "gvisor-single-job",
                "app.kubernetes.io/managed-by": "insight-platform-gvisor-launcher",
            },
            "annotations": {f"insight.platform/{index}": "value" for index in range(7)},
            "uid": "server-field",
        },
        "spec": {
            "nodeName": "gvisor-1",
            "runtimeClassName": "runsc",
            "schedulingGates": [],
            "containers": [
                {
                    "image": "insight-sandbox-guest@sha256:" + "a" * 64,
                    "securityContext": {"privileged": False},
                    "resources": {
                        "requests": {"cpu": "1", "memory": "1Gi"},
                        "limits": {"cpu": "1", "memory": "1Gi"},
                    },
                    "env": [],
                    "volumeMounts": [],
                }
            ],
            "volumes": [
                {
                    "name": "bootstrap-token",
                    "projected": {
                        "sources": [
                            {
                                "serviceAccountToken": {
                                    "audience": "insight.platform/sandbox-guest"
                                }
                            }
                        ]
                    },
                }
            ],
        },
        "status": {"phase": "Running"},
    }


class GvisorAdmissionQualificationTests(unittest.TestCase):
    def test_source_is_sanitized_and_fenced_before_positive_probe(self):
        probe = MODULE.sanitize_source(source_pod(), "platform-sandbox-guests")
        self.assertNotIn("status", probe)
        self.assertNotIn("uid", probe["metadata"])
        self.assertNotIn("nodeName", probe["spec"])
        self.assertEqual(probe["spec"]["schedulingGates"], [MODULE.START_GATE])

    def test_bypass_matrix_covers_runtime_identity_secret_and_host_authority(self):
        probe = MODULE.sanitize_source(source_pod(), "platform-sandbox-guests")
        cases = MODULE.mutation_cases(probe)
        self.assertEqual(
            set(cases),
            {
                "direct_node_binding",
                "ephemeral_container",
                "extra_fence_annotation",
                "extra_volume_mount",
                "host_network",
                "host_path_volume",
                "missing_start_gate",
                "mutable_image",
                "privileged_container",
                "resource_drift",
                "runc_runtime",
                "secret_env_from",
                "secret_env_value",
                "wrong_service_account",
                "wrong_token_audience",
            },
        )
        self.assertEqual(cases["runc_runtime"]["spec"]["runtimeClassName"], "runc")
        self.assertTrue(cases["secret_env_from"]["spec"]["containers"][0]["envFrom"])
        self.assertTrue(cases["host_path_volume"]["spec"]["volumes"][-1]["hostPath"])

    def test_any_accepted_bypass_fails_evidence(self):
        results = [
            {"case": "runc_runtime", "expected_accepted": False, "observed_accepted": True}
        ]
        report = MODULE.evidence("subject", "namespace", "source", True, results)
        self.assertFalse(report["passed"])
        self.assertRegex(report["evidence_digest"], r"^sha256:[0-9a-f]{64}$")

    def test_token_mutation_does_not_depend_on_volume_order(self):
        source = source_pod()
        source["spec"]["volumes"].insert(0, {"name": "scratch", "emptyDir": {}})
        probe = MODULE.sanitize_source(source, "platform-sandbox-guests")
        case = MODULE.mutation_cases(probe)["wrong_token_audience"]
        bootstrap = next(
            volume for volume in case["spec"]["volumes"] if volume["name"] == "bootstrap-token"
        )
        token = bootstrap["projected"]["sources"][0]["serviceAccountToken"]
        self.assertEqual(token["audience"], "kubernetes.default.svc")


if __name__ == "__main__":
    unittest.main()
