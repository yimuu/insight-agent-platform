import importlib.util
import pathlib
import unittest


SCRIPT = pathlib.Path(__file__).parents[1] / "check-platform-production-topology.py"
SPEC = importlib.util.spec_from_file_location("production_topology", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def node(name, labels, ready=True, unschedulable=False):
    return {
        "metadata": {
            "name": name,
            "labels": {
                "kubernetes.io/os": "linux",
                "kubernetes.io/arch": "amd64",
                **labels,
            },
        },
        "spec": {"unschedulable": unschedulable},
        "status": {
            "conditions": [{"type": "Ready", "status": "True" if ready else "False"}]
        },
    }


def valid_inputs():
    version = {
        "clientVersion": {"gitVersion": "v1.34.1"},
        "serverVersion": {"gitVersion": "v1.35.0"},
    }
    nodes = {
        "items": [
            node(
                "gvisor-1",
                {
                    MODULE.GVISOR_LABEL: "true",
                    MODULE.ATTESTOR_LABEL: "true",
                },
            ),
            node(
                "wasi-1",
                {
                    MODULE.WASI_LABEL: "true",
                    MODULE.ATTESTOR_LABEL: "true",
                },
            ),
        ]
    }
    runtime_class = {
        "apiVersion": "node.k8s.io/v1",
        "kind": "RuntimeClass",
        "metadata": {"name": "runsc"},
        "handler": "runsc",
        "scheduling": {
            "nodeSelector": {
                "kubernetes.io/arch": "amd64",
                "kubernetes.io/os": "linux",
                MODULE.GVISOR_LABEL: "true",
            }
        },
    }
    return version, nodes, runtime_class


class ProductionTopologyTests(unittest.TestCase):
    def test_accepts_disjoint_ready_wasi_and_runsc_pools(self):
        summary, digest = MODULE.validate_topology(*valid_inputs())
        self.assertEqual(summary["gvisor_node_count"], 1)
        self.assertEqual(summary["wasi_node_count"], 1)
        self.assertRegex(digest, r"^sha256:[0-9a-f]{64}$")

    def test_rejects_runc_fallback_and_pool_overlap(self):
        version, nodes, runtime_class = valid_inputs()
        nodes["items"][0]["metadata"]["labels"][MODULE.WASI_LABEL] = "true"
        runtime_class["handler"] = "runc"
        with self.assertRaisesRegex(ValueError, "overlap"):
            MODULE.validate_topology(version, nodes, runtime_class)

    def test_rejects_single_node_and_missing_attestor(self):
        version, nodes, runtime_class = valid_inputs()
        nodes["items"] = nodes["items"][:1]
        del nodes["items"][0]["metadata"]["labels"][MODULE.ATTESTOR_LABEL]
        with self.assertRaisesRegex(ValueError, "at least two"):
            MODULE.validate_topology(version, nodes, runtime_class)

    def test_rejects_unsupported_kubectl_version_skew(self):
        version, nodes, runtime_class = valid_inputs()
        version["clientVersion"]["gitVersion"] = "v1.32.0"
        with self.assertRaisesRegex(ValueError, "version skew"):
            MODULE.validate_topology(version, nodes, runtime_class)


if __name__ == "__main__":
    unittest.main()
