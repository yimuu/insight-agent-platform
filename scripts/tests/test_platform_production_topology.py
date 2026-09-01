import importlib.util
import pathlib
import unittest


SCRIPT = pathlib.Path(__file__).parents[1] / "check-platform-production-topology.py"
SPEC = importlib.util.spec_from_file_location("production_topology", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def node(name, runtime="containerd://2.1.4", ready=True):
    return {
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": name,
            "labels": {"kubernetes.io/os": "linux", "kubernetes.io/arch": "amd64"},
        },
        "spec": {},
        "status": {
            "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
            "nodeInfo": {"containerRuntimeVersion": runtime},
        },
    }


def service(name):
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "namespace": MODULE.CONTROL_NAMESPACE,
            "name": name,
            "labels": {"app.kubernetes.io/name": "insight-platform-sandbox"},
        },
        "spec": {"type": "ClusterIP", "ports": [{"port": 8080}]},
    }


def valid_inputs():
    version = {
        "clientVersion": {"gitVersion": "v1.34.1"},
        "serverVersion": {"gitVersion": "v1.35.0"},
    }
    nodes = {"apiVersion": "v1", "kind": "List", "items": [node("worker-1"), node("worker-2")]}
    crd = {
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {
            "name": "batchsandboxes.sandbox.opensandbox.io",
            "annotations": {
                "insight.platform/upstream-commit": MODULE.SOURCE_COMMIT,
            },
        },
        "spec": {
            "group": "sandbox.opensandbox.io",
            "scope": "Namespaced",
            "names": {"kind": "BatchSandbox", "plural": "batchsandboxes"},
            "versions": [{"name": "v1alpha1", "served": True, "storage": True}],
        },
        "status": {"conditions": [{"type": "Established", "status": "True"}]},
    }
    services = {
        "apiVersion": "v1",
        "kind": "List",
        "items": [
            service("sandbox-dispatcher"),
            service("opensandbox-server"),
            service("opensandbox-controller-metrics"),
        ],
    }
    ingresses = {"apiVersion": "v1", "kind": "List", "items": []}
    return version, nodes, crd, services, ingresses


class ProductionTopologyTests(unittest.TestCase):
    def test_accepts_exact_opensandbox_containerd_topology(self):
        summary, digest = MODULE.validate_topology(*valid_inputs())
        self.assertEqual(summary["provider"], "opensandbox_kubernetes")
        self.assertEqual(summary["ready_schedulable_node_count"], 2)
        self.assertRegex(digest, r"^sha256:[0-9a-f]{64}$")

    def test_rejects_non_containerd_runtime_and_public_ingress(self):
        version, nodes, crd, services, ingresses = valid_inputs()
        nodes["items"][0]["status"]["nodeInfo"]["containerRuntimeVersion"] = "docker://28.0"
        ingresses["items"].append({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "Ingress",
            "metadata": {"namespace": MODULE.CONTROL_NAMESPACE, "name": "public"},
        })
        with self.assertRaisesRegex(ValueError, "required containerd runtime"):
            MODULE.validate_topology(version, nodes, crd, services, ingresses)

    def test_rejects_single_node_or_unestablished_crd(self):
        version, nodes, crd, services, ingresses = valid_inputs()
        nodes["items"] = nodes["items"][:1]
        crd["status"]["conditions"] = []
        with self.assertRaisesRegex(ValueError, "at least two"):
            MODULE.validate_topology(version, nodes, crd, services, ingresses)

    def test_rejects_unsupported_kubectl_version_skew(self):
        version, nodes, crd, services, ingresses = valid_inputs()
        version["clientVersion"]["gitVersion"] = "v1.32.0"
        with self.assertRaisesRegex(ValueError, "version skew"):
            MODULE.validate_topology(version, nodes, crd, services, ingresses)


if __name__ == "__main__":
    unittest.main()
