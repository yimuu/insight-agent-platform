import copy
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


def resource(kind, name, *, namespace=None, **values):
    metadata = {"name": name}
    if namespace is not None:
        metadata["namespace"] = namespace
    return {
        "apiVersion": "v1",
        "kind": kind,
        "metadata": metadata,
        **values,
    }


def security_inputs():
    accounts = [
        resource(
            "ServiceAccount",
            name,
            namespace=namespace,
            automountServiceAccountToken=automount,
        )
        for (namespace, name), automount in {
            (MODULE.CONTROL_NAMESPACE, "sandbox-dispatcher"): False,
            (MODULE.CONTROL_NAMESPACE, "opensandbox-server"): True,
            (MODULE.CONTROL_NAMESPACE, "opensandbox-controller"): True,
            (MODULE.WORKLOAD_NAMESPACE, "sandbox-workload"): False,
        }.items()
    ]
    roles = [
        resource(
            "Role",
            "opensandbox-server",
            namespace=MODULE.WORKLOAD_NAMESPACE,
            rules=copy.deepcopy(MODULE.EXPECTED_SERVER_RULES),
        ),
        resource(
            "Role",
            "opensandbox-controller-leader-election",
            namespace=MODULE.CONTROL_NAMESPACE,
            rules=copy.deepcopy(MODULE.EXPECTED_CONTROLLER_LEADER_RULES),
        ),
    ]
    role_bindings = [
        resource(
            "RoleBinding",
            "opensandbox-server",
            namespace=MODULE.WORKLOAD_NAMESPACE,
            roleRef={"kind": "Role", "name": "opensandbox-server"},
            subjects=[{
                "kind": "ServiceAccount",
                "name": "opensandbox-server",
                "namespace": MODULE.CONTROL_NAMESPACE,
            }],
        ),
        resource(
            "RoleBinding",
            "opensandbox-controller-leader-election",
            namespace=MODULE.CONTROL_NAMESPACE,
            roleRef={"kind": "Role", "name": "opensandbox-controller-leader-election"},
            subjects=[{
                "kind": "ServiceAccount",
                "name": "opensandbox-controller",
                "namespace": MODULE.CONTROL_NAMESPACE,
            }],
        ),
    ]
    cluster_roles = [
        resource(
            "ClusterRole",
            "sandbox-opensandbox-controller",
            rules=copy.deepcopy(MODULE.EXPECTED_CONTROLLER_RULES),
        ),
        resource(
            "ClusterRole",
            "sandbox-opensandbox-server-namespace",
            rules=copy.deepcopy(MODULE.EXPECTED_SERVER_NAMESPACE_RULES),
        ),
    ]
    cluster_bindings = [
        resource(
            "ClusterRoleBinding",
            "sandbox-opensandbox-controller",
            roleRef={"kind": "ClusterRole", "name": "sandbox-opensandbox-controller"},
            subjects=[{
                "kind": "ServiceAccount",
                "name": "opensandbox-controller",
                "namespace": MODULE.CONTROL_NAMESPACE,
            }],
        ),
        resource(
            "ClusterRoleBinding",
            "sandbox-opensandbox-server-namespace",
            roleRef={"kind": "ClusterRole", "name": "sandbox-opensandbox-server-namespace"},
            subjects=[{
                "kind": "ServiceAccount",
                "name": "opensandbox-server",
                "namespace": MODULE.CONTROL_NAMESPACE,
            }],
        ),
    ]
    expressions = {
        "opensandbox-inactive-surfaces": "false",
        "opensandbox-batchsandbox": (
            f"system:serviceaccount:{MODULE.CONTROL_NAMESPACE}:opensandbox-server "
            f"system:serviceaccount:{MODULE.CONTROL_NAMESPACE}:opensandbox-controller "
            "armed-runner-v2 execd-installer"
        ),
        "opensandbox-pods": (
            f"system:serviceaccount:{MODULE.CONTROL_NAMESPACE}:opensandbox-controller "
            "sandbox-workload platform-sandbox-runner persistentVolumeClaim"
        ),
    }
    policies = []
    bindings = []
    for suffix, expression in expressions.items():
        name = f"sandbox-{suffix}"
        policies.append(resource(
            "ValidatingAdmissionPolicy",
            name,
            spec={"failurePolicy": "Fail", "validations": [{"expression": expression}]},
        ))
        bindings.append(resource(
            "ValidatingAdmissionPolicyBinding",
            name,
            spec={
                "policyName": name,
                "validationActions": ["Deny"],
                "matchResources": {"namespaceSelector": {"matchLabels": {
                    "insight.platform/sandbox-workload-namespace": "true"
                }}},
            },
        ))
    return tuple(
        {"apiVersion": "v1", "kind": "List", "items": values}
        for values in (
            accounts, roles, role_bindings, cluster_roles, cluster_bindings, policies, bindings
        )
    )


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
    return version, nodes, crd, services, ingresses, *security_inputs()


class ProductionTopologyTests(unittest.TestCase):
    def validate(self, inputs):
        expected = MODULE.batchsandbox_crd_digest(inputs[2])
        return MODULE.validate_topology(*inputs, expected_crd_digest=expected)

    def test_accepts_exact_opensandbox_containerd_topology(self):
        summary, digest = self.validate(valid_inputs())
        self.assertEqual(summary["provider"], "opensandbox_kubernetes")
        self.assertEqual(summary["ready_schedulable_node_count"], 2)
        self.assertEqual(
            summary["batchsandbox_crd_digest"],
            MODULE.batchsandbox_crd_digest(valid_inputs()[2]),
        )
        self.assertRegex(digest, r"^sha256:[0-9a-f]{64}$")

    def test_rejects_non_containerd_runtime_and_public_ingress(self):
        inputs = list(valid_inputs())
        inputs[1]["items"][0]["status"]["nodeInfo"]["containerRuntimeVersion"] = "docker://28.0"
        inputs[4]["items"].append({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "Ingress",
            "metadata": {"namespace": MODULE.CONTROL_NAMESPACE, "name": "public"},
        })
        with self.assertRaisesRegex(ValueError, "required containerd runtime"):
            self.validate(inputs)

    def test_rejects_single_node_or_unestablished_crd(self):
        inputs = list(valid_inputs())
        inputs[1]["items"] = inputs[1]["items"][:1]
        inputs[2]["status"]["conditions"] = []
        with self.assertRaisesRegex(ValueError, "at least two"):
            self.validate(inputs)

    def test_rejects_unsupported_kubectl_version_skew(self):
        inputs = list(valid_inputs())
        inputs[0]["clientVersion"]["gitVersion"] = "v1.32.0"
        with self.assertRaisesRegex(ValueError, "version skew"):
            self.validate(inputs)

    def test_rejects_batchsandbox_schema_drift_against_reviewed_digest(self):
        inputs = list(valid_inputs())
        reviewed_digest = MODULE.batchsandbox_crd_digest(inputs[2])
        inputs[2]["spec"]["versions"][0]["schema"] = {
            "openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": True}
        }
        with self.assertRaisesRegex(ValueError, "normalized contract digest drifted"):
            MODULE.validate_topology(
                *inputs,
                expected_crd_digest=reviewed_digest,
            )

    def test_rejects_rbac_or_admission_drift(self):
        inputs = list(valid_inputs())
        inputs[6]["items"][0]["rules"][0]["verbs"].append("watch")
        with self.assertRaisesRegex(ValueError, "Server workload Role drifted"):
            self.validate(inputs)

        inputs = list(valid_inputs())
        inputs[11]["items"][0]["spec"]["validationActions"] = ["Audit"]
        with self.assertRaisesRegex(ValueError, "does not deny"):
            self.validate(inputs)


if __name__ == "__main__":
    unittest.main()
