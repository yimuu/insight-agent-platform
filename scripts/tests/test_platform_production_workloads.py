import copy
import importlib.util
import pathlib
import unittest


SCRIPT = pathlib.Path(__file__).parents[1] / "check-platform-production-workloads.py"
SPEC = importlib.util.spec_from_file_location("production_workloads", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
DEPLOYMENT_CONFIG_DIGEST = "sha256:" + "a" * 64


def kubernetes_list(*resources):
    return {"apiVersion": "v1", "kind": "List", "items": list(resources)}


def labels(role):
    return {MODULE.ROLE_LABEL: role, "app.kubernetes.io/name": role.replace("_", "-")}


def pod_spec(role, digest):
    kubernetes_clients = {"opensandbox_server", "opensandbox_controller"}
    return {
        "serviceAccountName": role.replace("_", "-"),
        "automountServiceAccountToken": role in kubernetes_clients,
        "securityContext": {
            "runAsNonRoot": True,
            "seccompProfile": {"type": "RuntimeDefault"},
        },
        "containers": [
            {
                "name": role.replace("_", "-"),
                "image": f"registry.example/insight/{role}@{digest}",
                "securityContext": {
                    "allowPrivilegeEscalation": False,
                    "readOnlyRootFilesystem": True,
                    "capabilities": {"drop": ["ALL"]},
                },
                "resources": {
                    "requests": {"cpu": "100m", "memory": "128Mi", "ephemeral-storage": "64Mi"},
                    "limits": {"cpu": "1", "memory": "512Mi", "ephemeral-storage": "1Gi"},
                },
            }
        ],
    }


def deployment(role, digest):
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"namespace": "platform", "name": role, "generation": 3},
        "spec": {
            "replicas": 2,
            "selector": {"matchLabels": labels(role)},
            "template": {
                "metadata": {
                    "labels": labels(role),
                    "annotations": {
                        MODULE.CONFIG_DIGEST_ANNOTATION: DEPLOYMENT_CONFIG_DIGEST
                    },
                },
                "spec": pod_spec(role, digest),
            },
        },
        "status": {
            "observedGeneration": 3,
            "updatedReplicas": 2,
            "readyReplicas": 2,
            "unavailableReplicas": 0,
        },
    }


def pdb(role):
    return {
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": {"namespace": "platform", "name": role},
        "spec": {"minAvailable": 1, "selector": {"matchLabels": labels(role)}},
    }


def hpa(role):
    return {
        "apiVersion": "autoscaling/v2",
        "kind": "HorizontalPodAutoscaler",
        "metadata": {"namespace": "platform", "name": role},
        "spec": {
            "minReplicas": 2,
            "maxReplicas": 4,
            "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": role},
        },
    }


def valid_inputs():
    digests = {
        role: "sha256:" + format(index + 1, "064x")
        for index, role in enumerate(sorted(MODULE.COMPONENT_ROLES))
    }
    candidate = {
        "component_images": digests,
        "deployment_config_digest": DEPLOYMENT_CONFIG_DIGEST,
    }
    capacity = {
        "deployment_config_digest": candidate["deployment_config_digest"],
        "replicas": {role: {"min_replicas": 2, "max_replicas": 4} for role in digests},
        "hpa": {role: {"target_utilization_basis_points": 7000} for role in digests},
    }
    deployments = []
    pdbs = []
    hpas = []
    for role, digest in digests.items():
        deployments.append(deployment(role, digest))
        hpas.append(hpa(role))
        pdbs.append(pdb(role))
    policies = [
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"namespace": "platform", "name": "default-deny"},
            "spec": {"podSelector": {}, "policyTypes": ["Ingress", "Egress"]},
        }
    ]
    return (
        candidate,
        capacity,
        kubernetes_list(*deployments),
        kubernetes_list(),
        kubernetes_list(*policies),
        kubernetes_list(*pdbs),
        kubernetes_list(*hpas),
    )


class ProductionWorkloadTests(unittest.TestCase):
    def test_accepts_exact_ready_candidate_and_capacity_closure(self):
        summary, digest = MODULE.validate_workloads(*valid_inputs())
        self.assertEqual(set(summary["roles"]), MODULE.COMPONENT_ROLES)
        self.assertRegex(digest, r"^sha256:[0-9a-f]{64}$")

    def test_rejects_missing_role_and_mutable_image(self):
        inputs = list(copy.deepcopy(valid_inputs()))
        inputs[2]["items"] = [
            item for item in inputs[2]["items"] if MODULE.role_of(item) != "management_api"
        ]
        inputs[2]["items"][0]["spec"]["template"]["spec"]["containers"][0]["image"] = "registry.example/model:latest"
        with self.assertRaisesRegex(ValueError, "management_api must have at least one"):
            MODULE.validate_workloads(*inputs)

    def test_rejects_rollout_image_and_replica_drift(self):
        inputs = list(copy.deepcopy(valid_inputs()))
        workload = inputs[2]["items"][0]
        workload["status"]["readyReplicas"] = 1
        workload["spec"]["replicas"] = 8
        workload["spec"]["template"]["spec"]["containers"][0]["image"] = (
            "registry.example/drift@sha256:" + "f" * 64
        )
        with self.assertRaisesRegex(ValueError, "rollout is not fully updated"):
            MODULE.validate_workloads(*inputs)

    def test_rejects_pod_template_deployment_configuration_drift(self):
        inputs = list(copy.deepcopy(valid_inputs()))
        workload = inputs[2]["items"][0]
        workload["spec"]["template"]["metadata"]["annotations"][
            MODULE.CONFIG_DIGEST_ANNOTATION
        ] = "sha256:" + "f" * 64
        with self.assertRaisesRegex(
            ValueError, "deployment configuration digest differs from CandidateManifest"
        ):
            MODULE.validate_workloads(*inputs)

    def test_rejects_shared_identity_and_missing_default_deny(self):
        inputs = list(copy.deepcopy(valid_inputs()))
        first, second = inputs[2]["items"][:2]
        second["spec"]["template"]["spec"]["serviceAccountName"] = first["spec"]["template"]["spec"]["serviceAccountName"]
        inputs[4]["items"] = []
        with self.assertRaisesRegex(ValueError, "shares ServiceAccount"):
            MODULE.validate_workloads(*inputs)

    def test_rejects_security_baseline_and_hpa_drift(self):
        inputs = list(copy.deepcopy(valid_inputs()))
        role = MODULE.role_of(inputs[2]["items"][0])
        container = inputs[2]["items"][0]["spec"]["template"]["spec"]["containers"][0]
        del container["resources"]["limits"]["ephemeral-storage"]
        matching_hpa = next(h for h in inputs[6]["items"] if h["metadata"]["name"] == role)
        matching_hpa["spec"]["maxReplicas"] = 9
        with self.assertRaisesRegex(ValueError, "ephemeral-storage"):
            MODULE.validate_workloads(*inputs)

    def test_accepts_multiple_isolated_pools_for_one_component_role(self):
        inputs = list(copy.deepcopy(valid_inputs()))
        original = next(
            item for item in inputs[2]["items"] if MODULE.role_of(item) == "context_worker"
        )
        remote = copy.deepcopy(original)
        remote["metadata"]["name"] = "context_worker_remote"
        remote["spec"]["template"]["spec"]["serviceAccountName"] = "context-worker-remote"
        inputs[2]["items"].append(remote)
        original_hpa = next(
            item for item in inputs[6]["items"] if item["metadata"]["name"] == "context_worker"
        )
        remote_hpa = copy.deepcopy(original_hpa)
        remote_hpa["metadata"]["name"] = "context_worker_remote"
        remote_hpa["spec"]["scaleTargetRef"]["name"] = "context_worker_remote"
        inputs[6]["items"].append(remote_hpa)
        inputs[1]["replicas"]["context_worker"] = {"min_replicas": 4, "max_replicas": 8}

        summary, _ = MODULE.validate_workloads(*inputs)
        self.assertEqual(len(summary["roles"]["context_worker"]["workloads"]), 2)
        self.assertEqual(summary["roles"]["context_worker"]["min_replicas"], 4)
        self.assertEqual(summary["roles"]["context_worker"]["max_replicas"], 8)


if __name__ == "__main__":
    unittest.main()
