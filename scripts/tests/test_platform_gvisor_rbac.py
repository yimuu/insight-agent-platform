import importlib.util
import pathlib
import unittest


SCRIPT = pathlib.Path(__file__).parents[1] / "qualify-platform-gvisor-rbac.py"
SPEC = importlib.util.spec_from_file_location("gvisor_rbac", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class GvisorRbacQualificationTests(unittest.TestCase):
    def test_exact_matrix_passes(self):
        expected = {
            (verb, resource, api_group): allowed
            for verb, resource, api_group, allowed in MODULE.MATRIX
        }
        results = MODULE.evaluate(
            lambda verb, resource, api_group: expected[(verb, resource, api_group)]
        )
        evidence = MODULE.report(
            "system:serviceaccount:platform-sandbox-exec:launcher",
            "platform-sandbox-guests",
            results,
        )
        self.assertTrue(evidence["passed"])
        self.assertIsNone(evidence["failure_code"])
        self.assertRegex(evidence["evidence_digest"], r"^sha256:[0-9a-f]{64}$")

    def test_one_excess_permission_fails_the_matrix(self):
        results = MODULE.evaluate(
            lambda verb, resource, api_group: (
                True
                if (verb, resource, api_group) == ("get", "secrets", "")
                else next(
                    allowed
                    for candidate_verb, candidate_resource, candidate_group, allowed in MODULE.MATRIX
                    if (candidate_verb, candidate_resource, candidate_group)
                    == (verb, resource, api_group)
                )
            )
        )
        evidence = MODULE.report("subject", "namespace", results)
        self.assertFalse(evidence["passed"])

    def test_matrix_contains_every_forbidden_escalation_surface(self):
        denied = {
            (verb, resource, api_group)
            for verb, resource, api_group, allowed in MODULE.MATRIX
            if not allowed
        }
        self.assertTrue(
            {
                ("get", "pods/log", ""),
                ("create", "pods/exec", ""),
                ("create", "pods/attach", ""),
                ("create", "pods/portforward", ""),
                ("update", "pods/ephemeralcontainers", ""),
                ("get", "secrets", ""),
                ("get", "nodes", ""),
                ("get", "runtimeclasses", "node.k8s.io"),
            }.issubset(denied)
        )


if __name__ == "__main__":
    unittest.main()
