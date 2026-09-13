"""Prove the required installation lane uses actual immutable images and both consumers."""
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class InstallationCiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = json.loads(subprocess.check_output([
            "ruby", "-ryaml", "-rjson", "-e",
            "puts JSON.generate(YAML.safe_load(File.read(ARGV.fetch(0))))",
            str(ROOT/".github/workflows/ci.yml")], timeout=10))
        cls.job = cls.workflow["jobs"]["installation"]
        cls.steps = cls.job["steps"]

    def step(self, name):
        matches = [step for step in self.steps if step.get("name") == name]
        self.assertEqual(len(matches), 1)
        return matches[0]

    def python_step(self, name):
        script = self.step(name)["run"]
        match = re.fullmatch(r"python3 - <<'PY'\n(.*)\nPY\n?", script, re.S)
        self.assertIsNotNone(match)
        return match.group(1)

    def test_oci_store_is_fixed_and_enabled_before_build(self):
        setup = self.step("Enable the OCI image store before building installation images")
        self.assertEqual(setup["uses"], "docker/setup-docker-action@e43656e248c0bd0647d3f5c195d116aacf6fcaf4")
        self.assertEqual(setup["with"]["version"], "v28.0.4")
        self.assertEqual(json.loads(setup["with"]["daemon-config"]), {"features": {"containerd-snapshotter": True}})
        self.assertLess(self.steps.index(setup), self.steps.index(self.step("Build actual installation images")))
        self.assertIn("check-kind-image-store.sh", self.step("Require repository image descriptors")["run"])
        buildx = self.step("Use the Engine's BuildKit with its matching Buildx release")
        self.assertEqual(buildx["with"], {"version": "v0.22.0", "driver": "docker"})
        build = self.step("Build actual installation images")
        self.assertLess(self.steps.index(buildx), self.steps.index(build))
        self.assertEqual(build["run"].count("docker buildx build --load"), 2)
        self.assertIn("sigs.k8s.io/kind@v0.33.0", self.step("Install the fixed Kind client")["run"])
        helm = [step for step in self.steps if step.get("uses", "").startswith("azure/setup-helm@")]
        self.assertEqual(helm[0]["with"]["version"], "v4.2.3")

    def test_public_trust_consumer_is_in_the_required_contract_lane(self):
        steps = self.workflow["jobs"]["lint"]["steps"]
        selected = [step for step in steps if step.get("name") == "Verify shared installation Compose and Helm consumers"]
        self.assertEqual(len(selected), 1)
        self.assertIn("tools/tests/test_public_trust.py", selected[0]["run"])
        self.assertIn("tools/tests/test_installation_compose_public_trust.py", selected[0]["run"])

    def test_actual_consumers_are_sequential_and_required(self):
        self.assertEqual(self.job["if"], "needs.changes.outputs.runtime == 'true' || needs.changes.outputs.console == 'true'")
        compose = self.step("Qualify fresh initialization, readonly verification and stopped restart")
        kind = self.step("Qualify actual Helm after Compose fixture cleanup")
        tls = self.step("Qualify explicit AWS adapter with shared TLS and actual SDK")
        self.assertLess(self.steps.index(self.step("Resolve and verify actual repository image descriptors")), self.steps.index(tls))
        self.assertLess(self.steps.index(self.step("Cache exact installation dependency and Kind node images")), self.steps.index(tls))
        s3 = self.step("Qualify durable S3 version and controlled reconstruction primitives")
        self.assertLess(self.steps.index(tls), self.steps.index(s3))
        self.assertLess(self.steps.index(s3), self.steps.index(compose))
        self.assertIn("qualify-platform-installation-s3.py", s3["run"])
        self.assertIn("set -euo pipefail", s3["run"])
        self.assertNotIn("continue-on-error", s3)
        self.assertNotIn("if", s3)
        self.assertIn("qualify-platform-installation-tls.py", tls["run"])
        self.assertIn("set -euo pipefail", tls["run"])
        self.assertNotIn("continue-on-error", tls)
        self.assertNotIn("if", tls)
        self.assertLess(self.steps.index(compose), self.steps.index(kind))
        for step, script in ((compose, "qualify-platform-installation-compose.py"), (kind, "qualify-platform-installation-kind.py")):
            self.assertIn(script, step["run"])
            self.assertIn("set -euo pipefail", step["run"])
            self.assertIn('"$PLATFORM_INSTALLATION_RUNTIME_IMAGE"', step["run"])
            self.assertIn('"$PLATFORM_INSTALLATION_CONSOLE_IMAGE"', step["run"])
            self.assertNotIn("continue-on-error", step)
            self.assertNotIn("if", step)

    def test_only_safe_closed_evidence_files_are_uploaded(self):
        upload = self.step("Retain safe installation qualification evidence")
        self.assertEqual(upload["if"], "${{ always() }}")
        self.assertEqual(set(upload["with"]["path"].splitlines()), {
            "${{ runner.temp }}/installation-images.json", "${{ runner.temp }}/installation-compose.log",
            "${{ runner.temp }}/installation-tls.log",
            "${{ runner.temp }}/installation-s3.log", "${{ runner.temp }}/installation-s3.json",
            "${{ runner.temp }}/installation-kind.log", "${{ runner.temp }}/installation-kind.json"})
        self.assertNotIn("include-hidden-files", upload["with"])

    def image_resolution(self, temporary, behavior):
        root = Path(temporary)
        fake = root/"docker"
        fake.write_text("#!"+sys.executable+"\n"+'''import json, os, sys
reference = sys.argv[3]
role = "runtime" if "runtime" in reference else "console"
digest = "sha256:"+("a" if role == "runtime" else "b")*64
value = {"Id": "sha256:"+"0"*64, "Descriptor": {"digest": digest}, "Os": "linux", "Architecture": "amd64"}
if os.environ["FIXTURE_BEHAVIOR"] == "missing":
    value.pop("Descriptor")
if os.environ["FIXTURE_BEHAVIOR"] == "drift" and "@" in reference:
    value["Descriptor"]["digest"] = "sha256:"+"c"*64
print(json.dumps([value]))
''')
        fake.chmod(0o500)
        environment = dict(os.environ, PATH=str(root)+os.pathsep+os.environ["PATH"], GITHUB_ENV=str(root/"environment"), RUNNER_TEMP=str(root), FIXTURE_BEHAVIOR=behavior)
        return subprocess.run([sys.executable, "-c", self.python_step("Resolve and verify actual repository image descriptors")], cwd=ROOT, env=environment,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)

    def test_image_resolution_uses_descriptor_not_configuration_id(self):
        with tempfile.TemporaryDirectory() as temporary:
            result = self.image_resolution(temporary, "valid")
            self.assertEqual(result.returncode, 0, result.stderr)
            evidence = json.loads((Path(temporary)/"installation-images.json").read_text())
            self.assertEqual(evidence["runtime"]["reference"], "insight-installation-runtime@sha256:"+"a"*64)
            self.assertEqual(evidence["console"]["reference"], "insight-installation-console@sha256:"+"b"*64)
            self.assertNotIn("0"*64, (Path(temporary)/"environment").read_text())

    def test_missing_or_changed_repository_descriptor_stops_before_consumers(self):
        for behavior in ("missing", "drift"):
            with tempfile.TemporaryDirectory() as temporary:
                result = self.image_resolution(temporary, behavior)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((Path(temporary)/"installation-images.json").exists())
                self.assertFalse((Path(temporary)/"environment").exists())

    def test_dependency_cache_reads_owning_digests_without_mutable_tags(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fake = root/"docker"
            dependencies = {name: "fixture/"+name+"@sha256:"+digit*64 for name, digit in zip(("postgres", "nats", "s3", "openbao"), "abcd")}
            fake.write_text("#!"+sys.executable+"\n"+'''import json,os,sys
with open(os.environ['FIXTURE_CALLS'],'a') as output:
    output.write(json.dumps(sys.argv[1:])+'\\n')
if 'kubernetes-input' in sys.argv:
    print('{"schema_version":1}')
elif 'helm-values' in sys.argv:
    print(json.dumps({'plan': {'dependencies': json.loads(os.environ['FIXTURE_DEPENDENCIES'])}}))
''')
            fake.chmod(0o500)
            environment = dict(os.environ, PATH=str(root)+os.pathsep+os.environ["PATH"], FIXTURE_CALLS=str(root/"calls"),
                FIXTURE_DEPENDENCIES=json.dumps(dependencies), PLATFORM_INSTALLATION_RUNTIME_IMAGE="fixture/runtime@sha256:"+"e"*64,
                PLATFORM_INSTALLATION_CONSOLE_IMAGE="fixture/console@sha256:"+"f"*64)
            subprocess.run([sys.executable, "-c", self.python_step("Cache exact installation dependency and Kind node images")], cwd=ROOT, env=environment, check=True, timeout=10)
            calls = [json.loads(line) for line in (root/"calls").read_text().splitlines()]
            self.assertIn('kubernetes-input', calls[0])
            self.assertIn('helm-values', calls[1])
            for call in calls[:2]:
                self.assertIn('none', call)
                self.assertIn('--read-only', call)
                self.assertNotIn('/var/run/docker.sock', ' '.join(call))
            images = {call[1] for call in calls[2:]}
            self.assertTrue(set(dependencies.values()).issubset(images))
            self.assertEqual(len(images-set(dependencies.values())), 1)
            for call in calls[2:]:
                self.assertEqual(call[0], "pull")
                self.assertRegex(call[1], r"^[a-z0-9._/-]+@sha256:[a-f0-9]{64}$")



if __name__ == "__main__":
    unittest.main()
