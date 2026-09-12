"""Image identity, bounded children and cleanup ownership for the isolated Kind harness."""
import copy
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("installation_kind", ROOT/"tools/qualification/qualify-platform-installation-kind.py")
KIND = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(KIND)


class KindQualificationTests(unittest.TestCase):
    def plan(self, fixture):
        name = "artifact-gateway"
        return {"schema_version": 1, "namespace": fixture.namespace,
                "input": {"name": fixture.namespace, "network": {"topology": "kubernetes_local"}},
                "input_digest": "sha256:"+"e"*64, "runtime_image": fixture.runtime, "console_image": fixture.console,
                "dependencies": {name: name+"@sha256:"+character*64 for name, character in
                                 [("postgres", "1"), ("nats", "2"), ("s3", "3"), ("openbao", "4")]},
                "dependency_commands": {"s3": ["server"]},
                "dependency_stop_grace_seconds": {"s3": 45},
                "processes": [{"name": name, "binary": "platform-artifact-gateway", "uid": 10001,
                    "port": 8080, "observability_port": 9090,
                    "paths": {"process": name, "configuration_directory": "/run/insight/role/config",
                        "credential_directory": "/run/insight/role/credentials", "temporary_directory": "/var/lib/insight"}}]}

    def test_setup_uses_selected_runtime_typed_plan_and_exact_four_dependency_images(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = KIND.Fixture("runtime@sha256:"+"a"*64, "console@sha256:"+"b"*64, root)
            plan = self.plan(fixture)
            with mock.patch.object(fixture, "docker", side_effect=[json.dumps(plan["input"]).encode(), json.dumps({"plan": plan}).encode()]) as docker, mock.patch.object(fixture, "import_image") as imported:
                fixture.setup()
            self.assertEqual(imported.call_args_list, [mock.call(fixture.runtime, "runtime"), mock.call(fixture.console, "console")]+
                             [mock.call(image, name) for name, image in sorted(plan["dependencies"].items())])
            for call in docker.call_args_list:
                command = call.args
                self.assertEqual(command[:8], ("run", "--rm", "--network", "none", "--read-only", "--cap-drop", "ALL", "--security-opt"))
                self.assertEqual(command[command.index("--user")+1], f"{os.geteuid()}:{os.getegid()}")
                self.assertEqual(command[command.index("--entrypoint")+2], fixture.runtime)
            self.assertIn(f"type=bind,source={root/'input.json'},target=/installation-input/input.json,readonly", docker.call_args.args)
            self.assertEqual((root/"input.json").stat().st_mode & 0o777, 0o600)
            self.assertEqual(json.loads((root/"producer-plan.json").read_bytes()), plan)
            self.assertNotIn("localstack", json.dumps(plan))

    def test_plan_drift_legacy_dependencies_mutable_refs_and_ambiguous_json_stop_before_import(self):
        for change in ("input", "runtime", "console", "namespace", "localstack", "missing_s3", "mutable", "duplicate", "nonfinite"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                fixture = KIND.Fixture("runtime@sha256:"+"a"*64, "console@sha256:"+"b"*64, Path(temporary))
                plan = self.plan(fixture)
                original = json.dumps(plan["input"]).encode()
                if change == "input": plan["input"]["extra"] = True
                elif change in {"runtime", "console"}: plan[change+"_image"] = "foreign@sha256:"+"f"*64
                elif change == "namespace": plan["namespace"] = "foreign"
                elif change == "localstack": plan["dependencies"]["localstack"] = "old@sha256:"+"f"*64
                elif change == "missing_s3": del plan["dependencies"]["s3"]
                elif change == "mutable": plan["dependencies"]["s3"] = "s3:latest"
                encoded = json.dumps({"plan": plan}).encode()
                if change == "duplicate": encoded = b'{"plan":{},'+encoded[1:]
                elif change == "nonfinite": encoded = encoded.replace(b'"schema_version": 1', b'"schema_version": NaN')
                with mock.patch.object(fixture, "docker", side_effect=[original, encoded]), mock.patch.object(fixture, "import_image") as imported:
                    with self.assertRaises(Exception): fixture.setup()
                    imported.assert_not_called()

    def tls_pod(self, fixture):
        return {"metadata": {"name": "artifact-gateway-fixture", "namespace": fixture.namespace},
                "spec": {"containers": [{"name": "process", "image": fixture.runtime}]},
                "status": {"containerStatuses": [{"name": "process", "ready": True}]}}

    def test_tls_executes_only_serving_gateway_with_explicit_roots_and_real_openssl3_gate(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = KIND.Fixture("runtime", "console", Path(temporary))
            responses = [json.dumps({"items": [self.tls_pod(fixture)]}).encode(), b"OpenSSL 3.0.16 11 Feb 2025\n",
                         b"tls_probe exit=0 verified=1 issuer=0 hostname=0 other=0\n",
                         b"tls_probe exit=1 verified=0 issuer=1 hostname=0 other=0\n",
                         b"tls_probe exit=1 verified=0 issuer=0 hostname=1 other=0\n"]
            with mock.patch.object(fixture, "kube", side_effect=responses) as kube, mock.patch.object(fixture, "progress"):
                fixture.qualify_tls()
            self.assertEqual(kube.call_count, 5)
            self.assertEqual(kube.call_args_list[1].args[-2:], ("openssl", "version"))
            probes = kube.call_args_list[2:]
            for call in probes:
                self.assertEqual(call.kwargs, {"timeout": 15})
                self.assertEqual(call.args[3:7], ("artifact-gateway-fixture", "--container", "process", "--"))
                self.assertEqual(call.args[7:11], ("/bin/sh", "-c", KIND.LINUX_TLS_PROBE, "installation-tls"))
                self.assertEqual(call.args[11], "s3."+fixture.namespace+".svc.cluster.local")
            self.assertEqual(probes[0].args[-1], "/run/insight/role/credentials/ca.pem")
            self.assertEqual(probes[1].args[-1], "/etc/ssl/certs/ca-certificates.crt")
            self.assertEqual(probes[2].args[-2], "wrong.installation.invalid")
            self.assertTrue(fixture.report["tls"]["unknown_ca_rejected"])

    def test_tls_missing_cli_other_version_foreign_or_terminating_pod_cannot_qualify(self):
        for change in ("missing", "libressl", "openssl1", "image", "namespace", "terminating", "notready"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                fixture = KIND.Fixture("runtime", "console", Path(temporary))
                pod = self.tls_pod(fixture)
                if change == "image": pod["spec"]["containers"][0]["image"] = "foreign"
                elif change == "namespace": pod["metadata"]["namespace"] = "foreign"
                elif change == "terminating": pod["metadata"]["deletionTimestamp"] = "now"
                elif change == "notready": pod["status"]["containerStatuses"][0]["ready"] = False
                version = {"missing": KIND.QualificationFailure("command_failed:kubectl"),
                           "libressl": b"LibreSSL 3.3.6\n", "openssl1": b"OpenSSL 1.1.1\n"}.get(change, b"OpenSSL 3.0.16\n")
                with mock.patch.object(fixture, "kube", side_effect=[json.dumps({"items": [pod]}).encode(), version]) as kube, mock.patch.object(fixture, "progress"):
                    with self.assertRaises(KIND.QualificationFailure): fixture.qualify_tls()
                    self.assertLessEqual(kube.call_count, 2)
                self.assertNotIn("tls", fixture.report)

    def test_tls_closed_parser_never_equates_network_errors_or_partial_checks_with_rejection(self):
        for data in (b"tls_probe exit=124 verified=0 issuer=1 hostname=0 other=0\n",
                     b"tls_probe exit=1 verified=0 issuer=0 hostname=0 other=0\n",
                     b"tls_probe exit=127 verified=0 issuer=0 hostname=0 other=0\n",
                     b"tls_probe exit=1 verified=0 issuer=1 hostname=0 other=1\n",
                     b"tls_probe exit=1 verified=1 issuer=1 hostname=0 other=0\n",
                     b"tls_probe exit=1 verified=0 issuer=1 hostname=0 other=0\nraw PEM\n"):
            with self.subTest(data=data), self.assertRaises(KIND.QualificationFailure):
                KIND.tls_probe_evidence(data, "issuer")

    def test_actual_fixed_tls_shell_filters_raw_output_and_preserves_failed_exit_category(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            KIND.write(root/"timeout", ("#!"+sys.executable+"\nimport os,sys\nos.execvp(sys.argv[4],sys.argv[4:])\n").encode(), 0o500)
            KIND.write(root/"openssl", ("#!"+sys.executable+"\nimport os,sys\nprint('untrusted certificate/private diagnostic',file=sys.stderr)\nprint(os.environ['PROBE_LINE'],file=sys.stderr)\nsys.exit(int(os.environ['PROBE_EXIT']))\n").encode(), 0o500)
            for category, line, code in [("trusted", "Verification: OK", 0), ("issuer", "verify error:num=20:unable to get local issuer certificate", 1),
                                         ("hostname", "verify error:num=62:hostname mismatch", 1)]:
                env = dict(os.environ, PATH=str(root)+os.pathsep+os.environ["PATH"], PROBE_LINE=line, PROBE_EXIT=str(code))
                data = KIND.run(["/bin/sh", "-c", KIND.LINUX_TLS_PROBE, "fixture", "s3.fixture", "s3.fixture", "/public/ca"], timeout=3, environment=env)
                self.assertNotIn(b"untrusted", data)
                KIND.tls_probe_evidence(data, category)

    def test_sdk_diagnostic_parser_accepts_only_complete_closed_lines(self):
        for operation in KIND.SDK_OPERATIONS:
            for failure in KIND.SDK_FAILURES:
                line = f"installation_aws operation={operation} failure={failure}\n".encode()
                self.assertEqual(KIND.sdk_diagnostic_entries(line), [{"operation": operation, "failure": failure}])
        good = b"installation_aws operation=kms_create_key failure=dispatch\n"
        raw = b"private-error https://user:password@secret.invalid/key\n"
        rejected = [raw, b"prefix "+good, good.rstrip()+b" secret=private\n", good.replace(b"kms_create_key", b"other_operation"), good.replace(b"dispatch", b"other_failure"), good.replace(b" ", b"  ", 1), good.replace(b"\n", b"\r\n")]
        self.assertEqual(KIND.sdk_diagnostic_entries(b"".join(rejected)), [])
        self.assertEqual(KIND.sdk_diagnostic_entries(raw+good+raw), [{"operation": "kms_create_key", "failure": "dispatch"}])
        self.assertEqual(len(KIND.sdk_diagnostic_entries(good*32)), 32)
        for data in [good*33, b"x"*16_385]:
            with self.assertRaises(KIND.QualificationFailure): KIND.sdk_diagnostic_entries(data)

    def test_s3_diagnostics_use_only_the_eight_bucket_operations(self):
        self.assertEqual(KIND.S3_OPERATIONS, {
            "s3_head_bucket", "s3_get_bucket_tagging", "s3_get_bucket_versioning", "s3_get_bucket_cors",
            "s3_create_bucket", "s3_put_bucket_tagging", "s3_put_bucket_versioning", "s3_put_bucket_cors",
        })
        for operation in KIND.SDK_OPERATIONS:
            for failure in KIND.SDK_FAILURES:
                line = f"installation_s3 operation={operation} failure={failure}\n".encode()
                expected = [{"operation": operation, "failure": failure}] if operation in KIND.S3_OPERATIONS else []
                self.assertEqual(KIND.sdk_diagnostic_entries(line), expected)
        good = b"installation_s3 operation=s3_create_bucket failure=service\n"
        for line in [b"prefix "+good, good.rstrip()+b" secret=private\n", good.replace(b"service", b"private_value")]:
            self.assertEqual(KIND.sdk_diagnostic_entries(line), [])
        with self.assertRaises(KIND.QualificationFailure):
            KIND.sdk_diagnostic_entries(good * 33)

    def test_private_delivery_is_host_owned_atomic_and_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            os.chmod(parent, 0o700)
            target = parent/'session-token'
            for data in (b'fixture-first', b'fixture-renewed'):
                KIND.publish_delivery(target, data)
                self.assertEqual(target.read_bytes(), data)
                self.assertEqual(target.stat().st_uid, os.geteuid())
                self.assertEqual(target.stat().st_mode & 0o777, 0o600)
                self.assertEqual(parent.stat().st_mode & 0o777, 0o700)
                self.assertEqual(list(parent.iterdir()), [target])
            for data in (b'', b'x' * 1_048_577):
                with self.assertRaises(KIND.QualificationFailure):
                    KIND.publish_delivery(target, data)
                self.assertEqual(target.read_bytes(), b'fixture-renewed')
            with mock.patch.object(KIND.os, 'replace', side_effect=OSError('fixture')):
                with self.assertRaises(OSError):
                    KIND.publish_delivery(target, b'fixture-failed')
            self.assertEqual(target.read_bytes(), b'fixture-renewed')
            self.assertEqual(list(parent.iterdir()), [target])

    def test_session_delivery_uses_bounded_capture_without_node_host_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = KIND.Fixture('runtime', 'console', Path(directory))
            fixture.installation = Path(directory)/'delivery'
            fixture.installation.mkdir(mode=0o700)
            calls = []
            def kube(*args, **kwargs):
                calls.append(args)
                if args[0] == 'get':
                    return json.dumps({'items': [{'metadata': {'name': 'fixture-pod'}}]}).encode()
                if args[0] == 'exec':
                    self.assertEqual(args[1:8], ('-n', fixture.namespace, 'fixture-pod', '-c', 'delivery', '--', '/usr/bin/head'))
                    self.assertEqual(args[8:10], ('-c', '1048577'))
                    self.assertIn(args[10], ('/delivery/result.json', '/delivery/public-ca.pem', '/delivery/session-token'))
                    return b'fixture-private'
                return b''
            with mock.patch.object(fixture, 'kube', side_effect=kube):
                fixture.operation('session')
                fixture.operation('session')
                fixture.operation('public-trust')
            self.assertNotIn('cp', [call[0] for call in calls])
            self.assertEqual(sum(call[0] == 'exec' for call in calls), 8)
            self.assertEqual((fixture.installation/'session-token').stat().st_mode & 0o777, 0o600)

    def test_installer_diagnostics_are_exact_closed_owner_errors(self):
        for error in KIND.INSTALLATION_ERRORS:
            self.assertEqual(KIND.installation_diagnostic_entries(f"installation {error}\n".encode()), [error])
        for data in [b"installation secret\n", b"prefix installation Incomplete\n",
                     b"installation Incomplete secret=canary\n", b"installation Incomplete\r\n"]:
            self.assertEqual(KIND.installation_diagnostic_entries(data), [])
        for data in [b"x" * 16_385, b"installation Incomplete\n" * 33]:
            with self.assertRaises(KIND.QualificationFailure):
                KIND.installation_diagnostic_entries(data)
        owner = (ROOT/"crates/deployment/platform-deployment-contracts/src/installation.rs").read_text()
        variants = owner.split("pub enum InstallationError {", 1)[1].split("}", 1)[0]
        self.assertEqual({line.strip().rstrip(",") for line in variants.splitlines() if line.strip()}, KIND.INSTALLATION_ERRORS)

    def test_diagnostics_sanitize_failed_installer_logs(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture=KIND.Fixture("runtime", "console", Path(directory))
            pod={"metadata":{"name":"installation-1-fixture","namespace":fixture.namespace,
                 "ownerReferences":[{"kind":"Job","name":"installation-1","controller":True}]},
                 "spec":{"containers":[{"name":"installation","image":"runtime"}]},"status":{"phase":"Failed"}}
            with mock.patch.object(fixture,"kube",return_value=b"private-error\ninstallation_aws operation=kms_create_key failure=dispatch\ninstallation CredentialInvalid\n"):
                result=fixture.failure_sdk_diagnostics([pod])
            self.assertEqual(result,{"status":"observed","entries":[{"operation":"kms_create_key","failure":"dispatch"}],"installation_errors":["CredentialInvalid"]})
            pod['metadata']['namespace']='foreign'
            with mock.patch.object(fixture,"kube") as kube:
                self.assertEqual(fixture.failure_sdk_diagnostics([pod])['status'],'unknown')
                kube.assert_not_called()

    def test_controller_recovery_waits_for_old_pod_to_disappear(self):
        old = {"metadata": {"uid": "old", "deletionTimestamp": "2026-01-01T00:00:00Z"},
               "status": {"conditions": [{"type": "Ready", "status": "True"}]}}
        new = {"metadata": {"uid": "new"},
               "status": {"conditions": [{"type": "Ready", "status": "True"}]}}
        self.assertIsNone(KIND.completed_controller_replacement([old, new], "old"))
        self.assertIsNone(KIND.completed_controller_replacement([old], "old"))
        self.assertIsNone(KIND.completed_controller_replacement([], "old"))
        self.assertIs(KIND.completed_controller_replacement([new], "old"), new)
        new["metadata"]["deletionTimestamp"] = "2026-01-01T00:00:01Z"
        self.assertIsNone(KIND.completed_controller_replacement([new], "old"))
        del new["metadata"]["deletionTimestamp"]
        new["status"]["conditions"][0]["status"] = "False"
        self.assertIsNone(KIND.completed_controller_replacement([new], "old"))

    def test_controller_recovery_timeout_never_accepts_late_or_overlapping_pods(self):
        ready = lambda uid: {"metadata": {"uid": uid}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}
        with tempfile.TemporaryDirectory() as temporary:
            fixture = KIND.Fixture("runtime", "console", Path(temporary))
            for pods in ([ready("old"), ready("new")], [ready("new")]):
                with mock.patch.object(fixture, "kube", return_value=json.dumps({"items": pods}).encode()) as kube, mock.patch.object(KIND.time, "monotonic", side_effect=[0, 0, 121]):
                    with self.assertRaisesRegex(KIND.QualificationFailure, "controller_recovery_timeout"):
                        fixture.wait_controller_replacement("old")
                    self.assertEqual(kube.call_args.args[:2], ("get", "pods"))
                    self.assertEqual(kube.call_args.kwargs["timeout"], 15)
            responses = [json.dumps({"items": [ready("old"), ready("new")]}).encode(), json.dumps({"items": [ready("new")]}).encode()]
            with mock.patch.object(fixture, "kube", side_effect=responses), mock.patch.object(KIND.time, "monotonic", side_effect=[0, 0, 1, 2, 3]), mock.patch.object(KIND.time, "sleep") as sleep:
                self.assertEqual(fixture.wait_controller_replacement("old")["metadata"]["uid"], "new")
                sleep.assert_called_once_with(1)

    def archive(self, path, *, changed_layer=False, missing_index=False, architecture="arm64"):
        contents = {}
        def descriptor(value, media):
            data = value if isinstance(value, bytes) else json.dumps(value, separators=(",", ":")).encode()
            digest = "sha256:"+hashlib.sha256(data).hexdigest()
            contents["blobs/sha256/"+digest[7:]] = data
            return {"mediaType": media, "digest": digest, "size": len(data)}
        config = descriptor({"os": "linux", "architecture": architecture}, "application/vnd.oci.image.config.v1+json")
        layer = descriptor(b"exact-layer", "application/vnd.oci.image.layer.v1.tar")
        manifest = descriptor({"schemaVersion": 2, "config": config, "layers": [layer]}, "application/vnd.oci.image.manifest.v1+json")
        manifest["platform"] = {"os": "linux", "architecture": architecture}
        index = descriptor({"schemaVersion": 2, "manifests": [manifest]}, "application/vnd.oci.image.index.v1+json")
        contents["index.json"] = json.dumps({"schemaVersion": 2, "manifests": [manifest if missing_index else index]}).encode()
        if changed_layer:
            contents["blobs/sha256/"+layer["digest"][7:]] = b"wrong-layer"
        with tarfile.open(path, "w") as archive:
            for name, data in contents.items():
                member = tarfile.TarInfo(name)
                member.size = len(data)
                archive.addfile(member, io.BytesIO(data))
        return index["digest"], manifest["digest"], config["digest"]

    def test_actual_archive_graph_proves_index_manifest_configuration(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)/"image.tar"
            index, manifest, config = self.archive(path)
            result = KIND.archive_identity(path, index, "linux/arm64")
            self.assertEqual(result["manifest_digest"], manifest)
            self.assertEqual(result["config_digest"], config)
            self.assertNotEqual(index, config)

    def test_tampered_layer_or_missing_original_index_never_qualifies(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)/"image.tar"
            for options in ({"changed_layer": True}, {"missing_index": True}):
                index, _, _ = self.archive(path, **options)
                with self.assertRaises(KIND.QualificationFailure):
                    KIND.archive_identity(path, index, "linux/arm64")

    def test_amd64_archive_is_supported_and_wrong_platform_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)/"image.tar"
            index, _, _ = self.archive(path, architecture="amd64")
            self.assertEqual(KIND.archive_identity(path, index, "linux/amd64")["platform"], "linux/amd64")
            with self.assertRaises(KIND.QualificationFailure):
                KIND.archive_identity(path, index, "linux/arm64")

    def test_real_children_are_bounded_and_reaped(self):
        with self.assertRaises(KIND.QualificationFailure):
            KIND.run([sys.executable, "-c", "import time;time.sleep(30)"], timeout=.05)
        with self.assertRaises(KIND.QualificationFailure):
            KIND.run([sys.executable, "-c", "import sys;sys.stdout.buffer.write(b'x'*9000000)"], timeout=5)

    def test_matching_client_shim_preserves_streams_and_rejects_other_contexts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            docker = root/"docker"
            KIND.write(docker, ("#!"+sys.executable+"\nimport json,sys\nprint(json.dumps({'args':sys.argv[1:],'input':sys.stdin.read()}))\n").encode(), 0o500)
            shim = root/"kubectl"
            KIND.write(shim, KIND.client_shim("exact-node", root/"kubeconfig", "exact-context").encode(), 0o500)
            prefix = [str(shim), "--kubeconfig", str(root/"kubeconfig"), "--context", "exact-context"]
            environment = dict(os.environ, PATH=str(root)+os.pathsep+os.environ["PATH"])
            result = subprocess.run([*prefix, "exec", "pod", "--", "cat", "fixed-file"], input=b"stream-canary", stdout=subprocess.PIPE, env=environment, check=True, timeout=3)
            decoded = json.loads(result.stdout)
            self.assertEqual(decoded["input"], "stream-canary")
            self.assertEqual(decoded["args"][:6], ["exec", "-i", "exact-node", "/usr/bin/kubectl", "--kubeconfig", "/etc/kubernetes/admin.conf"])
            for arguments in ([*prefix[:-1], "foreign-context", "get", "pods"], [*prefix, "--server=https://foreign", "get", "pods"], [*prefix, "--kubeconfig", "foreign", "get", "pods"]):
                result = subprocess.run(arguments, stdout=subprocess.PIPE, env=environment, timeout=3)
                self.assertEqual(result.returncode, 64)
                self.assertEqual(result.stdout, b"")

    def test_cleanup_checks_only_declared_cluster_and_requires_confirmation(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = KIND.Fixture("runtime", "console", Path(temporary))
            fixture.started = True
            with mock.patch.object(fixture, "docker", side_effect=[(fixture.node+"\n").encode(), b""]), mock.patch.object(fixture, "owned_node"), mock.patch.object(KIND, "run") as run:
                fixture.close()
                command = run.call_args.args[0]
                self.assertEqual(command[:5], ["kind", "delete", "cluster", "--name", fixture.name])
                self.assertTrue(fixture.report["cleaned"])
            fixture.report["cleaned"] = False
            with mock.patch.object(fixture, "docker", return_value=b"foreign-node\n"), mock.patch.object(KIND, "run") as run:
                with self.assertRaises(KIND.QualificationFailure):
                    fixture.close()
                run.assert_not_called()
                self.assertFalse(fixture.report["cleaned"])
            with mock.patch.object(fixture, "docker", side_effect=KIND.QualificationFailure("daemon_unavailable")):
                with self.assertRaises(KIND.QualificationFailure):
                    fixture.close()
                self.assertFalse(fixture.report["cleaned"])

    def run_main(self, root, *, phase_failure=None, cleanup_failure=None):
        evidence = root.resolve()/"private-evidence"
        evidence.mkdir(mode=0o700)
        (evidence/"private-canary").write_bytes(b"not-in-report")
        fixture = mock.Mock()
        fixture.report = {"schema_version": 1, "cleaned": False}
        if phase_failure is not None:
            getattr(fixture, phase_failure).side_effect = KIND.QualificationFailure("external_outcome_unknown")
        def close():
            if cleanup_failure is not None:
                raise cleanup_failure
            fixture.report["cleaned"] = True
        fixture.close.side_effect = close
        arguments = ["qualify", "--runtime-image", "runtime@sha256:"+"a"*64,
                     "--console-image", "console@sha256:"+"b"*64, "--report", str(root/"report.json")]
        old_umask = os.umask(0o077)
        try:
            with mock.patch.object(sys, "argv", arguments), mock.patch.object(KIND.tempfile, "mkdtemp", return_value=str(evidence)), \
                    mock.patch.object(KIND, "Fixture", return_value=fixture), mock.patch("sys.stdout", new_callable=io.StringIO) as output:
                status = KIND.main()
        finally:
            os.umask(old_umask)
        report = json.loads((root/"report.json").read_bytes())
        self.assertNotIn("not-in-report", json.dumps(report)+output.getvalue())
        return status, report, fixture, evidence, json.loads(output.getvalue())

    def test_failed_installation_retains_exact_fixture_private_evidence_and_never_deletes(self):
        for phase in ("begin", "setup", "qualify"):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as temporary:
                status, report, fixture, evidence, output = self.run_main(Path(temporary), phase_failure=phase)
                self.assertEqual(status, 1)
                self.assertEqual(report["result"], "FAIL")
                self.assertFalse(report["cleaned"])
                self.assertEqual(report["evidence_directory"], str(evidence))
                self.assertEqual((evidence/"private-canary").read_bytes(), b"not-in-report")
                fixture.close.assert_not_called()
                fixture.diagnostics.assert_called_once()
                self.assertFalse(output["cleaned"])

    def test_pass_is_reported_only_after_exact_cluster_and_private_evidence_cleanup(self):
        with tempfile.TemporaryDirectory() as temporary:
            status, report, fixture, evidence, output = self.run_main(Path(temporary))
            self.assertEqual(status, 0)
            self.assertEqual(report["result"], "PASS")
            self.assertTrue(report["cleaned"])
            self.assertTrue(output["cleaned"])
            self.assertNotIn("evidence_directory", report)
            self.assertFalse(evidence.exists())
            fixture.close.assert_called_once()
            fixture.diagnostics.assert_not_called()

    def test_cleanup_failure_is_fail_and_preserves_evidence_with_no_raw_exception(self):
        with tempfile.TemporaryDirectory() as temporary:
            status, report, fixture, evidence, output = self.run_main(Path(temporary), cleanup_failure=OSError("raw credential diagnostic"))
            self.assertEqual(status, 1)
            self.assertEqual(report["result"], "FAIL")
            self.assertFalse(report["cleaned"])
            self.assertEqual(report["failure"], "qualification_cleanup_failed")
            self.assertTrue((evidence/"private-canary").exists())
            self.assertNotIn("raw credential", json.dumps(report))
            self.assertFalse(output["cleaned"])

    def test_fixture_drops_ambient_kubernetes_identity_and_helm_driver(self):
        with tempfile.TemporaryDirectory() as temporary:
            with mock.patch.dict(os.environ, {"KUBECONFIG": "foreign", "HELM_KUBETOKEN": "not-read", "HELM_DRIVER_SQL_CONNECTION_STRING": "not-read", "KIND_EXPERIMENTAL_DOCKER_NETWORK": "foreign"}):
                fixture = KIND.Fixture("runtime", "console", Path(temporary))
            self.assertEqual(fixture.environment["KUBECONFIG"], str(fixture.kubeconfig))
            self.assertEqual(fixture.environment["HELM_DRIVER"], "secret")
            self.assertNotIn("HELM_KUBETOKEN", fixture.environment)
            self.assertNotIn("HELM_DRIVER_SQL_CONNECTION_STRING", fixture.environment)
            self.assertNotIn("KIND_EXPERIMENTAL_DOCKER_NETWORK", fixture.environment)


if __name__ == "__main__":
    unittest.main()
