from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import test_platform_candidate_pipeline as candidate_fixture

ROOT = Path(__file__).resolve().parents[2]
PREPARE = ROOT / 'tools/qualification/prepare-platform-kind-local.rb'


def yaml_documents(text):
    return json.loads(subprocess.check_output(['ruby', '-ryaml', '-rjson', '-e', 'puts JSON.generate(YAML.load_stream(STDIN.read))'], input=text, text=True))


class KindConfigurationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        candidate_fixture.CandidatePipelineTests.setUpClass()
        subprocess.run(['cargo', 'build', '--locked', '--quiet', '-p', 'insight-platform-storage-tooling', '--bin', 'platform-database-role'], cwd=ROOT, check=True)
        metadata = json.loads(subprocess.check_output(
            ['cargo', 'metadata', '--locked', '--no-deps', '--format-version', '1'], cwd=ROOT))
        cls.role_tool = Path(metadata['target_directory']) / 'debug/platform-database-role'
        cls.owner = candidate_fixture.CandidatePipelineTests()
        cls.tool = candidate_fixture.CandidatePipelineTests.tool

    def seed(self, directory):
        runtime = directory / 'runtime'
        source = runtime / 'config'
        source.mkdir(parents=True)
        environment = directory / 'workers'
        environment.mkdir()
        _, configs, binaries = self.owner.worker_fixture(environment)
        source_names = {'orchestration-worker': 'orchestration', 'registry-validation-worker': 'registry-validation',
                        'context-worker': 'context-native', 'remote-context-worker': 'context-remote',
                        'capability-native-worker': 'capability-native', 'capability-remote-worker': 'capability-remote',
                        'artifact-data-worker': 'artifact-data'}
        for config_path in configs.glob('*.json'):
            name = config_path.stem.removeprefix('platform-')
            if name == 'sandbox-dispatcher':
                continue
            config = json.loads(config_path.read_text())
            # This fixture exercises real generation and owning catalog validation, not process startup.
            for key in ('artifact', 'egress', 'mcp_host', 'host', 'artifact_data_worker'):
                config[key] = {'endpoint': 'https://localhost:9443/'}
            config['live_delta'] = {'servers': ['tls://localhost:4222']}
            (source / (source_names.get(name, name) + '.json')).write_text(json.dumps(config))
        catalog = {'kms_key_bindings': [{'key_id': 'seed-key'}], 's3_storage_bindings': [{}]}
        for name in ('artifact-gateway', 'artifact-data', 'artifact-maintenance'):
            target = source / (name + '.json')
            config = json.loads(target.read_text()) if target.exists() else {}
            config['artifact_provider_catalog'] = copy.deepcopy(catalog)
            target.write_text(json.dumps(config))
        other = {
            'security-authority': {},
            'egress-broker': {'mcp_state_keys': {'keys': [{}]}, 'secret_provider_catalog': {'providers': [{}]}},
            'mcp-host': {'egress': {}}, 'mcp-resource-host': {'egress': {}},
            'callback-api': {'oauth_state': {'keys': [{}]}},
            'gateway-management': {'artifact_gateway': {'endpoint': 'https://127.0.0.1:19010/'}},
            'gateway-runtime': {'artifact_gateway': {'endpoint': 'https://127.0.0.1:19010/'}},
            'outbox-worker': {'schema_version': 1, 'stream': json.loads((ROOT / 'deploy/jetstream/committed-events-v1.json').read_text())},
            'history-maintenance': json.loads((environment / 'history-maintenance.json').read_text()),
        }
        for name, config in other.items():
            (source / (name + '.json')).write_text(json.dumps(config))
        (runtime / 'run-event-cursor-key').write_bytes(b'dedicated Kind render fixture cursor')
        return runtime, binaries

    def generate(self, directory, runtime, binaries):
        output = directory / 'generated'
        command = ['ruby', str(PREPARE), '--seed-runtime', str(runtime), '--worker-binaries', str(binaries),
                   '--qualification-tool', str(self.tool), '--output', str(output), '--git-commit', 'c' * 40,
                   '--platform-image-digest', 'sha256:' + 'a' * 64, '--platform-image-repository', 'example.invalid/runtime',
                   '--sandbox-runner-image-digest', 'sha256:' + 'b' * 64, '--sandbox-runner-image-repository', 'example.invalid/runner',
                   '--postgres-cidr', '10.0.0.1/32', '--nats-cidr', '10.0.0.2/32', '--localstack-pod-cidr', '10.0.0.3/32',
                   '--localstack-service-cidr', '10.0.0.4/32', '--kubernetes-api-service-cidr', '10.0.0.5/32',
                   '--kubernetes-api-endpoint-cidr', '10.0.0.6/32', '--kubernetes-api-endpoint-port', '6443',
                   '--kms-key-arn', 'arn:aws:kms:us-east-1:000000000000:key/kind-test',
                   '--readiness-secret-arn', 'arn:aws:secretsmanager:us-east-1:000000000000:secret:kind-test']
        return subprocess.run(command, cwd=ROOT, text=True, capture_output=True), output

    def test_remote_only_helm_scope_is_derived_from_actual_installed_codecs(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            runtime, binaries = self.seed(directory)
            path = runtime / 'config/capability-remote.json'
            config = json.loads(path.read_text())
            config['installed_mcp_codecs'] = []
            config['mcp_host'] = None
            path.write_text(json.dumps(config))
            config['worker_manifest']['execution_capabilities'] = json.loads(subprocess.check_output(
                [str(self.tool), 'print-worker-execution-capabilities', 'platform-capability-remote-worker', str(path)], cwd=ROOT))
            closure = {'schema_version': 1, 'http': config['installed_http_codecs'], 'grpc': config['installed_grpc_codecs'], 'mcp': []}
            config['worker_manifest']['adapter_runtime_digest'] = 'sha256:' + hashlib.sha256(json.dumps(closure, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            path.write_text(json.dumps(config))
            result, output = self.generate(directory, runtime, binaries)
            self.assertEqual(result.returncode, 0, result.stderr)
            values = yaml_documents((output / 'helm-values/capability-remote.yaml').read_text())[0]
            self.assertFalse(values['mcpHost']['enabled'])
            actual = json.loads((output / 'configs/capability-remote-worker.json').read_text())
            self.assertIsNone(actual['mcp_host'])
            self.assertEqual(actual['installed_mcp_codecs'], [])

    def test_real_generator_closes_all_roles_and_verified_manifests(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            runtime, binaries = self.seed(directory)
            result, output = self.generate(directory, runtime, binaries)
            self.assertEqual(result.returncode, 0, result.stderr)
            values = yaml_documents((output / 'helm-values/capability-remote.yaml').read_text())[0]
            self.assertTrue(values['mcpHost']['enabled'])
            candidate = json.loads((output / 'local-workload-candidate.json').read_text())
            self.assertEqual(len(candidate['component_images']), 18)
            self.assertIn('outbox_worker', candidate['component_images'])
            self.assertIn('history_maintenance', candidate['component_images'])
            for filename in ('management-gateway', 'runtime-gateway'):
                config = json.loads((output / 'configs' / (filename + '.json')).read_text())
                self.assertIn('.platform-artifacts.svc:', config['artifact_gateway']['endpoint'])
            evidence = json.loads((output / 'worker-executable-evidence.json').read_text())
            self.assertEqual({worker['binary'] for worker in evidence['workers']}, {worker['binary'] for worker in self.owner.executables})
            for worker in evidence['workers']:
                self.assertEqual(worker['worker_build_digest'], 'sha256:' + hashlib.sha256((binaries / worker['binary']).read_bytes()).hexdigest())
            for values, chart in [('outbox', 'outbox-worker'), ('history', 'history-maintenance'), ('registry', 'registry-validation-worker'), ('gateway', 'gateway'), ('sandbox', 'sandbox')]:
                rendered = subprocess.check_output(['helm', 'template', 'kind-test', str(ROOT / 'deploy/helm' / ('insight-platform-' + chart)), '--values', str(output / 'helm-values' / (values + '.yaml'))], text=True)
                docs = yaml_documents(rendered)
                deployments = [doc for doc in docs if doc and doc['kind'] == 'Deployment']
                self.assertTrue(deployments)
                if values in ('outbox', 'history'):
                    for deployment in deployments:
                        volumes = deployment['spec']['template']['spec']['volumes']
                        self.assertTrue(any('configMap' in volume for volume in volumes))
                if values == 'registry':
                    self.assertIn('insight-platform-registry-validation-artifact-client-tls', rendered)
                    self.assertIn('https://insight-platform-artifact-gateway.platform-artifacts.svc.cluster.local:8080', rendered)

    def test_missing_binary_or_old_manifest_fails_closed(self):
        for mutation in ('missing', 'old'):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                runtime, binaries = self.seed(directory)
                if mutation == 'missing':
                    (binaries / 'platform-history-maintenance').unlink()
                else:
                    path = runtime / 'config/artifact-maintenance.json'
                    config = json.loads(path.read_text())
                    config['worker']['worker_manifest']['manifest_version'] = 1
                    path.write_text(json.dumps(config))
                result, _ = self.generate(directory, runtime, binaries)
                self.assertNotEqual(result.returncode, 0)
                self.assertRegex(result.stderr, 'actual image executable missing|current owning worker manifest')

    def test_declared_unknown_capability_is_not_accepted_as_installed(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            runtime, binaries = self.seed(directory)
            path = runtime / 'config/orchestration.json'
            config = json.loads(path.read_text())
            config['worker_manifest']['execution_capabilities'] = {'schema_version': 1, 'capabilities': []}
            path.write_text(json.dumps(config))
            result, _ = self.generate(directory, runtime, binaries)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('owning deployment tool rejected', result.stderr)

    def test_kind_role_cli_rejects_untrusted_targets_and_credentials_before_connect(self):
        with tempfile.TemporaryDirectory() as temporary:
            credential = Path(temporary) / 'password'
            credential.write_text('a' * 32)
            credential.chmod(0o600)
            environment = dict(os.environ, PLATFORM_DATABASE_ROLE_ADMIN_URL='postgresql://insight:insight-local-only@remote.invalid:15432/insight')
            base = [str(self.role_tool), '--profile', 'kind-local', '15432', '--purpose', 'outbox', str(credential)]
            result = subprocess.run(base, env=environment, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('exact local development authority', result.stderr)
            for invalid in ('015432', '65536', '1023', '15432/insight'):
                command = base.copy()
                command[3] = invalid
                result = subprocess.run(command, env=environment, capture_output=True, text=True)
                self.assertIn('Kind loopback port invalid', result.stderr)
            command = base.copy()
            command[5] = 'superuser'
            self.assertIn('unknown database role purpose', subprocess.run(command, env=environment, capture_output=True, text=True).stderr)
            credential.chmod(0o644)
            self.assertIn('not private', subprocess.run(base, env=environment, capture_output=True, text=True).stderr)
            credential.unlink()
            real = Path(temporary) / 'real'
            real.write_text('a' * 32)
            real.chmod(0o600)
            credential.symlink_to(real)
            self.assertIn('private regular file', subprocess.run(base, env=environment, capture_output=True, text=True).stderr)

    def test_bootstrap_uses_owned_provisioning_separate_credentials_and_durable_acl(self):
        source = (ROOT / 'tools/qualification/bootstrap-platform-kind-local.sh').read_text()
        self.assertIn('"$database_role_bin" --profile kind-local', source)
        self.assertIn('"$jetstream_provision_bin" create "$root/deploy/jetstream/committed-events-v1.json"', source)
        self.assertLess(source.index('"$jetstream_provision_bin" create'), source.index('install_chart l4-outbox'))
        self.assertIn('--from-file=nats.conf="$root/deploy/dev/nats.conf"', source)
        self.assertIn('tls/registry-validation-client.pem', source)
        self.assertIn('--purpose artifact "$output/credentials/artifact"', source)
        generic_roles = source.split("done <<'DATABASE_SECRETS'", 1)[1].split('DATABASE_SECRETS', 1)[0]
        for role in ('artifact-gateway', 'artifact-data-reader', 'artifact-data-worker', 'artifact-maintenance', 'security-authority'):
            self.assertNotIn('insight-platform-' + role + '-database', generic_roles)
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        self.assertIn('qualify-platform-kind-database-roles.py --schema-bin', workflow)
        self.assertIn('-p test_platform_kind_configuration.py', workflow)
        self.assertIn('tls/outbox-client.pem', source)
        self.assertIn('tls/outbox-provision-client.pem', source)
        self.assertIn('docker create --platform "$docker_platform" --entrypoint /bin/false "$platform_config_digest"', source)
        self.assertIn('docker cp "$image_container:/usr/local/bin/."', source)
        self.assertNotRegex(source, r'docker (?:start|run).*image_container')
        docs = yaml_documents((ROOT / 'deploy/kind/dependencies.yaml').read_text())
        nats = next(doc for doc in docs if doc and doc['kind'] == 'Deployment' and doc['metadata']['name'] == 'nats')
        self.assertEqual(nats['spec']['template']['spec']['containers'][0]['args'], ['--config', '/etc/nats/config/nats.conf'])
        data = next(volume for volume in nats['spec']['template']['spec']['volumes'] if volume['name'] == 'data')
        self.assertEqual(data['persistentVolumeClaim']['claimName'], 'nats-jetstream')
        self.assertNotIn('emptyDir', data)


class KindCertificateTests(unittest.TestCase):
    def test_issued_certificates_validate_exact_service_dns(self):
        source = (ROOT / 'tools/qualification/bootstrap-platform-kind-local.sh').read_text()
        code = source.split('issue_server_certificate() {', 1)[1].split('"$kubectl_bin" -n platform-deps create secret generic nats-tls', 1)[0]
        code = 'issue_server_certificate() {' + code
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            runtime = directory / 'runtime'
            tls = runtime / 'tls'
            tls.mkdir(parents=True)
            output = directory / 'issued'
            output.mkdir()
            subprocess.run(['openssl', 'req', '-x509', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:prime256v1', '-nodes', '-days', '1',
                            '-subj', '/CN=isolated-kind-certificate-test', '-keyout', str(tls / 'ca-key.pem'), '-out', str(tls / 'ca.pem')], check=True, capture_output=True)
            script = directory / 'certificates.sh'
            script.write_text('set -euo pipefail\n' + code)
            result = subprocess.run(['bash', str(script)], env=dict(os.environ, seed_runtime=str(runtime), tls_output=str(output)), capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            names = {
                'nats-kind-server': 'nats.platform-deps.svc',
                'artifact-gateway-kind-server': 'insight-platform-artifact-gateway.platform-artifacts.svc',
                'artifact-data-kind-server': 'insight-platform-artifact-data-worker.platform-artifacts.svc',
                'egress-kind-server': 'l4-security-insight-platform-security-egress-egress.platform-egress.svc',
                'authority-kind-server': 'l4-security-insight-platform-security-egress-security-authority.platform-security-authority.svc',
                'mcp-host-kind-server': 'insight-platform-mcp-host.platform-mcp-host.svc',
                'mcp-resource-kind-server': 'insight-platform-mcp-resource-host.platform-mcp-host.svc',
            }
            for name, host in names.items():
                with self.subTest(service=name):
                    command = ['openssl', 'verify', '-CAfile', str(tls / 'ca.pem'), '-verify_hostname', host, str(output / (name + '.pem'))]
                    self.assertEqual(subprocess.run(command, capture_output=True).returncode, 0)
                    command[-2] = host + '.cluster.local'
                    self.assertEqual(subprocess.run(command, capture_output=True).returncode, 0)
                    command[-2] = 'different-service.invalid'
                    self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)


if __name__ == '__main__':
    unittest.main()
