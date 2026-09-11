from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

import test_platform_candidate_pipeline as candidate_fixture

ROOT = Path(__file__).resolve().parents[2]
PREPARE = ROOT / 'tools/qualification/prepare-platform-kind-local.rb'
KMS_KEY_ARN = 'arn:aws:kms:us-east-1:000000000000:key/12345678-1234-1234-1234-123456789abc'


def canonical_digest(value):
    return 'sha256:' + hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


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
        subprocess.run(['cargo', 'build', '--locked', '--quiet', '-p', 'insight-platform-artifact-service',
                        '--bin', 'platform-artifact-maintenance'], cwd=ROOT, check=True)
        cls.maintenance_tool = Path(metadata['target_directory']) / 'debug/platform-artifact-maintenance'
        subprocess.run(['cargo', 'build', '--locked', '--quiet', '-p', 'insight-platform-model-worker',
                        '--bin', 'platform-model-worker'], cwd=ROOT, check=True)
        cls.model_tool = Path(metadata['target_directory']) / 'debug/platform-model-worker'

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
            if name in ('sandbox-dispatcher', 'artifact-maintenance'):
                continue
            config = json.loads(config_path.read_text())
            # This fixture exercises real generation and owning catalog validation, not process startup.
            for key in ('artifact', 'egress', 'mcp_host', 'host', 'artifact_data_worker'):
                config[key] = {'endpoint': 'https://localhost:9443/'}
            config['live_delta'] = {'servers': ['tls://localhost:4222']}
            if name == 'model-worker':
                config['installed_adapters'].append(dict(config['installed_adapters'][0],
                    qualified_name='openai.responses/v1', adapter_contract_digest='sha256:' + 'b' * 64))
                config_path.write_text(json.dumps(config))
                config['worker_manifest']['execution_capabilities'] = json.loads(subprocess.check_output([
                    str(self.tool), 'print-worker-execution-capabilities', 'platform-model-worker', str(config_path)]))
                for adapter in config['installed_adapters']:
                    adapter['worker_manifest_digest'] = canonical_digest(config['worker_manifest'])
                config = dict(schema_version=1, observability_listen_address='127.0.0.1:9090',
                    worker_manifest=config['worker_manifest'], installed_adapters=config['installed_adapters'],
                    database_max_connections=4, database_acquire_timeout_milliseconds=5000,
                    egress_endpoint='https://localhost:8443/', egress_tls_server_name='localhost',
                    egress_connect_timeout_milliseconds=5000, egress_request_timeout_milliseconds=30000,
                    maximum_rpc_metadata_bytes=65536, maximum_rpc_payload_bytes=1048576,
                    live_delta=dict(servers=['tls://localhost:4222'], namespace='local',
                        connect_timeout_milliseconds=5000, publish_timeout_milliseconds=1000,
                        reconnect_backoff_milliseconds=250, drain_timeout_milliseconds=5000,
                        maximum_pending_messages=1024, maximum_pending_bytes=16777216),
                    receipt_ttl_seconds=3600, claim_scan_milliseconds=250,
                    claim_failure_backoff_milliseconds=100, drain_grace_milliseconds=30000)
            (source / (source_names.get(name, name) + '.json')).write_text(json.dumps(config))
        native = json.loads((source / 'context-native.json').read_bytes())
        binding = dict(schema_version=1, required_worker_manifest_digest=canonical_digest(native['worker_manifest']),
            adapter_contract_digest=native['native_catalog']['adapter_contract_digest'],
            installed_adapter_digest=native['native_catalog']['installed_adapter_digest'])
        binding['canonical_digest'] = canonical_digest(binding)
        dataset_path = source / 'context-dataset-worker.json'
        dataset = json.loads(dataset_path.read_bytes())
        items = ['bounded document index entry']
        dataset['sources'] = [dict(schema_version=1, binding=binding, items=items,
            source_manifest_digest=canonical_digest(dict(schema_version=1, items=[dict(content=items[0], ordinal=0)])))]
        dataset_path.write_text(json.dumps(dataset))
        kms = {'schema_version': 1, 'endpoint': 'https://kms.platform.example', 'region': 'us-east-1',
               'key_id': KMS_KEY_ARN, 'connect_timeout_milliseconds': 1000, 'operation_timeout_milliseconds': 5000}
        kms['kms_binding_digest'] = canonical_digest(dict(kms, provider='aws_kms'))
        storage = {'schema_version': 1, 'endpoint': 'https://s3.platform.example', 'region': 'us-east-1',
                   'bucket': 'platform-artifacts', 'force_path_style': True, 'kms_binding_digest': kms['kms_binding_digest'],
                   'connect_timeout_milliseconds': 1000, 'operation_timeout_milliseconds': 5000, 'maximum_object_bytes': 16 * 1024 * 1024}
        storage['storage_binding_digest'] = canonical_digest(dict(storage, backend='s3'))
        catalog = {'schema_version': 2, 'write_storage_binding_digest': storage['storage_binding_digest'],
                   'reference_key_bindings': [{'kind': 'aws_kms', 'config': kms}], 's3_storage_bindings': [storage]}
        for name in ('artifact-gateway', 'artifact-data'):
            target = source / (name + '.json')
            config = json.loads(target.read_text()) if target.exists() else {}
            config['artifact_provider_catalog'] = copy.deepcopy(catalog)
            target.write_text(json.dumps(config))
        other = {
            'security-authority': {},
            'egress-broker': {'mcp_state_keys': {'keys': [{}]}, 'secret_provider_catalog': {'schema_version': 2,
                'providers': [{'kind': 'aws_secrets_manager', 'config': {}}]}},
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

    def generate(self, directory, runtime, binaries, prepare=PREPARE):
        output = directory / 'generated'
        command = ['ruby', str(prepare), '--seed-runtime', str(runtime), '--worker-binaries', str(binaries),
                   '--qualification-tool', str(self.tool), '--output', str(output), '--git-commit', 'c' * 40,
                   '--platform-image-digest', 'sha256:' + 'a' * 64, '--platform-image-repository', 'example.invalid/runtime',
                   '--sandbox-runner-image-digest', 'sha256:' + 'b' * 64, '--sandbox-runner-image-repository', 'example.invalid/runner',
                   '--postgres-cidr', '10.0.0.1/32', '--nats-cidr', '10.0.0.2/32', '--localstack-pod-cidr', '10.0.0.3/32',
                   '--localstack-service-cidr', '10.0.0.4/32', '--kubernetes-api-service-cidr', '10.0.0.5/32',
                   '--kubernetes-api-endpoint-cidr', '10.0.0.6/32', '--kubernetes-api-endpoint-port', '6443',
                   '--kms-key-arn', KMS_KEY_ARN,
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

    def test_complete_kind_closure_fits_development_cpu_budget(self):
        charts = {
            'artifact': 'artifact', 'callback': 'callback-api',
            'capability-native': 'capability-native-worker', 'capability-remote': 'capability-remote-worker',
            'context': 'context-worker', 'gateway': 'gateway', 'mcp': 'mcp-host',
            'mcp-cleanup': 'mcp-cleanup-worker', 'model': 'model-worker', 'outbox': 'outbox-worker',
            'history': 'history-maintenance', 'orchestration': 'orchestration-worker',
            'registry': 'registry-validation-worker', 'remote-context': 'remote-context-worker',
            'security': 'security-egress', 'sandbox': 'sandbox',
        }
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            runtime, binaries = self.seed(directory)
            result, output = self.generate(directory, runtime, binaries)
            self.assertEqual(result.returncode, 0, result.stderr)
            total_cpu = 0
            largest_surge = 0
            busiest_worker = 0
            resource_profile = json.loads((ROOT / 'deploy/kind/workload-resources.json').read_text())
            self.assertEqual({path.stem for path in (output / 'helm-values').glob('*.yaml')}, set(charts))
            identity = {'schema_version': 1, 'profile': 'kind-local-mechanics', 'git_commit': 'c' * 40,
                        'platform_image_repository': 'example.invalid/runtime', 'platform_image_digest': 'sha256:' + 'a' * 64,
                        'sandbox_runner_image_repository': 'example.invalid/runner', 'sandbox_runner_image_digest': 'sha256:' + 'b' * 64,
                        'configuration_digests': json.loads((output / 'digests.json').read_text()),
                        'resource_profile': resource_profile}
            expected_digest = 'sha256:' + hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            self.assertEqual(json.loads((output / 'local-workload-candidate.json').read_text())['deployment_config_digest'], expected_digest)

            def millicores(request):
                request = str(request)
                return int(request[:-1]) if request.endswith('m') else int(float(request) * 1000)

            def remove_cpu_overrides(value):
                if isinstance(value, dict):
                    return {key: remove_cpu_overrides(item) for key, item in value.items()
                            if key != 'resources'}
                return value

            def normalize_cpu(document):
                normalized = copy.deepcopy(document)
                pod = normalized['spec']['template']['spec']
                for container in pod.get('containers', []) + pod.get('initContainers', []):
                    container.get('resources', {}).get('requests', {}).pop('cpu', None)
                return normalized

            for values, chart in charts.items():
                release_surge = 0
                chart_path = str(ROOT / 'deploy/helm' / ('insight-platform-' + chart))
                values_path = output / 'helm-values' / (values + '.yaml')
                rendered = subprocess.check_output(['helm', 'template', 'kind-test', chart_path,
                    '--values', str(values_path)], text=True)
                original_values = yaml_documents(values_path.read_text())[0]
                baseline_path = directory / 'baseline.json'
                baseline_path.write_text(json.dumps(remove_cpu_overrides(original_values)))
                baseline_docs = yaml_documents(subprocess.check_output(['helm', 'template', 'kind-test', chart_path,
                    '--values', str(baseline_path)], text=True))
                baseline = {doc['metadata']['name']: doc for doc in baseline_docs if doc and doc['kind'] == 'Deployment'}
                docs = yaml_documents(rendered)
                hpa_bounds = {doc['spec']['scaleTargetRef']['name']: doc['spec']['maxReplicas']
                              for doc in docs if doc and doc['kind'] == 'HorizontalPodAutoscaler'}
                for doc in docs:
                    if not doc or doc['kind'] != 'Deployment':
                        continue
                    pod = doc['spec']['template']['spec']
                    self.assertEqual(normalize_cpu(doc), normalize_cpu(baseline[doc['metadata']['name']]))
                    application_cpu = sum(millicores(container['resources']['requests']['cpu']) for container in pod['containers'])
                    restartable = 0
                    init_peak = 0
                    for container in pod.get('initContainers', []):
                        request = millicores(container.get('resources', {}).get('requests', {}).get('cpu', '0'))
                        if container.get('restartPolicy') == 'Always':
                            restartable += request
                            init_peak = max(init_peak, restartable)
                        else:
                            init_peak = max(init_peak, restartable + request)
                    cpu = max(application_cpu + restartable, init_peak)
                    replicas = max(doc['spec']['replicas'], hpa_bounds.get(doc['metadata']['name'], 0))
                    if values != 'sandbox':
                        self.assertEqual(replicas, 2)
                        spreads = any(constraint['topologyKey'] == 'topology.kubernetes.io/zone' and
                                      constraint['whenUnsatisfiable'] == 'DoNotSchedule' and constraint['maxSkew'] == 1
                                      for constraint in pod.get('topologySpreadConstraints', []))
                        busiest_worker += cpu if spreads else cpu * replicas
                        self.assertTrue(all(millicores(container['resources']['requests']['cpu']) ==
                                            resource_profile['rust_service_cpu_request_millicores'] for container in pod['containers']))
                    else:
                        busiest_worker += cpu * replicas
                    total_cpu += cpu * replicas
                    surge = doc['spec'].get('strategy', {}).get('rollingUpdate', {}).get('maxSurge', '25%')
                    surge = (replicas * int(surge[:-1]) + 99) // 100 if str(surge).endswith('%') else int(surge)
                    release_surge += cpu * surge
                # A Helm release can roll its multiple Deployments concurrently.
                largest_surge = max(largest_surge, release_surge)
            self.assertLessEqual(resource_profile['maximum_steady_platform_cpu_millicores'], 3000)
            self.assertLessEqual(total_cpu, resource_profile['maximum_steady_platform_cpu_millicores'])
            # Read the actual owning physical fixtures: the provider core holds two candidates
            # together; the domain journey can hold one larger candidate. execd is an ordinary
            # init container, so its request takes the Pod maximum rather than an extra sum.
            def fixture_cpu(path):
                source = path.read_text()
                match = re.search(r'fn resource_limits\(\).*?cpu_millicores:\s*([0-9_]+)', source, re.S)
                self.assertIsNotNone(match)
                return int(match.group(1).replace('_', ''))
            domain_cpu = fixture_cpu(ROOT / 'tests/qualification/tests/phase3_opensandbox.rs')
            provider_cpu = fixture_cpu(ROOT / 'crates/adapters/platform-opensandbox-client/tests/kubernetes_l3.rs')
            sandbox_values = yaml_documents((ROOT / 'deploy/helm/insight-platform-sandbox/values.yaml').read_text())[0]
            init_cpu = millicores(sandbox_values['server']['execdInitResources']['requests']['cpu'])
            sandbox_peak = max(domain_cpu, init_cpu, 2 * max(provider_cpu, init_cpu))
            dependency_cpu = 0
            for doc in yaml_documents((ROOT / 'deploy/kind/dependencies.yaml').read_text()):
                if not doc or doc['kind'] != 'Deployment':
                    continue
                pod = doc['spec']['template']['spec']
                self.assertEqual(pod['nodeSelector'], {'node-role.kubernetes.io/control-plane': ''})
                dependency_cpu += sum(millicores(container['resources']['requests']['cpu']) for container in pod['containers']) * doc['spec']['replicas']
            cluster = yaml_documents((ROOT / 'deploy/kind/cluster.yaml').read_text())[0]
            workers = sum(node['role'] == 'worker' for node in cluster['nodes'])
            self.assertEqual(workers, 2)
            # The two virtual workers each report the same four-CPU CI host. These checks prove
            # Kubernetes scheduling headroom only, not eight physical cores or host performance.
            self.assertLessEqual(total_cpu + largest_surge + dependency_cpu + 500 + sandbox_peak, workers * 4000)
            self.assertLessEqual(busiest_worker + largest_surge + 500 + sandbox_peak, 4000)

    def test_kind_resource_input_is_bounded_unique_and_closed(self):
        profile = (ROOT / 'deploy/kind/workload-resources.json').read_text()
        cases = [
            (profile.replace('"schema_version": 1', '"schema_version": 1, "schema_version": 1'), 'duplicate'),
            (profile.replace('"schema_version": 1', r'"schema_version": 1, "schema\u005fversion": 1'), 'duplicate'),
            (profile + ' ' * 4096, 'byte limit'),
            (profile.replace('"schema_version": 1', '"schema_version": 1.0'), 'invalid'),
            (profile.replace('"schema_version": 1', '"unexpected": true, "schema_version": 1'), 'invalid'),
            (profile.replace('"schema_version": 1', '"schema_version": NaN'), 'invalid'),
            (profile.replace('"schema_version": 1', '"schema_version": Infinity'), 'invalid'),
            (profile.replace('"schema_version": 1', '"schema_version": 1e999'), 'invalid'),
            ('[]', 'invalid'),
            (profile.encode('utf-16'), 'invalid'),
        ]
        for source, error in cases:
            with self.subTest(error=error), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                runtime, binaries = self.seed(directory)
                isolated = directory / 'isolated'
                prepare = isolated / 'tools/qualification/prepare.rb'
                prepare.parent.mkdir(parents=True)
                prepare.write_bytes(PREPARE.read_bytes())
                resource = isolated / 'deploy/kind/workload-resources.json'
                resource.parent.mkdir(parents=True)
                resource.write_bytes(source if isinstance(source, bytes) else source.encode('utf-8'))
                (isolated / 'deploy/helm').symlink_to(ROOT / 'deploy/helm', target_is_directory=True)
                result, _ = self.generate(directory, runtime, binaries, prepare)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(error, result.stderr)

    def test_maintenance_is_composed_without_a_cli_seed_and_binds_actual_image_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            runtime, binaries = self.seed(directory)
            self.assertFalse((runtime / 'config/artifact-maintenance.json').exists())
            source_files = {path.name: path.read_bytes() for path in (runtime / 'config').iterdir()}
            result, output = self.generate(directory, runtime, binaries)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(source_files, {path.name: path.read_bytes() for path in (runtime / 'config').iterdir()})
            path = output / 'configs/artifact-maintenance.json'
            config = json.loads(path.read_bytes())
            gateway = json.loads((output / 'configs/artifact-gateway.json').read_bytes())
            self.assertEqual(config['artifact_provider_catalog'], gateway['artifact_provider_catalog'])
            manifest = config['worker']['worker_manifest']
            projection = copy.deepcopy(config)
            del projection['worker']['worker_manifest']
            identity = {'schema_version': 1, 'profile': 'kind-local-artifact-maintenance', 'configuration': projection}
            self.assertEqual(manifest['adapter_runtime_digest'], 'sha256:' + hashlib.sha256(
                json.dumps(identity, sort_keys=True, separators=(',', ':')).encode()).hexdigest())
            catalog = json.loads(subprocess.check_output([str(self.tool), 'print-worker-execution-capabilities',
                'platform-artifact-maintenance', str(path)], cwd=ROOT))
            self.assertEqual(manifest['execution_capabilities'], catalog)
            binary = binaries / 'platform-artifact-maintenance'
            self.assertEqual(manifest['worker_build_digest'], 'sha256:' + hashlib.sha256(binary.read_bytes()).hexdigest())
            binary.write_bytes(binary.read_bytes() + b'changed after deployment evidence')
            rejected = subprocess.run([str(self.tool), 'validate-worker-deployment', str(output / 'worker-manifests'),
                str(output / 'worker-configs'), str(binaries), 'sha256:' + 'a' * 64], cwd=ROOT, capture_output=True, text=True)
            self.assertNotEqual(rejected.returncode, 0)

    def test_missing_binary_or_old_manifest_fails_closed(self):
        for mutation in ('missing', 'old'):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                runtime, binaries = self.seed(directory)
                if mutation == 'missing':
                    (binaries / 'platform-history-maintenance').unlink()
                else:
                    path = runtime / 'config/orchestration.json'
                    config = json.loads(path.read_text())
                    config['worker_manifest']['manifest_version'] = 1
                    path.write_text(json.dumps(config))
                result, _ = self.generate(directory, runtime, binaries)
                self.assertNotEqual(result.returncode, 0)
                self.assertRegex(result.stderr, 'actual image executable missing|current owning worker manifest')

    def test_manifest_references_follow_actual_image_bytes_without_changing_semantics(self):
        for rebuilt in (False, True):
            with self.subTest(rebuilt=rebuilt), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                runtime, binaries = self.seed(directory)
                source = runtime / 'config'
                before = {path.name: path.read_bytes() for path in source.iterdir()}
                if rebuilt:
                    for binary in ('platform-model-worker', 'platform-context-worker'):
                        (binaries / binary).write_bytes(('rebuilt image bytes: ' + binary).encode())
                result, output = self.generate(directory, runtime, binaries)
                self.assertEqual(result.returncode, 0, result.stderr)
                for old_name, new_name in (('model-worker.json', 'model-worker.json'),
                                           ('context-native.json', 'context-worker.json')):
                    old = json.loads(before[old_name])
                    new = json.loads((output / 'configs' / new_name).read_bytes())
                    old_manifest = old['worker_manifest']
                    new_manifest = new['worker_manifest']
                    self.assertEqual(old_manifest == new_manifest, not rebuilt)
                    self.assertEqual(dict(old_manifest, worker_build_digest=new_manifest['worker_build_digest']), new_manifest)
                    if old_name == 'model-worker.json':
                        self.assertEqual(new['installed_adapters'], [dict(adapter,
                            worker_manifest_digest=canonical_digest(new_manifest)) for adapter in old['installed_adapters']])
                old_sources = json.loads(before['context-dataset-worker.json'])['sources']
                new_sources = json.loads((output / 'configs/context-dataset-worker.json').read_bytes())['sources']
                self.assertEqual(len(old_sources), len(new_sources))
                for old, new in zip(old_sources, new_sources):
                    expected_binding = dict(old['binding'], required_worker_manifest_digest=canonical_digest(new_manifest))
                    expected_binding.pop('canonical_digest')
                    expected_binding['canonical_digest'] = canonical_digest(expected_binding)
                    self.assertEqual(new, dict(old, binding=expected_binding))
                self.assertEqual(before, {path.name: path.read_bytes() for path in source.iterdir()})

    def test_invalid_seed_manifest_references_cannot_be_repaired_by_rebinding(self):
        for mutation in ('model-reference', 'dataset-reference', 'dataset-adapter', 'dataset-digest', 'dataset-duplicate', 'dataset-field', 'dataset-schema-float'):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                runtime, binaries = self.seed(directory)
                path = runtime / 'config' / ('model-worker.json' if mutation == 'model-reference' else 'context-dataset-worker.json')
                config = json.loads(path.read_bytes())
                if mutation == 'model-reference':
                    config['installed_adapters'][0]['worker_manifest_digest'] = 'sha256:' + 'f' * 64
                elif mutation == 'dataset-duplicate':
                    config['sources'].append(copy.deepcopy(config['sources'][0]))
                elif mutation == 'dataset-schema-float':
                    # Ruby Hash equality considers 1.0 equal to 1; the owning schema does not.
                    config['sources'][0]['binding']['schema_version'] = 1.0
                else:
                    binding = config['sources'][0]['binding']
                    key = {'dataset-reference': 'required_worker_manifest_digest', 'dataset-adapter': 'installed_adapter_digest',
                           'dataset-digest': 'canonical_digest', 'dataset-field': 'unknown'}[mutation]
                    binding[key] = 'sha256:' + 'f' * 64
                    if mutation != 'dataset-digest':
                        binding.pop('canonical_digest')
                        binding['canonical_digest'] = canonical_digest(binding)
                path.write_text(json.dumps(config))
                before = path.read_bytes()
                result, _ = self.generate(directory, runtime, binaries)
                self.assertNotEqual(result.returncode, 0)
                self.assertRegex(result.stderr, 'Model adapters must reference|Dataset source')
                self.assertEqual(path.read_bytes(), before)

    def test_rebound_model_configuration_passes_actual_worker_validation(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            runtime, binaries = self.seed(directory)
            seed = json.loads((runtime / 'config/model-worker.json').read_bytes())
            (binaries / 'platform-model-worker').write_bytes(self.model_tool.read_bytes())
            result, output = self.generate(directory, runtime, binaries)
            self.assertEqual(result.returncode, 0, result.stderr)
            path = output / 'configs/model-worker.json'
            config = json.loads(path.read_bytes())
            for stale in (False, True):
                with self.subTest(stale=stale):
                    if stale:
                        config['installed_adapters'][0]['worker_manifest_digest'] = canonical_digest(seed['worker_manifest'])
                    path.write_text(json.dumps(config))
                    decoded = subprocess.run([str(self.model_tool)], env={
                        'PLATFORM_MODEL_WORKER_CONFIG': str(path),
                        'PLATFORM_MODEL_WORKER_CONFIG_DIGEST': canonical_digest(config),
                        'PLATFORM_MODEL_WORKER_DATABASE_URL': 'postgresql://[',
                    }, capture_output=True, text=True, timeout=5)
                    self.assertEqual(decoded.returncode, 1)
                    reason = 'configuration is invalid' if stale else 'database is unavailable'
                    self.assertEqual(decoded.stderr.strip(), 'platform-model-worker failed: ' + reason)

    def test_generated_maintenance_configuration_passes_the_actual_service_decoder(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            runtime, binaries = self.seed(directory)
            result, output = self.generate(directory, runtime, binaries)
            self.assertEqual(result.returncode, 0, result.stderr)
            path = output / 'configs/artifact-maintenance.json'
            original = json.loads(path.read_bytes())

            def decode(config):
                path.write_text(json.dumps(config))
                # Invalid URL syntax stops immediately after the actual owning load/validate
                # path, before any database, AWS, listener, or worker startup side effect.
                return subprocess.run([str(self.maintenance_tool)], env={
                    'PLATFORM_ARTIFACT_MAINTENANCE_CONFIG': str(path),
                    'PLATFORM_ARTIFACT_MAINTENANCE_CONFIG_DIGEST': canonical_digest(config),
                    'PLATFORM_ARTIFACT_MAINTENANCE_DATABASE_URL': 'postgresql://[',
                }, capture_output=True, text=True, timeout=5)

            accepted = decode(original)
            self.assertEqual(accepted.returncode, 1)
            self.assertEqual(accepted.stderr.strip(), 'platform-artifact-maintenance failed: database unavailable')
            for mutation in ('claim-limit', 'provider-shape', 'unknown-field'):
                with self.subTest(mutation=mutation):
                    changed = copy.deepcopy(original)
                    if mutation == 'claim-limit':
                        changed['worker']['claim_batch'] = 0
                    elif mutation == 'provider-shape':
                        changed['artifact_provider_catalog']['reference_key_bindings'][0]['config']['key_id'] = 'invalid-key'
                    else:
                        changed['unexpected'] = True
                    rejected = decode(changed)
                    self.assertEqual(rejected.returncode, 1)
                    self.assertEqual(rejected.stderr.strip(), 'platform-artifact-maintenance failed: invalid configuration')

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
        self.assertIn('docker create --pull=never --platform "$docker_platform" --entrypoint /bin/false "$platform_digest"', source)
        self.assertIn('"$observed_descriptor" != "$platform_digest"', source)
        self.assertIn('"$observed_manifest" != "$platform_digest"', source)
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
