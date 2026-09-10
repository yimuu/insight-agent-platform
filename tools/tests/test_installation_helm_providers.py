"""Render the real chart against only task-owned, read-only Job/Pod API responses."""
import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import subprocess
import tempfile
import threading
import unittest

ROOT = Path(__file__).resolve().parents[2]
CHART = ROOT / 'deploy/helm/insight-platform-installation'
OWNER = 'a' * 32
DIGEST = 'sha256:' + 'a' * 64
IDENTITY = 'sha256:' + 'c' * 64
IMAGE = 'example/fixture@' + DIGEST


def fixed_command(phase):
    base = f'exec /usr/local/bin/platform-installation {phase} --input /installation-input/input.json --state /installation/private'
    if phase in ('provider-start', 'provider-observe'):
        return base + ' > /tmp/installation-result.json 2>&1'
    return base + ' --output /output' + ('' if phase == 'prepare' else ' --binaries /usr/local/bin') + ' > /tmp/installation-result.json'


def completed(phase, mode='initialize_once'):
    name = 'installation-' + phase + '-' + 'b' * 16
    container = {'name': 'installation', 'image': IMAGE, 'command': ['/bin/sh', '-ec'],
                 'args': [fixed_command(phase)], 'terminationMessagePath': '/tmp/installation-result.json'}
    job = {'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': {
        'name': name, 'namespace': 'helm-test', 'uid': phase + '-job-uid',
        'labels': {'insight.platform/installation': OWNER},
        'annotations': {'insight.platform/input-digest': DIGEST, 'insight.platform/phase': phase}},
        'spec': {'backoffLimit': 0, 'template': {'spec': {'containers': [copy.deepcopy(container)]}}},
        'status': {'succeeded': 1, 'conditions': [{'type': 'Complete', 'status': 'True'}]}}
    result = {'schema_version': 1, 'input_digest': DIGEST, 'identity_digest': IDENTITY}
    result.update({'mode': mode} if phase == 'provider-start' else {'phase': {
        'prepare': 'prepared', 'provider-observe': 'provider_ready', 'provision': 'ready', 'verify': 'ready'}[phase]})
    pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': name + '-pod', 'namespace': 'helm-test',
        'uid': phase + '-pod-uid', 'ownerReferences': [{'kind': 'Job', 'uid': job['metadata']['uid'], 'controller': True}]},
        'spec': {'containers': [container]}, 'status': {'phase': 'Succeeded', 'containerStatuses': [
            {'name': 'installation', 'state': {'terminated': {'exitCode': 0, 'message': json.dumps(result)}}}]}}
    proof = {'job': name, 'job_uid': job['metadata']['uid'], 'pod': pod['metadata']['name'],
             'pod_uid': pod['metadata']['uid'], 'identity_digest': IDENTITY}
    if phase == 'provider-start':
        proof['mode'] = mode
    return job, pod, proof


class HelmProviderTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        # Only rendering fields are fabricated here; the Rust test independently verifies that
        # helm_plan emits the exact shared server_arguments, rather than this fixture array.
        self.plan = {'schema_version': 1, 'namespace': 'helm-test',
            'input': {'name': 'helm-test', 'network': {'topology': 'kubernetes_local'}}, 'input_digest': DIGEST,
            'runtime_image': IMAGE, 'console_image': IMAGE,
            'dependencies': {name: IMAGE for name in ('postgres', 'nats', 's3', 'openbao')},
            'dependency_commands': {'s3': ['-config_dir=/run/insight/s3', 'server', '-fixture-exact-preserved=true']},
            'dependency_stop_grace_seconds': {'s3': 45},
            'processes': [{'name': 'artifact-gateway', 'binary': 'platform-artifact-gateway', 'uid': 10001,
                           'port': 8080, 'observability_port': 9080}]}
        self.values = {'plan': self.plan, 'phase': 'prepare', 'owner': OWNER, 'namespaceUID': 'namespace-uid',
                       'node': 'fixture-node', 'storageClass': 'standard', 'operation': 'd' * 16,
                       'prepared': {}, 'providerStarted': {}, 'providerReady': {}, 'ready': {}}
        self.proofs = {}
        for phase, field in [('prepare', 'prepared'), ('provider-start', 'providerStarted'),
                             ('provider-observe', 'providerReady'), ('provision', 'ready')]:
            self.proofs[field] = completed(phase)
            self.values[field] = self.proofs[field][2]
        self.documents = [item for job, pod, _ in self.proofs.values() for item in (job, pod)]
        documents = self.documents
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass
            def do_GET(self):
                responses = {
                    '/version': {'major': '1', 'minor': '30', 'gitVersion': 'v1.30.0'},
                    '/api': {'kind': 'APIVersions', 'apiVersion': 'v1', 'versions': ['v1']},
                    '/apis': {'kind': 'APIGroupList', 'apiVersion': 'v1', 'groups': [
                        {'name': name, 'versions': [{'groupVersion': name + '/v1', 'version': 'v1'}],
                         'preferredVersion': {'groupVersion': name + '/v1', 'version': 'v1'}} for name in ('batch', 'apps')]},
                    '/api/v1': {'kind': 'APIResourceList', 'groupVersion': 'v1', 'resources': [
                        {'name': name, 'namespaced': True, 'kind': kind, 'verbs': ['get', 'list']}
                        for name, kind in [('pods', 'Pod'), ('configmaps', 'ConfigMap'), ('persistentvolumeclaims', 'PersistentVolumeClaim'), ('services', 'Service')]]},
                    '/apis/batch/v1': {'kind': 'APIResourceList', 'groupVersion': 'batch/v1', 'resources': [
                        {'name': 'jobs', 'namespaced': True, 'kind': 'Job', 'verbs': ['get', 'list']}]},
                    '/apis/apps/v1': {'kind': 'APIResourceList', 'groupVersion': 'apps/v1', 'resources': [
                        {'name': 'deployments', 'namespaced': True, 'kind': 'Deployment', 'verbs': ['get', 'list']}]},
                }
                for document in documents:
                    prefix = '/apis/batch/v1/namespaces/helm-test/jobs/' if document['kind'] == 'Job' else '/api/v1/namespaces/helm-test/pods/'
                    responses[prefix + document['metadata']['name']] = document
                value = responses.get(self.path.split('?')[0])
                self.send_response(200 if value else 404)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(value or {'kind': 'Status', 'apiVersion': 'v1', 'status': 'Failure', 'reason': 'NotFound', 'code': 404}).encode())
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.kube = self.root / 'kube.json'
        self.kube.write_text(json.dumps({'apiVersion': 'v1', 'kind': 'Config', 'clusters': [{'name': 'fixture', 'cluster': {
            'server': f'http://127.0.0.1:{self.server.server_port}'}}], 'contexts': [{'name': 'fixture', 'context': {'cluster': 'fixture', 'user': 'fixture'}}],
            'current-context': 'fixture', 'users': [{'name': 'fixture', 'user': {'token': 'nonsecret-fixture'}}]}))
        self.kube.chmod(0o600)

    def render(self, phase):
        self.values['phase'] = phase
        path = self.root / 'values.json'
        path.write_text(json.dumps(self.values))
        return subprocess.run(['helm', 'template', 'installation', str(CHART), '--namespace', 'helm-test', '--values', str(path),
            '--dry-run=server', '--disable-openapi-validation', '--kubeconfig', str(self.kube)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)

    def accepted(self, phase):
        result = self.render(phase)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        source = "require 'yaml'; require 'json'; puts JSON.generate(STDIN.read.split(/^---\\s*$/).map { |s| YAML.safe_load(s, permitted_classes: [], aliases: false) }.compact)"
        return json.loads(subprocess.check_output(['ruby', '-e', source], input=result.stdout, timeout=10))

    def test_prepare_scopes_four_configs_and_data_without_starting_services(self):
        docs = self.accepted('prepare')
        self.assertFalse(any(doc['kind'] in ('Deployment', 'Service', 'Secret', 'Role', 'ClusterRole') for doc in docs))
        job = next(doc for doc in docs if doc['kind'] == 'Job')
        mounts = job['spec']['template']['spec']['containers'][0]['volumeMounts']
        for name in ('postgres', 'nats', 's3', 'openbao'):
            self.assertIn({'name': 'dependency-' + name, 'mountPath': '/output/dependencies/' + name}, mounts)
        for name in ('nats', 's3', 'openbao'):
            self.assertIn({'name': name + '-data', 'mountPath': '/output/' + name + '-data'}, mounts)
        self.assertNotIn('localstack', json.dumps(docs))

    def test_provider_jobs_are_single_attempt_private_only_closed_commands(self):
        for phase in ('provider-start', 'provider-observe'):
            with self.subTest(phase=phase):
                docs = self.accepted(phase)
                self.assertEqual([doc['kind'] for doc in docs], ['Job'])
                job = docs[0]
                pod = job['spec']['template']['spec']
                container = pod['containers'][0]
                self.assertEqual(container['args'], [fixed_command(phase)])
                self.assertEqual(job['spec']['backoffLimit'], 0)
                self.assertEqual(pod['restartPolicy'], 'Never')
                self.assertIs(pod['automountServiceAccountToken'], False)
                self.assertEqual({volume['name'] for volume in pod['volumes']}, {'installation-private', 'installation-input', 'temporary'})
                self.assertIn({'name': 'installation-input', 'mountPath': '/installation-input/input.json', 'subPath': 'input.json', 'readOnly': True}, container['volumeMounts'])
                self.assertEqual(container['securityContext']['capabilities'], {'drop': ['ALL']})
                self.assertFalse(any(env['name'].startswith('AWS_') for env in container['env']))

    def test_dependencies_publish_bao_service_only_and_consume_exact_shared_s3_command(self):
        self.values['providerReady'] = {}
        docs = self.accepted('dependencies')
        self.assertEqual({doc['metadata']['name'] for doc in docs if doc['kind'] == 'Service'}, {'postgres', 'nats', 's3', 'openbao'})
        deployments = {doc['metadata']['name']: doc for doc in docs if doc['kind'] == 'Deployment'}
        self.assertEqual(set(deployments), {'postgres', 'nats', 's3'})
        pod = deployments['s3']['spec']['template']['spec']
        container = pod['containers'][0]
        self.assertEqual(pod['terminationGracePeriodSeconds'], 45)
        self.assertEqual(container['command'], ['/usr/bin/weed'])
        self.assertEqual(container['args'], self.plan['dependency_commands']['s3'])
        self.assertEqual(container['securityContext']['runAsUser'], 10001)
        self.assertTrue(container['securityContext']['readOnlyRootFilesystem'])
        self.assertIn({'name': 'configuration', 'mountPath': '/run/insight/s3', 'readOnly': True}, container['volumeMounts'])
        self.assertEqual([port['port'] for doc in docs if doc['kind'] == 'Service' and doc['metadata']['name'] == 's3' for port in doc['spec']['ports']], [8333])
        self.assertNotIn('installation-private', json.dumps(deployments))

    def test_second_dependencies_starts_normal_bao_only_after_actual_provider_ready(self):
        docs = self.accepted('dependencies')
        deployments = {doc['metadata']['name']: doc for doc in docs if doc['kind'] == 'Deployment'}
        self.assertEqual(set(deployments), {'postgres', 'nats', 's3', 'openbao'})
        container = deployments['openbao']['spec']['template']['spec']['containers'][0]
        self.assertEqual(container['command'], ['/usr/bin/bao'])
        self.assertEqual(container['args'], ['server', '-config=/run/insight-openbao/serve.json'])
        self.assertFalse(any(doc['kind'] == 'Job' for doc in docs))
        self.assertNotIn('initialize.json', json.dumps(docs))

    def test_second_dependencies_rejects_fabricated_provider_ready_or_old_ready_boolean(self):
        saved = copy.deepcopy(self.values['providerReady'])
        self.values['providerReady']['job'] = 'fabricated-job'
        self.assertNotEqual(self.render('dependencies').returncode, 0)
        self.values['providerReady'] = saved
        self.proofs['providerReady'][0]['status'] = {'conditions': []}
        self.assertNotEqual(self.render('dependencies').returncode, 0)
        self.values['providerReady'] = {}
        self.values['ready'] = True
        self.assertNotEqual(self.render('dependencies').returncode, 0)

    def test_provision_requires_provider_ready_and_only_runs_normal_bao(self):
        docs = self.accepted('provision')
        deployments = {doc['metadata']['name']: doc for doc in docs if doc['kind'] == 'Deployment'}
        self.assertEqual(set(deployments), {'postgres', 'nats', 's3', 'openbao'})
        pod = deployments['openbao']['spec']['template']['spec']
        container = pod['containers'][0]
        self.assertEqual(container['command'], ['/usr/bin/bao'])
        self.assertEqual(container['args'], ['server', '-config=/run/insight-openbao/serve.json'])
        self.assertEqual(container['securityContext']['runAsUser'], 10001)
        self.assertNotIn('initialize.json', json.dumps(docs))
        job = next(doc for doc in docs if doc['kind'] == 'Job')
        env = {item['name']: item['value'] for item in job['spec']['template']['spec']['containers'][0]['env']}
        self.assertEqual(env['AWS_SHARED_CREDENTIALS_FILE'], '/installation/private/s3-artifact-gateway-credentials')
        self.assertEqual(env['SSL_CERT_FILE'], '/installation/private/ca.pem')
        self.assertNotIn('AWS_SECRET_ACCESS_KEY', env)
        self.values['providerReady'] = {}
        self.assertNotEqual(self.render('provision').returncode, 0)

    def test_serving_mounts_only_its_role_and_verify_emits_only_readonly_job(self):
        docs = self.accepted('serving')
        deployments = {doc['metadata']['name']: doc for doc in docs if doc['kind'] == 'Deployment'}
        for name in ('artifact-gateway', 'console'):
            spec = deployments[name]['spec']['template']['spec']
            claims = [volume['persistentVolumeClaim'] for volume in spec['volumes'] if 'persistentVolumeClaim' in volume]
            self.assertEqual(claims, [{'claimName': 'role-' + name, 'readOnly': True}])
        docs = self.accepted('verify')
        self.assertEqual([doc['kind'] for doc in docs], ['Job'])
        self.assertEqual(docs[0]['spec']['template']['spec']['containers'][0]['args'], [fixed_command('verify')])

    def test_old_ready_does_not_replace_missing_current_provider_proof(self):
        for field in ('providerStarted', 'providerReady'):
            with self.subTest(field=field):
                saved = self.values[field]
                self.values[field] = {}
                self.assertNotEqual(self.render('serving').returncode, 0)
                self.values[field] = saved
        job = self.proofs['providerReady'][0]
        job['status'] = {'conditions': []}
        self.assertNotEqual(self.render('serving').returncode, 0)

    def test_provider_start_mode_must_equal_actual_owner_envelope(self):
        self.values['providerStarted']['mode'] = 'serve'
        self.assertNotEqual(self.render('dependencies').returncode, 0)
        pod = self.proofs['providerStarted'][1]
        state = pod['status']['containerStatuses'][0]['state']['terminated']
        result = json.loads(state['message'])
        result['mode'] = 'serve'
        state['message'] = json.dumps(result)
        self.accepted('dependencies')

    def test_failed_plus_complete_cannot_be_accepted(self):
        for field in ('prepared', 'providerStarted', 'providerReady', 'ready'):
            with self.subTest(field=field):
                conditions = self.proofs[field][0]['status']['conditions']
                conditions.append({'type': 'Failed', 'status': 'True'})
                self.assertNotEqual(self.render('serving').returncode, 0)
                conditions.pop()

    def test_provider_job_uid_owner_command_phase_or_identity_tampering_is_rejected(self):
        job, pod, _ = self.proofs['providerReady']
        original_job, original_pod = copy.deepcopy(job), copy.deepcopy(pod)
        for case in ('job_uid', 'pod_uid', 'owner', 'pod_owner', 'phase', 'image', 'command', 'job_command', 'identity'):
            job.clear(); job.update(copy.deepcopy(original_job))
            pod.clear(); pod.update(copy.deepcopy(original_pod))
            if case == 'job_uid': job['metadata']['uid'] = 'foreign'
            elif case == 'pod_uid': pod['metadata']['uid'] = 'foreign'
            elif case == 'owner': job['metadata']['labels']['insight.platform/installation'] = 'f' * 32
            elif case == 'pod_owner': pod['metadata']['ownerReferences'][0]['uid'] = 'foreign'
            elif case == 'phase': job['metadata']['annotations']['insight.platform/phase'] = 'provision'
            elif case == 'image': pod['spec']['containers'][0]['image'] = 'foreign@' + DIGEST
            elif case == 'command': pod['spec']['containers'][0]['args'] = [fixed_command('provider-observe').removesuffix(' 2>&1')]
            elif case == 'job_command': job['spec']['template']['spec']['containers'][0]['args'] = ['echo forged']
            else: self.values['providerReady']['identity_digest'] = 'sha256:' + 'f' * 64
            with self.subTest(case=case):
                self.assertNotEqual(self.render('provision').returncode, 0)

    def test_completion_json_rejects_wrong_schema_duplicate_fields_and_mode_as_phase(self):
        state = self.proofs['providerReady'][1]['status']['containerStatuses'][0]['state']['terminated']
        original = state['message']
        for invalid in (original.replace('"schema_version": 1', '"schema_version": "1"'),
                        original.replace('"schema_version": 1', '"schema_version": true'),
                        original[:-1] + ',"phase":"provider_ready"}',
                        original[:-1] + ',"\\u0070hase":"provider_ready"}',
                        original.replace('"phase"', '"mode"')):
            state['message'] = invalid
            self.assertNotEqual(self.render('provision').returncode, 0)

    def test_values_have_no_legacy_dependency_or_boolean_phase_escape(self):
        self.plan['dependencies']['localstack'] = IMAGE
        self.assertNotEqual(self.render('prepare').returncode, 0)
        del self.plan['dependencies']['localstack']
        self.values['providerReady'] = True
        self.assertNotEqual(self.render('prepare').returncode, 0)


if __name__ == '__main__':
    unittest.main()
