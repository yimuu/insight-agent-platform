"""Real Helm rendering against a task-only API fixture, plus host recovery/file boundaries."""
import argparse
import base64
import copy
import functools
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import socket
import struct
import sys
import tempfile
import threading
import time
import unittest
from urllib.parse import parse_qs, urlparse
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('installation_helm', ROOT/'tools/install/platform_helm.py')
INSTALL = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(INSTALL)


@functools.lru_cache(maxsize=1)
def owner_plan():
    metadata = json.loads(subprocess.check_output(['cargo', 'metadata', '--locked', '--no-deps', '--format-version', '1'], cwd=ROOT, timeout=30))
    binary = Path(metadata['target_directory'])/'debug/platform-installation'
    if not binary.exists():
        subprocess.run(['cargo', 'build', '--locked', '-p', 'insight-platform-installation-tooling', '--bin', 'platform-installation'], cwd=ROOT, check=True, timeout=600)
    digest = 'sha256:'+'a'*64
    with tempfile.TemporaryDirectory() as temporary:
        path = Path(temporary).resolve()/'input.json'
        path.write_bytes(subprocess.check_output([str(binary), 'kubernetes-input', 'helm-test', digest], cwd=ROOT, timeout=30))
        return INSTALL.validate_plan(json.loads(subprocess.check_output([str(binary), 'helm-plan', '--input', str(path), '--runtime-image', 'example/platform@'+digest, '--console-image', 'example/console@'+digest], timeout=30)))


def completed(plan, phase):
    name = 'installation-'+phase+'-'+'b'*16
    provider = phase in ('provider-start', 'provider-observe')
    extra = '' if provider else ' --output /output' + ('' if phase == 'prepare' else ' --binaries /usr/local/bin')
    suffix = ' 2>&1' if provider else ''
    container = {'name': 'installation', 'image': plan['runtime_image'], 'command': ['/bin/sh', '-ec'], 'args': [f'exec /usr/local/bin/platform-installation {phase} --input /installation-input/input.json --state /installation/private{extra} > /tmp/installation-result.json{suffix}'], 'terminationMessagePath': '/tmp/installation-result.json'}
    job = {'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': {'name': name, 'namespace': 'helm-test', 'uid': phase+'-job-uid', 'labels': {INSTALL.OWNER: 'a'*32}, 'annotations': {INSTALL.DIGEST: plan['input_digest'], 'insight.platform/phase': phase}}, 'spec': {'backoffLimit': 0, 'template': {'spec': {'containers': [container]}}}, 'status': {'succeeded': 1, 'conditions': [{'type': 'Complete', 'status': 'True'}]}}
    field = 'mode' if phase == 'provider-start' else 'phase'
    message = json.dumps({'schema_version': 1, field: {'prepare':'prepared','provider-start':'initialize_once','provider-observe':'provider_ready'}.get(phase,'ready'), 'input_digest': plan['input_digest'], 'identity_digest': 'sha256:'+'c'*64})
    pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': name+'-pod', 'namespace': 'helm-test', 'uid': phase+'-pod-uid', 'ownerReferences': [{'kind': 'Job', 'uid': job['metadata']['uid'], 'controller': True}]}, 'spec': {'containers': [container]}, 'status': {'phase': 'Succeeded', 'containerStatuses': [{'name': 'installation', 'state': {'terminated': {'exitCode': 0, 'message': message}}}]}}
    proof = INSTALL.completion(job, [pod], plan=plan, owner='a'*32, phase=phase, job_name=name)
    return job, pod, proof


class HelmTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.plan = copy.deepcopy(owner_plan())
        _, _, started = completed(self.plan, 'provider-start')
        _, _, observed = completed(self.plan, 'provider-observe')
        self.values = {'providerStarted': started, 'providerReady': observed, 'plan': self.plan, 'phase': 'prepare', 'owner': 'a'*32, 'namespaceUID': 'namespace-uid', 'node': 'fixture-node', 'storageClass': 'standard', 'operation': 'b'*16, 'prepared': {}, 'ready': {}}

    def render(self, *, server=None):
        file = self.root/'values.json'
        file.write_text(json.dumps(self.values))
        command = ['helm', 'template', 'installation', str(INSTALL.CHART), '--namespace', 'helm-test', '--values', str(file)]
        if server:
            kube = self.root/'kube.json'
            kube.write_text(json.dumps({'apiVersion': 'v1', 'kind': 'Config', 'clusters': [{'name': 'fixture', 'cluster': {'server': f'http://127.0.0.1:{server.server_port}'}}], 'contexts': [{'name': 'fixture', 'context': {'cluster': 'fixture', 'user': 'fixture'}}], 'current-context': 'fixture', 'users': [{'name': 'fixture', 'user': {'token': 'nonsecret-api-fixture'}}]}))
            # This fixture supplies read-only discovery/Job/Pod responses, not a Kubernetes
            # OpenAPI server. Helm still renders and evaluates the real lookup gate.
            command += ['--dry-run=server', '--disable-openapi-validation', '--kubeconfig', str(kube)]
        return subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)

    def api(self, documents):
        documents = list(documents)
        for phase in ('provider-start', 'provider-observe'):
            job, pod, _ = completed(self.plan, phase)
            if not any(value['metadata']['name'] == job['metadata']['name'] for value in documents):
                documents.extend((job, pod))
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass
            def do_GET(self):
                path = self.path.split('?')[0]
                responses = {
                    '/version': {'major': '1', 'minor': '30', 'gitVersion': 'v1.30.0'},
                    '/api': {'kind': 'APIVersions', 'apiVersion': 'v1', 'versions': ['v1']},
                    '/apis': {'kind': 'APIGroupList', 'apiVersion': 'v1', 'groups': [{'name': name, 'versions': [{'groupVersion': name+'/v1', 'version': 'v1'}], 'preferredVersion': {'groupVersion': name+'/v1', 'version': 'v1'}} for name in ('batch', 'apps')]},
                    '/api/v1': {'kind': 'APIResourceList', 'groupVersion': 'v1', 'resources': [{'name': name, 'namespaced': True, 'kind': kind, 'verbs': ['get', 'list']} for name, kind in [('pods', 'Pod'), ('configmaps', 'ConfigMap'), ('persistentvolumeclaims', 'PersistentVolumeClaim'), ('services', 'Service')]]},
                    '/apis/apps/v1': {'kind': 'APIResourceList', 'groupVersion': 'apps/v1', 'resources': [{'name': 'deployments', 'namespaced': True, 'kind': 'Deployment', 'verbs': ['get', 'list']}]},
                    '/apis/batch/v1': {'kind': 'APIResourceList', 'groupVersion': 'batch/v1', 'resources': [{'name': 'jobs', 'namespaced': True, 'kind': 'Job', 'verbs': ['get', 'list']}]},
                }
                for document in documents:
                    prefix = '/apis/batch/v1/namespaces/helm-test/jobs/' if document['kind'] == 'Job' else '/api/v1/namespaces/helm-test/pods/'
                    responses[prefix+document['metadata']['name']] = document
                value = responses.get(path)
                self.send_response(200 if value else 404)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(value or {'kind': 'Status', 'apiVersion': 'v1', 'status': 'Failure', 'reason': 'NotFound', 'code': 404}).encode())
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server

    def documents(self, output):
        # The repository's existing Helm qualification also uses Ruby's safe YAML reader.
        source = "require 'yaml'; require 'json'; puts JSON.generate(STDIN.read.split(/^---\\s*$/).map { |s| YAML.safe_load(s, permitted_classes: [], aliases: false) }.compact)"
        return json.loads(subprocess.check_output(['ruby', '-e', source], input=output, timeout=10))

    def test_prepare_contains_only_scoped_pvcs_input_and_closed_job(self):
        result = self.render()
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        documents = self.documents(result.stdout)
        self.assertFalse(any(item['kind'] in ('Deployment', 'Service', 'Role', 'ClusterRole', 'Secret') for item in documents))
        job = next(item for item in documents if item['kind'] == 'Job')
        pod = job['spec']['template']['spec']
        self.assertIs(pod['automountServiceAccountToken'], False)
        self.assertEqual(pod['dnsPolicy'], 'ClusterFirst')
        self.assertEqual(pod['dnsConfig']['options'], [{'name': 'ndots', 'value': '1'}])
        self.assertEqual(job['spec']['backoffLimit'], 0)
        self.assertEqual(pod['containers'][0]['terminationMessagePath'], '/tmp/installation-result.json')
        self.assertIn({'name': 'installation-input', 'mountPath': '/installation-input/input.json', 'subPath': 'input.json', 'readOnly': True}, pod['containers'][0]['volumeMounts'])
        self.assertNotIn('docker.sock', result.stdout.decode())
        self.assertEqual(next(item for item in documents if item['kind'] == 'ConfigMap')['immutable'], True)

    def test_phase_switch_and_boolean_cannot_bypass_real_completion(self):
        self.values['phase'] = 'serving'
        self.assertNotEqual(self.render().returncode, 0)
        self.values['phase'] = 'prepare'
        self.values['readyOverride'] = True
        self.assertNotEqual(self.render().returncode, 0)

    def test_live_job_proof_enables_exact_role_mounts_without_api_permissions(self):
        prepare_job, prepare_pod, self.values['prepared'] = completed(self.plan, 'prepare')
        job, pod, self.values['ready'] = completed(self.plan, 'provision')
        self.values['phase'] = 'serving'
        server = self.api([prepare_job, prepare_pod, job, pod])
        result = self.render(server=server)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        documents = self.documents(result.stdout)
        deployments = {item['metadata']['name']: item for item in documents if item['kind'] == 'Deployment'}
        for deployment in deployments.values():
            dns = deployment['spec']['template']['spec']
            self.assertEqual(dns['dnsPolicy'], 'ClusterFirst')
            self.assertEqual(dns['dnsConfig']['options'], [{'name': 'ndots', 'value': '1'}])
        for process in self.plan['processes']:
            spec = deployments[process['name']]['spec']['template']['spec']
            self.assertIs(spec['automountServiceAccountToken'], False)
            claims = [volume['persistentVolumeClaim'] for volume in spec['volumes'] if 'persistentVolumeClaim' in volume]
            self.assertEqual(claims, [{'claimName': 'role-'+process['name'], 'readOnly': True}])
            self.assertEqual(spec['containers'][0]['securityContext']['runAsUser'], 10001)
            self.assertTrue(spec['containers'][0]['args'][0].endswith('/'+process['binary']))
        console = deployments['console']['spec']['template']['spec']
        self.assertEqual(console['containers'][0]['volumeMounts'][0]['subPath'], 'config.json')
        self.assertEqual(console['containers'][0]['securityContext']['runAsUser'], 1000)
        bao = deployments['openbao']['spec']['template']['spec']['containers'][0]
        self.assertEqual(bao['command'], ['/usr/bin/bao'])
        self.assertEqual(bao['args'], ['server','-config=/run/insight-openbao/serve.json'])
        self.assertNotIn('localstack', deployments)
        self.assertNotIn('installation-private', json.dumps(deployments))

    def test_uid_failed_status_tampered_envelope_or_command_each_block_serving(self):
        prep, prep_pod, self.values['prepared'] = completed(self.plan, 'prepare')
        job, pod, self.values['ready'] = completed(self.plan, 'provision')
        self.values['phase'] = 'serving'
        for change in ('uid', 'failed', 'message', 'command'):
            changed_job, changed_pod = copy.deepcopy(job), copy.deepcopy(pod)
            if change == 'uid': changed_job['metadata']['uid'] = 'replaced-job'
            elif change == 'failed': changed_job['status']['conditions'][0]['status'] = 'False'
            elif change == 'message': changed_pod['status']['containerStatuses'][0]['state']['terminated']['message'] = '{}'
            else: changed_pod['spec']['containers'][0]['args'] = ['echo forged-ready']
            server = self.api([prep, prep_pod, changed_job, changed_pod])
            with self.subTest(change=change):
                self.assertNotEqual(self.render(server=server).returncode, 0)

    def test_verification_cannot_start_missing_roles_before_its_own_completion(self):
        prepare_job, prepare_pod, self.values['prepared'] = completed(self.plan, 'prepare')
        job, pod, self.values['ready'] = completed(self.plan, 'provision')
        self.values['phase'] = 'verify'
        result = self.render(server=self.api([prepare_job, prepare_pod, job, pod]))
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        documents = self.documents(result.stdout)
        deployments = {item['metadata']['name'] for item in documents if item['kind'] == 'Deployment'}
        self.assertFalse(deployments)
        self.assertEqual({item['kind'] for item in documents}, {'Job'})
        current = next(item for item in documents if item['kind'] == 'Job')
        self.assertIn('platform-installation verify ', current['spec']['template']['spec']['containers'][0]['args'][0])

    def test_host_completion_rejects_changed_image_pod_owner_ambiguous_and_failed_process(self):
        job, pod, _ = completed(self.plan, 'provision')
        for change in ('image', 'owner', 'ambiguous', 'exit', 'identity', 'duplicate-json'):
            changed = copy.deepcopy(pod)
            pods = [changed]
            if change == 'image': changed['spec']['containers'][0]['image'] = 'other:latest'
            elif change == 'owner': changed['metadata']['ownerReferences'][0]['uid'] = 'other-job'
            elif change == 'ambiguous': pods.append(copy.deepcopy(pod))
            elif change == 'exit': changed['status']['containerStatuses'][0]['state']['terminated']['exitCode'] = 1
            else:
                result = json.loads(changed['status']['containerStatuses'][0]['state']['terminated']['message'])
                result['input_digest'] = 'sha256:'+'d'*64
                text = json.dumps(result)
                if change == 'duplicate-json': text = '{"schema_version":1,'+text[1:]
                changed['status']['containerStatuses'][0]['state']['terminated']['message'] = text
            with self.subTest(change=change), self.assertRaises(INSTALL.InstallationFailure):
                INSTALL.completion(job, pods, plan=self.plan, owner='a'*32, phase='provision', job_name=job['metadata']['name'])

    def job_waiter(self):
        private = self.root/'private'
        INSTALL.private_directory(private)
        args = argparse.Namespace(directory=private, kubeconfig=self.root/'kubeconfig', context='fixture', node='fixture', storage_class='standard')
        installation = INSTALL.Installation(args, self.plan)
        installation.state.update(owner='a'*32, namespace_uid='namespace-uid')
        job, _, _ = completed(self.plan, 'provision')
        return installation, job

    def test_job_waiter_complete_returns_same_job_without_mutation(self):
        installation, job = self.job_waiter()
        previous = copy.deepcopy(installation.state)
        with mock.patch.object(INSTALL, 'command', return_value=json.dumps(job).encode()) as calls, mock.patch.object(INSTALL.time, 'monotonic', return_value=0), mock.patch.object(INSTALL.time, 'sleep') as sleep:
            self.assertEqual(installation.wait_job('provision', job['metadata']['name']), job)
        self.assertEqual(installation.state, previous)
        self.assertEqual(calls.call_count, 1)
        self.assertIn('get', calls.call_args.args[0])
        self.assertEqual(calls.call_args.kwargs['timeout'], 15)
        sleep.assert_not_called()

    def test_job_waiter_failed_wins_over_complete_and_never_exposes_status_message(self):
        installation, job = self.job_waiter()
        job['status']['conditions'].append({'type': 'Failed', 'status': 'True', 'message': 'untrusted private failure output'})
        with mock.patch.object(INSTALL, 'command', return_value=json.dumps(job).encode()) as calls, mock.patch.object(INSTALL.time, 'monotonic', return_value=0), mock.patch.object(INSTALL.time, 'sleep') as sleep:
            with self.assertRaisesRegex(INSTALL.InstallationFailure, '^installation Job failed$'):
                installation.wait_job('provision', job['metadata']['name'])
        self.assertEqual(calls.call_count, 1)
        self.assertFalse(installation.state['ready'])
        sleep.assert_not_called()

    def test_job_waiter_running_waits_for_complete_of_the_same_uid(self):
        installation, job = self.job_waiter()
        running = copy.deepcopy(job)
        running['status'] = {'active': 1, 'conditions': [{'type': 'Complete', 'status': 'False'}, {'type': 'Failed', 'status': 'False'}]}
        with mock.patch.object(INSTALL, 'command', side_effect=[json.dumps(value).encode() for value in [running, job]]) as calls, mock.patch.object(INSTALL.time, 'monotonic', return_value=0), mock.patch.object(INSTALL.time, 'sleep') as sleep:
            self.assertEqual(installation.wait_job('provision', job['metadata']['name']), job)
        self.assertEqual(calls.call_count, 2)
        self.assertEqual(calls.call_args_list[0], calls.call_args_list[1])
        sleep.assert_called_once_with(1)

    def test_job_waiter_foreign_uid_precedes_complete_acceptance(self):
        installation, job = self.job_waiter()
        running = copy.deepcopy(job)
        running['status'] = {'active': 1}
        changed = copy.deepcopy(job)
        changed['metadata']['uid'] = 'foreign-job-uid'
        with mock.patch.object(INSTALL, 'command', side_effect=[json.dumps(value).encode() for value in [running, changed]]) as calls, mock.patch.object(INSTALL.time, 'monotonic', return_value=0), mock.patch.object(INSTALL.time, 'sleep'):
            with self.assertRaisesRegex(INSTALL.InstallationFailure, 'UID changed'):
                installation.wait_job('provision', job['metadata']['name'])
        self.assertEqual(calls.call_count, 2)
        self.assertFalse(installation.state['ready'])

    def test_job_waiter_rejects_foreign_identity_before_terminal_state(self):
        installation, job = self.job_waiter()
        for field in ['name', 'namespace', 'owner', 'digest', 'phase', 'uid', 'backoff']:
            changed = copy.deepcopy(job)
            if field in ['name', 'namespace', 'uid']:
                changed['metadata'][field] = '' if field == 'uid' else 'foreign'
            elif field == 'owner': changed['metadata']['labels'][INSTALL.OWNER] = 'f'*32
            elif field == 'digest': changed['metadata']['annotations'][INSTALL.DIGEST] = 'sha256:'+'f'*64
            elif field == 'phase': changed['metadata']['annotations']['insight.platform/phase'] = 'prepare'
            else: changed['spec']['backoffLimit'] = 1
            with self.subTest(field=field), mock.patch.object(INSTALL, 'command', return_value=json.dumps(changed).encode()), mock.patch.object(INSTALL.time, 'monotonic', return_value=0), mock.patch.object(INSTALL.time, 'sleep') as sleep:
                with self.assertRaisesRegex(INSTALL.InstallationFailure, 'identity differs'):
                    installation.wait_job('provision', job['metadata']['name'])
                sleep.assert_not_called()

    def test_job_waiter_deadline_bounds_get_and_rejects_late_complete(self):
        installation, job = self.job_waiter()
        with mock.patch.object(INSTALL, 'command', return_value=json.dumps(job).encode()) as calls, mock.patch.object(INSTALL.time, 'monotonic', side_effect=[0, 899.5, 900]), mock.patch.object(INSTALL.time, 'sleep') as sleep:
            with self.assertRaisesRegex(INSTALL.InstallationFailure, 'timed out'):
                installation.wait_job('provision', job['metadata']['name'])
        self.assertEqual(calls.call_args.kwargs['timeout'], .5)
        self.assertFalse(installation.state['ready'])
        sleep.assert_not_called()

    def test_private_files_reject_symlinks_hardlinks_and_immutable_drift(self):
        private = self.root/'private'
        INSTALL.private_directory(private)
        file = private/'state.json'
        INSTALL.persist(file, {'schema_version': 1}, immutable=True)
        inode = file.stat().st_ino
        INSTALL.persist(file, {'schema_version': 1}, immutable=True)
        self.assertEqual(file.stat().st_ino, inode)
        with self.assertRaises(INSTALL.InstallationFailure): INSTALL.persist(file, {'schema_version': 2}, immutable=True)
        os.link(file, private/'linked')
        with self.assertRaises(INSTALL.InstallationFailure): INSTALL.read_file(file, private=True)
        (private/'linked').unlink()
        alias = private/'alias'
        alias.symlink_to(file)
        with self.assertRaises(OSError): INSTALL.read_file(alias, private=True)
        broken = private/'broken'
        broken.symlink_to(private/'missing')
        with self.assertRaises(OSError): INSTALL.persist_bytes(broken, b'refuse overwrite')
        self.assertTrue(broken.is_symlink())

    def test_pure_owner_reads_private_input_as_host_uid_without_credentials_or_network(self):
        declaration = self.root/'input.json'
        declaration.write_text(json.dumps(self.plan['input']))
        declaration.chmod(0o600)
        (self.root/'kubeconfig').write_text('{}')
        directory = self.root/'rendered'
        arguments = ['platform_helm', 'render', '--input', str(declaration), '--directory', str(directory),
            '--runtime-image', self.plan['runtime_image'], '--console-image', self.plan['console_image'],
            '--kubeconfig', str(self.root/'kubeconfig'), '--context', 'fixture', '--node', 'fixture-node']
        with mock.patch.object(sys, 'argv', arguments), mock.patch.object(INSTALL, 'command', return_value=json.dumps(self.plan).encode()) as calls, mock.patch('builtins.print'):
            INSTALL.main()
        self.assertEqual(calls.call_count, 1)
        arguments = calls.call_args.args[0]
        self.assertEqual(arguments[arguments.index('--user')+1], str(os.geteuid())+':'+str(os.getegid()))
        self.assertEqual(arguments[arguments.index('--network')+1], 'none')
        self.assertEqual(arguments[arguments.index('--cap-drop')+1], 'ALL')
        self.assertEqual(arguments.count('--mount'), 1)
        self.assertNotIn('/var/run/docker.sock', ' '.join(arguments))
        self.assertEqual(declaration.stat().st_mode & 0o777, 0o600)

    def test_json_capacity_and_version_and_process_ports_are_strict(self):
        for data in (b'{"x":1,"x":2}', b'{"x":NaN}', json.dumps([0]*257).encode(), b'['*34+b'0'+b']'*34):
            with self.assertRaises(INSTALL.InstallationFailure): INSTALL.decode(data)
        wrong = copy.deepcopy(self.plan)
        wrong['schema_version'] = True
        with self.assertRaises(INSTALL.InstallationFailure): INSTALL.validate_plan(wrong)
        wrong = copy.deepcopy(self.plan)
        wrong['processes'][0]['port'] = 65536
        with self.assertRaises(INSTALL.InstallationFailure): INSTALL.validate_plan(wrong)

    def test_shared_s3_shutdown_grace_has_no_missing_or_weakened_default(self):
        for value in (None, {}, {'s3': 10}, {'s3': True}, {'s3': '45'}, {'s3': 45.0}, {'s3': 45, 'other': 45}):
            with self.subTest(value=value):
                wrong = copy.deepcopy(self.plan)
                if value is None:
                    del wrong['dependency_stop_grace_seconds']
                else:
                    wrong['dependency_stop_grace_seconds'] = value
                with self.assertRaises(INSTALL.InstallationFailure): INSTALL.validate_plan(wrong)

    def test_verify_before_ready_has_no_cluster_actions_and_foreign_namespace_is_not_adopted(self):
        private = self.root/'private'
        INSTALL.private_directory(private)
        args = argparse.Namespace(directory=private, kubeconfig=self.root/'kubeconfig', context='fixture', node='fixture', storage_class='standard')
        installation = INSTALL.Installation(args, self.plan)
        with mock.patch.object(INSTALL, 'command') as calls:
            with self.assertRaises(INSTALL.InstallationFailure): installation.run('verify')
            calls.assert_not_called()
        foreign = {'metadata': {'uid': 'foreign', 'labels': {}, 'annotations': {}}}
        with mock.patch.object(INSTALL, 'command', return_value=json.dumps(foreign).encode()) as calls:
            with self.assertRaises(INSTALL.InstallationFailure): installation.namespace_owner()
            self.assertEqual(len(calls.call_args_list), 1)
            self.assertIn('get', calls.call_args.args[0])

    def test_real_child_output_and_timeout_are_bounded(self):
        import sys
        with self.assertRaises(INSTALL.InstallationFailure):
            INSTALL.command([sys.executable, '-c', 'import time; time.sleep(5)'], timeout=.05)
        with self.assertRaises(INSTALL.InstallationFailure):
            INSTALL.command([sys.executable, '-c', 'import sys; sys.stdout.buffer.write(b"x"*2000000)'])
        self.assertEqual(INSTALL.command([sys.executable, '-c', 'print("bounded")']), b'bounded\n')

    def test_host_phase_order_lost_completion_recovery_and_restart_only_verify(self):
        private = self.root/'private'
        INSTALL.private_directory(private)
        args = argparse.Namespace(directory=private, kubeconfig=self.root/'kubeconfig', context='fixture', node='fixture', storage_class='standard')
        installation = INSTALL.Installation(args, self.plan)
        installation.state.update(owner='a'*32, namespace_uid='namespace-uid')
        installation.save()
        phases = []
        fail_wait_once = [True]
        def response(arguments, **_):
            if 'upgrade' in arguments:
                values = json.loads((private/'helm-values.json').read_text())
                saved = json.loads((private/'helm-state.json').read_text())
                self.assertEqual(values['operation'], saved['operation'])
                if values['phase'] == 'dependencies' and saved['phase'] in ('provision', 'verify'):
                    self.assertNotEqual(saved['ready'].get('job'), 'installation-'+saved['phase']+'-'+saved['operation'])
                else:
                    self.assertEqual(values['phase'], saved['phase'])
                if values['phase'] == 'serving': self.assertTrue(saved['ready'])
                phases.append(values['phase'])
                return b''
            if 'rollout' in arguments: return b''
            phase = installation.state['phase']
            kind = arguments[arguments.index('get')+1]
            if kind == 'job' and phase == 'provision' and fail_wait_once[0]:
                fail_wait_once[0] = False
                raise INSTALL.InstallationFailure('lost response')
            if kind == 'pvc':
                names = {'installation-private', 'postgres-data', 'nats-data', 's3-data', 'openbao-data', 'dependency-postgres', 'dependency-nats', 'dependency-s3', 'dependency-openbao', 'role-console'} | {'role-'+p['name'] for p in self.plan['processes']}
                return json.dumps({'items': [{'metadata': {'name': name, 'uid': 'uid-'+name, 'labels': {INSTALL.OWNER: 'a'*32}, 'annotations': {INSTALL.DIGEST: self.plan['input_digest'], 'insight.platform/namespace-uid': 'namespace-uid'}}} for name in names]}).encode()
            job, pod, _ = completed(self.plan, phase)
            actual = f"installation-{phase}-{installation.state['operation']}"
            job['metadata']['name'] = actual
            pod['metadata']['name'] = actual+'-pod'
            return json.dumps(job if kind == 'job' else {'items': [pod]}).encode()
        with mock.patch.object(installation, 'namespace_owner'), mock.patch.object(installation, 'ensure_provider'), mock.patch.object(installation, 'provider_stop'), mock.patch.object(installation, 'observe_serving_provider'), mock.patch.object(INSTALL, 'command', side_effect=response):
            with self.assertRaises(INSTALL.InstallationFailure): installation.run('up')
            interrupted = installation.state['operation']
            self.assertEqual(phases, ['prepare', 'dependencies', 'provision'])
            self.assertFalse(installation.state['ready'])
            with mock.patch('builtins.print'):
                installation.run('up')
            self.assertEqual(phases[-3:], ['dependencies', 'provision', 'serving'])
            # The second provision Helm call recovered the same Job operation, not a new create.
            self.assertNotEqual(interrupted, installation.state['operation'])  # only serving changes it
            phases.clear()
            with mock.patch('builtins.print'):
                installation.run('up')
            self.assertEqual(phases, ['dependencies', 'verify', 'serving'])

    def session_api(self):
        """Real kubectl transport, including channel-v5 exec and UID-conditional DELETE."""
        private = self.root/'private'
        INSTALL.private_directory(private)
        args = argparse.Namespace(directory=private, kubeconfig=self.root/'session-kube.json', context='fixture', node='fixture-node', storage_class='standard')
        installation = INSTALL.Installation(args, self.plan)
        installation.state.update(owner='a'*32, namespace_uid='namespace-uid', ready={'identity_digest': 'sha256:'+'c'*64})
        installation.save()
        state = {'pods': {}, 'created': 0, 'deletes': [], 'execs': [], 'lose_create': False, 'replace_delete': False, 'replace_after_token': False, 'expired': False, 'lose_token': False}
        tenant = 'ten_00000000-0000-7000-8000-000000000001'
        expiry = int(time.time())+900
        claims = json.dumps({'tenant_id': tenant, 'iat': expiry-900, 'exp': expiry}).encode()
        token = b'eyJhbGciOiJSUzI1NiJ9.'+base64.urlsafe_b64encode(claims).rstrip(b'=')+b'.c2lnbmF0dXJl\n'
        envelope = {'schema_version': 1, 'input_digest': self.plan['input_digest'], 'identity_digest': installation.state['ready']['identity_digest'], 'session_file': '/installation/private/session-token', 'tenant_id': tenant, 'endpoint': self.plan['input']['network']['console_origin'], 'expires_at_unix_seconds': expiry}
        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'
            def log_message(self, *_): pass
            def respond(self, value, code=200):
                body = json.dumps(value).encode()
                self.send_response(code)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            def read_json(self):
                if self.headers.get('Transfer-Encoding', '').lower() == 'chunked':
                    data = bytearray()
                    while True:
                        size = int(self.rfile.readline(64).split(b';')[0].strip(), 16)
                        assert size <= 65536 and len(data)+size <= 65536
                        if not size:
                            assert self.rfile.readline(64) == b'\r\n'
                            return json.loads(data)
                        data.extend(self.rfile.read(size))
                        assert self.rfile.read(2) == b'\r\n'
                size = int(self.headers.get('Content-Length', '0'))
                assert size <= 65536
                return json.loads(self.rfile.read(size))
            def do_GET(self):
                parsed = urlparse(self.path)
                path = parsed.path
                if path.endswith('/exec'):
                    return self.exec_stream(parsed)
                discovery = {
                    '/api': {'kind': 'APIVersions', 'apiVersion': 'v1', 'versions': ['v1']},
                    '/apis': {'kind': 'APIGroupList', 'apiVersion': 'v1', 'groups': []},
                    '/api/v1': {'kind': 'APIResourceList', 'apiVersion': 'v1', 'groupVersion': 'v1', 'resources': [{'name': 'pods', 'singularName': 'pod', 'namespaced': True, 'kind': 'Pod', 'verbs': ['get', 'create', 'delete', 'list', 'watch']}]},
                    '/version': {'major': '1', 'minor': '33', 'gitVersion': 'v1.33.9'},
                }
                if path in discovery: return self.respond(discovery[path])
                name = path.rsplit('/', 1)[-1]
                if name == 'pods': return self.respond({'apiVersion': 'v1', 'kind': 'PodList', 'metadata': {'resourceVersion': '1'}, 'items': list(state['pods'].values())})
                if name in state['pods']: return self.respond(state['pods'][name])
                return self.respond({'apiVersion': 'v1', 'kind': 'Status', 'status': 'Failure', 'reason': 'NotFound', 'code': 404}, 404)
            def do_POST(self):
                pod = self.read_json()
                assert self.path.split('?')[0] == '/api/v1/namespaces/helm-test/pods'
                name = pod['metadata']['name']
                if name in state['pods']:
                    return self.respond({'apiVersion': 'v1', 'kind': 'Status', 'status': 'Failure', 'reason': 'AlreadyExists', 'code': 409}, 409)
                state['created'] += 1
                pod['metadata'].update(uid='session-uid-'+str(state['created']), resourceVersion='1')
                pod['status'] = {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}], 'containerStatuses': [{'name': 'session', 'ready': True, 'state': {'running': {}}}]}
                state['pods'][name] = pod
                if state['lose_create']:
                    state['lose_create'] = False
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    return
                self.respond(pod, 201)
            def exec_stream(self, parsed):
                commands = parse_qs(parsed.query)['command']
                assert commands[0] == 'cat' and commands[1] in {'/tmp/session-result.json', '/installation/private/session-token'}
                state['execs'].append(commands[1])
                if commands[1].endswith('session-result.json'):
                    value = dict(envelope)
                    if state['expired']: value['expires_at_unix_seconds'] = int(time.time())-1
                    output = json.dumps(value).encode()
                else:
                    output = token
                    if state['replace_after_token']:
                        name = parsed.path.split('/')[-2]
                        state['pods'][name]['metadata']['uid'] = 'replacement-uid'
                key = self.headers['Sec-WebSocket-Key']
                accept = base64.b64encode(hashlib.sha1((key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
                self.send_response(101)
                self.send_header('Upgrade', 'websocket')
                self.send_header('Connection', 'Upgrade')
                self.send_header('Sec-WebSocket-Accept', accept)
                self.send_header('Sec-WebSocket-Protocol', 'v5.channel.k8s.io')
                self.end_headers()
                def frame(payload, opcode=2):
                    size = len(payload)
                    header = bytes([128|opcode, size]) if size < 126 else bytes([128|opcode, 126])+struct.pack('!H', size)
                    self.wfile.write(header+payload)
                    self.wfile.flush()
                frame(b'\x01'+output)
                if state['lose_token'] and commands[1].endswith('session-token'):
                    state['lose_token'] = False
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    return
                frame(b'\x03'+b'{"status":"Success"}')
                frame(struct.pack('!H', 1000), opcode=8)
                self.close_connection = True
            def do_DELETE(self):
                body = self.read_json()
                state['deletes'].append(body)
                name = urlparse(self.path).path.rsplit('/', 1)[-1]
                if state['replace_delete']:
                    state['pods'][name]['metadata']['uid'] = 'replacement-uid'
                if body.get('preconditions', {}).get('uid') != state['pods'][name]['metadata']['uid']:
                    return self.respond({'apiVersion': 'v1', 'kind': 'Status', 'status': 'Failure', 'reason': 'Conflict', 'code': 409}, 409)
                del state['pods'][name]
                self.respond({'apiVersion': 'v1', 'kind': 'Status', 'status': 'Success'})
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        args.kubeconfig.write_text(json.dumps({'apiVersion': 'v1', 'kind': 'Config', 'clusters': [{'name': 'fixture', 'cluster': {'server': f'http://127.0.0.1:{server.server_port}'}}], 'contexts': [{'name': 'fixture', 'context': {'cluster': 'fixture', 'user': 'fixture'}}], 'current-context': 'fixture', 'users': [{'name': 'fixture', 'user': {'token': 'nonsecret-api-fixture'}}]}))
        real_command = INSTALL.command
        def no_fixture_openapi(arguments, **kwargs):
            # The fixture implements actual create/get/exec/delete HTTP behavior; production
            # keeps kubectl's normal OpenAPI validation against its selected real cluster.
            return real_command(arguments+(['--validate=false'] if 'create' in arguments else []), **kwargs)
        patch = mock.patch.object(INSTALL, 'command', side_effect=no_fixture_openapi)
        patch.start()
        self.addCleanup(patch.stop)
        return installation, state, token

    def test_real_session_exec_delivery_is_private_and_delete_has_uid_precondition(self):
        installation, state, token = self.session_api()
        with mock.patch.object(installation, 'namespace_owner'), mock.patch('builtins.print') as printed:
            result = installation.deliver_session(explicit=True)
        file = installation.directory/'session-token'
        self.assertEqual(file.read_bytes(), token)
        self.assertEqual(file.stat().st_mode & 0o777, 0o600)
        self.assertEqual(result['session_file'], str(file))
        self.assertNotIn(token.decode().strip(), str(printed.call_args_list))
        self.assertEqual(state['deletes'][0]['preconditions'], {'uid': 'session-uid-1'})
        self.assertFalse(state['pods'])
        self.assertEqual(state['execs'], ['/tmp/session-result.json', '/installation/private/session-token'])
        pod = installation.session_pod('a'*32)
        self.assertIn({'name': 'input', 'mountPath': '/installation-input/input.json', 'subPath': 'input.json', 'readOnly': True}, pod['spec']['containers'][0]['volumeMounts'])

    def test_real_lost_create_and_exec_responses_resume_same_nonce_without_resigning(self):
        installation, state, token = self.session_api()
        state['lose_create'] = True
        with mock.patch.object(installation, 'namespace_owner'), mock.patch('builtins.print'):
            with self.assertRaises(INSTALL.InstallationFailure): installation.deliver_session(explicit=True)
            journal = json.loads((installation.directory/'session-delivery.json').read_text())
            self.assertFalse(journal['complete'])
            state['lose_token'] = True
            with self.assertRaises(INSTALL.InstallationFailure): installation.deliver_session(explicit=True)
            self.assertFalse((installation.directory/'session-token').exists())
            installation.deliver_session(explicit=True)
        self.assertEqual(state['created'], 1)
        final = json.loads((installation.directory/'session-delivery.json').read_text())
        self.assertEqual(final['nonce'], journal['nonce'])
        self.assertEqual((installation.directory/'session-token').read_bytes(), token)

    def test_completed_session_expiry_is_actionable_without_resigning_or_changing_ready_state(self):
        installation, state, token = self.session_api()
        with mock.patch.object(installation, 'namespace_owner'), mock.patch('builtins.print'):
            installation.deliver_session(explicit=True)
        journal_file = installation.directory/'session-delivery.json'
        original = journal_file.read_bytes()
        saved_state = json.dumps(installation.state, sort_keys=True)
        expiry = json.loads(original)['envelope']['expires_at_unix_seconds']
        with mock.patch.object(installation, 'namespace_owner'), \
             mock.patch.object(INSTALL.time, 'time', return_value=expiry), \
             mock.patch.object(INSTALL, 'command') as command:
            with self.assertRaises(INSTALL.SessionExpired) as failure:
                installation.deliver_session(explicit=False)
        command.assert_not_called()
        self.assertEqual(state['created'], 1)
        self.assertEqual(journal_file.read_bytes(), original)
        self.assertEqual((installation.directory/'session-token').read_bytes(), token)
        self.assertEqual(json.dumps(installation.state, sort_keys=True), saved_state)
        message = INSTALL.failure_message(failure.exception)
        self.assertIn('is ready', message)
        self.assertIn("operation 'session'", message)
        self.assertIn('No new session was issued', message)
        self.assertNotIn(token.decode().strip(), message)
        INSTALL.persist_bytes(installation.directory/'session-token', token+b'changed')
        with mock.patch.object(installation, 'namespace_owner'), \
             mock.patch.object(INSTALL.time, 'time', return_value=expiry), \
             mock.patch.object(INSTALL, 'command') as command:
            with self.assertRaises(INSTALL.InstallationFailure) as failure:
                installation.deliver_session(explicit=False)
        self.assertNotIsInstance(failure.exception, INSTALL.SessionExpired)
        self.assertNotIn('is ready', INSTALL.failure_message(failure.exception))
        self.assertNotIn('private-canary', INSTALL.failure_message(INSTALL.InstallationFailure('private-canary')))
        command.assert_not_called()
        INSTALL.persist_bytes(installation.directory/'session-token', token)
        for invalid_expiry in (True, None, '1', -1):
            changed = json.loads(original)
            changed['envelope']['expires_at_unix_seconds'] = invalid_expiry
            INSTALL.persist(journal_file, changed)
            with self.subTest(expiry=invalid_expiry), \
                 mock.patch.object(installation, 'namespace_owner'), \
                 mock.patch.object(INSTALL, 'command') as command:
                with self.assertRaises(INSTALL.InstallationFailure) as failure:
                    installation.deliver_session(explicit=False)
                self.assertNotIsInstance(failure.exception, INSTALL.SessionExpired)
                command.assert_not_called()

    def test_real_expiry_and_same_name_replacement_fail_without_publishing_or_deleting_replacement(self):
        installation, state, _ = self.session_api()
        state['expired'] = True
        with mock.patch.object(installation, 'namespace_owner'), mock.patch('builtins.print'):
            with self.assertRaises(INSTALL.InstallationFailure): installation.deliver_session(explicit=True)
            self.assertFalse((installation.directory/'session-token').exists())
            self.assertNotIn('/installation/private/session-token', state['execs'])
            state['expired'] = False
            state['replace_after_token'] = True
            with self.assertRaises(INSTALL.InstallationFailure): installation.deliver_session(explicit=True)
            self.assertFalse((installation.directory/'session-token').exists())
            self.assertFalse(state['deletes'])

    def test_real_delete_race_is_rejected_by_api_uid_precondition(self):
        installation, state, token = self.session_api()
        state['replace_delete'] = True
        with mock.patch.object(installation, 'namespace_owner'), mock.patch('builtins.print'):
            with self.assertRaises(INSTALL.InstallationFailure): installation.deliver_session(explicit=True)
        self.assertTrue(state['pods'])
        self.assertEqual(next(iter(state['pods'].values()))['metadata']['uid'], 'replacement-uid')
        self.assertEqual(state['deletes'][0]['preconditions']['uid'], 'session-uid-1')
        self.assertEqual((installation.directory/'session-token').read_bytes(), token)

    def test_missing_workload_pure_verify_and_old_ready_pending_verification_never_apply_serving(self):
        private = self.root/'private'
        INSTALL.private_directory(private)
        args = argparse.Namespace(directory=private, kubeconfig=self.root/'kubeconfig', context='fixture', node='fixture', storage_class='standard')
        installation = INSTALL.Installation(args, self.plan)
        _, _, prior = completed(self.plan, 'provision')
        installation.state.update(owner='a'*32, ready=prior, phase='serving', namespace_uid='namespace-uid')
        installation.save()
        config = {'metadata': {'labels': {INSTALL.OWNER: 'a'*32}}, 'immutable': True, 'data': {'input.json': json.dumps(self.plan['input'])}}
        with mock.patch.object(installation, 'namespace_owner'), mock.patch.object(installation, 'get', side_effect=lambda kind, name: config if kind == 'configmap' else None), mock.patch.object(INSTALL, 'command') as commands:
            with self.assertRaises(INSTALL.InstallationFailure): installation.run('verify')
            commands.assert_not_called()
        def pending(arguments, **_):
            if 'upgrade' in arguments or 'rollout' in arguments: return b''
            raise INSTALL.InstallationFailure('current verification has no successful result')
        with mock.patch.object(installation, 'namespace_owner'), mock.patch.object(installation, 'observe_serving_provider'), mock.patch.object(INSTALL, 'command', side_effect=pending) as commands:
            with self.assertRaises(INSTALL.InstallationFailure): installation.run('up')
            self.assertEqual(installation.state['ready'], prior)
            self.assertEqual(installation.state['phase'], 'verify')
            self.assertEqual(len([call.args[0] for call in commands.call_args_list if 'upgrade' in call.args[0]]), 2)
            self.assertEqual(json.loads((private/'helm-values.json').read_text())['phase'], 'verify')

    def test_completed_pure_verify_checks_readiness_without_starting_or_signing(self):
        private = self.root/'private'
        INSTALL.private_directory(private)
        args = argparse.Namespace(directory=private, kubeconfig=self.root/'kubeconfig', context='fixture', node='fixture', storage_class='standard')
        installation = INSTALL.Installation(args, self.plan)
        installation.state.update(ready={'identity_digest': 'sha256:'+'c'*64}, phase='serving')
        with mock.patch.object(installation, 'namespace_owner'), mock.patch.object(installation, 'verify_existing_workloads') as verify, mock.patch.object(installation, 'run_job') as job, mock.patch.object(installation, 'apply') as apply, mock.patch.object(INSTALL, 'command', return_value=b'') as calls, mock.patch('builtins.print'):
            installation.run('verify')
            verify.assert_called_once()
            job.assert_called_once_with('verify')
            apply.assert_not_called()
            self.assertTrue(all('rollout' in call.args[0] for call in calls.call_args_list))
            self.assertFalse((private/'session-delivery.json').exists())


if __name__ == '__main__':
    unittest.main()
