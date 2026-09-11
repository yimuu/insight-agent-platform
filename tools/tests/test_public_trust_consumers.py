"""Read-only export consumers exercise real host files and bounded mock child/API effects."""
import argparse
import copy
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

from tools.tests import test_public_trust as certificate_fixture
from tools.tests.test_installation_helm import INSTALL as HELM, completed, owner_plan

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT/'tools/install'))
import public_trust as TRUST
import platform_compose as COMPOSE
import platform_native as NATIVE


class ConsumerTrustTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        certificate_fixture.PublicTrustTests.setUpClass()
        cls.pem = certificate_fixture.PublicTrustTests.pem

    @classmethod
    def tearDownClass(cls):
        certificate_fixture.PublicTrustTests.tearDownClass()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name).resolve()
        args = argparse.Namespace(directory=self.directory, kubeconfig=self.directory/'kubeconfig',
                                  context='fixture', node='fixture-node', storage_class='standard')
        self.helm = HELM.Installation(args, copy.deepcopy(owner_plan()))
        self.helm.state.update(owner='a'*32, namespace_uid='namespace-uid', prepared=completed(self.helm.plan, 'prepare')[2],
                               ready=completed(self.helm.plan, 'provision')[2], phase='provision', operation='e'*16)
        self.helm.save()
        self.input_digest = self.helm.plan['input_digest']
        self.identity_digest = self.helm.state['ready']['identity_digest']
        self.proof = {'schema_version': 1, 'phase': 'ready', 'input_digest': self.input_digest, 'identity_digest': self.identity_digest}
        self.envelope = dict(schema_version=1, input_digest=self.input_digest, identity_digest=self.identity_digest,
                             certificate_pem=self.pem.decode(), certificate_sha256='sha256:'+TRUST.hashlib.sha256(self.pem).hexdigest())
        self.encoded = json.dumps(self.envelope).encode()
        self.pods, self.created, self.execs, self.deletes = {}, [], [], []
        self.lose_create, self.replace_after_exec, self.lose_delete, self.mutate = False, False, False, None

    def remember(self):
        TRUST.remember_ready(self.directory, json.dumps(self.proof).encode(), input_digest=self.input_digest)

    def api(self, arguments, **options):
        if 'create' in arguments:
            pod = json.loads(Path(arguments[arguments.index('--filename')+1]).read_bytes())
            pod['metadata']['uid'] = 'trust-pod-'+str(len(self.created)+1)
            pod['status'] = {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}],
                'containerStatuses': [{'name': 'public-trust', 'restartCount': 0, 'ready': True, 'state': {'running': {'startedAt': 'now'}}}]}
            if self.mutate: self.mutate(pod)
            self.created.append(copy.deepcopy(pod))
            self.pods[pod['metadata']['name']] = pod
            if self.lose_create:
                self.lose_create = False
                raise HELM.InstallationFailure('create response lost')
            return json.dumps(pod).encode()
        if 'exec' in arguments:
            self.assertEqual(arguments[-4:], ['public-trust', '--', 'cat', '/tmp/public-trust.json'])
            self.assertLessEqual(options['maximum'], TRUST.MAX_RESPONSE_BYTES)
            self.execs.append(arguments)
            if self.replace_after_exec:
                next(iter(self.pods.values()))['metadata']['uid'] = 'foreign-replacement'
            return self.encoded
        if 'delete' in arguments:
            name = arguments[arguments.index('--raw')+1].split('/')[-1]
            options = json.loads(Path(arguments[arguments.index('--filename')+1]).read_bytes())
            self.assertEqual(options['preconditions'], {'uid': self.pods[name]['metadata']['uid']})
            self.deletes.append(options)
            del self.pods[name]
            if self.lose_delete:
                self.lose_delete = False
                raise HELM.InstallationFailure('delete response lost')
            return b''
        self.fail('unexpected transport operation')

    def get(self, kind, name, **_):
        self.assertEqual(kind, 'pod')
        return copy.deepcopy(self.pods.get(name))

    def export(self):
        with mock.patch.object(self.helm, 'namespace_owner'), mock.patch.object(self.helm, 'get', side_effect=self.get), \
                mock.patch.object(HELM, 'command', side_effect=self.api), mock.patch('sys.stdout', new_callable=io.StringIO) as output:
            result = self.helm.deliver_public_trust()
            self.assertNotIn('BEGIN CERTIFICATE', output.getvalue())
            return result

    def test_helm_reads_current_owner_with_only_readonly_mounts_and_same_file_is_not_rewritten(self):
        before = copy.deepcopy(self.helm.state)
        result = self.export()
        path = self.directory/'public-ca.pem'
        metadata = path.stat()
        self.assertEqual(self.export(), result)
        self.assertEqual((path.stat().st_ino, path.stat().st_mtime_ns), (metadata.st_ino, metadata.st_mtime_ns))
        self.assertEqual(len(self.created), 2)
        self.assertEqual(len(self.execs), 2)  # Completed old delivery never replaces a current owner read.
        self.assertEqual(len(self.deletes), 2)
        self.assertFalse(self.pods)
        self.assertEqual(self.helm.state, before)
        pod = self.created[0]
        self.assertFalse(pod['spec']['automountServiceAccountToken'])
        self.assertEqual(pod['spec']['restartPolicy'], 'Never')
        self.assertTrue(pod['spec']['volumes'][0]['persistentVolumeClaim']['readOnly'])
        self.assertTrue(pod['spec']['containers'][0]['volumeMounts'][0]['readOnly'])
        self.assertNotIn('--output', pod['spec']['containers'][0]['args'][0])
        self.assertNotIn('--binaries', pod['spec']['containers'][0]['args'][0])
        self.assertFalse((self.directory/'session-delivery.json').exists())

    def test_create_response_loss_reuses_original_pod_and_missing_unknown_never_recreates(self):
        self.lose_create = True
        with self.assertRaises(HELM.InstallationFailure): self.export()
        pending = (self.directory/'public-trust-intent.json').read_bytes()
        self.export()
        self.assertEqual(len(self.created), 1)
        (self.directory/'public-trust-intent.json').write_bytes(pending)
        with self.assertRaisesRegex(HELM.InstallationFailure, 'outcome is unknown'): self.export()
        self.assertEqual(len(self.created), 1)
        self.assertEqual((self.directory/'public-trust-intent.json').read_bytes(), pending)

    def test_delete_response_loss_resolves_only_old_uid_then_requires_fresh_owner_read(self):
        self.lose_delete = True
        with self.assertRaises(HELM.InstallationFailure): self.export()
        self.assertTrue(json.loads((self.directory/'public-trust-intent.json').read_bytes())['complete'])
        self.export()
        self.assertEqual(len(self.created), 2)
        self.assertEqual(len(self.execs), 2)

    def test_foreign_uid_after_read_never_publishes_or_deletes_replacement(self):
        self.replace_after_exec = True
        with self.assertRaises(HELM.InstallationFailure): self.export()
        self.assertFalse((self.directory/'public-ca.pem').exists())
        self.assertFalse(self.deletes)
        self.assertTrue(self.pods)

    def test_writable_private_mount_wrong_image_identity_and_extra_authority_block_read(self):
        changes = [lambda p: p['spec']['volumes'][0]['persistentVolumeClaim'].update(readOnly=False),
                   lambda p: p['spec']['containers'][0]['volumeMounts'][0].update(readOnly=False),
                   lambda p: p['spec']['containers'][0].update(image='foreign:latest'),
                   lambda p: p['metadata']['annotations'].update({HELM.DIGEST: 'sha256:'+'f'*64}),
                   lambda p: p['spec'].update(automountServiceAccountToken=True),
                   lambda p: p['spec']['containers'][0].update(args=['arbitrary command']),
                   lambda p: p['spec']['containers'][0].update(env=[{'name': 'PRIVATE', 'value': 'do-not-output'}])]
        for change in changes:
            self.pods.clear()
            intent = self.directory/'public-trust-intent.json'
            if intent.exists(): intent.unlink()
            self.mutate = change
            with self.subTest(change=changes.index(change)), self.assertRaises(HELM.InstallationFailure): self.export()
            self.assertFalse(self.execs)
            self.assertFalse(self.deletes)
            self.assertFalse((self.directory/'public-ca.pem').exists())

    def test_compose_export_uses_existing_ready_proof_and_only_declared_readonly_service(self):
        self.remember()
        service = {'image': 'runtime@sha256:'+'a'*64, 'user': '0:0', 'entrypoint': ['/usr/local/bin/platform-installation'],
                   'command': ['public-trust', '--input', '/installation-input/input.json', '--state', '/installation/private'],
                   'network_mode': 'none', 'read_only': True, 'restart': 'no', 'cap_drop': ['ALL'], 'security_opt': ['no-new-privileges:true'],
                   'volumes': [{'type': 'volume', 'source': 'installation-private', 'target': '/installation', 'read_only': True, 'volume': {'nocopy': True}},
                               {'type': 'bind', 'source': str(self.directory/'input.json'), 'target': '/installation-input/input.json', 'read_only': True}]}
        document = {'services': {'openbao': {'labels': {'insight.installation.input': self.input_digest}},
                                'installation-prepare': {'image': service['image']}, 'installation-public-trust': service}}
        with mock.patch.object(COMPOSE, 'command', return_value=self.encoded) as command, mock.patch('builtins.print'):
            COMPOSE.public_trust(['docker', 'compose'], self.directory, document)
        self.assertEqual(command.call_args.args[0], ['docker', 'compose', 'run', '--rm', '--no-deps', 'installation-public-trust'])
        for changed in ('writable', 'network', 'image', 'credentials'):
            altered = copy.deepcopy(document)
            selected = altered['services']['installation-public-trust']
            if changed == 'writable': selected['volumes'][0]['read_only'] = False
            elif changed == 'network': selected['network_mode'] = 'host'
            elif changed == 'image': selected['image'] = 'foreign:latest'
            else: selected['environment'] = {'PRIVATE': 'do-not-read'}
            with self.subTest(changed=changed), mock.patch.object(COMPOSE, 'command') as command, self.assertRaises(COMPOSE.InstallationFailure):
                COMPOSE.public_trust(['docker', 'compose'], self.directory, altered)
            command.assert_not_called()
        (self.directory/'ready-owner-proof.json').unlink()
        with mock.patch.object(COMPOSE, 'command') as command, self.assertRaises(FileNotFoundError):
            COMPOSE.public_trust(['docker', 'compose'], self.directory, document)
        command.assert_not_called()

    def test_helm_pre_ready_refuses_before_any_api_or_file_effect(self):
        self.helm.state['ready'] = {}
        with mock.patch.object(self.helm, 'namespace_owner') as namespace, self.assertRaises(HELM.InstallationFailure):
            self.helm.deliver_public_trust()
        namespace.assert_not_called()
        self.assertFalse((self.directory/'public-trust-intent.json').exists())

    def test_native_export_has_no_provider_environment_or_init_arguments(self):
        self.remember()
        args = argparse.Namespace(directory=self.directory, binaries=self.directory/'bin')
        group = mock.Mock()
        group.run.return_value = self.encoded
        installation = NATIVE.NativeInstallation(args, group)
        installation.plan = {'input_digest': self.input_digest}
        with mock.patch.object(installation, 'artifact'), mock.patch('builtins.print'):
            installation.deliver_public_trust()
        args, environment = group.run.call_args.args
        self.assertEqual(args, [str(self.directory/'bin/platform-installation'), 'public-trust', '--input', str(self.directory/'input.json'), '--state', str(self.directory/'private')])
        self.assertFalse(any(key.startswith('AWS_') or key.startswith('OPENBAO_') for key in environment))
        self.assertFalse((self.directory/'session-token').exists())


if __name__ == '__main__':
    unittest.main()
