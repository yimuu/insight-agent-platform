"""Bounded, identity-preserving provider Job and bare Pod recovery; no live cluster."""
import argparse
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from tools.tests.test_installation_helm import INSTALL, completed, owner_plan


class ProviderLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        directory = Path(self.temporary.name).resolve()/'private'
        INSTALL.private_directory(directory)
        args = argparse.Namespace(directory=directory, kubeconfig=directory/'kubeconfig', context='fixture', node='fixture-node', storage_class='standard')
        self.install = INSTALL.Installation(args, copy.deepcopy(owner_plan()))
        self.install.state.update(owner='a'*32, namespace_uid='namespace-uid', prepared=completed(self.install.plan, 'prepare')[2])
        self.install.save()

    def started(self):
        _, _, proof = completed(self.install.plan, 'provider-start')
        self.install.state['provider'].update(started=proof, start_intent={'job': proof['job'], 'uid': proof['job_uid']})
        return proof

    def initialization(self, **values):
        self.started()
        intent = dict(nonce='d'*32, uid='initializer-uid', deleting=False, terminated=False, complete=False)
        intent.update(values)
        self.install.state['provider']['initialization'] = intent
        pod = self.install.provider_pod(intent['nonce'])
        pod['metadata']['uid'] = 'initializer-uid'
        pod['status'] = {'phase': 'Running', 'containerStatuses': [{'name': 'openbao', 'restartCount': 0, 'state': {'running': {'startedAt': '2026-09-10T00:00:00Z'}}}]}
        return intent, pod

    def test_private_provider_journal_rejects_unknown_fields_invalid_states_and_unbound_proof(self):
        original = copy.deepcopy(self.install.state)
        for change in ('extra', 'old', 'missing-serve-intent', 'invalid-serve-intent', 'nonce', 'bool', 'missing-uid', 'wrong-mode', 'unbound-proof'):
            state = copy.deepcopy(original)
            if change == 'extra': state['provider']['authority'] = True
            elif change == 'old': state.pop('provider')
            elif change == 'missing-serve-intent': state['provider'].pop('serve_observe_intent')
            elif change == 'invalid-serve-intent': state['provider']['serve_observe_intent'] = {'job': 'installation-provider-start-'+'b'*16, 'uid': None}
            elif change == 'unbound-proof': state['provider']['ready'] = completed(self.install.plan, 'provider-observe')[2]
            else:
                proof = completed(self.install.plan, 'provider-start')[2]
                state['provider'].update(started=proof, start_intent={'job': proof['job'], 'uid': proof['job_uid']}, initialization=dict(nonce='d'*32, uid='initializer-uid', deleting=False, terminated=False, complete=False))
                if change == 'nonce': state['provider']['initialization']['nonce'] = '../arbitrary'
                if change == 'bool': state['provider']['initialization']['deleting'] = 1
                if change == 'missing-uid': state['provider']['initialization'].update(uid=None, deleting=True)
                if change == 'wrong-mode': state['provider']['started']['mode'] = 'serve'
            INSTALL.persist(self.install.file, state)
            with self.subTest(change=change), self.assertRaises(INSTALL.InstallationFailure), mock.patch.object(INSTALL, 'command') as calls:
                INSTALL.Installation(self.install.arguments, self.install.plan)
            calls.assert_not_called()

    def test_lost_start_job_create_response_never_reissues_permission_or_creates_replacement(self):
        with mock.patch.object(self.install, 'get', return_value=None), mock.patch.object(self.install, 'apply', side_effect=INSTALL.InstallationFailure('lost response')) as apply:
            with self.assertRaises(INSTALL.InstallationFailure): self.install.provider_job('provider-start', INSTALL.time.monotonic()+30)
        pending = copy.deepcopy(self.install.state['provider']['start_intent'])
        self.assertIsNone(pending['uid'])
        with mock.patch.object(self.install, 'get', return_value=None), mock.patch.object(self.install, 'apply') as apply:
            with self.assertRaisesRegex(INSTALL.InstallationFailure, 'disappeared'): self.install.provider_job('provider-start', INSTALL.time.monotonic()+30)
            apply.assert_not_called()
        self.assertEqual(self.install.state['provider']['start_intent'], pending)
        job, pod, _ = completed(self.install.plan, 'provider-start')
        job['metadata']['name'] = pending['job']
        pod['metadata']['name'] = pending['job']+'-pod'
        with mock.patch.object(self.install, 'get', return_value=job), mock.patch.object(self.install, 'wait_job', return_value=job), mock.patch.object(self.install, 'apply') as apply, mock.patch.object(INSTALL, 'command', return_value=json.dumps({'items': [pod]}).encode()):
            result = self.install.provider_job('provider-start', INSTALL.time.monotonic()+30)
        self.assertEqual(result['mode'], 'initialize_once')
        self.assertEqual(result['job'], pending['job'])
        apply.assert_not_called()

    def test_only_closed_failed_readonly_observation_can_be_repeated(self):
        self.started()
        job, pod, _ = completed(self.install.plan, 'provider-observe')
        job['status'] = {'failed': 1, 'conditions': [{'type': 'Failed', 'status': 'True'}]}
        pod['status']['phase'] = 'Failed'
        pod['status']['containerStatuses'][0]['state']['terminated'] = {'exitCode': 1, 'message': 'installation PrerequisiteUnavailable\n'}
        self.install.state['provider']['observe_intent'] = {'job': job['metadata']['name'], 'uid': job['metadata']['uid']}
        for change in ('valid', 'incomplete', 'command', 'image', 'namespace', 'owner', 'status', 'restart', 'private-message', 'false-exit'):
            changed = copy.deepcopy(pod)
            if change == 'command': changed['spec']['containers'][0]['args'] = ['unexpected command']
            if change == 'image': changed['spec']['containers'][0]['image'] = 'foreign:latest'
            if change == 'namespace': changed['metadata']['namespace'] = 'other'
            if change == 'owner': changed['metadata']['ownerReferences'][0]['uid'] = 'foreign-uid'
            if change == 'status': changed['status']['phase'] = 'Succeeded'
            if change == 'restart': changed['status']['containerStatuses'][0]['restartCount'] = 1
            if change == 'private-message': changed['status']['containerStatuses'][0]['state']['terminated']['message'] = 'installation PrerequisiteUnavailable\nprivate canary'
            if change == 'false-exit': changed['status']['containerStatuses'][0]['state']['terminated']['exitCode'] = False
            if change == 'incomplete': changed['status']['containerStatuses'][0]['state']['terminated']['message'] = 'installation Incomplete\n'
            with self.subTest(change=change), mock.patch.object(self.install, 'get', return_value=job), mock.patch.object(self.install, 'wait_job', return_value=job), mock.patch.object(INSTALL, 'command', return_value=json.dumps({'items': [changed]}).encode()), mock.patch.object(self.install, 'apply') as apply:
                if change in ('valid', 'incomplete'): self.assertIsNone(self.install.provider_job('provider-observe', INSTALL.time.monotonic()+30))
                else:
                    with self.assertRaises(INSTALL.InstallationFailure): self.install.provider_job('provider-observe', INSTALL.time.monotonic()+30)
                apply.assert_not_called()

    def test_initialize_create_loss_reuses_only_original_pod_and_records_ready_before_stop(self):
        self.started()
        with mock.patch.object(self.install, 'apply'), mock.patch.object(self.install, 'get', return_value=None), mock.patch.object(INSTALL, 'command', side_effect=INSTALL.InstallationFailure('lost create')) as commands:
            with self.assertRaises(INSTALL.InstallationFailure): self.install.ensure_provider()
            self.assertEqual(commands.call_count, 1)
            self.assertIn('create', commands.call_args.args[0])
        original = copy.deepcopy(self.install.state['provider']['initialization'])
        with mock.patch.object(self.install, 'apply'), mock.patch.object(self.install, 'get', return_value=None), mock.patch.object(INSTALL, 'command') as commands:
            with self.assertRaisesRegex(INSTALL.InstallationFailure, 'no replacement'): self.install.ensure_provider()
            commands.assert_not_called()
        self.assertEqual(self.install.state['provider']['initialization'], original)
        pod = self.install.provider_pod(original['nonce'])
        pod['metadata']['uid'] = 'original-created-uid'
        pod['status'] = {'containerStatuses': [{'name': 'openbao', 'restartCount': 0, 'state': {'running': {}}}]}
        # A real Running timestamp is nonempty; empty state objects never assert process liveness.
        pod['status']['containerStatuses'][0]['state']['running']['startedAt'] = '2026-09-10T00:00:00Z'
        observed = completed(self.install.plan, 'provider-observe')[2]
        def stopped():
            saved = INSTALL.decode(INSTALL.read_file(self.install.file, private=True))
            self.assertEqual(saved['provider']['ready'], observed)
            self.assertEqual(saved['provider']['initialization']['uid'], 'original-created-uid')
        with mock.patch.object(self.install, 'apply'), mock.patch.object(self.install, 'get', return_value=pod), mock.patch.object(self.install, 'provider_job', return_value=observed), mock.patch.object(self.install, 'provider_stop', side_effect=stopped) as stop, mock.patch.object(INSTALL, 'command') as commands:
            self.install.ensure_provider()
            stop.assert_called_once()
            commands.assert_not_called()

    def test_exact_uid_stop_and_finalizer_test_precede_serving_and_survive_patch_response_loss(self):
        intent, running = self.initialization()
        terminated = copy.deepcopy(running)
        terminated['metadata']['deletionTimestamp'] = '2026-09-10T00:00:01Z'
        terminated['status']['containerStatuses'][0]['state'] = {'terminated': {'exitCode': 0}}
        calls = []
        def mutate(arguments, **_):
            calls.append(arguments)
            if 'delete' in arguments:
                body = INSTALL.decode(INSTALL.read_file(self.install.directory/'provider-delete.json', private=True))
                self.assertEqual(body['preconditions'], {'uid': intent['uid']})
                self.assertEqual(body['gracePeriodSeconds'], 30)
            elif 'patch' in arguments:
                body = INSTALL.decode(INSTALL.read_file(self.install.directory/'provider-finalizer.json', private=True))
                self.assertEqual(body[:2], [{'op': 'test', 'path': '/metadata/uid', 'value': intent['uid']}, {'op': 'test', 'path': '/metadata/finalizers', 'value': ['insight.platform/provider-initialization-evidence']}])
                self.assertTrue(self.install.state['provider']['initialization']['terminated'])
                raise INSTALL.InstallationFailure('lost finalizer response')
            return b''
        with mock.patch.object(self.install, 'get', side_effect=[running, terminated]), mock.patch.object(INSTALL, 'command', side_effect=mutate), mock.patch.object(INSTALL.time, 'sleep'):
            with self.assertRaises(INSTALL.InstallationFailure): self.install.provider_stop()
        self.assertTrue(intent['terminated'])
        self.assertFalse(intent['complete'])
        finalizing = copy.deepcopy(terminated)
        finalizing['metadata']['finalizers'] = []
        with mock.patch.object(self.install, 'get', side_effect=[finalizing, None]), mock.patch.object(INSTALL, 'command') as commands:
            self.install.provider_stop()
            commands.assert_not_called()
        self.assertTrue(intent['complete'])
        self.assertEqual(len(calls), 2)

    def test_missing_changed_or_unclean_initializer_never_grants_clean_stop(self):
        intent, pod = self.initialization()
        for change in ('missing', 'uid', 'finalizer', 'restart', 'nonzero', 'bool-exit', 'null-exit', 'bool-restart', 'null-restart', 'status-name', 'initContainers', 'ephemeralContainers', 'hostPID', 'hostIPC', 'hostNetwork', 'resources'):
            changed = copy.deepcopy(pod)
            if change == 'uid': changed['metadata']['uid'] = 'replacement'
            if change == 'finalizer': changed['metadata']['finalizers'].append('foreign/finalizer')
            if change == 'restart': changed['status']['containerStatuses'][0]['restartCount'] = 1
            if change in ('bool-restart', 'null-restart'): changed['status']['containerStatuses'][0]['restartCount'] = False if change == 'bool-restart' else None
            if change == 'status-name': changed['status']['containerStatuses'][0]['name'] = 'another-process'
            if change in ('initContainers', 'ephemeralContainers'): changed['spec'][change] = [{'name': 'extra-process'}]
            if change in ('hostPID', 'hostIPC', 'hostNetwork'): changed['spec'][change] = True
            if change == 'resources': changed['spec']['containers'][0]['resources']['limits']['memory'] = '2Gi'
            if change in ('nonzero', 'bool-exit', 'null-exit'):
                changed['metadata']['deletionTimestamp'] = '2026-09-10T00:00:01Z'
                changed['status']['containerStatuses'][0]['state'] = {'terminated': {'exitCode': {'nonzero':1,'bool-exit':False,'null-exit':None}[change]}}
            with self.subTest(change=change), mock.patch.object(self.install, 'get', return_value=None if change == 'missing' else changed), mock.patch.object(INSTALL, 'command') as commands:
                with self.assertRaises(INSTALL.InstallationFailure): self.install.provider_stop()
                commands.assert_not_called()
                self.assertFalse(intent['complete'])

    def test_ready_proof_never_authorizes_a_null_uid_delete_precondition(self):
        intent, _ = self.initialization(uid=None)
        proof = completed(self.install.plan, 'provider-observe')[2]
        self.install.state['provider'].update(ready=proof, observe_intent={'job': proof['job'], 'uid': proof['job_uid']})
        with self.assertRaises(INSTALL.InstallationFailure):
            INSTALL.provider_state(self.install.state['provider'], self.install.state['prepared'])

    def ready_provider(self):
        self.initialization(deleting=True, terminated=True, complete=True)
        proof = completed(self.install.plan, 'provider-observe')[2]
        self.install.state['provider'].update(ready=proof, observe_intent={'job': proof['job'], 'uid': proof['job_uid']})
        self.install.save()

    def serve_resources(self):
        self.ready_provider()
        def meta(name, uid):
            return {'name': name, 'uid': uid, 'namespace': self.install.namespace,
                    'labels': {INSTALL.OWNER: self.install.state['owner'], 'insight.platform/process': 'openbao'}}
        spec = copy.deepcopy(self.install.provider_pod('d'*32)['spec'])
        spec['containers'][0]['args'] = ['server', '-config=/run/insight-openbao/serve.json']
        spec['volumes'][2]['emptyDir']['sizeLimit'] = '128Mi'
        labels = {INSTALL.OWNER: self.install.state['owner'], 'insight.platform/process': 'openbao'}
        deployment = {'metadata': meta('openbao', 'deployment-uid'), 'spec': {'replicas': 1, 'selector': {'matchLabels': labels}, 'template': {'spec': copy.deepcopy(spec)}}}
        replica = {'metadata': meta('openbao-rs', 'replica-uid'), 'spec': {'template': {'spec': copy.deepcopy(spec)}}}
        replica['metadata']['ownerReferences'] = [{'kind': 'Deployment', 'name': 'openbao', 'uid': 'deployment-uid', 'controller': True}]
        pod = {'metadata': meta('openbao-rs-pod', 'normal-pod-uid'), 'spec': copy.deepcopy(spec),
               'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}], 'containerStatuses': [
                   {'name': 'openbao', 'ready': True, 'containerID': 'containerd://'+'e'*64, 'state': {'running': {'startedAt': '2026-09-10T00:00:00Z'}}}]}}
        pod['metadata']['ownerReferences'] = [{'kind': 'ReplicaSet', 'name': 'openbao-rs', 'uid': 'replica-uid', 'controller': True}]
        resources = {('configmap', 'installation-input'): {'metadata': meta('installation-input', 'input-uid'), 'immutable': True, 'data': {'input.json': json.dumps(self.install.plan['input'])}},
                     ('deployment', 'openbao'): deployment, ('replicaset', 'openbao-rs'): replica,
                     ('service', 'openbao'): {'metadata': meta('openbao', 'service-uid'), 'spec': {'selector': labels, 'ports': [{'port': 8200, 'targetPort': 8200}]}}}
        for name in ('dependency-openbao', 'openbao-data'):
            claim = {'metadata': meta(name, name+'-uid')}
            claim['metadata']['annotations'] = {INSTALL.DIGEST: self.install.plan['input_digest'], 'insight.platform/namespace-uid': self.install.state['namespace_uid']}
            self.install.state['pvcs'][name] = name+'-uid'
            resources[('pvc', name)] = claim
        return resources, pod

    def test_normal_guard_binds_input_volume_deployment_chain_and_actual_process(self):
        resources, pod = self.serve_resources()
        with mock.patch.object(self.install, 'get', side_effect=lambda kind, name, **_: resources[(kind, name)]), mock.patch.object(INSTALL, 'command', return_value=json.dumps({'items': [pod]}).encode()) as commands:
            self.assertEqual(self.install.serving_provider_identity(INSTALL.time.monotonic()+30),
                             ('deployment-uid', 'replica-uid', 'normal-pod-uid', 'containerd://'+'e'*64))
            self.assertTrue(all('get' in call.args[0] for call in commands.call_args_list))

    def test_normal_guard_rejects_foreign_input_owner_or_any_replaced_process_boundary(self):
        resources, pod = self.serve_resources()
        for change in ('input', 'mutable-input', 'volume-digest', 'volume-uid', 'namespace', 'owner', 'service-selector', 'deployment', 'replica-uid', 'bare-init', 'deleting', 'multiple', 'unready', 'container-id', 'image', 'command', 'private-root', 'privilege', 'api-token'):
            docs, candidate = copy.deepcopy(resources), copy.deepcopy(pod)
            items = [candidate]
            if change == 'input': docs[('configmap', 'installation-input')]['data']['input.json'] = '{}'
            elif change == 'mutable-input': docs[('configmap', 'installation-input')]['immutable'] = False
            elif change == 'volume-digest': docs[('pvc', 'openbao-data')]['metadata']['annotations'][INSTALL.DIGEST] = 'sha256:'+'f'*64
            elif change == 'volume-uid': docs[('pvc', 'openbao-data')]['metadata']['uid'] = 'replacement'
            elif change == 'namespace': candidate['metadata']['namespace'] = 'foreign'
            elif change == 'owner': candidate['metadata']['labels'][INSTALL.OWNER] = 'foreign'
            elif change == 'service-selector': docs[('service', 'openbao')]['spec']['selector'] = {}
            elif change == 'deployment': docs[('replicaset', 'openbao-rs')]['metadata']['ownerReferences'][0]['uid'] = 'foreign'
            elif change == 'replica-uid': candidate['metadata']['ownerReferences'][0]['uid'] = 'foreign'
            elif change == 'bare-init': candidate['metadata']['ownerReferences'] = []
            elif change == 'deleting': candidate['metadata']['deletionTimestamp'] = 'now'
            elif change == 'multiple': items.append(copy.deepcopy(candidate))
            elif change == 'unready': candidate['status']['containerStatuses'][0]['ready'] = False
            elif change == 'container-id': candidate['status']['containerStatuses'][0]['containerID'] = 'unbounded/invalid'
            elif change == 'image': candidate['spec']['containers'][0]['image'] = 'foreign'
            elif change == 'command': candidate['spec']['containers'][0]['args'] = ['server', '-config=/run/insight-openbao/initialize.json']
            elif change == 'private-root': candidate['spec']['volumes'][0]['persistentVolumeClaim']['claimName'] = 'installation-private'
            elif change == 'privilege': candidate['spec']['containers'][0]['securityContext']['privileged'] = True
            elif change == 'api-token': candidate['spec']['automountServiceAccountToken'] = True
            with self.subTest(change=change), mock.patch.object(self.install, 'get', side_effect=lambda kind, name, **_: docs[(kind, name)]), mock.patch.object(INSTALL, 'command', return_value=json.dumps({'items': items}).encode()):
                with self.assertRaises(INSTALL.InstallationFailure):
                    self.install.serving_provider_identity(INSTALL.time.monotonic()+30)

    def test_entry_completed_or_pending_intent_must_resolve_then_get_fresh_current_proof(self):
        for old_result in ('completed', 'pending-completes', 'pending-unknown'):
            with self.subTest(old_result=old_result):
                self.ready_provider()
                old = {'job': 'installation-provider-observe-'+'f'*16, 'uid': 'old-job'}
                self.install.state['provider']['serve_observe_intent'] = copy.deepcopy(old)
                init = copy.deepcopy(self.install.state['provider'])
                calls = []
                def observe(phase, deadline, *, intent_key):
                    self.assertEqual(intent_key, 'serve_observe_intent')
                    calls.append(copy.deepcopy(self.install.state['provider'][intent_key]))
                    if len(calls) == 1:
                        self.assertEqual(calls[-1], old)
                        if old_result == 'pending-unknown': raise INSTALL.InstallationFailure('unknown')
                    else:
                        self.assertIsNone(calls[-1])
                    return {'identity_digest': self.install.state['prepared']['identity_digest']}
                with mock.patch.object(self.install, 'provider_job', side_effect=observe), mock.patch.object(self.install, 'serving_provider_identity', return_value=('same',)) as identity:
                    if old_result == 'pending-unknown':
                        with self.assertRaises(INSTALL.InstallationFailure): self.install.observe_serving_provider()
                        self.assertEqual(len(calls), 1)
                        identity.assert_not_called()
                        self.assertEqual(self.install.state['provider']['serve_observe_intent'], old)
                    else:
                        self.install.observe_serving_provider()
                        self.assertEqual(len(calls), 2)
                        self.assertIsNone(self.install.state['provider']['serve_observe_intent'])
                self.assertEqual({k:v for k,v in self.install.state['provider'].items() if k != 'serve_observe_intent'},
                                 {k:v for k,v in init.items() if k != 'serve_observe_intent'})

    def test_fresh_serve_job_creation_loss_does_not_change_pending_provision_or_init_proof(self):
        self.ready_provider()
        self.install.state.update(phase='provision', operation='e'*16)
        before = copy.deepcopy(self.install.state)
        with mock.patch.object(self.install, 'get', return_value=None), mock.patch.object(INSTALL, 'command', side_effect=INSTALL.InstallationFailure('create response lost')):
            with self.assertRaises(INSTALL.InstallationFailure):
                self.install.provider_job('provider-observe', INSTALL.time.monotonic()+30, intent_key='serve_observe_intent')
        saved = INSTALL.decode(INSTALL.read_file(self.install.file, private=True))
        intent = saved['provider']['serve_observe_intent']
        self.assertIsNone(intent['uid'])
        self.assertEqual((saved['phase'], saved['operation']), ('provision', 'e'*16))
        for key in ('started', 'ready', 'observe_intent'): self.assertEqual(saved['provider'][key], before['provider'][key])
        values = INSTALL.decode(INSTALL.read_file(self.install.directory/'helm-values.json', private=True))
        self.assertEqual(values['phase'], 'provider-observe')
        self.assertEqual(values['operation'], intent['job'].removeprefix('installation-provider-observe-'))
        self.assertNotEqual(values['operation'], saved['operation'])
        with mock.patch.object(self.install, 'get', return_value=None), mock.patch.object(INSTALL, 'command') as commands:
            with self.assertRaisesRegex(INSTALL.InstallationFailure, 'disappeared'):
                self.install.provider_job('provider-observe', INSTALL.time.monotonic()+30, intent_key='serve_observe_intent')
            commands.assert_not_called()
        self.assertEqual(self.install.state['provider']['serve_observe_intent'], intent)

    def test_dependency_apply_preserves_pending_nonce_on_success_and_response_loss(self):
        for phase in ('provision', 'verify'):
            for failed in (False, True):
                self.install.state.update(phase=phase, operation='e'*16)
                with self.subTest(phase=phase, failed=failed), mock.patch.object(INSTALL, 'command', side_effect=INSTALL.InstallationFailure('lost') if failed else None):
                    if failed:
                        with self.assertRaises(INSTALL.InstallationFailure): self.install.apply('dependencies')
                    else: self.install.apply('dependencies')
                saved = INSTALL.decode(INSTALL.read_file(self.install.file, private=True))
                self.assertEqual((saved['phase'], saved['operation']), (phase, 'e'*16))
                self.assertEqual(INSTALL.decode(INSTALL.read_file(self.install.directory/'helm-values.json', private=True))['phase'], 'dependencies')

    def test_replaced_provider_identity_fails_gate_and_retains_current_observation_intent(self):
        self.ready_provider()
        for pair in (('pod-one', 'pod-two'), ('container-one', 'container-two')):
            self.install.state['provider']['serve_observe_intent'] = None
            intent = {'job': 'installation-provider-observe-'+'f'*16, 'uid': 'current-job'}
            def observe(*_, **__):
                self.install.state['provider']['serve_observe_intent'] = copy.deepcopy(intent)
                self.install.save()
                return {'identity_digest': self.install.state['prepared']['identity_digest']}
            with self.subTest(pair=pair), mock.patch.object(self.install, 'serving_provider_identity', side_effect=pair), mock.patch.object(self.install, 'provider_job', side_effect=observe):
                with self.assertRaisesRegex(INSTALL.InstallationFailure, 'changed'): self.install.observe_serving_provider()
            self.assertEqual(self.install.state['provider']['serve_observe_intent'], intent)

    def test_transient_observations_share_one_deadline_and_late_proof_is_rejected(self):
        self.ready_provider()
        deadlines = []
        def observe(_phase, deadline, **_):
            deadlines.append(deadline)
            return None if len(deadlines) < 3 else {'identity_digest': self.install.state['prepared']['identity_digest']}
        with mock.patch.object(self.install, 'serving_provider_identity', return_value=('same',)), mock.patch.object(self.install, 'provider_job', side_effect=observe), mock.patch.object(INSTALL.time, 'sleep'):
            self.install.observe_serving_provider()
        self.assertEqual(len(deadlines), 3)
        self.assertEqual(len(set(deadlines)), 1)
        clock = [0]
        def late(*_, **__):
            clock[0] = 181
            return {'identity_digest': self.install.state['prepared']['identity_digest']}
        with mock.patch.object(INSTALL.time, 'monotonic', side_effect=lambda: clock[0]), mock.patch.object(self.install, 'serving_provider_identity', return_value=('same',)), mock.patch.object(self.install, 'provider_job', side_effect=late):
            with self.assertRaisesRegex(INSTALL.InstallationFailure, 'timed out'): self.install.observe_serving_provider()

    def test_real_job_consumer_waits_existing_pending_then_creates_fresh_readonly_job(self):
        self.ready_provider()
        self.install.state.update(phase='provision', operation='e'*16)
        original = copy.deepcopy(self.install.state['provider'])
        old_name = 'installation-provider-observe-'+'f'*16
        def result(name):
            job, pod, _ = completed(self.install.plan, 'provider-observe')
            job['metadata'].update(name=name, uid='job-'+name[-16:])
            pod['metadata'].update(name=name+'-pod', uid='pod-'+name[-16:])
            pod['metadata']['ownerReferences'][0]['uid'] = job['metadata']['uid']
            return job, pod
        jobs = {old_name: result(old_name)}
        self.install.state['provider']['serve_observe_intent'] = {'job': old_name, 'uid': jobs[old_name][0]['metadata']['uid']}
        created, waited = [], []
        def api(arguments, **_):
            if 'upgrade' in arguments:
                values = INSTALL.decode(INSTALL.read_file(self.install.directory/'helm-values.json', private=True))
                self.assertEqual(values['phase'], 'provider-observe')
                name = 'installation-provider-observe-'+values['operation']
                self.assertNotEqual(name, old_name)
                self.assertNotIn(name, jobs)
                self.assertEqual((self.install.state['phase'], self.install.state['operation']), ('provision', 'e'*16))
                jobs[name] = result(name)
                created.append(name)
                return b''
            self.assertIn('get', arguments)
            kind = arguments[arguments.index('get')+1]
            if kind == 'job':
                name = arguments[arguments.index('get')+2]
                if name not in jobs: return b''
                job = copy.deepcopy(jobs[name][0])
                if '--ignore-not-found' not in arguments:
                    waited.append(name)
                    if name == old_name and waited.count(old_name) == 1:
                        job['status'] = {'active': 1, 'conditions': []}
                return json.dumps(job).encode()
            self.assertEqual(kind, 'pods')
            name = arguments[arguments.index('--selector')+1].removeprefix('job-name=')
            return json.dumps({'items': [jobs[name][1]]}).encode()
        with mock.patch.object(INSTALL, 'command', side_effect=api), mock.patch.object(INSTALL.time, 'sleep'), mock.patch.object(self.install, 'serving_provider_identity', return_value=('same-normal-process',)):
            self.install.observe_serving_provider()
        self.assertEqual(len(created), 1)
        self.assertEqual(waited[:2], [old_name, old_name])
        self.assertEqual(waited[-1], created[0])
        self.assertIsNone(self.install.state['provider']['serve_observe_intent'])
        for key in ('started', 'ready', 'observe_intent', 'initialization'):
            self.assertEqual(self.install.state['provider'][key], original[key])
        self.assertEqual((self.install.state['phase'], self.install.state['operation']), ('provision', 'e'*16))

    def test_existing_serve_job_uid_change_never_creates_replacement(self):
        self.ready_provider()
        job, _, _ = completed(self.install.plan, 'provider-observe')
        intent = {'job': job['metadata']['name'], 'uid': 'frozen-job-uid'}
        self.install.state['provider']['serve_observe_intent'] = copy.deepcopy(intent)
        with mock.patch.object(self.install, 'get', return_value=job), mock.patch.object(INSTALL, 'command') as commands:
            with self.assertRaisesRegex(INSTALL.InstallationFailure, 'identity differs'):
                self.install.observe_serving_provider()
            commands.assert_not_called()
        self.assertEqual(self.install.state['provider']['serve_observe_intent'], intent)

    def test_run_gate_failure_preserves_pending_provision_and_never_runs_provision(self):
        self.ready_provider()
        self.install.state.update(phase='provision', operation='e'*16)
        def gate():
            self.assertEqual((self.install.state['phase'], self.install.state['operation']), ('provision', 'e'*16))
            raise INSTALL.InstallationFailure('current observation unavailable')
        with mock.patch.object(self.install, 'namespace_owner'), mock.patch.object(self.install, 'ensure_provider'), \
                mock.patch.object(self.install, 'observe_serving_provider', side_effect=gate), mock.patch.object(self.install, 'run_job') as provision, mock.patch.object(INSTALL, 'command'):
            with self.assertRaises(INSTALL.InstallationFailure): self.install.run('up')
            provision.assert_not_called()
        saved = INSTALL.decode(INSTALL.read_file(self.install.file, private=True))
        self.assertEqual((saved['phase'], saved['operation']), ('provision', 'e'*16))

    def test_pure_verify_never_adds_provider_gate_or_reapplies_dependencies(self):
        self.ready_provider()
        self.install.state.update(phase='serving', ready=completed(self.install.plan, 'provision')[2])
        with mock.patch.object(self.install, 'namespace_owner'), mock.patch.object(self.install, 'verify_existing_workloads'), \
                mock.patch.object(self.install, 'observe_serving_provider') as gate, mock.patch.object(self.install, 'apply') as apply, \
                mock.patch.object(self.install, 'run_job') as verify, mock.patch.object(INSTALL, 'command'), mock.patch('builtins.print'):
            self.install.run('verify')
            gate.assert_not_called()
            apply.assert_not_called()
            verify.assert_called_once_with('verify')


if __name__ == '__main__':
    unittest.main()
