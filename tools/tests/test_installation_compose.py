"""Host installation tests use the real private filesystem and a recorded owner boundary."""
import argparse
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest import mock

ROOT = next(path for path in Path(__file__).resolve().parents if (path / 'Cargo.toml').is_file())
sys.path.insert(0, str(ROOT/'tools/install'))
SPEC = importlib.util.spec_from_file_location('installation_compose', ROOT / 'tools/install/platform_compose.py')
INSTALL = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(INSTALL)
from provider_lifecycle import ContainerState
QUALIFICATION_SPEC = importlib.util.spec_from_file_location('installation_compose_qualification',
    ROOT / 'tools/qualification/qualify-platform-installation-compose.py')
QUALIFY = importlib.util.module_from_spec(QUALIFICATION_SPEC)
QUALIFICATION_SPEC.loader.exec_module(QUALIFY)


class ComposeInstallationTests(unittest.TestCase):
    def test_qualification_destination_selection_uses_readonly_owning_producer(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary).resolve()/'destinations.json'
            path.write_text('[]')
            image = 'example/runtime@sha256:'+'a'*64
            command = QUALIFY.declaration_command(image, 'fixture', path)
            self.assertEqual(command[-2:], ['--remote-context-destinations', '/remote-context-destinations.json'])
            self.assertIn('type=bind,source='+str(path)+',target=/remote-context-destinations.json,readonly', command)
            self.assertEqual(command[command.index('--user')+1], f'{os.geteuid()}:{os.getegid()}')
            self.assertEqual(command[command.index('--network')+1], 'none')
            link = path.parent/'link'
            link.symlink_to(path)
            with self.assertRaises(QUALIFY.OWNER.InstallationFailure):
                QUALIFY.declaration_command(image, 'fixture', link)
            link.unlink()
            os.link(path, link)
            with self.assertRaises(QUALIFY.OWNER.InstallationFailure):
                QUALIFY.declaration_command(image, 'fixture', path)
            default = QUALIFY.declaration_command(image, 'fixture')
            self.assertNotIn('--mount', default)
            self.assertNotIn('--remote-context-destinations', default)

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.directory = self.root / 'private'
        self.input = self.root / 'input.json'
        self.input.write_bytes(b'{"credential_free":"input"}\n')
        self.arguments = argparse.Namespace(operation='render', input=self.input, directory=self.directory,
            runtime_image='sha256:' + 'a' * 64, console_image='sha256:' + 'b' * 64)
        self.document = {'name': 'test-installation', 'services': {
            'installation-ready': {}, 'postgres': {}, 'nats': {}, 's3': {}, 'openbao': {}, 'openbao-initialize': {},
            'gateway-management': {}, 'gateway-runtime': {}, 'console': {}}}
        for service in self.document['services'].values():
            service.update(image='sha256:'+'a'*64, labels={'insight.installation.input':'sha256:'+'c'*64})
        self.encoded = json.dumps(self.document).encode()

    def test_pure_render_and_replay_publish_exact_public_input_without_private_mounts(self):
        with mock.patch.object(INSTALL, 'command', return_value=self.encoded) as commands:
            with INSTALL.installation_lock(self.directory):
                first = INSTALL.rendered(self.arguments)
            with INSTALL.installation_lock(self.directory):
                self.assertEqual(INSTALL.rendered(self.arguments), first)
        for call in commands.call_args_list:
            arguments = call.args[0]
            self.assertEqual(arguments.count('--mount'), 1)
            self.assertIn('--network', arguments)
            self.assertIn('none', arguments)
            self.assertNotIn('/var/run/docker.sock', ' '.join(arguments))
            self.assertNotIn('/installation/private', ' '.join(arguments))
        published = self.directory / 'input.json'
        self.assertEqual(published.read_bytes(), self.input.read_bytes())
        self.assertEqual(stat.S_IMODE(published.stat().st_mode), 0o444)
        self.assertEqual(published.stat().st_nlink, 1)
        self.assertEqual(stat.S_IMODE((self.directory / 'compose.json').stat().st_mode), 0o600)

    def test_failed_owner_validation_publishes_nothing(self):
        with INSTALL.installation_lock(self.directory):
            with mock.patch.object(INSTALL, 'command', side_effect=INSTALL.InstallationFailure('invalid')):
                with self.assertRaises(INSTALL.InstallationFailure):
                    INSTALL.rendered(self.arguments)
        self.assertFalse((self.directory / 'input.json').exists())
        self.assertFalse((self.directory / 'compose.json').exists())

    def test_input_or_generated_composition_drift_never_replaces_completed_files(self):
        with INSTALL.installation_lock(self.directory):
            with mock.patch.object(INSTALL, 'command', return_value=self.encoded):
                INSTALL.rendered(self.arguments)
                original = self.input.read_bytes()
                self.input.write_bytes(b'{"changed":true}')
                with self.assertRaises(INSTALL.InstallationFailure):
                    INSTALL.rendered(self.arguments)
                self.input.write_bytes(original)
            with mock.patch.object(INSTALL, 'command', return_value=b'{"name":"different"}'):
                with self.assertRaises(INSTALL.InstallationFailure):
                    INSTALL.rendered(self.arguments)
        self.assertEqual((self.directory / 'input.json').read_bytes(), original)
        self.assertEqual((self.directory / 'compose.json').read_bytes(), self.encoded)

    def test_concurrent_command_and_symlink_hardlink_lock_fail_before_owner(self):
        with INSTALL.installation_lock(self.directory):
            with self.assertRaises(INSTALL.InstallationFailure):
                with INSTALL.installation_lock(self.directory):
                    self.fail('second command acquired the lock')
        lock = self.directory / '.installation-lock'
        os.link(lock, self.directory / 'alias')
        with self.assertRaises(INSTALL.InstallationFailure):
            with INSTALL.installation_lock(self.directory):
                self.fail('hardlinked lock accepted')
        (self.directory / 'alias').unlink()
        lock.unlink()
        lock.symlink_to(self.input)
        with self.assertRaises(OSError):
            with INSTALL.installation_lock(self.directory):
                self.fail('symlink lock accepted')

    def test_startup_waits_for_provision_and_readiness_before_private_session(self):
        self.arguments.operation = 'up'
        calls = []
        with mock.patch.object(INSTALL, 'rendered', return_value=(['docker', 'compose'], self.document)), \
             mock.patch.object(INSTALL, 'command', side_effect=lambda args, **kw: calls.append(args)), \
             mock.patch.object(INSTALL, 'session', side_effect=lambda *args: calls.append(['session'])), \
             mock.patch.object(INSTALL, 'remember_ready'), mock.patch.object(INSTALL, 'public_trust', side_effect=lambda *args: calls.append(['public-trust'])), \
             mock.patch.object(INSTALL, 'retain_images'), \
             mock.patch.object(INSTALL, 'composition_snapshot', return_value={name: ContainerState('d'*64, True) for name in self.document['services']}), \
             mock.patch.object(INSTALL, 'ensure_provider'):
            INSTALL.execute(self.arguments)
        self.assertEqual(calls[0][-1], 'installation-prepare')
        self.assertEqual(calls[3][-1], 'installation-provision')
        self.assertIn('gateway-management', calls[4])
        self.assertEqual(calls[5][-1], 'installation-ready')
        self.assertEqual(calls[6], ['public-trust'])
        self.assertEqual(calls[7], ['session'])
        self.assertFalse(any('down' in item or '--volumes' in item for item in calls))

    def test_provision_or_verify_failure_never_launches_or_delivers_session(self):
        for operation, failing in [('up', 'installation-provision'), ('verify', 'installation-verify')]:
            self.arguments.operation = operation
            calls = []
            def command(args, **kwargs):
                calls.append(args)
                if args[-1] == failing:
                    raise INSTALL.InstallationFailure('safe rejected')
            with mock.patch.object(INSTALL, 'rendered', return_value=(['docker', 'compose'], self.document)), \
                 mock.patch.object(INSTALL, 'command', side_effect=command), \
                 mock.patch.object(INSTALL, 'session') as session, \
                 mock.patch.object(INSTALL, 'retain_images'), \
             mock.patch.object(INSTALL, 'composition_snapshot', return_value={name: ContainerState('d'*64, True) for name in self.document['services']}), \
             mock.patch.object(INSTALL, 'ensure_provider'):
                with self.assertRaises(INSTALL.InstallationFailure):
                    INSTALL.execute(self.arguments)
            session.assert_not_called()
            self.assertFalse(any('gateway-management' in item for item in calls))

    def test_retention_tags_preserve_exact_artifacts_without_becoming_execution_identity(self):
        self.arguments.operation = 'up'
        tags = {}
        effects = []
        def command(arguments, **kwargs):
            if arguments[1:3] == ['image', 'inspect']:
                return (tags.get(arguments[3], arguments[3]) + '\n').encode()
            if arguments[1:3] == ['image', 'ls']:
                return tags.get(arguments[5].removeprefix('reference='), '').encode()
            self.assertEqual(arguments[1], 'tag')
            effects.append(arguments)
            tags[arguments[3]] = arguments[2]
        with mock.patch.object(INSTALL, 'command', side_effect=command):
            INSTALL.retain_images(self.arguments, self.document)
            INSTALL.retain_images(self.arguments, self.document)
        self.assertEqual(len(effects), 2)

        self.assertEqual(tags['insight-installation-retained/test-installation:runtime'], self.arguments.runtime_image)
        self.assertEqual(tags['insight-installation-retained/test-installation:console'], self.arguments.console_image)
        tags['insight-installation-retained/test-installation:runtime'] = 'sha256:' + 'c' * 64
        with mock.patch.object(INSTALL, 'command', side_effect=command):
            with self.assertRaises(INSTALL.InstallationFailure):
                INSTALL.retain_images(self.arguments, self.document)
        self.assertEqual(len(effects), 2)

    def test_readonly_operations_do_not_create_missing_image_retention_reference(self):
        for operation in ('verify', 'session'):
            self.arguments.operation = operation
            with mock.patch.object(INSTALL, 'command', side_effect=[self.arguments.runtime_image.encode(), b'']) as commands:
                with self.assertRaises(INSTALL.InstallationFailure):
                    INSTALL.retain_images(self.arguments, self.document)
            self.assertTrue(all(call.args[0][1] == 'image' for call in commands.call_args_list))

    def test_immutable_image_and_unsafe_input_bounds(self):
        for value in ('image:latest', 'sha256:' + 'A' * 64, 'image@sha256:' + 'a' * 63):
            with self.assertRaises(argparse.ArgumentTypeError):
                INSTALL.image(value)
        self.input.unlink()
        self.input.symlink_to(self.root / 'missing')
        with self.assertRaises(INSTALL.InstallationFailure):
            INSTALL.regular(self.input, 262144)

    def test_drift_qualification_requires_rejection_and_restores_only_fixture_change(self):
        for verification in ('reject', 'accept', 'repair'):
            state = {'digest': b'original', 'restored': False}
            def command(arguments, **kwargs):
                script = arguments[-1]
                if arguments[0] == 'docker':
                    self.assertEqual(arguments[arguments.index('--user') + 1], '0:0')
                if script == 'sha256sum /fixture/config.json':
                    return state['digest']
                if script.startswith('test ! -e '):
                    state['digest'] = b'changed'
                elif script == 'verify':
                    if verification == 'repair':
                        state['digest'] = b'original'
                    if verification != 'accept':
                        raise QUALIFY.OWNER.InstallationFailure('rejected')
                elif script == 'mv /fixture/.qualification-original /fixture/config.json':
                    state.update(digest=b'original', restored=True)
                else:
                    self.fail('unexpected fixture command')
                return b''
            with mock.patch.object(QUALIFY, 'command', side_effect=command), \
                 mock.patch.object(QUALIFY, 'wrapper_command', side_effect=lambda base, operation, **kw: command([*base, operation], **kw)):
                if verification == 'reject':
                    QUALIFY.verify_rejects_configuration_drift('fresh-project', self.arguments.runtime_image, ['wrapper'])
                else:
                    with self.assertRaises(QUALIFY.OWNER.InstallationFailure):
                        QUALIFY.verify_rejects_configuration_drift('fresh-project', self.arguments.runtime_image, ['wrapper'])
            self.assertTrue(state['restored'])
            self.assertEqual(state['digest'], b'original')

    def test_actual_owner_diagnostic_discards_raw_output_and_accepts_only_closed_errors(self):
        output = io.StringIO()
        script = ('import sys; print("unsafe-token-canary"); '
                  'print("installation PrerequisiteUnavailable",file=sys.stderr); '
                  'print("installation https://private.invalid/key",file=sys.stderr); sys.exit(1)')
        with contextlib.redirect_stdout(output):
            with self.assertRaises(QUALIFY.OWNER.InstallationFailure):
                QUALIFY.wrapper_command([sys.executable, '-c', script], 'up', timeout=5)
        self.assertEqual(json.loads(output.getvalue()), {'owner_operation': 'up',
            'failure': 'command_failed', 'installation_errors': ['PrerequisiteUnavailable'],
            'docker_errors': []})
        self.assertNotIn('unsafe-token-canary', output.getvalue())
        self.assertNotIn('private.invalid', output.getvalue())
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            QUALIFY.wrapper_command([sys.executable, '-c', 'print("private-success-canary")'], 'verify', timeout=5)
        self.assertEqual(output.getvalue(), '')

    def test_actual_docker_address_pool_diagnostic_keeps_only_fixed_code(self):
        output = io.StringIO()
        script = ('import sys; print("secret-url-canary all predefined address pools have been fully subnetted", '
                  'file=sys.stderr); sys.exit(1)')
        with contextlib.redirect_stdout(output):
            with self.assertRaises(QUALIFY.OWNER.InstallationFailure):
                QUALIFY.wrapper_command([sys.executable, '-c', script], 'up', timeout=5)
        self.assertEqual(json.loads(output.getvalue()), {'owner_operation': 'up',
            'failure': 'command_failed', 'installation_errors': [],
            'docker_errors': ['address_pool_exhausted']})
        self.assertNotIn('secret-url-canary', output.getvalue())
        self.assertNotIn('predefined', output.getvalue())

    def test_actual_owner_timeout_is_bounded_and_has_no_raw_diagnostic(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            with self.assertRaises(QUALIFY.OWNER.InstallationFailure):
                QUALIFY.wrapper_command([sys.executable, '-c', 'import time; time.sleep(10)'], 'verify', timeout=0.05)
        self.assertEqual(json.loads(output.getvalue()), {'owner_operation': 'verify',
            'failure': 'timeout', 'installation_errors': [], 'docker_errors': []})

    def test_cleanup_includes_initializer_profile_and_rejects_each_remaining_resource_kind(self):
        document = {'services': {'installation-prepare': {'image': 'runtime'},
            'console': {'image': 'console'}, 'openbao-initialize': {}}, 'volumes': {}}
        for remaining in (None, 'container', 'volume', 'network'):
            with self.subTest(remaining=remaining):
                stopped = False
                def command(arguments, **kwargs):
                    nonlocal stopped
                    if 'down' in arguments:
                        self.assertIn('--profile', arguments)
                        self.assertEqual(arguments[arguments.index('--profile')+1], 'initialize')
                        stopped = True
                        return b''
                    if arguments[1] == 'ps':
                        return b'retained-container' if stopped and remaining == 'container' else b''
                    if arguments[1:3] == ['volume', 'ls']:
                        return b'retained-volume' if stopped and remaining == 'volume' else b''
                    if arguments[1:3] == ['network', 'ls']:
                        return b'retained-network' if stopped and remaining == 'network' else b''
                    if arguments[1:3] == ['image', 'inspect']:
                        return b'actual-image-id'
                    if arguments[1:3] == ['image', 'ls']:
                        return b''
                    self.fail('unexpected cleanup command')
                with mock.patch.object(QUALIFY, 'command', side_effect=command):
                    if remaining:
                        with self.assertRaises(QUALIFY.OWNER.InstallationFailure):
                            QUALIFY.cleanup('fixture', ['docker', 'compose'], document)
                    else:
                        QUALIFY.cleanup('fixture', ['docker', 'compose'], document)
                self.assertTrue(stopped)

    def test_snapshot_uses_owner_identity_only_with_readonly_private_fixture_mount(self):
        with mock.patch.object(QUALIFY, 'command', return_value=b'exact-owner-json-digest') as command:
            QUALIFY.snapshot('fresh-fixture', self.arguments.runtime_image)
        arguments = command.call_args.args[0]
        self.assertEqual(arguments[arguments.index('--user') + 1], '0:0')
        self.assertEqual(arguments[arguments.index('--network') + 1], 'none')
        self.assertEqual(arguments[arguments.index('--cap-drop') + 1], 'ALL')
        self.assertIn('type=volume,source=fresh-fixture_installation-private,target=/installation,readonly', arguments)

    def test_preready_trust_qualification_requires_exact_rejection_and_unchanged_files(self):
        for changed in (False, True):
            with self.subTest(changed=changed), \
                 mock.patch.object(QUALIFY, 'trust_state_snapshot', side_effect=['before', 'after' if changed else 'before']), \
                 mock.patch.object(QUALIFY, 'command', return_value=b'') as command:
                if changed:
                    with self.assertRaises(QUALIFY.OWNER.InstallationFailure):
                        QUALIFY.public_trust_rejects_before_ready('fresh-fixture', self.arguments.runtime_image, self.input)
                else:
                    QUALIFY.public_trust_rejects_before_ready('fresh-fixture', self.arguments.runtime_image, self.input)
                command.assert_called_once()
                arguments = command.call_args.args[0]
                self.assertIn('type=volume,source=fresh-fixture_installation-private,target=/installation,readonly', arguments)
                self.assertIn('type=bind,source='+str(self.input)+',target=/installation-input/input.json,readonly', arguments)
                self.assertEqual(arguments[arguments.index('--network')+1], 'none')
                self.assertIn('installation Incomplete', arguments[-1])
                self.assertIn('test ! -s /tmp/public.json', arguments[-1])
                self.assertNotIn('provider-start', arguments[-1])

    def test_qualification_declares_only_an_installed_destination_not_a_provider_account(self):
        value = {'network': {'providers': {'backend': 's3_open_bao'}, 'console_origin': 'http://127.0.0.1:8088'}, 'model_destinations': []}
        result = QUALIFY.qualification_input(value, 18088)
        self.assertEqual(result['network']['console_origin'], 'http://127.0.0.1:18088')
        self.assertEqual(result['model_destinations'], [{'protocol': 'open_ai_responses', 'endpoint': {'scheme': 'https', 'host': 'api.openai.com', 'port': 443, 'base_path': '/'}, 'region': 'global'}])
        with self.assertRaises(QUALIFY.OWNER.InstallationFailure): QUALIFY.qualification_input(result, 18088)
        value = {'network': {'providers': {'backend': 'aws'}}, 'model_destinations': []}
        with self.assertRaises(QUALIFY.OWNER.InstallationFailure): QUALIFY.qualification_input(value, 18088)

    def test_qualification_cleanup_rejects_foreign_objects_before_any_removal(self):
        calls = []
        def command(arguments, **kwargs):
            calls.append(arguments)
            if arguments[1] == 'ps':
                return b'container\n'
            if arguments[1] == 'inspect':
                return json.dumps([{'Config': {'Labels': {
                    'com.docker.compose.project': 'another-installation',
                    'com.docker.compose.service': 'console'}}}]).encode()
            self.fail('cleanup proceeded past mismatched ownership')
        with mock.patch.object(QUALIFY, 'command', side_effect=command):
            with self.assertRaises(QUALIFY.OWNER.InstallationFailure):
                QUALIFY.cleanup('fresh-project', ['docker', 'compose'], self.document)
        self.assertFalse(any('down' in call or 'rm' in call for call in calls))

    def test_reconstruction_drains_serving_before_each_dependency_and_removes_exact_ids(self):
        self.check_reconstruction()

    def test_failed_stop_or_identity_drift_preserves_all_containers_and_volumes(self):
        for failure in ('s3_exit', 'gateway_exit', 'changed_identity', 'bool_exit'):
            with self.subTest(failure=failure):
                self.check_reconstruction(failure)

    def test_reconstruction_requires_current_shared_shutdown_contract_before_effects(self):
        for grace, signal in ((None, 'SIGINT'), ('10s', 'SIGINT'), ('45s', None), ('45s', 'SIGTERM')):
            self.document['services']['s3']['stop_grace_period'] = grace
            self.document['services']['nats']['stop_signal'] = signal
            with mock.patch.object(QUALIFY, 'command') as commands:
                with self.assertRaises(QUALIFY.OWNER.InstallationFailure):
                    QUALIFY.stop_and_remove_current_containers(self.document)
                commands.assert_not_called()

    def check_reconstruction(self, failure=None):
        self.document['services']['s3']['stop_grace_period'] = '45s'
        self.document['services']['nats']['stop_signal'] = 'SIGINT'
        names = ['gateway-management', 'gateway-runtime', 'console', 'nats', 's3', 'openbao', 'postgres']
        states = {name: ContainerState(format(index, '064x'), True) for index, name in enumerate(names, 1)}
        original = dict(states)
        by_id = {value.identity: name for name, value in states.items()}
        calls = []
        def snapshot(*_):
            if failure == 'changed_identity' and all(not state.running for state in states.values()):
                return {**states, 's3': ContainerState('f'*64, False)}
            return dict(states)
        def command(arguments, **kwargs):
            calls.append(arguments)
            if arguments[1] == 'stop':
                for identifier in arguments[4:]:
                    states[by_id[identifier]] = ContainerState(identifier, False)
                return b''
            if arguments[1] == 'inspect':
                identifier = arguments[-1]
                name = by_id[identifier]
                exit_code = 137 if (failure == 's3_exit' and name == 's3') or (failure == 'gateway_exit' and name == 'gateway-management') else 0
                if failure == 'bool_exit': exit_code = False
                return json.dumps({'identity': identifier, 'running': False, 'exit_code': exit_code}).encode()
            if arguments[1] == 'rm':
                self.assertIsNone(failure, 'failed stop removed its recovery evidence')
                self.assertEqual(arguments[2:], [original[name].identity for name in names])
                self.assertNotIn('--volumes', arguments)
                return b''
            self.fail('unexpected reconstruction effect')
        with mock.patch.object(QUALIFY, 'command', side_effect=command), \
             mock.patch.object(QUALIFY, 'composition_snapshot', side_effect=snapshot):
            if failure:
                with self.assertRaises(QUALIFY.OWNER.InstallationFailure):
                    QUALIFY.stop_and_remove_current_containers(self.document)
                self.assertFalse(any(call[1] == 'rm' for call in calls))
            else:
                QUALIFY.stop_and_remove_current_containers(self.document)
                stops = [call for call in calls if call[1] == 'stop']
                self.assertEqual(stops, [
                    ['docker', 'stop', '--time', '35', *[original[name].identity for name in names[:3]]],
                    *[['docker', 'stop', '--time', '45' if name == 's3' else '30', original[name].identity] for name in names[3:]]])


if __name__ == '__main__':
    unittest.main()
