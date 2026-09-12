"""Actual declaration/plan binaries and Helm rendering; no installation or provider service."""
import copy
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile
import unittest
from unittest import mock

from tools.tests import test_installation_helm as helm_tests

ROOT = Path(__file__).resolve().parents[2]


def digest(value):
    return 'sha256:' + hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


class RemoteContextInstallationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        metadata = json.loads(subprocess.check_output(['cargo', 'metadata', '--locked', '--no-deps', '--format-version', '1'], cwd=ROOT, timeout=30))
        cls.binary = Path(metadata['target_directory']) / 'debug/platform-installation'
        if not cls.binary.is_file():
            raise RuntimeError('build the current platform-installation binary before consumer tests')
        cls.certificates = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.certificates.cleanup)
        root = Path(cls.certificates.name).resolve()
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                        '-keyout', str(root/'fixture-key.pem'), '-out', str(root/'fixture-ca.pem'),
                        '-days', '1', '-subj', '/CN=Remote Context Installation Fixture'],
                       stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                       check=True, timeout=20)
        endpoint = {'scheme': 'https', 'host': 'documents.example.test', 'port': 443, 'base_path': '/search'}
        cls.grant = {
            'schema_version': 1,
            'protocol_contract_digest': digest({'contract': 'insight.context.remote_search.json', 'version': 1,
                'method': 'POST', 'media_type': 'application/json', 'request': 'bounded_inline_query_projection_cursor_v1', 'response': 'closed_items_cursor_revision_v1'}),
            'result_mapping_digest': digest({'contract': 'insight.context.remote_search.result_mapping', 'version': 1,
                'source_identity': 'canonical_json_sha256', 'locator': 'canonical_json_sha256', 'content': 'identity',
                'structured_fields': 'identity', 'score': 'millionths', 'classification': 'bounded_by_current_request', 'unknown_fields': 'reject'}),
            'endpoint': endpoint, 'endpoint_identity_digest': digest(endpoint), 'region': 'global',
            'credential_injections': [], 'trusted_root_pem': (root/'fixture-ca.pem').read_text(),
            'maximum_request_bytes': 8192, 'maximum_response_bytes': 65536,
        }

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.destination_file = self.root/'destinations.json'
        self.destination_file.write_text(json.dumps([self.grant]))
        self.package = 'sha256:'+'a'*64
        self.image = 'example/platform@'+self.package

    def cli(self, *args, success=True):
        result = subprocess.run([str(self.binary), *map(str, args)], cwd=ROOT,
                                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        if success:
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            return json.loads(result.stdout)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, b'')

    def declaration(self, topology, *, selected=True):
        args = [topology+'-input', 'helm-test', self.package]
        if topology == 'native':
            args += ['--output', self.root/'output', '--port-base', '28000']
        if selected:
            args += ['--remote-context-destinations', self.destination_file]
        return self.cli(*args)

    def helm_plan(self):
        path = self.root/'input.json'
        path.write_text(json.dumps(self.declaration('kubernetes')))
        return self.cli('helm-values', '--input', path, '--runtime-image', self.image, '--console-image', self.image)['plan']

    def test_three_declaration_commands_derive_optional_role_without_manual_network_patch(self):
        for topology in ['compose', 'kubernetes', 'native']:
            original = self.declaration(topology, selected=False)
            selected = self.declaration(topology)
            self.assertEqual(original['remote_context_destinations'], [])
            self.assertEqual(selected['remote_context_destinations'], [self.grant])
            self.assertNotIn('context-remote', [item['process'] for item in original['network']['processes']])
            role = next(item for item in selected['network']['processes'] if item['process'] == 'context-remote')
            self.assertIsNone(role['listen_address'])
            self.assertIsNone(role['service_origin'])
            credentials = [item for item in selected['credentials']['files'] if item['process'] == 'context-remote']
            self.assertIn('context-worker-client-key.pem', [item['file_name'] for item in credentials])
            self.assertNotIn('egress-broker-client-key.pem', [item['file_name'] for item in credentials])

    def test_compose_consumes_only_the_optional_role_volume(self):
        path = self.root/'input.json'
        path.write_text(json.dumps(self.declaration('compose')))
        plan = self.cli('compose', '--input', path, '--runtime-image', self.image, '--console-image', self.image)
        role = plan['services']['context-remote']
        self.assertTrue(role['command'][0].endswith('platform-remote-context-worker'))
        self.assertEqual([volume['source'] for volume in role['volumes']], ['role-context-remote'])
        self.assertTrue(role['volumes'][0]['read_only'])

    def test_authenticated_physical_header_mapping_round_trips_without_secret_material(self):
        grant = copy.deepcopy(self.grant)
        grant['credential_injections'] = [{'kind': 'header', 'purpose': 'document_search_key', 'name': 'x-document-key'}]
        self.destination_file.write_text(json.dumps([grant]))
        result = self.declaration('compose')
        self.assertEqual(result['remote_context_destinations'], [grant])
        grant['credential_injections'][0]['name'] = 'X-Document-Key'
        self.destination_file.write_text(json.dumps([grant]))
        self.cli('compose-input', 'helm-test', self.package, '--remote-context-destinations', self.destination_file, success=False)

    def test_helm_publication_gate_renders_selected_role_and_its_private_pvc(self):
        plan = self.helm_plan()
        fixture = helm_tests.HelmTests(methodName='test_complete_chart_gates_roles_without_workload_management_permissions')
        self.addCleanup(fixture.doCleanups)
        with mock.patch.object(helm_tests, 'owner_plan', return_value=plan):
            fixture.setUp()
        fixture.test_complete_chart_gates_roles_without_workload_management_permissions()

    def test_optional_declaration_file_rejects_links_bad_pem_unknown_fields_and_duplicate_json(self):
        link = self.root/'alias.json'
        link.symlink_to(self.destination_file)
        self.cli('compose-input', 'helm-test', self.package, '--remote-context-destinations', link, success=False)
        os.link(self.destination_file, self.root/'hardlink.json')
        self.cli('compose-input', 'helm-test', self.package, '--remote-context-destinations', self.destination_file, success=False)
        (self.root/'hardlink.json').unlink()
        for key, value in [('trusted_root_pem', 'bad certificate'), ('context_deployment', {})]:
            grant = copy.deepcopy(self.grant)
            grant[key] = value
            self.destination_file.write_text(json.dumps([grant]))
            self.cli('compose-input', 'helm-test', self.package, '--remote-context-destinations', self.destination_file, success=False)
        self.destination_file.write_text(json.dumps([self.grant]).replace('"schema_version": 1', '"schema_version": 1, "schema_version": 1', 1))
        self.cli('compose-input', 'helm-test', self.package, '--remote-context-destinations', self.destination_file, success=False)

    def test_native_plan_includes_optional_executable_and_existing_start_order(self):
        # Owner handoff checks real filesystem metadata/host headers; these files are never run.
        document = self.declaration('native')
        path = self.root/'native-input.json'
        path.write_text(json.dumps(document))
        binaries, console = self.root/'bin', self.root/'console'
        binaries.mkdir()
        (console/'server-dist').mkdir(parents=True)
        (console/'dist').mkdir()
        header = bytearray(64)
        if platform.system() == 'Darwin':
            header[:4] = bytes.fromhex('cffaedfe')
            header[4:8] = (0x0100000c if platform.machine() == 'arm64' else 0x01000007).to_bytes(4, 'little')
            header[12:16] = (2).to_bytes(4, 'little')
        else:
            header[:6] = b'\x7fELF\x02\x01'
            header[16:18] = (2).to_bytes(2, 'little')
            header[18:20] = (183 if platform.machine() == 'aarch64' else 62).to_bytes(2, 'little')
        # The public owning Helm plan supplies the same closed process-to-binary mapping.
        names = {item['binary'] for item in self.helm_plan()['processes']}
        names.update(['platform-installation', 'platform-schema', 'platform-database-role', 'platform-dev-bootstrap', 'platform-jetstream-provision', 'node'])
        for name in names:
            (binaries/name).write_bytes(header)
            (binaries/name).chmod(0o755)
        for name in ['main.js', 'config.js', 'gateway-server.js', 'process.js']:
            (console/'server-dist'/name).write_text('// bounded handoff fixture\n')
        (console/'dist/index.html').write_text('<!doctype html><title>Fixture</title>')
        (console/'dist/compiler.wasm').write_bytes(b'\0asm\x01\0\0\0')
        (console/'dist/compiler.worker-fixture.js').write_text('// handoff fixture; never executed\n')
        plan = self.cli('native-plan', '--input', path, '--output', self.root/'output', '--binaries', binaries, '--console-directory', console, '--node', binaries/'node')
        role = next(item for item in plan['processes'] if item['process'] == 'context-remote')
        self.assertEqual(role['executable_file'], str(binaries/'platform-remote-context-worker'))
        self.assertEqual(role['environment_file'], str(self.root/'output/roles/context-remote/environment'))
        order = [item['process'] for item in plan['processes']]
        self.assertLess(order.index('egress-broker'), order.index('context-remote'))
        self.assertIn(role['executable_file'], [item['path'] for item in plan['artifacts']])


if __name__ == '__main__':
    unittest.main()
