"""Exercise the owning renderer and Compose's dependency DAG without a host installer."""
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from tools.tests.test_installation_helm import installer_binary

ROOT=Path(__file__).resolve().parents[2]

class ComposeInstallationTests(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory();self.addCleanup(self.temporary.cleanup)
        self.root=Path(self.temporary.name).resolve()
        self.binary=installer_binary();self.digest='sha256:'+'a'*64
        self.input=self.root/'input.json'
        self.input.write_bytes(subprocess.check_output([str(self.binary),'compose-input','compose-test',self.digest]))
        self.document=json.loads(subprocess.check_output([str(self.binary),'compose','--input',str(self.input),
            '--runtime-image','example/runtime@'+self.digest,'--console-image','example/console@'+self.digest]))

    def test_standard_up_has_complete_acyclic_initialization_dependencies(self):
        services=self.document['services']
        self.assertNotIn('openbao-initialize',services)
        self.assertNotIn('profiles',services['installation-bootstrap'])
        self.assertEqual(services['installation-bootstrap']['command'][0],'bootstrap')
        self.assertEqual(services['openbao']['command'],['server','-config=/run/insight-openbao/serve.json'])
        self.assertEqual(services['installation-provision']['depends_on']['installation-bootstrap']['condition'],'service_completed_successfully')
        def ancestors(name,seen=()):
            self.assertNotIn(name,seen,'dependency cycle')
            result={name}
            for dependency in services[name].get('depends_on',{}): result |= ancestors(dependency,(*seen,name))
            return result
        required={'installation-prepare','openbao','installation-bootstrap','postgres','s3','nats','installation-provision'}
        for name,service in services.items():
            if name.startswith('installation-') or name in ('postgres','nats','s3','openbao','console'):continue
            self.assertTrue(required <= ancestors(name),name)
            self.assertEqual([v['source'] for v in service['volumes']],['role-'+name])
        self.assertTrue(required <= ancestors('console'))

    def test_admin_operations_do_not_run_or_issue_sessions_during_up(self):
        for name in ('installation-session','installation-public-trust','installation-verify','installation-provider-observe'):
            self.assertEqual(self.document['services'][name]['profiles'],['operations'])
        trust=self.document['services']['installation-public-trust']
        self.assertEqual(trust['network_mode'],'none')
        self.assertTrue(all(v['read_only'] for v in trust['volumes']))
        self.assertNotIn('docker.sock',json.dumps(self.document))

if __name__=='__main__':unittest.main()
