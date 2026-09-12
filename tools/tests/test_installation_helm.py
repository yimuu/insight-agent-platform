"""Render the actual declarative chart; no host controller or Kubernetes API fixture."""
import copy
from functools import lru_cache
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CHART = ROOT/'deploy/helm/insight-platform-installation'


@lru_cache()
def installer_binary():
    metadata=json.loads(subprocess.check_output(['cargo','metadata','--locked','--no-deps','--format-version','1'],cwd=ROOT))
    binary=Path(metadata['target_directory'])/'debug/platform-installation'
    if not binary.is_file(): raise RuntimeError('build the current platform-installation binary before consumer tests')
    return binary


def owner_plan():
    binary = installer_binary()
    digest = 'sha256:'+'a'*64
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory).resolve()/'input.json'
        path.write_bytes(subprocess.check_output([str(binary), 'kubernetes-input', 'helm-test', digest]))
        return json.loads(subprocess.check_output([str(binary), 'helm-values', '--input', str(path),
            '--runtime-image', 'example/runtime@'+digest, '--console-image', 'example/console@'+digest]))['plan']


def render(plan, **overrides):
    values = dict(plan=plan, node='test-node', **overrides)
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory)/'values.json'; path.write_text(json.dumps(values))
        result = subprocess.run(['helm','template','installation',str(CHART), '--namespace',plan['namespace'], '-f',str(path)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    return result


class HelmTests(unittest.TestCase):
    def setUp(self):
        self.plan = owner_plan()

    def test_complete_chart_gates_roles_without_workload_management_permissions(self):
        result = render(self.plan)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        # Ruby/Psych is already the repository's YAML contract parser.
        documents = json.loads(subprocess.check_output(['ruby','-rjson','-ryaml','-e',
            'puts JSON.generate(YAML.load_stream(STDIN.read))'], input=result.stdout))
        jobs = [d for d in documents if d and d.get('kind') == 'Job']
        self.assertEqual(len(jobs),1)
        job = jobs[0]['spec']['template']['spec']
        self.assertIn('platform-installation install',job['containers'][0]['args'][0])
        for document in documents:
            if not document or document.get('kind') != 'Deployment': continue
            name=document['metadata']['name']; pod=document['spec']['template']['spec']
            self.assertFalse(pod['automountServiceAccountToken'])
            gate=pod['initContainers'][0]
            expected='prepared' if name in ('postgres','nats','s3','openbao') else 'ready'
            self.assertEqual(gate['args'][-1],expected)
            self.assertTrue(all(m['readOnly'] for m in gate['volumeMounts']))
            input_mount=next(m for m in gate['volumeMounts'] if m['name']=='installation-input')
            self.assertEqual(input_mount['mountPath'],'/installation-input/input.json')
            self.assertEqual(input_mount['subPath'],'input.json')
            self.assertNotIn('installation-private',[v['name'] for v in pod['volumes']])
        self.assertNotIn('docker.sock',result.stdout.decode())
        self.assertNotIn('kind: RoleBinding',result.stdout.decode())
        operations=[d for d in documents if d and d.get('kind')=='CronJob']
        self.assertEqual({d['metadata']['name'] for d in operations},{'installation-session','installation-public-trust'})
        for operation in operations:
            self.assertTrue(operation['spec']['suspend'])
            pod=operation['spec']['jobTemplate']['spec']['template']['spec']
            self.assertFalse(pod['automountServiceAccountToken'])
            container=pod['containers'][0]
            self.assertNotIn('cat ',container['args'][0])
            self.assertEqual(container['readinessProbe']['exec']['command'][-1],'/delivery/complete')
            mount=next(m for m in container['volumeMounts'] if m['name']=='installation-private')
            self.assertEqual(mount['readOnly'],operation['metadata']['name']=='installation-public-trust')

    def test_operator_cannot_supply_fake_completed_phase(self):
        for values in ({'phase':'serving'}, {'ready':{'identity_digest':'sha256:'+'b'*64}}, {'providerReady':True}):
            self.assertNotEqual(render(self.plan,**values).returncode,0)

    def test_chart_rejects_foreign_namespace_mutable_images_and_command_injection(self):
        for mutate in (lambda p:p.update(namespace='other'),
                       lambda p:p.update(runtime_image='example/runtime:latest'),
                       lambda p:p['processes'][0].update(binary='platform-gateway;false')):
            plan=copy.deepcopy(self.plan);mutate(plan)
            self.assertNotEqual(render(plan).returncode,0)


if __name__ == '__main__': unittest.main()
