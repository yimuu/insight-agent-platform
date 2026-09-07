import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('diagnostics', ROOT / 'tools/qualification/collect-kind-diagnostics.py')
diagnostics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(diagnostics)


class KindDiagnosticsTests(unittest.TestCase):
    def test_pod_projection_excludes_credentials_and_keeps_failure_evidence(self):
        pod = {'metadata': {'name': 'worker', 'namespace': 'platform-model-worker', 'annotations': {'private': 'secret-marker'}},
               'spec': {'nodeName': 'worker-a', 'containers': [{'name': 'worker', 'args': ['secret-marker'],
                   'env': [{'name': 'KEY', 'value': 'secret-marker'}],
                   'resources': {'requests': {'cpu': '50m', 'memory': '256Mi'}}}]},
               'status': {'phase': 'Pending', 'conditions': [{'type': 'PodScheduled', 'status': 'False',
                   'reason': 'Unschedulable', 'message': 'Insufficient cpu secret-marker'}],
                   'containerStatuses': [{'name': 'worker', 'restartCount': 2, 'lastState': {'terminated': {'exitCode': 137, 'reason': 'OOMKilled', 'message': 'secret-marker'}}}]}}
        result = diagnostics.summarize('pods', {'items': [pod]})
        text = json.dumps(result)
        self.assertNotIn('secret-marker', text)
        self.assertIn('insufficient_cpu', text)
        self.assertIn('OOMKilled', text)
        self.assertEqual(result['items'][0]['containers'][0]['requests']['cpu'], '50m')

    def test_collection_is_bounded_and_retains_truncation_information(self):
        result = diagnostics.summarize('events', {'items': [
            {'metadata': {'name': str(index)}, 'reason': 'secret-marker', 'message': 'secret-marker' * 4000} for index in range(256)]})
        self.assertEqual(result['omitted_items'], 128)
        self.assertNotIn('secret-marker', json.dumps(result))

    def test_subprocess_read_has_a_real_byte_and_time_limit(self):
        with self.assertRaises(diagnostics.CaptureLimitExceeded):
            diagnostics.capture([sys.executable, '-c', 'import sys; sys.stdout.write("x" * 1000000)'], limit=1024)
        with self.assertRaises(subprocess.TimeoutExpired):
            diagnostics.capture([sys.executable, '-c', 'import time; time.sleep(10)'], timeout=0.1)
        result = diagnostics.capture([sys.executable, '-c', 'import sys; print("ok"); sys.stderr.write("private-marker")'])
        self.assertEqual(result.stdout, b'ok\n')

    def test_partial_api_failure_preserves_other_evidence_and_exact_context(self):
        replies = [subprocess.TimeoutExpired('kubectl', 25),
                   subprocess.CompletedProcess([], 0, b'{"items":[]}', b''),
                   subprocess.CompletedProcess([], 1, b'', b'private-error-marker')]
        with patch.object(diagnostics, 'capture', side_effect=replies) as command:
            report = diagnostics.collect(Path('/specific/kubeconfig'), 'kind-productization-123')
        self.assertIn('pods', report)
        self.assertEqual(len(report['errors']), 2)
        self.assertNotIn('private-error-marker', json.dumps(report))
        for call in command.call_args_list:
            self.assertEqual(call.args[0][1:5], ['--kubeconfig', '/specific/kubeconfig', '--context', 'kind-productization-123'])
        with self.assertRaises(ValueError), patch.object(diagnostics, 'capture') as command:
            diagnostics.collect(Path('/specific/kubeconfig'), 'production')
        command.assert_not_called()

    def test_workflow_uploads_diagnostics_before_cluster_deletion(self):
        workflow = (ROOT / '.github/workflows/productization-journey.yml').read_text()
        self.assertLess(workflow.index('Capture bounded Kind scheduling diagnostics'), workflow.index('Preserve bounded runtime diagnostics'))
        self.assertLess(workflow.index('productization-kind-diagnostics.json', workflow.index('Preserve bounded runtime diagnostics')),
                        workflow.index('Always remove the disposable Kind cluster'))


if __name__ == '__main__':
    unittest.main()
