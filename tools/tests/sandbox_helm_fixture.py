#!/usr/bin/env python3
"""Explicit render-only Sandbox manifest. These bytes are never release/deployment evidence."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]


def fixture(tool=None):
    chart = json.loads(subprocess.check_output(['ruby', '-ryaml', '-rjson', '-e', 'puts JSON.generate(YAML.load_file(ARGV.fetch(0)))', str(ROOT / 'deploy/helm/insight-platform-sandbox/values.yaml')], text=True))
    fields = chart['runtimeContract']
    runtime = {'schema_version': 1, 'provider': 'open_sandbox_kubernetes',
               'opensandbox_server_release_digest': chart['images']['server']['digest'],
               'batchsandbox_controller_digest': chart['images']['controller']['digest']}
    for wire, name in [('lifecycle_schema_digest', 'lifecycleSchemaDigest'), ('batchsandbox_crd_digest', 'batchSandboxCrdDigest'),
                       ('kubernetes_provider_template_digest', 'kubernetesProviderTemplateDigest'), ('runner_protocol_digest', 'runnerProtocolDigest'),
                       ('container_runtime_digest', 'containerRuntimeDigest'), ('network_policy_digest', 'networkPolicyDigest')]:
        runtime[wire] = fields[name]
    with tempfile.TemporaryDirectory() as directory:
        config = Path(directory) / 'config.json'
        config.write_text('{}')
        command = ([str(tool)] if tool else ['cargo', 'run', '--locked', '--quiet', '-p', 'insight-platform-contract-tooling', '--bin', 'platform-qualification', '--'])
        catalog = json.loads(subprocess.check_output(command + ['print-worker-execution-capabilities', 'platform-sandbox-dispatcher', str(config)], cwd=ROOT))
    encoded = json.dumps(runtime, sort_keys=True, separators=(',', ':')).encode()
    return {'dispatcher': {'workerManifest': {
        'manifest_version': 2, 'worker_role': 'sandbox-dispatcher', 'work_class': 'sandbox',
        'adapter_runtime_digest': 'sha256:' + hashlib.sha256(encoded).hexdigest(),
        'worker_build_digest': 'sha256:' + hashlib.sha256(b'explicit inert Sandbox Helm render fixture; never deploy').hexdigest(),
        'execution_capabilities': catalog, 'protocol_version': 1,
        'max_concurrency': chart['dispatcher']['worker']['maximumConcurrency'],
        'critical_control_reserved_slots': chart['dispatcher']['worker']['criticalControlReservedSlots']}}}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    arguments = parser.parse_args()
    arguments.output.write_text(json.dumps(fixture(os.environ.get('PLATFORM_TEST_QUALIFICATION_TOOL'))))
