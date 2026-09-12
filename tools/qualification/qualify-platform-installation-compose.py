#!/usr/bin/env python3
"""Qualify new local installation containers; retain no session token in the report."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import selectors
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools/install'))
from provider_lifecycle import LifecycleFailure, composition_snapshot
import public_trust as TRUST
sys.path.insert(0, str(ROOT / 'tools/qualification'))
import installation_support as OWNER


def command(arguments, timeout=300, stage='docker_command'):
    return bounded_command(arguments, stage, timeout=timeout)


def bounded_command(arguments, operation, timeout=300):
    if operation not in ('up', 'verify', 'prepare', 'public-trust-before-ready', 'docker_command'):
        raise OWNER.InstallationFailure('invalid qualification command stage')
    allowed = {'InvalidInput', 'InvalidEndpoint', 'InvalidRoleClosure', 'InvalidPath',
        'UnsupportedTopology', 'IdentityDrift', 'ConfigurationDrift', 'ForeignState',
        'CredentialInvalid', 'PrerequisiteUnavailable', 'SchemaMismatch',
        'ExternalOutcomeUnknown', 'Conflict', 'Incomplete'}
    buffers = [bytearray(), bytearray()]
    process = subprocess.Popen(arguments, stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    failure = None
    deadline = time.monotonic() + timeout
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ, 0)
            selector.register(process.stderr, selectors.EVENT_READ, 1)
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    failure = 'timeout'
                    raise OWNER.InstallationFailure('owner observation timed out')
                for key, _ in selector.select(min(remaining, 1)):
                    chunk = os.read(key.fd, 65536)
                    if not chunk:
                        selector.unregister(key.fileobj)
                    elif len(buffers[key.data]) + len(chunk) > OWNER.MAXIMUM:
                        failure = 'output_bound'
                        raise OWNER.InstallationFailure('owner observation output exceeds its bound')
                    else:
                        buffers[key.data].extend(chunk)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            failure = 'timeout'
            raise OWNER.InstallationFailure('owner observation timed out')
        try:
            exit_code = process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            failure = 'timeout'
            raise OWNER.InstallationFailure('owner observation timed out') from None
        if time.monotonic() >= deadline:
            failure = 'timeout'
            raise OWNER.InstallationFailure('owner observation timed out')
        if exit_code:
            failure = 'command_failed'
            raise OWNER.InstallationFailure('installation owner rejected qualification')
        return bytes(buffers[0])
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
        process.stdout.close()
        process.stderr.close()
        if failure:
            codes = set()
            docker_codes = set()
            for data in buffers:
                for line in data.splitlines():
                    if b'all predefined address pools have been fully subnetted' in line:
                        docker_codes.add('address_pool_exhausted')
                    if line.startswith(b'installation '):
                        suffix = line[len(b'installation '):]
                        if any(suffix == code.encode() for code in allowed):
                            codes.add(suffix.decode('ascii'))
            print(json.dumps({'owner_operation': operation, 'failure': failure,
                'installation_errors': sorted(codes), 'docker_errors': sorted(docker_codes)},
                separators=(',', ':')), flush=True)
        for data in buffers:
            data[:] = b'\x00' * len(data)
            data.clear()


def selected_image(value):
    if not re.fullmatch(r'(?:[A-Za-z0-9._/:-]+@)?sha256:[a-f0-9]{64}', value):
        raise argparse.ArgumentTypeError('immutable image required')
    return value


def declaration_command(runtime_image, project, remote_context_file=None):
    arguments = ['docker', 'run', '--rm', '--network', 'none', '--read-only', '--cap-drop', 'ALL']
    if remote_context_file is not None:
        path = Path(remote_context_file)
        if not path.is_absolute() or any(part in ('.', '..') for part in path.parts):
            raise OWNER.InstallationFailure('invalid public destination declaration path')
        for parent in path.parents:
            if parent.is_symlink() or not parent.is_dir():
                raise OWNER.InstallationFailure('invalid public destination declaration parent')
        metadata = path.lstat()
        if path.is_symlink() or not path.is_file() or metadata.st_nlink != 1 or not 1 <= metadata.st_size <= 262144:
            raise OWNER.InstallationFailure('invalid public destination declaration file')
        # The producer, not this harness, validates the owning grant and derives selected roles.
        arguments += ['--user', f'{os.geteuid()}:{os.getegid()}', '--mount',
                      'type=bind,source='+str(path)+',target=/remote-context-destinations.json,readonly']
    arguments += ['--entrypoint', '/usr/local/bin/platform-installation', runtime_image,
                  'compose-input', project, runtime_image.rsplit('@', 1)[-1]]
    if remote_context_file is not None:
        arguments += ['--remote-context-destinations', '/remote-context-destinations.json']
    return arguments


def qualification_input(value, console_port):
    """Provisioned model policies exercise internal Artifact bytes, with no model calls."""
    if value['network']['providers']['backend'] != 's3_open_bao':
        raise OWNER.InstallationFailure('qualification provider input differs')
    value['network']['console_origin'] = 'http://127.0.0.1:'+str(console_port)
    return value


def snapshot(project, runtime):
    # Only hashes of owner JSON are observed; no credential bytes leave the volume.
    output = command(['docker', 'run', '--rm', '--network', 'none', '--read-only', '--user', '0:0', '--cap-drop', 'ALL',
        '--mount', 'type=volume,source='+project+'_installation-private,target=/installation,readonly',
        '--entrypoint', '/bin/sh', runtime, '-ec',
        'find /installation/private -maxdepth 1 -type f -name "*.json" -exec sha256sum {} \\; | sort'])
    if not output.strip():
        raise OWNER.InstallationFailure('installation snapshot is empty')
    return hashlib.sha256(output).hexdigest()


def trust_state_snapshot(project, runtime):
    """Hash every private file across export; no credential content leaves the read-only volume."""
    output = command(['docker', 'run', '--rm', '--network', 'none', '--read-only', '--user', '0:0', '--cap-drop', 'ALL',
        '--mount', 'type=volume,source='+project+'_installation-private,target=/installation,readonly',
        '--entrypoint', '/bin/sh', runtime, '-ec',
        'find /installation/private -maxdepth 1 -type f -exec sha256sum {} \\; | sort'])
    if not output.strip():
        raise OWNER.InstallationFailure('public trust installation snapshot is empty')
    return hashlib.sha256(output).hexdigest()


def public_trust_rejects_before_ready(project, runtime, declaration):
    """Exercise the actual owner on a read-only mount before any provider initialization."""
    before = trust_state_snapshot(project, runtime)
    command(['docker', 'run', '--rm', '--network', 'none', '--read-only', '--user', '0:0', '--cap-drop', 'ALL',
        '--security-opt', 'no-new-privileges:true', '--tmpfs', '/tmp:rw,noexec,nosuid,size=1048576',
        '--mount', 'type=volume,source='+project+'_installation-private,target=/installation,readonly',
        '--mount', 'type=bind,source='+str(declaration)+',target=/installation-input/input.json,readonly',
        '--entrypoint', '/bin/sh', runtime, '-ec',
        'set +e; /usr/local/bin/platform-installation public-trust --input /installation-input/input.json '
        '--state /installation/private > /tmp/public.json 2> /tmp/error; result=$?; set -e; '
        'test "$result" -eq 1; test ! -s /tmp/public.json; '
        'case "$(cat /tmp/error)" in "installation Incomplete") ;; '
        '"installation InvalidInput") printf "%s\\n" "installation InvalidInput" >&2; exit 1 ;; '
        '*) printf "%s\\n" "installation ExternalOutcomeUnknown" >&2; exit 1 ;; esac'], timeout=30, stage='public-trust-before-ready')
    if trust_state_snapshot(project, runtime) != before:
        raise OWNER.InstallationFailure('pre-Ready public trust export changed private state')


def public_trust_readonly_export(project, runtime, compose, directory, document):
    input_digest = document['services']['openbao']['labels']['insight.installation.input']
    proof = OWNER.decode(command([*compose, 'run', '--rm', '--no-deps', 'installation-verify']))
    before = trust_state_snapshot(project, runtime)
    output = command([*compose, 'run', '--rm', '--no-deps', 'installation-public-trust'], timeout=30)
    pem, digest = TRUST.certificate(output, input_digest=input_digest, identity_digest=proof['identity_digest'])
    path = directory/'public-ca.pem'
    OWNER.persist_bytes(path, pem, immutable=True)
    repeated = command([*compose, 'run', '--rm', '--no-deps', 'installation-public-trust'], timeout=30)
    if repeated != output or trust_state_snapshot(project, runtime) != before:
        raise OWNER.InstallationFailure('public trust export changed installed state')
    return digest


def verify_rejects_configuration_drift(project, runtime, compose):
    """Change only this new fixture's public Console config; always restore its exact bytes."""
    mounted = ['docker', 'run', '--rm', '--network', 'none', '--read-only', '--user', '0:0',
        '--mount', 'type=volume,source='+project+'_role-console,target=/fixture',
        '--entrypoint', '/bin/sh', runtime, '-ec']
    original = command([*mounted, 'sha256sum /fixture/config.json']).strip()
    command([*mounted, 'test ! -e /fixture/.qualification-original; cp -p /fixture/config.json /fixture/.qualification-original; printf "\\n" >> /fixture/config.json'])
    try:
        changed = command([*mounted, 'sha256sum /fixture/config.json']).strip()
        if changed == original:
            raise OWNER.InstallationFailure('fixture did not change configuration')
        try:
            command([*compose, 'run', '--rm', '--no-deps', 'installation-verify'], timeout=180, stage='verify')
        except OWNER.InstallationFailure:
            pass
        else:
            raise OWNER.InstallationFailure('verification accepted configuration drift')
        if command([*mounted, 'sha256sum /fixture/config.json']).strip() != changed:
            raise OWNER.InstallationFailure('verification repaired configuration drift')
    finally:
        command([*mounted, 'mv /fixture/.qualification-original /fixture/config.json'])
    if command([*mounted, 'sha256sum /fixture/config.json']).strip() != original:
        raise OWNER.InstallationFailure('fixture configuration restore differed')


def cleanup(project, compose, document):
    # Namespace was fresh and its random name was chosen here. Refuse unexpected objects rather
    # than applying cleanup to any resource outside the exact owner-generated closure.
    containers = command(['docker', 'ps', '--all', '--quiet', '--filter', 'label=com.docker.compose.project='+project]).split()
    expected_services = set(document['services'])
    for identifier in containers:
        item = json.loads(command(['docker', 'inspect', identifier.decode()]))[0]
        labels = item['Config']['Labels']
        if labels.get('com.docker.compose.project') != project or labels.get('com.docker.compose.service') not in expected_services:
            raise OWNER.InstallationFailure('fixture container ownership differs')
    volumes = command(['docker', 'volume', 'ls', '--quiet', '--filter', 'label=com.docker.compose.project='+project]).split()
    for identifier in volumes:
        item = json.loads(command(['docker', 'volume', 'inspect', identifier.decode()]))[0]
        labels = item['Labels']
        name = labels.get('com.docker.compose.volume')
        if labels.get('com.docker.compose.project') != project or name not in document['volumes'] or labels.get('insight.installation.input') != document['volumes'][name]['labels']['insight.installation.input']:
            raise OWNER.InstallationFailure('fixture volume ownership differs')
    command([*compose, '--profile', 'operations', 'down', '--volumes'], timeout=180)
    for arguments in (
        ['docker', 'ps', '--all', '--quiet', '--filter', 'label=com.docker.compose.project='+project],
        ['docker', 'volume', 'ls', '--quiet', '--filter', 'label=com.docker.compose.project='+project],
        ['docker', 'network', 'ls', '--quiet', '--filter', 'label=com.docker.compose.project='+project],
    ):
        if command(arguments).strip():
            raise OWNER.InstallationFailure('fixture resources remain after cleanup')


def stop_and_remove_current_containers(document):
    """Drain serving before dependencies, proving each original container exited cleanly."""
    services = document['services']
    dependencies = ('nats', 's3', 'openbao', 'postgres')
    serving = [name for name in services if not name.startswith('installation-')
               and name not in dependencies]
    if (not serving or services['s3'].get('stop_grace_period') != '45s'
            or services['nats'].get('stop_signal') != 'SIGINT'):
        raise OWNER.InstallationFailure('shared dependency shutdown protocol differs')
    s3_grace = int(services['s3']['stop_grace_period'].removesuffix('s'))
    original = composition_snapshot(document, command)
    names = [*serving, *dependencies]
    if any(name not in original or not original[name].running for name in names):
        raise OWNER.InstallationFailure('reconstruction requires every original service running')

    def stop(group, grace):
        identifiers = [original[name].identity for name in group]
        command(['docker', 'stop', '--time', str(grace), *identifiers], timeout=grace + 30)
        for identifier in identifiers:
            state = OWNER.decode(command(['docker', 'inspect', '--format',
                '{"identity":{{json .Id}},"running":{{json .State.Running}},"exit_code":{{json .State.ExitCode}}}', identifier]))
            if (set(state) != {'identity', 'running', 'exit_code'} or state['identity'] != identifier
                    or state['running'] is not False or type(state['exit_code']) is not int
                    or state['exit_code'] != 0):
                raise OWNER.InstallationFailure('original service did not stop cleanly')

    stop(serving, 35)
    for name in dependencies:
        stop([name], s3_grace if name == 's3' else 30)
    stopped = composition_snapshot(document, command)
    if any(name not in stopped or stopped[name].identity != original[name].identity
           or stopped[name].running for name in names):
        raise OWNER.InstallationFailure('stopped container identity changed before reconstruction')
    command(['docker', 'rm', *[original[name].identity for name in names]], timeout=120)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime-image', type=selected_image, required=True)
    parser.add_argument('--console-image', type=selected_image, required=True)
    parser.add_argument('--remote-context-destinations', type=Path)
    parser.add_argument('--subnet', help='Explicit non-overlapping subnet for this disposable fixture')
    args = parser.parse_args()
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        console_port = probe.getsockname()[1]
    project = 'insight-installation-'+uuid.uuid4().hex[:12]
    if command(['docker', 'ps', '--all', '--quiet', '--filter', 'label=com.docker.compose.project='+project]).strip():
        raise OWNER.InstallationFailure('fixture namespace already exists')
    # Keep the private declaration/host intent available if cleanup is uncertain. This path may
    # contain a private session file; only the closed report and its safe location are printed.
    directory = Path(tempfile.mkdtemp(prefix='insight-compose-qualification-')).resolve()
    document = None
    compose = None
    report = None
    try:
        value = json.loads(command(declaration_command(args.runtime_image, project, args.remote_context_destinations)))
        if args.remote_context_destinations is not None and not value['remote_context_destinations']:
            raise OWNER.InstallationFailure('selected Context qualification requires a destination')
        value = qualification_input(value, console_port)
        endpoint = urlsplit(value['network']['providers']['artifact'])
        if endpoint.scheme != 'https' or endpoint.hostname != 's3.localhost' or endpoint.port is None:
            raise OWNER.InstallationFailure('qualification public object endpoint differs')
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', endpoint.port))
        declaration = directory/'input.json'
        declaration.write_text(json.dumps(value, sort_keys=True)+'\n')
        # InstallationInputV1 contains topology and credential file references, never secret bytes.
        # Readers run as both root without DAC_OVERRIDE and UID 10001. The enclosing fresh
        # directory remains 0700; only this explicit read-only bind must be readable across UIDs.
        os.chmod(declaration, 0o644)
        document = OWNER.decode(command(['docker', 'run', '--rm', '--network', 'none', '--read-only',
            '--user', f'{os.geteuid()}:{os.getegid()}', '--cap-drop', 'ALL',
            '--mount', f'type=bind,source={declaration},target={declaration},readonly',
            '--entrypoint', '/usr/local/bin/platform-installation', args.runtime_image,
            'compose', '--input', str(declaration), '--runtime-image', args.runtime_image,
            '--console-image', args.console_image]))
        if args.subnet:
            import ipaddress
            subnet = ipaddress.ip_network(args.subnet, strict=True)
            if subnet.version != 4 or not subnet.is_private:
                raise OWNER.InstallationFailure('private fixture subnet required')
            document['networks']['default']['ipam'] = {'config': [{'subnet': str(subnet)}]}
        OWNER.persist(directory/'compose.json', document, immutable=True)
        compose = ['docker', 'compose', '--file', str(directory/'compose.json'), '--project-name', project]
        command([*compose, 'run', '--rm', '--no-deps', 'installation-prepare'], stage='prepare')
        public_trust_rejects_before_ready(project, args.runtime_image, declaration)
        print('Checking fresh one-shot initialization and all serving readiness', flush=True)
        command([*compose, 'up', '-d'], timeout=600, stage='up')
        command([*compose, 'run', '--rm', '--no-deps', 'installation-ready'], timeout=180)
        public_ca_digest = public_trust_readonly_export(project, args.runtime_image, compose, directory, document)
        before = snapshot(project, args.runtime_image)
        command([*compose, 'run', '--rm', '--no-deps', 'installation-verify'], timeout=180, stage='verify')
        if snapshot(project, args.runtime_image) != before:
            raise OWNER.InstallationFailure('readonly verify changed installation evidence')
        print('Checking configuration drift rejection without repair', flush=True)
        verify_rejects_configuration_drift(project, args.runtime_image, compose)
        command([*compose, 'run', '--rm', '--no-deps', 'installation-verify'], timeout=180, stage='verify')
        print('Checking controlled dependency and serving container recreation with frozen identity', flush=True)
        stop_and_remove_current_containers(document)
        command([*compose, 'up', '-d'], timeout=300, stage='up')
        command([*compose, 'run', '--rm', '--no-deps', 'installation-ready'], timeout=180)
        if snapshot(project, args.runtime_image) != before:
            raise OWNER.InstallationFailure('restart changed frozen installation evidence')
        print('Checking explicit session delivery after ordinary startup', flush=True)
        command(['docker', 'run', '--rm', '--network', 'none', '--read-only', '--user', '0:0', '--cap-drop', 'ALL',
            '--mount', 'type=volume,source='+project+'_installation-private,target=/installation,readonly',
            '--entrypoint', '/usr/bin/test', args.runtime_image, '!', '-e', '/installation/private/session-token'])
        session_container = project+'-session'
        delivery = OWNER.decode(command([*compose, 'run', '--no-deps', '--name', session_container, 'installation-session']))
        command(['docker', 'cp', session_container+':/installation/private/session-token', str(directory/'session-token')])
        token = OWNER.read_file(directory/'session-token', private=True, maximum=16384)
        if (delivery['schema_version'] != 1 or not delivery['tenant_id'].startswith('ten_')
                or delivery['input_digest'] != document['services']['openbao']['labels']['insight.installation.input']
                or token.count(b'.') != 2 or not token.endswith(b'\n')):
            raise OWNER.InstallationFailure('explicit session delivery differs')
        command(['docker', 'rm', session_container])
        report = {'schema_version':1, 'result':'passed', 'runtime_image':args.runtime_image, 'direct_compose_up':True, 'explicit_session_only':True,
            'console_image':args.console_image, 'all_roles_and_console_ready':True,
            'readonly_verify':True, 'drift_rejected_without_repair':True,
            'public_trust_rejected_before_ready':True,
            'public_trust_readonly_mount_and_immutable_delivery':True,
            'public_ca_file_sha256':public_ca_digest,
            'stopped_restart_preserved_identity_and_configuration':True,
            'controlled_provider_container_recreation':True,
            'internal_model_policy_artifact_stage_and_readback':True,
            'remote_context_destination_installed':bool(value['remote_context_destinations']),
            'remote_context_provider_dispatch_tested':False,
            'model_provider_requests':False}
    finally:
        if report is not None and compose is not None and document is not None:
            try:
                cleanup(project, compose, document)
            except (OWNER.InstallationFailure, OSError, ValueError, subprocess.SubprocessError):
                report.update(result='failed', cleaned=False, failure='cleanup_incomplete', evidence_directory=str(directory))
                OWNER.persist(directory/'report.json', report, immutable=True)
                raise
            report['cleaned'] = True
        else:
            failed = {'schema_version': 1, 'result': 'failed', 'runtime_image': args.runtime_image,
                'console_image': args.console_image, 'project': project, 'cleaned': False,
                'evidence_directory': str(directory), 'failure': 'installation_qualification_incomplete'}
            OWNER.persist(directory/'report.json', failed, immutable=True)
        print('Private fixture evidence directory: '+str(directory), flush=True)
    OWNER.persist(directory/'report.json', report, immutable=True)
    print(json.dumps(report), flush=True)
    print('PASS only task-owned containers, volumes and network removed', flush=True)


if __name__ == '__main__':
    def interrupted(_number, _frame):
        raise OWNER.InstallationFailure('qualification interrupted')
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        main()
    except (OWNER.InstallationFailure, LifecycleFailure, OSError, ValueError, subprocess.SubprocessError):
        raise SystemExit('Installation qualification failed; no existing installation was reset') from None
