#!/usr/bin/env python3
"""Exercise actual Kind provisioning against a disposable, loopback-only PostgreSQL authority."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
POSTGRES = 'docker.io/library/postgres@sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777'
ARTIFACT_ROLES = ['artifact-gateway', 'artifact-data-reader', 'artifact-data-worker', 'artifact-maintenance']


def execute(command, *, environment=None, expected=True):
    result = subprocess.run(command, env=environment, text=True, capture_output=True, timeout=60)
    if expected and result.returncode:
        raise RuntimeError('local qualification command failed: ' + result.stderr[-2048:])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--schema-bin', type=Path, required=True)
    parser.add_argument('--role-bin', type=Path, required=True)
    args = parser.parse_args()
    for binary in (args.schema_bin, args.role_bin):
        if not binary.is_file() or not os.access(binary, os.X_OK):
            parser.error('explicit built schema and role executables are required')
    name = 'insight-kind-role-qualification-' + uuid.uuid4().hex[:12]
    container = execute(['docker', 'run', '--detach', '--rm', '--name', name,
                         '--label', 'insight.platform/qualification=kind-database-roles', '--publish', '127.0.0.1::5432',
                         '--env', 'POSTGRES_USER=insight', '--env', 'POSTGRES_PASSWORD=insight-local-only',
                         '--env', 'POSTGRES_DB=insight', POSTGRES]).stdout.strip()
    try:
        for _ in range(80):
            if execute(['docker', 'exec', container, 'pg_isready', '--host', '127.0.0.1', '-U', 'insight', '-d', 'insight'], expected=False).returncode == 0:
                break
            time.sleep(0.25)
        else:
            raise RuntimeError('isolated PostgreSQL fixture did not become ready')
        bindings = json.loads(execute(['docker', 'inspect', container, '--format', '{{json .NetworkSettings.Ports}}']).stdout)
        binding = bindings['5432/tcp'][0]
        if binding['HostIp'] != '127.0.0.1':
            raise RuntimeError('fixture must remain loopback-only')
        port = binding['HostPort']
        authority = f'postgresql://insight:insight-local-only@127.0.0.1:{port}/insight'
        execute([str(args.schema_bin.resolve()), 'provision'], environment=dict(os.environ, PLATFORM_DATABASE_URL=authority))
        execute([str(args.schema_bin.resolve()), 'verify'], environment=dict(os.environ, PLATFORM_DATABASE_URL=authority))
        with tempfile.TemporaryDirectory(prefix='insight-kind-role-credentials-') as temporary:
            directory = Path(temporary)
            artifact = directory / 'artifact'
            artifact.mkdir(mode=0o700)
            passwords = {}
            for purpose in ['outbox', 'history', 'security-authority'] + ARTIFACT_ROLES:
                passwords[purpose] = secrets.token_hex(16)
                parent = artifact if purpose in ARTIFACT_ROLES else directory
                password_file = parent / (purpose + '-password')
                password_file.write_text(passwords[purpose])
                password_file.chmod(0o600)

            def provision(purpose, expected=True):
                credential = artifact if purpose == 'artifact' else directory / (purpose + '-password')
                return execute([str(args.role_bin.resolve()), '--profile', 'kind-local', port, '--purpose', purpose, str(credential)],
                               environment=dict(os.environ, PLATFORM_DATABASE_ROLE_ADMIN_URL=authority), expected=expected)

            def sql(statement, purpose=None, expected=True):
                role = 'insight_' + purpose.replace('-', '_') + '_dev' if purpose else 'insight'
                password = passwords[purpose] if purpose else 'insight-local-only'
                return execute(['docker', 'exec', '--env', 'PGPASSWORD=' + password, container,
                                'psql', '--no-psqlrc', '--host', '127.0.0.1', '--username', role, '--dbname', 'insight',
                                '--set', 'ON_ERROR_STOP=1', '--tuples-only', '--no-align', '--command', statement], expected=expected)

            for purpose in ['outbox', 'history', 'security-authority', 'artifact']:
                provision(purpose)
                provision(purpose)  # Exact owned re-provisioning remains idempotent.
            positive = {
                'security-authority': 'SELECT * FROM insight_platform.secret_bindings LIMIT 0',
                'artifact-gateway': 'SELECT d.resource_version_id FROM insight_platform.deployments d JOIN insight_platform.resources r ON r.tenant_id=d.tenant_id AND r.resource_id=d.resource_id JOIN insight_platform.resource_versions v ON v.tenant_id=d.tenant_id AND v.resource_version_id=d.resource_version_id LIMIT 0',
                'artifact-data-reader': 'SELECT * FROM insight_platform.artifact_blobs LIMIT 0',
                'artifact-data-worker': 'SELECT * FROM insight_platform.scheduler_state FOR UPDATE',
                'artifact-maintenance': 'SELECT * FROM insight_platform.scheduler_tenant_state FOR UPDATE',
                'outbox': 'SELECT event_id,tenant_id FROM insight_platform.events LIMIT 0',
                'history': 'SELECT insight_platform.history_scan_runs(NULL,NULL,NULL,NULL,NULL,1)',
            }
            negative = {
                'security-authority': 'SELECT * FROM insight_platform.artifact_blobs LIMIT 0',
                'artifact-gateway': 'UPDATE insight_platform.tenants SET version=version WHERE false',
                'artifact-data-reader': 'DELETE FROM insight_platform.artifact_blobs WHERE false',
                'artifact-data-worker': 'DELETE FROM insight_platform.artifact_blobs WHERE false',
                'artifact-maintenance': 'UPDATE insight_platform.tenants SET version=version WHERE false',
                'outbox': 'SELECT payload FROM insight_platform.events LIMIT 0',
                'history': 'SELECT * FROM insight_platform.runs LIMIT 0',
            }
            for purpose, statement in positive.items():
                sql(statement, purpose)
                rejected = sql(negative[purpose], purpose, expected=False)
                if rejected.returncode == 0 or 'permission denied' not in rejected.stderr:
                    raise RuntimeError('least-privilege negative did not reject for ' + purpose)
            rejected = sql('UPDATE insight_platform.deployments SET tenant_id=tenant_id WHERE false', 'artifact-gateway', expected=False)
            if rejected.returncode == 0 or 'permission denied' not in rejected.stderr:
                raise RuntimeError('Artifact Gateway deployment mutation was not rejected')

            # Fail after preceding members have been visited; the whole cohort must roll back.
            sql("COMMENT ON ROLE insight_artifact_maintenance_dev IS 'unowned fixture identity'")
            original = passwords['artifact-gateway']
            (artifact / 'artifact-gateway-password').write_text(secrets.token_hex(16))
            rejected = provision('artifact', expected=False)
            if rejected.returncode == 0 or 'ownership check failed' not in rejected.stderr:
                raise RuntimeError('role ownership conflict was accepted')
            sql(positive['artifact-gateway'], 'artifact-gateway')  # Original password still works.
            (artifact / 'artifact-gateway-password').write_text(original)
            sql("COMMENT ON ROLE insight_artifact_maintenance_dev IS 'Insight development Artifact Maintenance v1'")
            sql('GRANT insight_history_dev TO insight_artifact_gateway_dev')
            rejected = provision('artifact', expected=False)
            if rejected.returncode == 0 or 'ownership check failed' not in rejected.stderr:
                raise RuntimeError('unexpected inherited role membership was accepted')
            sql('REVOKE insight_history_dev FROM insight_artifact_gateway_dev')
            provision('artifact')
        print('Kind database-role qualification passed: seven actual credential boundaries, ownership/membership rejection and atomic cohort rollback')
    finally:
        # Only the container created by this invocation is removed; no existing DB is reset.
        execute(['docker', 'rm', '--force', container], expected=False)


if __name__ == '__main__':
    main()
