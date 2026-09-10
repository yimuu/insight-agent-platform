#!/usr/bin/env python3
"""Actual non-owner installation roles on a new, loopback-only disposable PostgreSQL authority."""
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

POSTGRES='docker.io/library/postgres@sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777'
PURPOSES=['runtime','outbox','history','security-authority','artifact']

def execute(command, *, environment=None, success=True):
    result=subprocess.run(command,env=environment,text=True,capture_output=True,timeout=90)
    if success and result.returncode:
        raise RuntimeError(f'{Path(command[0]).name} qualification step failed: {result.stderr[-1500:]}')
    return result

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary-directory',type=Path,required=True)
    args=parser.parse_args()
    binaries={name:args.binary_directory.resolve()/name for name in ['platform-installation','platform-schema','platform-database-role']}
    for binary in binaries.values():
        if not binary.is_file() or not os.access(binary,os.X_OK):parser.error('built installation, schema and role tools are required')
    with tempfile.TemporaryDirectory(prefix='insight-installation-role-') as temporary:
        parent=Path(temporary).resolve()
        source=parent/'input.json'
        value=json.loads(execute([str(binaries['platform-installation']),'compose-input','role-qualification','sha256:'+'a'*64]).stdout)
        value['network']['topology']='native'
        value['network']['database']['host']='127.0.0.1'
        for index,process in enumerate(value['network']['processes']):
            port=20000+index*2
            process['observability_address']=f'127.0.0.1:{port}'
            if process['listen_address']:
                process['listen_address']=f'127.0.0.1:{port+1}'
                scheme=process['service_origin'].split(':')[0]
                process['service_origin']=f'{scheme}://localhost:{port+1}'
        password=secrets.token_hex(16)
        name='insight-installation-role-'+uuid.uuid4().hex[:12]
        container=execute(['docker','run','--detach','--rm','--name',name,'--label','insight.platform/qualification=installation-database','--publish','127.0.0.1::5432','--env','POSTGRES_USER=insight_installation_admin','--env','POSTGRES_PASSWORD='+password,'--env','POSTGRES_DB=insight_platform',POSTGRES]).stdout.strip()
        try:
            for _ in range(120):
                ready=execute(['docker','exec',container,'pg_isready','--host','127.0.0.1','-U','insight_installation_admin','-d','insight_platform'],success=False)
                if ready.returncode==0:break
                time.sleep(.25)
            else:raise RuntimeError('new PostgreSQL authority did not become ready')
            ports=json.loads(execute(['docker','inspect',container,'--format','{{json .NetworkSettings.Ports}}']).stdout)['5432/tcp']
            if len(ports)!=1 or ports[0]['HostIp']!='127.0.0.1':raise RuntimeError('fixture is not exact loopback')
            port=int(ports[0]['HostPort'])
            value['network']['database']['port']=port
            source.write_text(json.dumps(value));source.chmod(0o600)
            root=parent/'state'
            prepared=json.loads(execute([str(binaries['platform-installation']),'prepare','--input',str(source),'--state',str(root)]).stdout)
            authority=f'postgresql://insight_installation_admin:{password}@127.0.0.1:{port}/insight_platform'
            environment=dict(os.environ,PLATFORM_DATABASE_URL=authority,PLATFORM_DATABASE_ROLE_ADMIN_URL=authority,PLATFORM_INSTALLATION_INPUT_DIGEST=prepared['input_digest'],PLATFORM_INSTALLATION_IDENTITY_DIGEST=prepared['identity_digest'])
            execute([str(binaries['platform-schema']),'provision'],environment=environment)
            def role(purpose,mode='create',success=True):
                return execute([str(binaries['platform-database-role']),'--installation',str(root/'input.json'),mode,'--purpose',purpose,str(root),str(root/(purpose+'-evidence.json'))],environment=environment,success=success)
            def sql(statement,purpose=None,success=True):
                username='insight_'+purpose.replace('-','_')+'_dev' if purpose else 'insight_installation_admin'
                credential=(root/(purpose+'-password')).read_text() if purpose else password
                return execute(['docker','exec','--env','PGPASSWORD='+credential,container,'psql','--no-psqlrc','--host','127.0.0.1','--username',username,'--dbname','insight_platform','--set','ON_ERROR_STOP=1','--tuples-only','--no-align','--command',statement],success=success)
            for purpose in PURPOSES:
                role(purpose)
                before=(root/(purpose+'-evidence.json')).read_bytes()
                role(purpose,'verify')
                role(purpose) # Existing evidence makes this an exact read-only verification.
                assert (root/(purpose+'-evidence.json')).read_bytes()==before
            sql('SELECT * FROM insight_platform.runs LIMIT 0','runtime')
            sql('UPDATE insight_platform.resources SET version=version WHERE false','runtime')
            for statement in ['CREATE TABLE insight_platform.forbidden(id integer)','CREATE TEMPORARY TABLE forbidden(id integer)','CREATE SCHEMA forbidden','DELETE FROM insight_platform.runs WHERE false']:
                purpose='history' if statement.startswith('DELETE') else 'runtime'
                result=sql(statement,purpose,success=False)
                if result.returncode==0:raise RuntimeError('runtime DDL/restricted role boundary accepted')
            original=(root/'runtime-password').read_text()
            (root/'runtime-password').write_text(secrets.token_hex(16))
            rejected=role('runtime','verify',False)
            if rejected.returncode==0 or original in rejected.stderr or 'postgresql://' in rejected.stderr:raise RuntimeError('credential drift was accepted or disclosed')
            (root/'runtime-password').write_text(original)
            sql('GRANT DELETE ON insight_platform.events TO insight_outbox_dev')
            for mode in ['verify','create']:
                if role('outbox',mode,False).returncode==0:raise RuntimeError('privilege drift was repaired or accepted')
            sql('DELETE FROM insight_platform.events WHERE false','outbox') # Verify did not repair.
            sql('REVOKE DELETE ON insight_platform.events FROM insight_outbox_dev')
            role('outbox','verify')
            sql('GRANT insight_history_dev TO insight_runtime_dev')
            if role('runtime','verify',False).returncode==0:raise RuntimeError('unexpected membership was accepted')
            sql('REVOKE insight_history_dev FROM insight_runtime_dev')
            role('runtime','verify')
            print('Installation database qualification passed: eight non-owner credential roles, current-schema verification, DML success, DDL/TEMP denial, password/membership/ACL drift rejection, no repair on restart')
        finally:
            execute(['docker','rm','--force',container],success=False)

if __name__=='__main__':main()
