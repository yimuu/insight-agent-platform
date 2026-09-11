#!/usr/bin/env python3
"""Qualify pinned LocalStack custom TLS with isolated canary resources and actual AWS SDK calls."""
import importlib.util
import json
import os
import signal
import sys
from pathlib import Path
import socket
import ssl
import subprocess
import tempfile
import time

repo = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('fixture', repo / 'tools/qualification/qualify-platform-installation-aws.py')
owner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(owner)
def main():
    fixture = owner.IsolatedAwsFixture()
    host = 'localstack.tls-qualification.svc.cluster.local'
    with tempfile.TemporaryDirectory(prefix='insight-custom-tls-') as temporary:
        root = Path(temporary).resolve()
        producer_environment = owner.fixture_environment()
        producer_environment['INSIGHT_TLS_FIXTURE_DIRECTORY'] = str(root)
        with tempfile.TemporaryFile() as producer_log:
            result = subprocess.run(['cargo','test','--locked','-p','insight-platform-deployment-tooling','--test','tls_fixture_material','export_shared_producer_tls_fixture_material','--','--ignored','--exact'],env=producer_environment,cwd=repo,stdin=subprocess.DEVNULL,stdout=producer_log,stderr=subprocess.STDOUT,timeout=600)
        if result.returncode != 0 or sorted(path.name for path in root.iterdir()) != ['ca.pem','server.crt','server.key','wrong-ca.pem']:
            raise owner.QualificationError('shared_tls_producer_failed')
        combined = root/'server.pem'
        combined.write_bytes((root/'server.key').read_bytes()+(root/'server.crt').read_bytes())
        os.chmod(combined,0o600)
        try:
            fixture.container_id=owner.command(['docker','create','--name',fixture.name,'--label',owner.LABEL+'='+fixture.nonce,'--publish','127.0.0.1::4566','--memory','1g','--cpus','2','--pids-limit','256','--tmpfs','/var/lib/localstack:rw,nosuid,nodev,size=268435456','--tmpfs','/tmp:rw,nosuid,nodev,size=134217728','--mount','type=bind,source='+str(combined)+',target=/run/insight-localstack/server.pem,readonly','--mount','type=bind,source='+str(root/'ca.pem')+',target=/run/insight-localstack/ca.pem,readonly','--mount','type=bind,source='+str(root/'wrong-ca.pem')+',target=/run/insight-localstack/wrong-ca.pem,readonly','--env','SERVICES=s3,kms,secretsmanager','--env','EAGER_SERVICE_LOADING=1','--env','CUSTOM_SSL_CERT_PATH=/run/insight-localstack/server.pem','--env','SKIP_SSL_CERT_DOWNLOAD=1',fixture.image])
            owner.command(['docker','start',fixture.container_id])
            info=json.loads(owner.command(['docker','inspect',fixture.container_id]))[0]
            binding=info['NetworkSettings']['Ports']['4566/tcp'][0]
            assert binding['HostIp']=='127.0.0.1'
            port=int(binding['HostPort'])
            context=ssl.create_default_context(cafile=str(root/'ca.pem'))
            def request(ctx,server_name):
                with socket.create_connection(('127.0.0.1',port),timeout=2) as tcp:
                    with ctx.wrap_socket(tcp,server_hostname=server_name) as tls:
                        tls.sendall(('GET /_localstack/health HTTP/1.1\r\nHost: '+host+'\r\nConnection: close\r\n\r\n').encode())
                        return tls.recv(16384)
            deadline=time.monotonic()+60
            while True:
                try:
                    if b'200' in request(context,host).split(b'\r\n')[0]: break
                except OSError: pass
                if time.monotonic()>deadline: raise RuntimeError('tls fixture unavailable')
                time.sleep(.25)
            for ctx,name in [(ssl.create_default_context(),host),(context,'wrong.tls-qualification.svc.cluster.local')]:
                try: request(ctx,name)
                except ssl.SSLCertVerificationError: pass
                else: raise RuntimeError('tls verification unexpectedly succeeded')
            linux_tls = owner.command(['docker','exec',fixture.container_id,'python3','-c','''import json,socket,ssl
assert ssl.OPENSSL_VERSION.startswith("OpenSSL 3.")
host = "localstack.tls-qualification.svc.cluster.local"
def handshake(ca, name):
    context = ssl.create_default_context(cafile=ca)
    with socket.create_connection(("127.0.0.1",4566),timeout=5) as tcp:
        with context.wrap_socket(tcp,server_hostname=name): pass
handshake("/run/insight-localstack/ca.pem",host)
for ca,name in [("/run/insight-localstack/wrong-ca.pem",host),("/run/insight-localstack/ca.pem","wrong.tls-qualification.svc.cluster.local")]:
    try: handshake(ca,name)
    except ssl.SSLCertVerificationError: pass
    else: raise RuntimeError("expected certificate rejection")
print(json.dumps({"linux_openssl":ssl.OPENSSL_VERSION,"positive":True,"wrong_ca_rejected":True,"wrong_san_rejected":True}))
'''])
            print('PASS shared producer TLS material; Linux '+linux_tls,flush=True)
            def aws(*args):
                return owner.command(['docker','exec','--env','AWS_ACCESS_KEY_ID=test','--env','AWS_SECRET_ACCESS_KEY=test',fixture.container_id,'awslocal','--endpoint-url','https://localhost.localstack.cloud:4566','--ca-bundle','/run/insight-localstack/ca.pem',*args])
            aws('s3api','create-bucket','--bucket',fixture.bucket)
            aws('s3api','put-bucket-versioning','--bucket',fixture.bucket,'--versioning-configuration','{"Status":"Enabled"}')
            fixture.key_arn=aws('kms','create-key','--query','KeyMetadata.Arn','--output','text')
            fixture.endpoint='https://localhost.localstack.cloud:'+str(port)
            environment=fixture.environment()
            environment['SSL_CERT_FILE']=str(root/'ca.pem')
            environment['SSL_CERT_DIR']='/etc/ssl/certs'
            for negative, overrides in [('unknown-ca', {'SSL_CERT_FILE': str(root/'wrong-ca.pem')}), ('wrong-san', {'PLATFORM_TEST_AWS_ENDPOINT': 'https://127.0.0.1:'+str(port)})]:
                negative_env = dict(environment, **overrides)
                with tempfile.TemporaryFile() as log:
                    result = subprocess.run(['cargo','test','--locked','-p','insight-platform-artifact-broker','--lib',owner.TEST,'--','--ignored','--exact','--nocapture'],env=negative_env,cwd=repo,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,timeout=600)
                    log.seek(0)
                    evidence = log.read(1048576).decode(errors='replace')
                if result.returncode == 0 or 'StorageUnavailable' not in evidence:
                    raise owner.QualificationError('sdk_tls_rejection_evidence_missing')
                print('PASS actual SDK TLS rejection '+negative,flush=True)
            with tempfile.NamedTemporaryFile(prefix='insight-custom-tls-sdk-', suffix='.log', mode='w') as log:
                result=subprocess.run(['cargo','test','--locked','-p','insight-platform-artifact-broker','--lib',owner.TEST,'--','--ignored','--exact','--nocapture'],env=environment,cwd=repo,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,timeout=600)
                log.flush()
                text=Path(log.name).read_text()
            assert result.returncode==0 and 'test result: ok. 1 passed; 0 failed; 0 ignored;' in text
            print('PASS actual S3+KMS SDK with SSL_CERT_FILE and SSL_CERT_DIR, exact owning Artifact test',flush=True)
        finally:
            fixture.close()
            print('own fixture removed',flush=True)

if __name__ == '__main__':
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    try:
        main()
    except KeyboardInterrupt:
        print('tls_qualification_interrupted', file=sys.stderr)
        raise SystemExit(130) from None
    except (owner.QualificationError, OSError, RuntimeError, AssertionError, subprocess.SubprocessError):
        print('tls_qualification_failed', file=sys.stderr)
        raise SystemExit(1) from None
