#!/usr/bin/env python3
"""Create short-lived credentials for an isolated Outbox NATS fixture; never production material."""
from pathlib import Path
import argparse
import subprocess
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("directory", type=Path)
args = parser.parse_args()
root = args.directory.resolve()
root.mkdir(parents=True, exist_ok=True)
if any(root.iterdir()):
    raise SystemExit("fixture directory must be empty")
root.chmod(0o700)
repository = next(parent for parent in Path(__file__).resolve().parents if (parent / "Cargo.toml").is_file())
def run(args):
    subprocess.run(args,check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
ca=root/'ca.pem';cakey=root/'ca-key.pem'
if True:run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','2','-subj','/CN=Architecture Outbox fixture CA','-keyout',str(cakey),'-out',str(ca)])
for name,san,usage in [('server','DNS:localhost,IP:127.0.0.1','serverAuth'),('publisher','URI:spiffe://insight.platform/workload/outbox-worker','clientAuth'),('provision','URI:spiffe://insight.platform/workload/local-outbox-provisioner','clientAuth'),('live','URI:spiffe://insight.platform/workload/local-nats-client','clientAuth')]:
    key=root/f'{name}-key.pem';csr=root/f'{name}.csr';crt=root/f'{name}.pem';ext=root/f'{name}.ext'
    ext.write_text(f'basicConstraints=CA:FALSE\nkeyUsage=digitalSignature\nextendedKeyUsage={usage}\nsubjectAltName={san}\n')
    run(['openssl','req','-new','-newkey','rsa:2048','-nodes','-subj',f'/CN={name}','-keyout',str(key),'-out',str(csr)])
    run(['openssl','x509','-req','-in',str(csr),'-CA',str(ca),'-CAkey',str(cakey),'-CAcreateserial','-days','2','-extfile',str(ext),'-out',str(crt)])
(root/'nats.conf').write_bytes((repository / 'deploy/dev/nats.conf').read_bytes())
for p in root.glob('*-key.pem'):p.chmod(0o600)
print('Dedicated Outbox TLS fixture certificates and exact development NATS configuration prepared')
