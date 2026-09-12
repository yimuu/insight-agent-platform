"""Native read-only trust delivery; container paths are verified by real Compose/Helm tests."""
import argparse
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock
from tools.tests import test_public_trust as certificate_fixture
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'tools/install'))
import public_trust as TRUST
import platform_native as NATIVE

class ConsumerTrustTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        certificate_fixture.PublicTrustTests.setUpClass()
        cls.pem=certificate_fixture.PublicTrustTests.pem
    @classmethod
    def tearDownClass(cls):
        certificate_fixture.PublicTrustTests.tearDownClass()
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory(); self.addCleanup(self.temporary.cleanup)
        self.directory=Path(self.temporary.name).resolve()
        self.input_digest='sha256:'+'a'*64; self.identity_digest='sha256:'+'b'*64
        self.encoded=json.dumps(dict(schema_version=1,input_digest=self.input_digest,identity_digest=self.identity_digest,
            certificate_pem=self.pem.decode(),certificate_sha256='sha256:'+TRUST.hashlib.sha256(self.pem).hexdigest())).encode()
    def remember(self):
        TRUST.remember_ready(self.directory,json.dumps(dict(schema_version=1,phase='ready',input_digest=self.input_digest,identity_digest=self.identity_digest)).encode(),input_digest=self.input_digest)

    def test_native_export_has_no_provider_environment_or_init_arguments(self):
        self.remember()
        args = argparse.Namespace(directory=self.directory, binaries=self.directory/'bin')
        group = mock.Mock()
        group.run.return_value = self.encoded
        installation = NATIVE.NativeInstallation(args, group)
        installation.plan = {'input_digest': self.input_digest}
        with mock.patch.object(installation, 'artifact'), mock.patch('builtins.print'):
            installation.deliver_public_trust()
        args, environment = group.run.call_args.args
        self.assertEqual(args, [str(self.directory/'bin/platform-installation'), 'public-trust', '--input', str(self.directory/'input.json'), '--state', str(self.directory/'private')])
        self.assertFalse(any(key.startswith('AWS_') or key.startswith('OPENBAO_') for key in environment))
        self.assertFalse((self.directory/'session-token').exists())
