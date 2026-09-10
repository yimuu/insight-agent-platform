"""Closed public-certificate delivery and exact private-file replay; no services."""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('public_trust', ROOT/'tools/install/public_trust.py')
TRUST = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(TRUST)


class PublicTrustTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.certificates = tempfile.TemporaryDirectory()
        root = Path(cls.certificates.name)
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                        '-keyout', str(root/'fixture-key.pem'), '-out', str(root/'fixture-ca.pem'),
                        '-days', '1', '-subj', '/CN=Installation Public Trust Test'],
                       stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                       check=True, timeout=15)
        cls.pem = (root/'fixture-ca.pem').read_bytes()

    @classmethod
    def tearDownClass(cls):
        cls.certificates.cleanup()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name).resolve()
        self.input_digest, self.identity_digest = 'sha256:'+'a'*64, 'sha256:'+'b'*64
        self.envelope = {'schema_version': 1, 'input_digest': self.input_digest,
                         'identity_digest': self.identity_digest, 'certificate_pem': self.pem.decode(),
                         'certificate_sha256': 'sha256:'+hashlib.sha256(self.pem).hexdigest()}

    def deliver(self, envelope=None):
        return TRUST.deliver(self.directory, json.dumps(envelope or self.envelope).encode(),
                             input_digest=self.input_digest, identity_digest=self.identity_digest)

    def test_exact_delivery_is_private_and_replay_does_not_rewrite(self):
        result = self.deliver()
        path = self.directory/'public-ca.pem'
        original = path.stat()
        self.assertEqual(path.read_bytes(), self.pem)
        self.assertEqual(stat.S_IMODE(original.st_mode), 0o600)
        self.assertEqual(original.st_nlink, 1)
        self.assertEqual(self.deliver(), result)
        self.assertEqual((path.stat().st_ino, path.stat().st_mtime_ns), (original.st_ino, original.st_mtime_ns))
        self.assertEqual(set(result), {'schema_version', 'input_digest', 'identity_digest', 'certificate_file', 'certificate_sha256'})
        self.assertNotIn('BEGIN CERTIFICATE', json.dumps(result))

    def test_identity_swap_unknown_private_fields_or_bad_schema_publish_nothing(self):
        for change in ({'identity_digest': 'sha256:'+'c'*64}, {'input_digest': 'sha256:'+'c'*64},
                       {'private_key': 'DO-NOT-OUTPUT'}, {'schema_version': True}, {'schema_version': 2},
                       {'certificate_sha256': 'sha256:'+'c'*64}):
            with self.subTest(change=list(change)):
                with self.assertRaises(TRUST.PublicTrustFailure) as error:
                    self.deliver(dict(self.envelope, **change))
                self.assertNotIn('DO-NOT-OUTPUT', str(error.exception))
                self.assertFalse((self.directory/'public-ca.pem').exists())

    def test_decoder_rejects_duplicate_keys_nan_oversized_and_trailing_documents(self):
        encoded = json.dumps(self.envelope).encode()
        for value in (encoded[:-1]+b',"schema_version":1}', encoded[:-1]+b',"private":NaN}',
                      b' '*TRUST.MAX_RESPONSE_BYTES+b'{}', encoded+b'{}', b'\xff', b'['*2000):
            with self.subTest(length=len(value)), self.assertRaises(TRUST.PublicTrustFailure):
                TRUST.certificate(value, input_digest=self.input_digest, identity_digest=self.identity_digest)

    def test_only_one_bounded_parseable_certificate_is_accepted(self):
        for pem in (self.pem*2, self.pem+b'private', b'-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n',
                    b'-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n',
                    b'-----BEGIN CERTIFICATE-----\n'+b'A'*16384+b'\n-----END CERTIFICATE-----\n'):
            changed = dict(self.envelope, certificate_pem=pem.decode(), certificate_sha256='sha256:'+hashlib.sha256(pem).hexdigest())
            with self.subTest(length=len(pem)), self.assertRaises(TRUST.PublicTrustFailure):
                self.deliver(changed)

    def test_foreign_mode_links_and_changed_bytes_are_never_replaced(self):
        for kind in ('bytes', 'mode', 'symlink', 'hardlink', 'directory'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory(dir=self.directory) as temporary:
                directory = Path(temporary)
                target = directory/'public-ca.pem'
                other = directory/'other'
                other.write_bytes(b'preserve')
                if kind == 'directory': target.mkdir()
                elif kind == 'symlink': target.symlink_to(other)
                elif kind == 'hardlink': os.link(other, target)
                else:
                    target.write_bytes(b'preserve' if kind == 'bytes' else self.pem)
                    target.chmod(0o644 if kind == 'mode' else 0o600)
                before = target.lstat()
                with self.assertRaises((TRUST.PublicTrustFailure, OSError)):
                    TRUST.deliver(directory, json.dumps(self.envelope).encode(), input_digest=self.input_digest, identity_digest=self.identity_digest)
                after = target.lstat()
                self.assertEqual((after.st_ino, after.st_mode, after.st_size), (before.st_ino, before.st_mode, before.st_size))
                self.assertEqual(other.read_bytes(), b'preserve')

    def test_symlink_ancestor_and_nonprivate_directory_are_rejected(self):
        actual = self.directory/'actual'
        actual.mkdir(mode=0o700)
        alias = self.directory/'alias'
        alias.symlink_to(actual, target_is_directory=True)
        for directory in (alias, self.directory):
            if directory == self.directory: directory.chmod(0o755)
            with self.subTest(directory=directory.name), self.assertRaises(TRUST.PublicTrustFailure):
                TRUST.deliver(directory, json.dumps(self.envelope).encode(), input_digest=self.input_digest, identity_digest=self.identity_digest)
        self.directory.chmod(0o700)

    def test_only_actual_ready_owner_envelope_can_anchor_a_later_export(self):
        proof = {'schema_version': 1, 'phase': 'ready', 'input_digest': self.input_digest, 'identity_digest': self.identity_digest}
        for changed in (dict(proof, phase='prepared'), dict(proof, schema_version=True), dict(proof, private_key='private'),
                        dict(proof, input_digest='sha256:'+'c'*64)):
            with self.assertRaises(TRUST.PublicTrustFailure):
                TRUST.remember_ready(self.directory, json.dumps(changed).encode(), input_digest=self.input_digest)
        with self.assertRaises(FileNotFoundError): TRUST.ready_identity(self.directory, input_digest=self.input_digest)
        TRUST.remember_ready(self.directory, json.dumps(proof).encode(), input_digest=self.input_digest)
        self.assertEqual(TRUST.ready_identity(self.directory, input_digest=self.input_digest), self.identity_digest)
        before = (self.directory/'ready-owner-proof.json').stat()
        TRUST.remember_ready(self.directory, json.dumps(proof, indent=2).encode(), input_digest=self.input_digest)
        self.assertEqual((self.directory/'ready-owner-proof.json').stat().st_mtime_ns, before.st_mtime_ns)
        with self.assertRaises(TRUST.PublicTrustFailure):
            TRUST.remember_ready(self.directory, json.dumps(dict(proof, identity_digest='sha256:'+'c'*64)).encode(), input_digest=self.input_digest)

    def test_publication_race_never_overwrites_a_foreign_destination(self):
        original_link = os.link
        def race(source, destination, **options):
            Path(destination).write_bytes(b'foreign-public-file')
            Path(destination).chmod(0o600)
            return original_link(source, destination, **options)
        with mock.patch.object(TRUST.os, 'link', side_effect=race), self.assertRaises(FileExistsError):
            self.deliver()
        self.assertEqual((self.directory/'public-ca.pem').read_bytes(), b'foreign-public-file')
        with self.assertRaises(TRUST.PublicTrustFailure): self.deliver()


if __name__ == '__main__':
    unittest.main()
