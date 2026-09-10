import copy
import importlib.util
import json
from pathlib import Path
import unittest

SOURCE = Path(__file__).resolve().parents[1] / 'qualification/browser_sandbox_evidence.py'
SPEC = importlib.util.spec_from_file_location('browser_sandbox_evidence', SOURCE)
EVIDENCE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EVIDENCE)


def fixture():
    return {'schema_version': 1, 'scope': 'linux_chromium_nss_sandbox_availability', 'nonce': 'a' * 32,
            'passed': False, 'cleanup': True, 'image': 'sha256:' + 'b' * 64, 'architecture': 'arm64', 'base': EVIDENCE.BASE,
            'browser': {'schema_version': 1, 'passed': False, 'checks': [], 'binary_version': 'Google Chrome for Testing 153.0.8010.12',
                        'failure': 'sandbox_unavailable', 'phase': 'sandbox', 'sandbox_diagnostic': 'unknown',
                        'startup': {'exit_code': None, 'signal': 'SIGABRT', 'stderr_bytes': 1792,
                                    'source_location': 'content/browser/zygote_host/zygote_host_impl_linux.cc:237',
                                    'errno_class': None, 'categories': ['namespace', 'invalid'], 'user_network_namespace': True}}}


class BrowserSandboxEvidenceTests(unittest.TestCase):
    def test_failed_chromium_and_successful_namespace_probe_remain_failed(self):
        value = fixture()
        self.assertEqual(EVIDENCE.read_report(json.dumps(value).encode()), value)
        value['passed'] = True
        with self.assertRaises(EVIDENCE.EvidenceError):
            EVIDENCE.read_report(json.dumps(value).encode())

    def test_positive_requires_both_actual_sandbox_checks_and_cleanup(self):
        value = fixture()
        value['browser'] = {'schema_version': 1, 'passed': True, 'checks': [EVIDENCE.CHECK],
                            'binary_version': 'Google Chrome for Testing 153.0.8010.12', 'browser_version': 'Chrome/153.0.8010.12'}
        value['passed'] = True
        EVIDENCE.read_report(json.dumps(value).encode())
        value['cleanup'] = False
        with self.assertRaises(EVIDENCE.EvidenceError): EVIDENCE.read_report(json.dumps(value).encode())
        value['passed'] = False
        EVIDENCE.read_report(json.dumps(value).encode())

    def test_no_claim_transfers_to_nss_tls_or_s3(self):
        for check in ('tls_verified', 'nss_trusted', 's3_put_passed'):
            value = fixture()
            value['browser']['checks'] = [check]
            with self.assertRaises(EVIDENCE.EvidenceError): EVIDENCE.read_report(json.dumps(value).encode())

    def test_no_raw_errors_urls_or_unbounded_output_survive(self):
        for location in ('failure', 'sandbox_diagnostic'):
            value = fixture()
            value['browser'][location] = 'https://private.invalid/?credential=secret'
            with self.assertRaises(EVIDENCE.EvidenceError) as error: EVIDENCE.read_report(json.dumps(value).encode())
            self.assertEqual(str(error.exception), 'invalid sandbox evidence')
        value = fixture()
        value['browser']['startup']['raw_log'] = 'private'
        with self.assertRaises(EVIDENCE.EvidenceError): EVIDENCE.read_report(json.dumps(value).encode())
        with self.assertRaises(EVIDENCE.EvidenceError): EVIDENCE.read_report(b' ' * 8193)

    def test_duplicate_keys_nan_bool_schema_and_foreign_image_are_rejected(self):
        raw = json.dumps(fixture()).encode()
        for invalid in (raw[:-1] + b',"passed":false}', raw.replace(b'"schema_version": 1', b'"schema_version": NaN', 1),
                        raw.replace(b'"schema_version": 1', b'"schema_version": true', 1), raw.replace(EVIDENCE.BASE.encode(), b'foreign:latest')):
            with self.assertRaises(EVIDENCE.EvidenceError): EVIDENCE.read_report(invalid)

    def test_nested_diagnostics_are_typed_and_closed(self):
        for field, invalid in [('stderr_bytes', 8193), ('signal', 'private text'), ('source_location', '/private/key'),
                               ('categories', ['namespace', 'namespace']), ('user_network_namespace', 1)]:
            value = copy.deepcopy(fixture())
            value['browser']['startup'][field] = invalid
            with self.subTest(field=field), self.assertRaises(EVIDENCE.EvidenceError): EVIDENCE.read_report(json.dumps(value).encode())


if __name__ == '__main__': unittest.main()
