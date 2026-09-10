"""Validate bounded sandbox-only evidence; this never qualifies browser NSS, TLS or S3."""
import json
import re

BASE = 'sha256:eff16c30e6f3f4af0a03fa4b706120d5e9b0891c344a27d64559aff5900a4a27'
CHECK = 'chromium_namespace_and_seccomp_sandbox'
CODES = {'input_bound', 'input_invalid', 'ca_invalid', 'browser_binary_invalid', 'browser_version_invalid', 'sandbox_unavailable',
         'cdp_timeout', 'cdp_rejected', 'cdp_identity_invalid', 'cdp_unavailable', 'browser_evaluation_failed', 'fixture_unavailable'}
CATEGORIES = {'permission', 'namespace', 'crashpad', 'resource', 'invalid', 'missing', 'zygote_pid', 'zygote_message'}
ERRNOS = {'Invalid argument', 'Operation not permitted', 'Permission denied', 'Function not implemented', 'No such file or directory'}


class EvidenceError(ValueError):
    def __init__(self):
        super().__init__('invalid sandbox evidence')


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise EvidenceError()
        result[key] = value
    return result


def _fail():
    raise EvidenceError()


def _digest(value):
    return isinstance(value, str) and re.fullmatch(r'sha256:[a-f0-9]{64}', value) is not None


def read_report(raw):
    try:
        if not isinstance(raw, bytes) or len(raw) > 8192:
            _fail()
        report = json.loads(raw, object_pairs_hook=_object, parse_constant=lambda _: _fail())
        if not isinstance(report, dict) or set(report) != {'schema_version', 'scope', 'nonce', 'passed', 'cleanup', 'image', 'architecture', 'base', 'browser'}:
            _fail()
        if (type(report['schema_version']) is not int or report['schema_version'] != 1
                or report['scope'] != 'linux_chromium_nss_sandbox_availability'
                or not isinstance(report['nonce'], str) or not re.fullmatch('[a-f0-9]{32}', report['nonce'])
                or not _digest(report['image']) or report['base'] != BASE or report['architecture'] not in {'arm64', 'amd64'}
                or type(report['passed']) is not bool or type(report['cleanup']) is not bool):
            _fail()
        browser = report['browser']
        required = {'schema_version', 'passed', 'checks', 'binary_version'}
        allowed = required | {'browser_version', 'failure', 'phase', 'sandbox_diagnostic', 'startup'}
        if not isinstance(browser, dict) or not required.issubset(browser) or set(browser) - allowed:
            _fail()
        if (type(browser['schema_version']) is not int or browser['schema_version'] != 1
                or type(browser['passed']) is not bool or browser['binary_version'] != 'Google Chrome for Testing 153.0.8010.12'
                or browser['checks'] not in ([], [CHECK])):
            _fail()
        if 'browser_version' in browser and browser['browser_version'] not in {'Chrome/153.0.8010.12', 'HeadlessChrome/153.0.8010.12'}:
            _fail()
        if browser['passed']:
            if set(browser) != required | {'browser_version'} or browser['checks'] != [CHECK]:
                _fail()
        else:
            if browser.get('failure') not in CODES or browser.get('phase') not in {'input', 'sandbox'}:
                _fail()
            if 'sandbox_diagnostic' in browser and browser['sandbox_diagnostic'] not in {'unknown', 'kernel_namespace_denied'}:
                _fail()
        if 'startup' in browser:
            startup = browser['startup']
            if not isinstance(startup, dict) or set(startup) != {'exit_code', 'signal', 'stderr_bytes', 'source_location', 'errno_class', 'categories', 'user_network_namespace'}:
                _fail()
            if (startup['exit_code'] is not None and (type(startup['exit_code']) is not int or not 0 <= startup['exit_code'] <= 255)
                    or startup['signal'] not in {None, 'SIGABRT', 'SIGTRAP', 'SIGKILL', 'SIGTERM', 'SIGSEGV', 'SIGSYS'}
                    or type(startup['stderr_bytes']) is not int or not 0 <= startup['stderr_bytes'] <= 8192
                    or startup['source_location'] not in {None, 'content/browser/zygote_host/zygote_host_impl_linux.cc:237'}
                    or startup['errno_class'] is not None and startup['errno_class'] not in ERRNOS
                    or not isinstance(startup['categories'], list) or len(startup['categories']) > len(CATEGORIES)
                    or any(not isinstance(item, str) or item not in CATEGORIES for item in startup['categories'])
                    or len(set(startup['categories'])) != len(startup['categories'])
                    or type(startup['user_network_namespace']) is not bool):
                _fail()
        if report['passed'] != (browser['passed'] and report['cleanup']):
            _fail()
        return report
    except (TypeError, KeyError, UnicodeError, json.JSONDecodeError):
        raise EvidenceError() from None
