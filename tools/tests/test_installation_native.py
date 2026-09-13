"""Behavioral coverage of the native deployment adapter; no provider qualification is implied."""
import hashlib
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from types import SimpleNamespace

TOOLS = Path(__file__).resolve().parents[1] / "install"
sys.path.insert(0, str(TOOLS))
import native_runtime as runtime
import platform_native as native


class NativeRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="insight-native-test-")
        self.root = Path(self.temporary.name).resolve()
        self.root.chmod(0o700)
        self.group = runtime.ProcessGroup(self.root / "logs", log_bound=4096)

    def tearDown(self):
        if self.group is not None:
            self.group.close(grace=0.1, kill_grace=2)
        self.temporary.cleanup()

    def python(self, source, **options):
        return self.group.start([sys.executable, "-c", source], runtime.clean_environment(self.root), **options)

    def test_required_zero_exit_interrupts_in_progress_readiness_and_reaps_other_children(self):
        running = self.python("import time; time.sleep(60)", required=True)
        self.python("import time; time.sleep(.05)", required=True)
        started = time.monotonic()
        with self.assertRaisesRegex(runtime.NativeFailure, "required-process-exited"):
            self.group.run([sys.executable, "-c", "import time; time.sleep(60)"],
                           runtime.clean_environment(self.root), timeout=5)
        self.assertLess(time.monotonic() - started, 2)
        children = list(self.group.children)
        self.group.close(grace=0.1, kill_grace=2)
        self.group = None
        self.assertIsNotNone(running.process.returncode)
        self.assertTrue(all(child.process.returncode is not None for child in children))

    def test_partial_spawn_failure_cleans_only_owned_live_handles(self):
        foreign = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            child = self.python("import time; time.sleep(60)", required=True)
            with self.assertRaises(OSError):
                self.group.start([str(self.root / "missing")], runtime.clean_environment(self.root), required=True)
            self.group.close(grace=0.1, kill_grace=2)
            self.group = None
            self.assertIsNotNone(child.process.returncode)
            self.assertIsNone(foreign.poll())
        finally:
            foreign.terminate()
            foreign.wait(timeout=3)

    def test_cleanup_continues_after_pipe_error_and_reports_any_unreaped_handle(self):
        first = self.python("import time; time.sleep(60)", required=True)
        second = self.python("import time; time.sleep(60)", required=True)
        with patch.object(self.group, "pump", side_effect=OSError("synthetic closed pipe")):
            with self.assertRaisesRegex(runtime.NativeFailure, "cleanup-io-failure"):
                self.group.close(grace=0.05, kill_grace=0.1)
        self.assertIsNotNone(first.process.returncode)
        self.assertIsNotNone(second.process.returncode)
        self.group = None

    def test_oneshot_timeout_and_output_bound_do_not_leave_children_running(self):
        for script, capture, reason in (
            ("import time; time.sleep(60)", False, "owner-command-timeout"),
            ("import os; os.write(1,b'x'*2097152)", True, "owner-output-exceeds-bound"),
        ):
            with self.assertRaisesRegex(runtime.NativeFailure, reason):
                self.group.run([sys.executable, "-c", script], runtime.clean_environment(self.root),
                               timeout=0.2 if not capture else 3, capture=capture)
        children = list(self.group.children)
        self.group.close(grace=0.1, kill_grace=2)
        self.group = None
        self.assertTrue(all(child.process.returncode is not None for child in children))

    def test_log_flood_is_drained_without_unbounded_disk_and_signal_cleanup_escalates(self):
        script = ("import os,signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                  "os.write(1,b'x'*2097152); os.write(2,b'y'*2097152); time.sleep(60)")
        child = self.python(script, required=True)
        until = time.monotonic() + 3
        while child.logged < 4096 and time.monotonic() < until:
            self.group.pump(0.01)
        for _ in range(60):
            self.group.pump(0.001)
        with runtime.signals(self.group):
            os.kill(os.getpid(), signal.SIGTERM)
            with self.assertRaisesRegex(runtime.NativeFailure, "interrupted"):
                self.group.pump()
        started = time.monotonic()
        self.group.close(grace=0.1, kill_grace=2)
        self.group = None
        self.assertLess(time.monotonic() - started, 2)
        self.assertEqual(child.process.returncode, -signal.SIGKILL)
        logs = list((self.root / "logs").glob("*.log"))
        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0].stat().st_size, 4096)
        self.assertEqual(logs[0].stat().st_mode & 0o777, 0o600)

    def test_supervisor_excludes_second_up_but_allows_separate_session_lock(self):
        with runtime.lock(self.root, ".supervisor-lock"):
            with self.assertRaisesRegex(runtime.NativeFailure, "already-active"):
                with runtime.lock(self.root, ".supervisor-lock"):
                    self.fail("second supervisor acquired lock")
            with runtime.lock(self.root, ".installation-lock"):
                runtime.freeze(self.root, "session-envelope.json", b'{"session_file":"private"}')
        self.assertTrue((self.root / "session-envelope.json").exists())

    def test_frozen_declaration_rejects_drift_hardlinks_and_symlink_substitution(self):
        with runtime.lock(self.root, ".installation-lock"):
            path = runtime.freeze(self.root, "input.json", b'{"schema_version":1}')
            runtime.freeze(self.root, "input.json", path.read_bytes())
            with self.assertRaisesRegex(runtime.NativeFailure, "drift"):
                runtime.freeze(self.root, "input.json", b'{"schema_version":2}')
        original = path.read_bytes()
        os.link(path, self.root / "hardlink")
        with self.assertRaisesRegex(runtime.NativeFailure, "unsafe-file"):
            runtime.read_file(path, 1024, private=True)
        (self.root / "hardlink").unlink()
        (self.root / "link").symlink_to(path)
        with self.assertRaisesRegex(runtime.NativeFailure, "unsafe-file"):
            runtime.read_file(self.root / "link", 1024, private=True)
        self.assertEqual(path.read_bytes(), original)
        with self.assertRaisesRegex(runtime.NativeFailure, "duplicate-json"):
            runtime.decode_json(b'{"schema_version":1,"schema_version":2}')

    def test_artifact_bytes_and_read_during_mutation_are_rechecked(self):
        path = self.root / "artifact"
        path.write_bytes(b"original")
        path.chmod(0o600)
        artifact = {"path": str(path), "bytes_digest": "sha256:" + hashlib.sha256(b"original").hexdigest(), "executable": False}
        runtime.verify_artifact(artifact)
        path.write_bytes(b"modified")
        with self.assertRaisesRegex(runtime.NativeFailure, "artifact-drift"):
            runtime.verify_artifact(artifact)
        with self.assertRaisesRegex(runtime.NativeFailure, "file-changed"):
            with runtime.checked_file(path, 1024):
                path.write_bytes(b"changed again")

    def test_explicit_role_environment_does_not_inherit_credentials_or_runtime_injection(self):
        env_file = self.root / "environment"
        env_file.write_text("PLATFORM_FIXTURE='literal $(command) `text`'\nAWS_EC2_METADATA_DISABLED='true'\n")
        env_file.chmod(0o600)
        with patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "ambient", "DATABASE_URL": "ambient",
                                     "NODE_OPTIONS": "--inspect", "HTTPS_PROXY": "https://proxy.invalid",
                                     "LD_PRELOAD": "/foreign", "HOME": "/foreign"}):
            environment = runtime.process_environment(env_file, self.root)
        self.assertEqual(environment["HOME"], str(self.root))
        self.assertEqual(environment["PLATFORM_FIXTURE"], "literal $(command) `text`")
        for name in ("AWS_ACCESS_KEY_ID", "DATABASE_URL", "NODE_OPTIONS", "HTTPS_PROXY", "LD_PRELOAD"):
            self.assertNotIn(name, environment)
        env_file.write_text("NODE_OPTIONS='--inspect'\n")
        with self.assertRaisesRegex(runtime.NativeFailure, "invalid-role-environment"):
            runtime.process_environment(env_file, self.root)

    def test_port_preflight_refuses_an_unowned_listener_without_signalling_it(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            port = listener.getsockname()[1]
            document = {"network": {"processes": [], "console_origin": f"http://127.0.0.1:{port}",
                "database": {"port": 18000}, "nats_port": 18001,
                "providers": {"artifact": "https://localhost:18002", "openbao": "https://localhost:18003"}}}
            with self.assertRaisesRegex(runtime.NativeFailure, "already-in-use"):
                native.probe_ports(document, {})
            self.assertEqual(listener.getsockname()[1], port)

    def test_bootstrap_owner_rejects_scripts_before_execution(self):
        executable = self.root / "platform-installation"
        executable.write_text("#!/bin/sh\ntouch should-never-exist\n")
        executable.chmod(0o700)
        with self.assertRaisesRegex(runtime.NativeFailure, "unsupported-host-binary"):
            native.bootstrap_executable(executable)

    def test_session_envelope_never_prints_extra_credentials_or_invalid_expiry(self):
        arguments = SimpleNamespace(directory=self.root, binaries=self.root)
        installation = native.NativeInstallation(arguments, self.group)
        installation.plan = {"input_digest": "sha256:" + "a" * 64}
        installation.document = {"network": {"console_origin": "http://127.0.0.1:28104"}}
        installation.state.mkdir(mode=0o700)
        token = installation.state / "session-token"
        token.write_bytes(b"a.b.c\n")
        token.chmod(0o600)
        original = {"schema_version": 1, "input_digest": installation.plan["input_digest"],
            "identity_digest": "sha256:" + "b" * 64, "tenant_id": "ten_12345678-1234-7123-8123-123456789012",
            "endpoint": "http://127.0.0.1:28104", "session_file": str(token),
            "expires_at_unix_seconds": int(time.time()) + 800}
        for changes in ({"token": "must-never-print"}, {"schema_version": True},
                        {"expires_at_unix_seconds": 0}, {"expires_at_unix_seconds": int(time.time()) + 10000},
                        {"tenant_id": "foreign"}, {"identity_digest": "invalid"}):
            value = dict(original, **changes)
            output = io.StringIO()
            with patch.object(installation, "owner_command", return_value=json.dumps(value).encode()), redirect_stdout(output):
                with self.assertRaisesRegex(runtime.NativeFailure, "invalid-session-delivery"):
                    installation.deliver_session()
            self.assertEqual(output.getvalue(), "")
        output = io.StringIO()
        with patch.object(installation, "owner_command", return_value=json.dumps(original).encode()), redirect_stdout(output):
            installation.deliver_session()
        self.assertEqual(json.loads(output.getvalue()), original)
        self.assertNotIn("a.b.c", output.getvalue())

    def test_serving_commands_take_remaining_deadline_and_reject_later_mutation(self):
        installation = native.NativeInstallation(SimpleNamespace(directory=self.root, binaries=self.root), self.group)
        with patch.object(native.time, "monotonic", return_value=100):
            installation.serving_deadline = 102
            self.assertEqual(installation.budget(150), 2)
            self.assertEqual(installation.budget(1), 1)
        with patch.object(native.time, "monotonic", return_value=103):
            with self.assertRaisesRegex(runtime.NativeFailure, "serving-startup-timeout"):
                installation.docker(["docker", "stop", "should-not-run"])
        self.assertEqual(len(self.group.children), 0)

    def test_roles_wait_for_actual_dependency_readiness_before_next_spawn(self):
        installation = native.NativeInstallation(SimpleNamespace(directory=self.root, binaries=self.root), self.group)
        installation.output.mkdir(mode=0o700)
        first_ready, second_ready = self.root / "first-ready", self.root / "second-ready"
        roles = []
        for name, body in (
            ("first", f"time.sleep(.1); pathlib.Path({str(first_ready)!r}).write_text('ready')"),
            ("second", f"assert pathlib.Path({str(first_ready)!r}).exists(); pathlib.Path({str(second_ready)!r}).write_text('ready')"),
        ):
            executable = self.root / name
            executable.write_text(f"#!{sys.executable}\nimport time,pathlib\n{body}\ntime.sleep(5)\n")
            executable.chmod(0o700)
            environment = self.root / (name + ".env")
            environment.write_text("PLATFORM_TEST=value\n")
            environment.chmod(0o600)
            roles.append({"process": name, "executable_file": str(executable),
                "environment_file": str(environment), "temporary_directory": str(installation.output / "temporary" / name)})
        console = self.root / "console.py"
        console.write_text("import time; time.sleep(5)\n")
        installation.plan = {"processes": roles, "artifacts": [], "console": {
            "node_file": sys.executable, "entrypoint_file": str(console),
            "configuration_file": str(self.root / "unused.json"), "bundle_directory": str(self.root)}}
        installation.serving_deadline = time.monotonic() + 3
        observations = []
        def ready(operation, *, process=None):
            self.assertEqual(operation, "ready")
            observations.append(process)
            if process is not None:
                marker = first_ready if process == "first" else second_ready
                script = (f"import pathlib,time; p=pathlib.Path({str(marker)!r})\n"
                          "while not p.exists(): time.sleep(.01)\n")
                self.group.run([sys.executable, "-c", script], runtime.clean_environment(self.root),
                               timeout=installation.budget(120))
                if process == "first":
                    self.assertFalse(second_ready.exists())
        with patch.object(installation, "artifact"), patch.object(installation, "owner_command", side_effect=ready):
            installation.start_roles()
        self.assertTrue(second_ready.exists())
        self.assertEqual(observations, ["first", "second", None])
        self.assertEqual(len([child for child in self.group.children if child.required]), 3)

    def test_serving_deadline_prevents_new_process_and_clips_owner_probe(self):
        installation = native.NativeInstallation(SimpleNamespace(directory=self.root, binaries=self.root), self.group)
        installation.plan = {"artifacts": []}
        installation.serving_deadline = time.monotonic() + .15
        with patch.object(installation, "artifact"), patch.object(self.group, "run", return_value=b"ready") as command:
            installation.owner_command("ready", process="egress-broker")
            self.assertLessEqual(command.call_args.kwargs["timeout"], .15)
            self.assertEqual(command.call_args.args[0][-2:], ["--process", "egress-broker"])
        installation.serving_deadline = time.monotonic() - 1
        with patch.object(installation, "artifact"):
            with self.assertRaisesRegex(runtime.NativeFailure, "serving-startup-timeout"):
                installation.owner_command("ready", process="egress-broker")
        self.assertEqual(self.group.children, [])

    def test_fresh_preparation_creates_owned_paths_but_missing_existing_data_is_not_recreated(self):
        installation = native.NativeInstallation(SimpleNamespace(directory=self.root, binaries=self.root), self.group)
        directories = [installation.output, installation.output / "dependencies", installation.output / "roles",
                       installation.output / "postgres-data", installation.output / "nats-data",
                       installation.output / "s3-data", installation.output / "openbao-data"]
        installation.plan = {"preparation_directories": [str(path) for path in directories]}
        installation.prepare_directories()
        for directory in directories:
            self.assertTrue(directory.is_dir())
            self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
        installation.state.mkdir(mode=0o700)
        lost = installation.output / "openbao-data"
        lost.rmdir()
        with self.assertRaises(FileNotFoundError):
            installation.prepare_directories()
        self.assertFalse(lost.exists())

    def test_input_is_staged_before_owner_and_node_failure_precedes_frozen_state(self):
        source = self.root / "declaration.json"
        source.write_bytes(b'{"schema_version":2}')
        source.chmod(0o600)
        arguments = SimpleNamespace(directory=self.root, binaries=self.root, input=source,
                                    console_directory=self.root, node=self.root / "node")
        installation = native.NativeInstallation(arguments, self.group)
        owner_calls = []
        def owner(arguments, _environment, **_options):
            owner_calls.append(arguments)
            if "native-plan" in arguments:
                staged = Path(arguments[arguments.index("--input") + 1])
                self.assertNotEqual(staged, source)
                self.assertEqual(staged.read_bytes(), source.read_bytes())
                self.assertEqual(staged.stat().st_mode & 0o777, 0o600)
                # A caller replacing its own source cannot change what the owner reads.
                source.write_bytes(b'{"schema_version":99}')
                self.assertEqual(staged.read_bytes(), b'{"schema_version":2}')
                return json.dumps({"artifacts": [], "console": {"node_file": str(arguments[-1])}}).encode()
            return b"v22.0.0\n"
        with patch.object(native, "bootstrap_executable"), patch.object(installation, "artifact"), patch.object(self.group, "run", side_effect=owner):
            with self.assertRaisesRegex(runtime.NativeFailure, "unsupported-node-version"):
                installation.preflight()
        self.assertEqual(len(owner_calls), 2)
        self.assertFalse((self.root / "private").exists())
        self.assertFalse((self.root / "native-plan.json").exists())
        self.assertFalse(list(self.root.glob(".preflight-*")))

    def test_repeated_termination_keeps_supervisor_lock_until_children_are_reaped(self):
        fixture = self.root / "supervisor"
        fixture.mkdir(mode=0o700)
        child_ready = fixture / "child-ready"
        ready = fixture / "ready"
        child_script = ("import signal,time,pathlib; signal.signal(signal.SIGTERM,signal.SIG_IGN); "
                        f"pathlib.Path({str(child_ready)!r}).write_text('ready'); time.sleep(3)")
        script = f'''
import sys,pathlib
sys.path.insert(0,{str(TOOLS)!r})
import platform_native as native
import native_runtime as runtime
class Group(runtime.ProcessGroup):
    def close(self, **options):
        return super().close(grace=.5,kill_grace=1)
class Installation(native.NativeInstallation):
    def preflight(self): pass
    def start(self):
        child=self.group.start([sys.executable,'-c',{child_script!r}], self.environment, required=True)
        while not pathlib.Path({str(child_ready)!r}).exists(): self.group.pump(.01)
        pathlib.Path({str(ready)!r}).write_text(str(child.process.pid))
        while True: self.group.pump(.01)
native.ProcessGroup=Group
native.NativeInstallation=Installation
sys.argv=['native','up','--input',{str(fixture / 'input.json')!r},'--directory',{str(fixture)!r},'--binaries',{str(fixture)!r},'--console-directory',{str(fixture)!r},'--node',{str(fixture / 'node')!r}]
try: native.main()
except runtime.NativeFailure: pass
'''
        supervisor = subprocess.Popen([sys.executable, "-c", script], stdin=subprocess.DEVNULL,
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            deadline = time.monotonic() + 3
            while not ready.exists() and time.monotonic() < deadline and supervisor.poll() is None:
                time.sleep(.01)
            self.assertTrue(ready.exists())
            child_pid = int(ready.read_text())
            supervisor.terminate()
            time.sleep(.05)
            supervisor.terminate()
            with self.assertRaisesRegex(runtime.NativeFailure, "already-active"):
                with runtime.lock(fixture, ".supervisor-lock"):
                    self.fail("supervisor unlocked before cleanup")
            self.assertEqual(supervisor.wait(timeout=3), 0)
            with self.assertRaises(ProcessLookupError):
                os.kill(child_pid, 0)
            with runtime.lock(fixture, ".supervisor-lock"):
                pass
        finally:
            if supervisor.poll() is None:
                supervisor.kill()
            supervisor.wait(timeout=3)


if __name__ == "__main__":
    unittest.main()
