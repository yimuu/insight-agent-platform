"""One-shot provider start/recovery boundaries with callbacks only; no services or credentials."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("provider_lifecycle_under_test", ROOT / "tools/install/provider_lifecycle.py")
LIFE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = LIFE
SPEC.loader.exec_module(LIFE)
INPUT = "sha256:" + "a" * 64
IDENTITY = "sha256:" + "b" * 64
INIT_ID, SERVE_ID, FOREIGN_ID = "1" * 64, "2" * 64, "3" * 64


def envelope(field, value, identity=IDENTITY):
    return {"schema_version": 1, "input_digest": INPUT, "identity_digest": identity, field: value}


class CallbackFixture:
    def __init__(self, mode="initialize_once", initial=None, serving=None):
        self.permission = envelope("mode", mode)
        self.states = {"openbao-initialize": initial, "openbao": serving}
        self.observations = []
        self.actions = []
        self.now = 0.0
        self.on_observe = None
        self.on_stop = None

    def request_start(self):
        self.actions.append(("request",))
        if isinstance(self.permission, Exception):
            raise self.permission
        return self.permission

    def observe(self):
        self.actions.append(("observe",))
        if self.on_observe:
            self.on_observe(self)
        value = self.observations.pop(0) if self.observations else envelope("phase", "provider_ready")
        if isinstance(value, Exception):
            raise value
        return value

    def start(self, name):
        self.actions.append(("start", name))
        self.states[name] = LIFE.ContainerState(INIT_ID if name == "openbao-initialize" else SERVE_ID, True)

    def stop(self, name, identity):
        self.actions.append(("stop", name, identity))
        if self.on_stop:
            self.on_stop(self)
        else:
            self.states[name] = LIFE.ContainerState(identity, False)

    def pause(self, duration):
        self.now += duration

    def run(self, timeout=3):
        with patch.object(LIFE.time, "monotonic", side_effect=lambda: self.now):
            return LIFE.ensure_provider(input_digest=INPUT, request_start=self.request_start,
                observe=self.observe, container=self.states.get, start=self.start, stop=self.stop,
                timeout=timeout, pause=self.pause)

    def mutations(self):
        return [action for action in self.actions if action[0] in ("start", "stop")]


class ProviderLifecycleTests(unittest.TestCase):
    def test_first_start_observes_ready_before_stopping_initializer_and_starting_serve(self):
        fixture = CallbackFixture()
        self.assertEqual(fixture.run(), envelope("phase", "provider_ready"))
        self.assertEqual(fixture.actions, [("request",), ("start", "openbao-initialize"), ("observe",),
            ("stop", "openbao-initialize", INIT_ID), ("start", "openbao"), ("observe",)])

    def test_requested_response_loss_only_observes_same_live_initializer(self):
        fixture = CallbackFixture(initial=LIFE.ContainerState(INIT_ID, True))
        fixture.permission = LIFE.OwnerFailure("ExternalOutcomeUnknown")
        fixture.observations = [LIFE.OwnerFailure("Incomplete"), LIFE.OwnerFailure("PrerequisiteUnavailable"),
                                envelope("phase", "provider_ready")]
        self.assertEqual(fixture.run(), envelope("phase", "provider_ready"))
        self.assertNotIn(("start", "openbao-initialize"), fixture.actions)
        self.assertEqual(fixture.mutations(), [("stop", "openbao-initialize", INIT_ID), ("start", "openbao")])

    def test_requested_missing_or_stopped_initializer_cannot_start_again(self):
        for initial in (None, LIFE.ContainerState(INIT_ID, False)):
            with self.subTest(initial=initial):
                fixture = CallbackFixture(initial=initial)
                fixture.permission = LIFE.OwnerFailure("ExternalOutcomeUnknown")
                with self.assertRaisesRegex(LIFE.LifecycleFailure, "original live container"):
                    fixture.run()
                self.assertEqual(fixture.mutations(), [])
                self.assertNotIn(("observe",), fixture.actions)

    def test_requested_with_existing_serving_container_is_ambiguous_even_if_stopped(self):
        fixture = CallbackFixture(initial=LIFE.ContainerState(INIT_ID, True), serving=LIFE.ContainerState(SERVE_ID, False))
        fixture.permission = LIFE.OwnerFailure("ExternalOutcomeUnknown")
        with self.assertRaises(LIFE.LifecycleFailure):
            fixture.run()
        self.assertEqual(fixture.mutations(), [])

    def test_first_permission_cannot_adopt_existing_container(self):
        for service in ("openbao-initialize", "openbao"):
            for running in (False, True):
                with self.subTest(service=service, running=running):
                    fixture = CallbackFixture()
                    fixture.states[service] = LIFE.ContainerState(INIT_ID, running)
                    with self.assertRaisesRegex(LIFE.LifecycleFailure, "existing container"):
                        fixture.run()
                    self.assertEqual(fixture.mutations(), [])

    def test_both_modes_running_fails_before_requesting_permission(self):
        fixture = CallbackFixture(initial=LIFE.ContainerState(INIT_ID, True), serving=LIFE.ContainerState(SERVE_ID, True))
        with self.assertRaises(LIFE.LifecycleFailure):
            fixture.run()
        self.assertEqual(fixture.actions, [])

    def test_ready_recovery_of_live_initializer_does_not_reinitialize(self):
        fixture = CallbackFixture("serve", initial=LIFE.ContainerState(INIT_ID, True))
        fixture.run()
        self.assertEqual(fixture.mutations(), [("stop", "openbao-initialize", INIT_ID), ("start", "openbao")])

    def test_ready_missing_volume_never_initializes_or_claims_ready(self):
        for serving in (None, LIFE.ContainerState(SERVE_ID, False), LIFE.ContainerState(SERVE_ID, True)):
            with self.subTest(serving=serving):
                fixture = CallbackFixture("serve", serving=serving)
                fixture.observations = [LIFE.OwnerFailure("Incomplete") for _ in range(5)]
                with self.assertRaisesRegex(LIFE.LifecycleFailure, "timed out"):
                    fixture.run(timeout=2)
                self.assertNotIn(("start", "openbao-initialize"), fixture.actions)
                self.assertFalse(any(action[0] == "stop" for action in fixture.actions))

    def test_ready_existing_server_only_observes(self):
        fixture = CallbackFixture("serve", serving=LIFE.ContainerState(SERVE_ID, True))
        fixture.run()
        self.assertEqual(fixture.actions, [("request",), ("observe",)])

    def test_container_identity_change_during_observe_never_stops_replacement(self):
        for mode in ("initialize_once", "serve"):
            fixture = CallbackFixture(mode, serving=LIFE.ContainerState(SERVE_ID, True) if mode == "serve" else None)
            service = "openbao" if mode == "serve" else "openbao-initialize"
            fixture.on_observe = lambda value: value.states.__setitem__(service, LIFE.ContainerState(FOREIGN_ID, True))
            with self.assertRaisesRegex(LIFE.LifecycleFailure, "changed during observation"):
                fixture.run()
            self.assertFalse(any(action[0] == "stop" for action in fixture.actions))
            self.assertNotIn(("start", "openbao"), fixture.actions)

    def test_initializer_disappearing_during_observe_is_not_a_new_start_permission(self):
        fixture = CallbackFixture(initial=LIFE.ContainerState(INIT_ID, True))
        fixture.permission = LIFE.OwnerFailure("ExternalOutcomeUnknown")
        fixture.on_observe = lambda value: value.states.__setitem__("openbao-initialize", None)
        with self.assertRaises(LIFE.LifecycleFailure):
            fixture.run()
        self.assertEqual(fixture.mutations(), [])

    def test_stop_must_preserve_expected_container_identity(self):
        fixture = CallbackFixture()
        fixture.on_stop = lambda value: value.states.__setitem__("openbao-initialize", LIFE.ContainerState(FOREIGN_ID, False))
        with self.assertRaisesRegex(LIFE.LifecycleFailure, "original identity"):
            fixture.run()
        self.assertIn(("stop", "openbao-initialize", INIT_ID), fixture.actions)
        self.assertNotIn(("start", "openbao"), fixture.actions)

    def test_known_owner_rejection_never_turns_into_initialization_permission(self):
        for code in ("ForeignState", "IdentityDrift", "ConfigurationDrift", "CredentialInvalid", "Incomplete"):
            fixture = CallbackFixture()
            fixture.permission = LIFE.OwnerFailure(code)
            with self.assertRaises(LIFE.OwnerFailure):
                fixture.run()
            self.assertEqual(fixture.mutations(), [])

    def test_foreign_ready_identity_cannot_authorize_mode_transition(self):
        fixture = CallbackFixture()
        fixture.observations = [envelope("phase", "provider_ready", "sha256:" + "c" * 64)]
        with self.assertRaises(LIFE.LifecycleFailure):
            fixture.run()
        self.assertEqual(fixture.mutations(), [("start", "openbao-initialize")])

    def test_ready_arriving_after_total_deadline_is_rejected(self):
        fixture = CallbackFixture("serve", serving=LIFE.ContainerState(SERVE_ID, True))
        fixture.on_observe = lambda value: setattr(value, "now", 4)
        with self.assertRaisesRegex(LIFE.LifecycleFailure, "timed out"):
            fixture.run(timeout=3)
        self.assertEqual(fixture.mutations(), [])

    def test_owner_response_is_strict_bounded_and_never_emits_raw_error(self):
        good = subprocess.CompletedProcess(["owner"], 0, json.dumps(envelope("mode", "serve")).encode(), b"")
        self.assertEqual(LIFE.owner_result(good, INPUT, "mode"), envelope("mode", "serve"))
        for invalid in (
            good.stdout[:-1] + b',"mode":"serve"}',
            good.stdout.replace(b'"schema_version": 1', b'"schema_version": NaN'),
        ):
            with self.assertRaises(LIFE.LifecycleFailure):
                LIFE.owner_result(subprocess.CompletedProcess(["owner"], 0, invalid, b""), INPUT, "mode")
        for change in ({"schema_version": True}, {"mode": "initialize"}, {"input_digest": "sha256:" + "f" * 64}, {"extra": "private"}):
            invalid = envelope("mode", "serve") | change
            with self.assertRaises(LIFE.LifecycleFailure):
                LIFE.owner_result(subprocess.CompletedProcess(["owner"], 0, json.dumps(invalid).encode(), b""), INPUT, "mode")
        raw = b"private credential https://user:password@invalid/secret\n"
        for stderr in (raw, raw + b"installation Incomplete\n", b"installation Incomplete\ninstallation ForeignState\n"):
            with self.assertRaises(LIFE.LifecycleFailure) as error:
                LIFE.owner_result(subprocess.CompletedProcess(["owner"], 1, b"", stderr), INPUT, "mode")
            self.assertNotIn("private credential", str(error.exception))
            self.assertNotIn("password", str(error.exception))
        for stdout, stderr in ((b"x" * 16385, b""), (b"", b"x" * 65537)):
            with self.assertRaises(LIFE.LifecycleFailure):
                LIFE.owner_result(subprocess.CompletedProcess(["owner"], 1, stdout, stderr), INPUT, "mode")


class CompositionSnapshotTests(unittest.TestCase):
    def fixture(self):
        image = "fixture@sha256:" + "d" * 64
        image_id = "sha256:" + "e" * 64
        document = {"name": "own-project", "services": {"openbao": {"image": image, "labels": {"insight.installation.input": INPUT}}}}
        metadata = {"identity": SERVE_ID, "running": True, "image": image_id, "labels": {
            "com.docker.compose.project": "own-project", "com.docker.compose.service": "openbao", "insight.installation.input": INPUT}}
        return document, metadata, image_id

    def snapshot(self, document, rows, image_id):
        calls = []
        def run(arguments):
            calls.append(arguments)
            if arguments[:3] == ["docker", "image", "inspect"]:
                return (image_id + "\n").encode()
            if arguments[:2] == ["docker", "ps"]:
                return ("\n".join(row["identity"] for row in rows) + "\n").encode() if rows else b""
            if arguments[:2] == ["docker", "inspect"]:
                return json.dumps(next(row for row in rows if row["identity"] == arguments[-1])).encode()
            raise AssertionError("unexpected non-readonly Docker command")
        result = LIFE.composition_snapshot(document, run)
        return result, calls

    def test_exact_owner_and_image_snapshot_uses_only_readonly_commands(self):
        document, row, image_id = self.fixture()
        result, calls = self.snapshot(document, [row], image_id)
        self.assertEqual(result, {"openbao": LIFE.ContainerState(SERVE_ID, True)})
        self.assertTrue(all(command[:2] in (["docker", "image"], ["docker", "ps"], ["docker", "inspect"]) for command in calls))

    def test_same_name_different_input_project_or_image_is_foreign(self):
        for field in ("input", "project", "service", "image", "running"):
            with self.subTest(field=field):
                document, row, image_id = self.fixture()
                if field == "input": row["labels"]["insight.installation.input"] = "sha256:" + "f" * 64
                elif field == "project": row["labels"]["com.docker.compose.project"] = "foreign"
                elif field == "service": row["labels"]["com.docker.compose.service"] = "unexpected-service"
                elif field == "image": row["image"] = "sha256:" + "f" * 64
                else: row["running"] = "true"
                with self.assertRaisesRegex(LIFE.LifecycleFailure, "foreign composition"):
                    self.snapshot(document, [row], image_id)

    def test_duplicate_service_claims_and_unknown_image_identity_are_rejected(self):
        document, row, image_id = self.fixture()
        duplicate = copy.deepcopy(row)
        duplicate["identity"] = FOREIGN_ID
        with self.assertRaisesRegex(LIFE.LifecycleFailure, "multiple containers"):
            self.snapshot(document, [row, duplicate], image_id)
        with self.assertRaisesRegex(LIFE.LifecycleFailure, "image identity"):
            self.snapshot(document, [row], "mutable-image")

    def test_oneoff_is_excluded_only_after_foreign_image_and_input_checks(self):
        document, row, image_id = self.fixture()
        row["labels"]["com.docker.compose.oneoff"] = "true"
        self.assertEqual(self.snapshot(document, [row], image_id)[0], {})
        row["image"] = "sha256:" + "f" * 64
        with self.assertRaises(LIFE.LifecycleFailure):
            self.snapshot(document, [row], image_id)


if __name__ == "__main__":
    unittest.main()
