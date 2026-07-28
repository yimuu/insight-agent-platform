#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


PROBE_PATH = Path(__file__).with_name("privacy_stream_probe.py")
SPEC = importlib.util.spec_from_file_location("privacy_stream_probe", PROBE_PATH)
assert SPEC is not None and SPEC.loader is not None
PROBE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROBE)


def frame(event: str, observed_at_ns: int) -> dict[str, object]:
    return {
        "event": event,
        "observed_at_ns": observed_at_ns,
        "data_bytes": 1,
        "data_sha256": "0" * 64,
    }


class PrivacyTimelineTests(unittest.TestCase):
    def test_pre_delete_delta_and_eof_pass(self) -> None:
        self.assertEqual(
            PROBE.evaluate_timeline(
                [frame("response.output_text.delta", 10)],
                20,
            ),
            [],
        )

    def test_post_delete_delta_fails(self) -> None:
        failures = PROBE.evaluate_timeline(
            [
                frame("response.output_text.delta", 10),
                frame("response.output_text.delta", 21),
            ],
            20,
        )
        self.assertTrue(any("after" in failure for failure in failures))

    def test_post_delete_comment_frame_fails(self) -> None:
        parsed = PROBE.parse_frame([": keep-alive"], 21)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["event"], "comment")
        failures = PROBE.evaluate_timeline(
            [frame("response.output_text.delta", 10), parsed],
            20,
        )
        self.assertTrue(any("comment" in failure for failure in failures))

    def test_every_terminal_or_error_frame_after_delete_fails(self) -> None:
        for event in sorted(PROBE.TERMINAL_EVENTS):
            with self.subTest(event=event):
                failures = PROBE.evaluate_timeline(
                    [
                        frame("response.output_text.delta", 10),
                        frame(event, 21),
                    ],
                    20,
                )
                self.assertTrue(any(event in failure for failure in failures))


if __name__ == "__main__":
    unittest.main()
