import importlib.util
import pathlib
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name("context_window_oracle.py")
SPEC = importlib.util.spec_from_file_location("context_window_oracle", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
ORACLE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ORACLE)


class ContextWindowOracleTests(unittest.TestCase):
    def test_selects_one_contiguous_newest_suffix(self):
        page = {
            "data": {
                "messages": [
                    {
                        "message_order": order,
                        "role": "assistant" if order % 2 == 0 else "user",
                        "content_hash": f"sha256:{order:064x}",
                        "content": {"text": "x" * (4 if order > 2 else 500)},
                    }
                    for order in range(5, 0, -1)
                ]
            }
        }
        result = ORACLE.derive_window(page, None, 0, 6, 5, 150)
        self.assertEqual(result["selected_message_orders"], [3, 4, 5])
        self.assertEqual(result["first_rejected_message_order"], 2)

    def test_oversized_newest_message_yields_empty_bounded_suffix(self):
        page = {
            "data": {
                "messages": [
                    {
                        "message_order": 2,
                        "role": "assistant",
                        "content_hash": f"sha256:{2:064x}",
                        "content": {"text": "x" * 1000},
                    },
                    {
                        "message_order": 1,
                        "role": "user",
                        "content_hash": f"sha256:{1:064x}",
                        "content": {"text": "small"},
                    },
                ]
            }
        }
        result = ORACLE.derive_window(page, {"summary": "ok"}, 0, 3, 2, 20)
        self.assertEqual(result["selected_message_orders"], [])
        self.assertEqual(result["first_rejected_message_order"], 2)


if __name__ == "__main__":
    unittest.main()
