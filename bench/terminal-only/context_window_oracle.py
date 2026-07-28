#!/usr/bin/env python3
"""Independently derive the bounded recent-message suffix for qualification."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any


def canonical_json_tokens(value: Any) -> int:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return math.ceil(len(encoded) / 4)


def derive_window(
    page: dict[str, Any],
    summary: Any,
    boundary: int,
    before_order: int,
    recent_limit: int,
    token_budget: int,
) -> dict[str, Any]:
    raw_messages = page.get("data", {}).get("messages")
    if not isinstance(raw_messages, list):
        raise ValueError("message page does not contain data.messages")
    if boundary < 0 or before_order <= boundary:
        raise ValueError("message boundaries are invalid")
    if recent_limit <= 0 or token_budget <= 0:
        raise ValueError("limits must be positive")

    candidates: list[dict[str, Any]] = []
    seen_orders: set[int] = set()
    for raw in raw_messages:
        if not isinstance(raw, dict):
            raise ValueError("message page contains a non-object")
        order = raw.get("message_order")
        role = raw.get("role")
        content_hash = raw.get("content_hash")
        if (
            isinstance(order, bool)
            or not isinstance(order, int)
            or not isinstance(role, str)
            or role not in {"user", "assistant"}
            or not isinstance(content_hash, str)
            or "content" not in raw
        ):
            raise ValueError("message page contains an invalid message")
        if order in seen_orders:
            raise ValueError("message page contains duplicate message_order")
        seen_orders.add(order)
        if boundary < order < before_order:
            candidates.append(
                {
                    "message_order": order,
                    "role": role,
                    "content_hash": content_hash,
                    "content": raw["content"],
                }
            )

    # The repository first obtains the newest N rows, then the runtime adds
    # them newest-first until the first candidate would exceed the budget.
    candidates.sort(key=lambda item: item["message_order"], reverse=True)
    candidates = candidates[:recent_limit]
    selected_newest: list[dict[str, Any]] = []
    rejected_order: int | None = None
    for item in candidates:
        tentative = [*selected_newest, item]
        candidate_context = {
            "summary": summary,
            "messages": list(reversed(tentative)),
        }
        if canonical_json_tokens(candidate_context) > token_budget:
            rejected_order = item["message_order"]
            break
        selected_newest.append(item)

    selected = list(reversed(selected_newest))
    context = {"summary": summary, "messages": selected}
    return {
        "boundary": boundary,
        "before_message_order": before_order,
        "recent_limit": recent_limit,
        "token_budget": token_budget,
        "candidate_message_orders": [
            item["message_order"] for item in reversed(candidates)
        ],
        "selected_message_orders": [item["message_order"] for item in selected],
        "first_rejected_message_order": rejected_order,
        "estimated_context_tokens": canonical_json_tokens(context),
        "messages": [
            {
                "message_order": item["message_order"],
                "role": item["role"],
                "content_hash": item["content_hash"],
            }
            for item in selected
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--messages", required=True)
    parser.add_argument("--context", required=True)
    parser.add_argument("--boundary", type=int, required=True)
    parser.add_argument("--before-order", type=int, required=True)
    parser.add_argument("--recent-limit", type=int, required=True)
    parser.add_argument("--token-budget", type=int, required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    page = json.loads(Path(args.messages).read_text(encoding="utf-8"))
    context = json.loads(Path(args.context).read_text(encoding="utf-8"))
    if not isinstance(context, dict) or "summary" not in context:
        raise SystemExit("context must contain a summary field")
    result = derive_window(
        page,
        context["summary"],
        args.boundary,
        args.before_order,
        args.recent_limit,
        args.token_budget,
    )
    Path(args.output).write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
