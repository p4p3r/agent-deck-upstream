"""Focused regression coverage for the conductor acceptance-only handoff.

An idle Codex send must stop blocking in ``session send --wait``: the bridge
claims one reply owner, obtains an exact body-free acceptance receipt, and
lets the existing exact-generation watcher deliver the eventual output.
"""

from __future__ import annotations

import json
import subprocess
import sys
import types
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).parent.parent))
try:
    import toml  # noqa: F401
except ModuleNotFoundError:
    sys.modules["toml"] = types.SimpleNamespace(load=lambda *_a, **_k: {})

import bridge  # noqa: E402


def _completed(returncode: int = 0, stdout: str = "", stderr: str = ""):
    return subprocess.CompletedProcess(["agent-deck"], returncode, stdout, stderr)


def _receipt():
    return {
        "receipt_id": "6b1e73a6-f908-40a6-8af8-a40ff68b3c20",
        "instance_id": "instance-1",
        "codex_session_id": "thread-1",
        "turn_generation": "thread-1:turn-new",
        "accepted_at": "2026-09-24T01:02:03Z",
    }


def _accepted_payload():
    receipt = _receipt()
    return json.dumps(
        {
            "schema_version": 1,
            "success": True,
            "acceptance": "accepted",
            "instance_id": receipt["instance_id"],
            "delivery": "submitted",
            "submitted": True,
            "accepted_turn_kind": "codex_rollout",
            "accepted_turn": receipt,
        }
    )


def _unsupported_payload():
    return json.dumps(
        {
            "schema_version": 1,
            "success": False,
            "acceptance": "not_accepted",
            "code": "UNSUPPORTED_TARGET",
        }
    )


def test_exact_acceptance_returns_receipt_without_legacy_wait():
    receipt = _receipt()
    with mock.patch(
        "bridge.run_cli",
        return_value=_completed(stdout=_accepted_payload()),
    ) as cli:
        result = bridge.send_to_conductor(
            "conductor-codex",
            "private body",
            profile="work",
            wait_for_reply=True,
            claim_late_reply=True,
        )

    assert result == (False, "", receipt)
    cli.assert_called_once()
    assert "--acceptance-only" in cli.call_args.args
    assert "--wait" not in cli.call_args.args
    assert "--json" not in cli.call_args.args
    assert (
        bridge._wait_send_reservations[("work", "conductor-codex")]
        == receipt["receipt_id"]
    )
    bridge._release_late_reply_claim("conductor-codex", "work", receipt)


def test_legacy_wait_fallback_requires_exact_pretransport_unsupported_result():
    legacy_complete = json.dumps(
        {
            "success": True,
            "completion": "complete",
            "content": "legacy answer",
        }
    )
    with mock.patch(
        "bridge.run_cli",
        side_effect=[
            _completed(1, stdout=_unsupported_payload()),
            _completed(stdout=legacy_complete),
        ],
    ) as cli:
        result = bridge.send_to_conductor(
            "conductor-claude",
            "question",
            wait_for_reply=True,
        )

    assert result == (True, "legacy answer", False)
    assert cli.call_count == 2
    assert "--acceptance-only" in cli.call_args_list[0].args
    assert "--wait" not in cli.call_args_list[0].args
    assert "--wait" in cli.call_args_list[1].args


def test_indeterminate_delivery_is_uncertain_and_never_retried():
    payload = json.dumps(
        {
            "schema_version": 1,
            "success": False,
            "acceptance": "indeterminate",
            "code": "ACCEPTANCE_INDETERMINATE",
            "delivery": "unverified",
            "submitted": False,
        }
    )
    with mock.patch(
        "bridge.run_cli",
        return_value=_completed(1, stdout=payload),
    ) as cli:
        result = bridge.send_to_conductor(
            "conductor-codex",
            "question",
            wait_for_reply=True,
        )

    assert result == (False, "", bridge._WAIT_SEND_DELIVERY_UNCERTAIN)
    cli.assert_called_once()
    assert "--acceptance-only" in cli.call_args.args
    notice = bridge._delivery_uncertain_notice("semgrep")
    assert "failed" not in notice.lower()
    assert "may have" in notice.lower()
    assert "not retried" in notice.lower()


def test_malformed_or_privacy_violating_result_fails_closed_without_retry():
    private_result = json.dumps(
        {
            **json.loads(_accepted_payload()),
            "content": "private completed response",
        }
    )
    oversized_result = " " * 2049

    for stdout in (private_result, oversized_result, "not-json"):
        with mock.patch(
            "bridge.run_cli",
            return_value=_completed(stdout=stdout),
        ) as cli:
            assert bridge.send_to_conductor(
                "conductor-codex",
                "question",
                wait_for_reply=True,
            ) == (False, "", bridge._WAIT_SEND_DELIVERY_UNCERTAIN)
        cli.assert_called_once()
