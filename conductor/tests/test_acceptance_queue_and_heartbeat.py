"""Regressions for acceptance ownership outside direct chat handlers."""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import types
from collections import deque
from pathlib import Path
from types import SimpleNamespace
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


def _accepted_result():
    receipt = _receipt()
    return _completed(stdout=json.dumps({
        "schema_version": 1,
        "success": True,
        "acceptance": "accepted",
        "instance_id": receipt["instance_id"],
        "delivery": "submitted",
        "submitted": True,
        "accepted_turn_kind": "codex_rollout",
        "accepted_turn": receipt,
    }))


def _uncertain_result():
    return _completed(1, stdout=json.dumps({
        "schema_version": 1,
        "success": False,
        "acceptance": "indeterminate",
        "code": "ACCEPTANCE_INDETERMINATE",
        "delivery": "unverified",
        "submitted": False,
    }), stderr="timeout after submit")


def _unsupported_result():
    return _completed(1, stdout=json.dumps({
        "schema_version": 1,
        "success": False,
        "acceptance": "not_accepted",
        "code": "UNSUPPORTED_TARGET",
    }))


async def _no_sleep(_seconds: float) -> None:
    return None


async def _clear_bridge_owners() -> None:
    tasks = list(bridge._pending_reply_tasks.values())
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    bridge._pending_reply_tasks.clear()
    bridge._wait_send_reservations.clear()
    bridge._message_queue.clear()


def test_queued_acceptance_registers_exact_watcher_without_failure_or_resend():
    delivered = []
    cli_calls = []

    async def callback(text: str) -> None:
        delivered.append(text)

    def run_cli(*args, **kwargs):
        cli_calls.append(args)
        return _accepted_result()

    async def scenario() -> None:
        await _clear_bridge_owners()
        bridge._message_queue["conductor-ops"] = deque([
            ("private body", "work", callback),
        ])
        try:
            with mock.patch("bridge.asyncio.sleep", new=_no_sleep), mock.patch(
                "bridge.get_session_status", return_value="idle",
            ), mock.patch("bridge.run_cli", side_effect=run_cli), mock.patch(
                "bridge.get_session_output_state",
                return_value=("exact answer", "thread-1:turn-new"),
            ), mock.patch("bridge.get_session_output") as latest:
                await bridge._drain_queue()
                tasks = list(bridge._pending_reply_tasks.values())
                if tasks:
                    await asyncio.gather(*tasks)
                latest.assert_not_called()
        finally:
            await _clear_bridge_owners()

    asyncio.run(scenario())
    assert delivered == ["exact answer"]
    assert len(cli_calls) == 1
    assert "--acceptance-only" in cli_calls[0]
    assert "--wait" not in cli_calls[0]


def test_queued_watcher_ignores_stale_latest_output_generation():
    delivered = []

    async def callback(text: str) -> None:
        delivered.append(text)

    async def scenario() -> None:
        await _clear_bridge_owners()
        bridge._message_queue["conductor-ops"] = deque([
            ("private body", "work", callback),
        ])
        try:
            with mock.patch("bridge.asyncio.sleep", new=_no_sleep), mock.patch(
                "bridge.get_session_status", return_value="idle",
            ), mock.patch("bridge.run_cli", return_value=_accepted_result()), mock.patch(
                "bridge.get_session_output_state", side_effect=[
                    ("wrong turn", "thread-1:turn-old"),
                    ("right turn", "thread-1:turn-new"),
                ],
            ):
                await bridge._drain_queue()
                tasks = list(bridge._pending_reply_tasks.values())
                if tasks:
                    await asyncio.gather(*tasks)
        finally:
            await _clear_bridge_owners()

    asyncio.run(scenario())
    assert delivered == ["right turn"]


def test_queued_non_codex_fallback_delivers_legacy_bound_response_once():
    delivered = []

    async def callback(text: str) -> None:
        delivered.append(text)

    legacy_result = _completed(stdout=json.dumps({
        "success": True,
        "completion": "complete",
        "content": "legacy answer",
    }))

    async def scenario() -> None:
        await _clear_bridge_owners()
        bridge._message_queue["conductor-legacy"] = deque([
            ("private body", "work", callback),
        ])
        try:
            with mock.patch("bridge.asyncio.sleep", new=_no_sleep), mock.patch(
                "bridge.get_session_status", return_value="idle",
            ), mock.patch("bridge.run_cli", side_effect=[
                _unsupported_result(), legacy_result,
            ]) as cli, mock.patch("bridge.get_session_output") as latest:
                await bridge._drain_queue()
                assert cli.call_count == 2
                assert "--acceptance-only" in cli.call_args_list[0].args
                assert "--wait" in cli.call_args_list[1].args
                latest.assert_not_called()
        finally:
            await _clear_bridge_owners()

    asyncio.run(scenario())
    assert delivered == ["legacy answer"]


def test_queued_indeterminate_delivery_is_removed_once_and_never_retried():
    delivered = []
    cli_calls = []

    async def callback(text: str) -> None:
        delivered.append(text)

    def run_cli(*args, **kwargs):
        cli_calls.append(args)
        if len(cli_calls) > 1:
            raise AssertionError("indeterminate queued delivery was retried")
        return _uncertain_result()

    async def scenario() -> None:
        await _clear_bridge_owners()
        bridge._message_queue["conductor-ops"] = deque([
            ("private body", "work", callback),
        ])
        try:
            with mock.patch("bridge.asyncio.sleep", new=_no_sleep), mock.patch(
                "bridge.get_session_status", return_value="idle",
            ), mock.patch("bridge.run_cli", side_effect=run_cli):
                await bridge._drain_queue()
        finally:
            assert "conductor-ops" not in bridge._message_queue
            await _clear_bridge_owners()

    asyncio.run(scenario())
    assert len(cli_calls) == 1
    assert len(delivered) == 1
    assert "may have succeeded" in delivered[0]
    assert "not retried" in delivered[0]
    assert "failed" not in delivered[0].lower()


def test_queued_watcher_registration_failure_releases_reservation():
    delivered = []

    async def callback(text: str) -> None:
        delivered.append(text)

    async def scenario() -> None:
        await _clear_bridge_owners()
        key = ("work", "conductor-ops")
        bridge._message_queue["conductor-ops"] = deque([
            ("private body", "work", callback),
        ])
        try:
            with mock.patch("bridge.asyncio.sleep", new=_no_sleep), mock.patch(
                "bridge.get_session_status", return_value="idle",
            ), mock.patch("bridge.run_cli", return_value=_accepted_result()), mock.patch(
                "bridge._register_pending_reply", return_value=False,
            ):
                await bridge._drain_queue()
            assert key not in bridge._wait_send_reservations
            assert key not in bridge._pending_reply_tasks
            assert "conductor-ops" not in bridge._message_queue
        finally:
            await _clear_bridge_owners()

    asyncio.run(scenario())
    assert len(delivered) == 1
    assert "accepted" in delivered[0].lower()
    assert "not retried" in delivered[0].lower()


def test_queued_owner_collision_retains_item_without_transport_send():
    class StopDrain(Exception):
        pass

    sleeps = 0

    async def one_cycle(_seconds: float) -> None:
        nonlocal sleeps
        sleeps += 1
        if sleeps > 1:
            raise StopDrain

    async def scenario() -> None:
        await _clear_bridge_owners()
        key = ("work", "conductor-ops")
        bridge._wait_send_reservations[key] = "existing-owner"
        bridge._message_queue["conductor-ops"] = deque([
            ("private body", "work", None),
        ])
        try:
            with mock.patch("bridge.asyncio.sleep", new=one_cycle), mock.patch(
                "bridge.run_cli",
            ) as cli:
                try:
                    await bridge._drain_queue()
                except StopDrain:
                    pass
                cli.assert_not_called()
                assert len(bridge._message_queue["conductor-ops"]) == 1
        finally:
            await _clear_bridge_owners()

    asyncio.run(scenario())


def test_heartbeat_exact_acceptance_delivers_alert_and_post_hook_once():
    class StopHeartbeat(Exception):
        pass

    sleeps = 0
    post_payloads = []
    cli_calls = []
    real_sleep = asyncio.sleep

    async def heartbeat_sleep(_seconds: float) -> None:
        nonlocal sleeps
        sleeps += 1
        if sleeps > 1:
            raise StopHeartbeat

    def run_cli(*args, **kwargs):
        cli_calls.append(args)
        return _accepted_result()

    def invoke_hook(profile, hook_name, payload):
        if hook_name == "post-heartbeat":
            post_payloads.append(payload)
        return None

    async def scenario() -> None:
        await _clear_bridge_owners()
        slack_client = SimpleNamespace(chat_postMessage=mock.AsyncMock())
        slack_app = SimpleNamespace(client=slack_client)
        config = {
            "heartbeat_interval": 1,
            "telegram": {"configured": False, "user_id": None},
        }
        try:
            with mock.patch("bridge.asyncio.sleep", new=heartbeat_sleep), mock.patch(
                "bridge._os_heartbeat_daemon_installed", return_value=False,
            ), mock.patch("bridge.discover_conductors", return_value=[{
                "name": "semgrep", "profile": "work", "heartbeat_enabled": True,
            }]), mock.patch("bridge.get_sessions_list", return_value=[{
                "title": "worker", "group": "semgrep", "status": "waiting", "path": "/tmp/work",
            }]), mock.patch(
                "bridge.ensure_conductor_running", new=mock.AsyncMock(return_value=True),
            ), mock.patch("bridge.get_session_status", return_value="idle"), mock.patch(
                "bridge.hook_driven_interactive", return_value=(False, True),
            ), mock.patch("bridge.capture_pane", return_value=""), mock.patch(
                "bridge.invoke_hook", side_effect=invoke_hook,
            ), mock.patch("bridge.run_cli", side_effect=run_cli), mock.patch(
                "bridge.get_session_output_state",
                return_value=("NEED: inspect queue", "thread-1:turn-new"),
            ):
                try:
                    await bridge.heartbeat_loop(
                        config,
                        slack_app=slack_app,
                        slack_channel_id="C123",
                    )
                except StopHeartbeat:
                    pass
                tasks = list(bridge._pending_reply_tasks.values())
                if tasks:
                    await asyncio.gather(*tasks)
                await real_sleep(0)
                key = ("work", "conductor-semgrep")
                assert key not in bridge._wait_send_reservations
                assert key not in bridge._pending_reply_tasks
            assert slack_client.chat_postMessage.await_count == 1
        finally:
            await _clear_bridge_owners()

    asyncio.run(scenario())
    assert len(cli_calls) == 1
    assert "--acceptance-only" in cli_calls[0]
    assert "--wait" not in cli_calls[0]
    assert len(post_payloads) == 1
    assert post_payloads[0]["response"] == "NEED: inspect queue"
    assert post_payloads[0]["has_alerts"] is True
