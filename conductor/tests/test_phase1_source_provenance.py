"""RED-first coverage for the Phase 1 source-provenance safety gate.

These tests intentionally exercise the canonical embedded bridge source loaded
by ``conductor/tests/conftest.py``.  They do not import a deployed bridge or
touch operator configuration.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
from pathlib import Path
from unittest import mock

import pytest

import bridge


def _slack_config(channel_id: str, allowed_user_ids: list[str]) -> dict:
    return {
        "conductor": {
            "slack": {
                "bot_token": "xoxb-test",
                "app_token": "xapp-test",
                "channel_id": channel_id,
                "listen_mode": "all",
                "allowed_user_ids": allowed_user_ids,
            }
        }
    }


def _load_config(config: dict, env: dict[str, str]) -> dict:
    config_path = mock.MagicMock(spec=Path)
    config_path.exists.return_value = True
    with (
        mock.patch("bridge.CONFIG_PATH", config_path),
        mock.patch("bridge.toml.load", return_value=config),
        mock.patch(
            "bridge.discover_conductors",
            return_value=[{"name": "ops", "profile": "default"}],
        ),
        mock.patch.dict(os.environ, env, clear=True),
    ):
        return bridge.load_config()


@pytest.mark.parametrize(
    "reference", ["$SLACK_CHANNEL_ID", "${SLACK_CHANNEL_ID}"]
)
def test_slack_channel_environment_reference_resolves(reference: str) -> None:
    loaded = _load_config(
        _slack_config(reference, []),
        {"SLACK_CHANNEL_ID": "C0123456789"},
    )

    assert loaded["slack"]["configured"] is True
    assert loaded["slack"]["channel_id"] == "C0123456789"


@pytest.mark.parametrize("env", [{}, {"SLACK_CHANNEL_ID": ""}])
def test_missing_or_empty_slack_channel_reference_fails_closed(
    env: dict[str, str],
) -> None:
    # The tokens alone must not make Slack look configured when its channel
    # reference cannot be resolved.  With no other platform configured,
    # load_config's fail-closed result is SystemExit.
    with pytest.raises(SystemExit):
        _load_config(_slack_config("$SLACK_CHANNEL_ID", []), env)


def test_slack_user_environment_references_preserve_unresolved_denial() -> None:
    loaded = _load_config(
        _slack_config(
            "C0123456789",
            ["$SLACK_ALLOWED_USER", "${SLACK_MISSING_USER}"],
        ),
        {"SLACK_ALLOWED_USER": "U0123456789"},
    )

    # Do not filter the unresolved entry.  [] has the historical meaning
    # "allow all", whereas [""] remains a configured allowlist that denies
    # every real Slack user until the environment is repaired.
    assert loaded["slack"]["allowed_user_ids"] == ["U0123456789", ""]


class _FakeSlackApp:
    def __init__(self) -> None:
        self.events: dict[str, object] = {}
        self.commands: dict[str, object] = {}
        self.client = mock.MagicMock()

    def event(self, name: str):
        def register(callback):
            self.events[name] = callback
            return callback

        return register

    def command(self, name: str):
        def register(callback):
            self.commands[name] = callback
            return callback

        return register


def test_slack_message_requires_resolved_channel_and_user_match() -> None:
    loaded = _load_config(
        _slack_config("$SLACK_CHANNEL_ID", ["$SLACK_ALLOWED_USER"]),
        {
            "SLACK_CHANNEL_ID": "C0123456789",
            "SLACK_ALLOWED_USER": "U0123456789",
        },
    )
    fake_app = _FakeSlackApp()

    async def scenario() -> None:
        say = mock.AsyncMock()
        ensure = mock.AsyncMock(return_value=False)
        conductors = [{"name": "ops", "profile": "default"}]
        with (
            mock.patch.object(bridge, "HAS_SLACK", True),
            mock.patch("bridge.AsyncApp", return_value=fake_app, create=True),
            mock.patch("bridge.ensure_conductor_running", new=ensure),
            mock.patch("bridge.discover_conductors", return_value=conductors),
            mock.patch("bridge.get_default_conductor", return_value=conductors[0]),
            mock.patch("bridge.get_conductor_names", return_value=["ops"]),
        ):
            result = bridge.create_slack_app(loaded)
            assert result == (fake_app, "C0123456789")
            handler = fake_app.events["message"]

            # Each mismatch is rejected before the bridge may start or send to
            # a conductor.
            await handler(
                {
                    "channel": "C_WRONG",
                    "user": "U0123456789",
                    "text": "wrong channel",
                    "ts": "1",
                },
                say,
            )
            await handler(
                {
                    "channel": "C0123456789",
                    "user": "U_WRONG",
                    "text": "wrong user",
                    "ts": "2",
                },
                say,
            )
            ensure.assert_not_awaited()

            await handler(
                {
                    "channel": "C0123456789",
                    "user": "U0123456789",
                    "text": "authorized",
                    "ts": "3",
                },
                say,
            )
            ensure.assert_awaited_once_with("ops", "default")

    asyncio.run(scenario())


@pytest.mark.parametrize("env", [{}, {"SLACK_ALLOWED_USER": ""}])
def test_missing_or_empty_slack_user_reference_denies_all_senders(
    env: dict[str, str],
) -> None:
    loaded = _load_config(
        _slack_config("C0123456789", ["$SLACK_ALLOWED_USER"]), env
    )
    assert loaded["slack"]["allowed_user_ids"] == [""]
    fake_app = _FakeSlackApp()

    async def scenario() -> None:
        ensure = mock.AsyncMock(return_value=False)
        with (
            mock.patch.object(bridge, "HAS_SLACK", True),
            mock.patch("bridge.AsyncApp", return_value=fake_app, create=True),
            mock.patch("bridge.ensure_conductor_running", new=ensure),
        ):
            bridge.create_slack_app(loaded)
            await fake_app.events["message"](
                {
                    "channel": "C0123456789",
                    "user": "U_ANY_REAL_USER",
                    "text": "must be denied",
                    "ts": "4",
                },
                mock.AsyncMock(),
            )
            ensure.assert_not_awaited()

    asyncio.run(scenario())


def test_mixed_resolved_and_unresolved_allowlist_denies_every_sender() -> None:
    loaded = _load_config(
        _slack_config(
            "C0123456789",
            ["$SLACK_ALLOWED_USER", "$SLACK_MISSING_USER"],
        ),
        {"SLACK_ALLOWED_USER": "U0123456789"},
    )
    assert loaded["slack"]["allowed_user_ids"] == ["U0123456789", ""]
    fake_app = _FakeSlackApp()

    async def scenario() -> None:
        ensure = mock.AsyncMock(return_value=False)
        with (
            mock.patch.object(bridge, "HAS_SLACK", True),
            mock.patch("bridge.AsyncApp", return_value=fake_app, create=True),
            mock.patch("bridge.ensure_conductor_running", new=ensure),
        ):
            bridge.create_slack_app(loaded)
            await fake_app.events["message"](
                {
                    "channel": "C0123456789",
                    "user": "U0123456789",
                    "text": "valid member must still be denied",
                    "ts": "5",
                },
                mock.AsyncMock(),
            )
            ensure.assert_not_awaited()

    asyncio.run(scenario())


def test_deliberately_empty_allowlist_still_rejects_missing_sender() -> None:
    loaded = _load_config(_slack_config("C0123456789", []), {})
    fake_app = _FakeSlackApp()

    async def scenario() -> None:
        ensure = mock.AsyncMock(return_value=False)
        with (
            mock.patch.object(bridge, "HAS_SLACK", True),
            mock.patch("bridge.AsyncApp", return_value=fake_app, create=True),
            mock.patch("bridge.ensure_conductor_running", new=ensure),
        ):
            bridge.create_slack_app(loaded)
            for event_name in ("message", "app_mention"):
                await fake_app.events[event_name](
                    {
                        "channel": "C0123456789",
                        "text": "sender omitted",
                        "ts": "6",
                    },
                    mock.AsyncMock(),
                )
            ensure.assert_not_awaited()

    asyncio.run(scenario())


def test_app_mention_enforces_sender_channel_and_human_event_boundary() -> None:
    loaded = _load_config(
        _slack_config("C0123456789", ["U0123456789"]), {}
    )
    fake_app = _FakeSlackApp()

    async def scenario() -> None:
        ensure = mock.AsyncMock(return_value=False)
        conductors = [{"name": "ops", "profile": "default"}]
        with (
            mock.patch.object(bridge, "HAS_SLACK", True),
            mock.patch("bridge.AsyncApp", return_value=fake_app, create=True),
            mock.patch("bridge.ensure_conductor_running", new=ensure),
            mock.patch("bridge.discover_conductors", return_value=conductors),
            mock.patch("bridge.get_default_conductor", return_value=conductors[0]),
            mock.patch("bridge.get_conductor_names", return_value=["ops"]),
        ):
            bridge.create_slack_app(loaded)
            mention = fake_app.events["app_mention"]
            for rejected in (
                {
                    "channel": "C_WRONG",
                    "user": "U0123456789",
                    "text": "<@UBOT> wrong channel",
                    "ts": "7",
                },
                {
                    "channel": "C0123456789",
                    "user": "U0123456789",
                    "bot_id": "B0123",
                    "text": "<@UBOT> bot",
                    "ts": "8",
                },
                {
                    "channel": "C0123456789",
                    "user": "U0123456789",
                    "subtype": "message_changed",
                    "text": "<@UBOT> subtype",
                    "ts": "9",
                },
            ):
                await mention(rejected, mock.AsyncMock())
            ensure.assert_not_awaited()

            await mention(
                {
                    "channel": "C0123456789",
                    "user": "U0123456789",
                    "text": "<@UBOT> authorized mention",
                    "ts": "10",
                },
                mock.AsyncMock(),
            )
            ensure.assert_awaited_once_with("ops", "default")

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "command_name",
    ["/ad-status", "/ad-sessions", "/ad-restart", "/ad-help"],
)
@pytest.mark.parametrize(
    (
        "configured_channel",
        "allowed_users",
        "command_user",
        "command_channel",
        "allowed",
    ),
    [
        pytest.param(
            "C_CONFIGURED", ["U_ALLOWED"], "U_ALLOWED", "C_CONFIGURED", True,
            id="authorized-user-configured-channel",
        ),
        pytest.param(
            "C_CONFIGURED", ["U_ALLOWED"], "U_ALLOWED", "C_WRONG", False,
            id="wrong-channel",
        ),
        pytest.param(
            "C_CONFIGURED", ["U_ALLOWED"], "U_ALLOWED", None, False,
            id="missing-channel",
        ),
        pytest.param(
            "C_CONFIGURED", ["U_ALLOWED"], "U_ALLOWED", "D_DIRECT", False,
            id="dm-channel",
        ),
        pytest.param(
            "C_CONFIGURED", ["U_OTHER"], "U_ALLOWED", "C_CONFIGURED", False,
            id="unauthorized-user",
        ),
        pytest.param(
            "C_CONFIGURED", [""], "U_ALLOWED", "C_CONFIGURED", False,
            id="unresolved-user-reference",
        ),
        pytest.param(
            "C_CONFIGURED", [], "", "C_CONFIGURED", False,
            id="missing-sender-with-empty-allowlist",
        ),
        pytest.param(
            "", ["U_ALLOWED"], "U_ALLOWED", "C_CONFIGURED", False,
            id="unresolved-or-empty-configured-channel",
        ),
    ],
)
def test_slack_slash_commands_require_authorized_user_and_configured_channel(
    command_name: str,
    configured_channel: str,
    allowed_users: list[str],
    command_user: str,
    command_channel: str | None,
    allowed: bool,
) -> None:
    # Start from a syntactically usable app config, then override the two
    # authorization inputs directly. The empty configured-channel case is a
    # defense-in-depth check because load_config already refuses to configure
    # Slack when its channel env-ref resolves to empty.
    loaded = _load_config(
        _slack_config("C_CONFIGURED", ["U_ALLOWED"]), {}
    )
    loaded["slack"]["channel_id"] = configured_channel
    loaded["slack"]["allowed_user_ids"] = allowed_users
    loaded["slack"]["configured"] = True
    fake_app = _FakeSlackApp()

    async def scenario() -> None:
        ack = mock.AsyncMock()
        respond = mock.AsyncMock()
        command = {"user_id": command_user, "text": "ops"}
        if command_channel is not None:
            command["channel_id"] = command_channel

        conductor = {"name": "ops", "profile": "default"}
        status = {
            "total": 1,
            "running": 0,
            "waiting": 1,
            "idle": 0,
            "error": 0,
        }
        with (
            mock.patch.object(bridge, "HAS_SLACK", True),
            mock.patch("bridge.AsyncApp", return_value=fake_app, create=True),
            mock.patch("bridge.get_unique_profiles", return_value=["default"]) as profiles,
            mock.patch(
                "bridge.get_status_summary_all",
                return_value={"totals": status, "per_profile": {"default": status}},
            ) as status_summary,
            mock.patch(
                "bridge.get_sessions_list_all",
                return_value=[("default", {"title": "ops", "status": "waiting", "tool": "codex"})],
            ) as sessions_list,
            mock.patch("bridge.discover_conductors", return_value=[conductor]) as discover,
            mock.patch("bridge.get_default_conductor", return_value=conductor) as default_conductor,
            mock.patch("bridge.get_conductor_names", return_value=["ops"]) as names,
            mock.patch("bridge.run_cli", return_value=_completed()) as run_cli,
        ):
            assert bridge.create_slack_app(loaded) == (fake_app, configured_channel)
            await fake_app.commands[command_name](ack, respond, command)

        ack.assert_awaited_once_with()
        if allowed:
            assert respond.await_count >= 1
            assert all(
                call.args != ("⛔ Unauthorized. Contact your administrator.",)
                for call in respond.await_args_list
            )
            if command_name == "/ad-restart":
                run_cli.assert_called_once()
        else:
            respond.assert_awaited_once_with(
                "⛔ Unauthorized. Contact your administrator."
            )
            profiles.assert_not_called()
            status_summary.assert_not_called()
            sessions_list.assert_not_called()
            discover.assert_not_called()
            default_conductor.assert_not_called()
            names.assert_not_called()
            run_cli.assert_not_called()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("command_user", "command_channel"),
    [
        ("U_SUPPLIED_SECRET", "C_CONFIGURED"),
        ("U_ALLOWED", "C_SUPPLIED_SECRET"),
    ],
)
def test_slack_command_denial_logs_do_not_expose_supplied_identity(
    command_user: str,
    command_channel: str,
) -> None:
    loaded = _load_config(
        _slack_config("C_CONFIGURED", ["U_ALLOWED"]), {}
    )
    fake_app = _FakeSlackApp()

    async def scenario() -> None:
        ack = mock.AsyncMock()
        respond = mock.AsyncMock()
        with (
            mock.patch.object(bridge, "HAS_SLACK", True),
            mock.patch("bridge.AsyncApp", return_value=fake_app, create=True),
            mock.patch.object(bridge, "log") as denial_log,
        ):
            assert bridge.create_slack_app(loaded) == (fake_app, "C_CONFIGURED")
            await fake_app.commands["/ad-help"](
                ack,
                respond,
                {
                    "user_id": command_user,
                    "channel_id": command_channel,
                    "text": "",
                },
            )
            denial_calls = repr(
                denial_log.warning.call_args_list
                + denial_log.error.call_args_list
            )

        ack.assert_awaited_once_with()
        respond.assert_awaited_once_with(
            "⛔ Unauthorized. Contact your administrator."
        )
        assert command_user not in denial_calls
        assert command_channel not in denial_calls

    asyncio.run(scenario())


def _completed(returncode: int = 0, stderr: str = "") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(["agent-deck"], returncode, "", stderr)


def _calls_for(mock_cli: mock.Mock, *prefix: str) -> list[tuple]:
    return [
        call.args
        for call in mock_cli.call_args_list
        if call.args[: len(prefix)] == prefix
    ]


def test_conductor_recreation_uses_recorded_codex_runtime() -> None:
    async def no_sleep(_seconds: float) -> None:
        return None

    conductors = [{"name": "ops", "profile": "default", "agent": "codex"}]
    with (
        mock.patch("bridge.asyncio.sleep", new=no_sleep),
        mock.patch("bridge.get_session_status", side_effect=["unknown", "running"]),
        mock.patch("bridge.get_sessions_list", return_value=[]),
        mock.patch("bridge.discover_conductors", return_value=conductors),
        mock.patch(
            "bridge.run_cli",
            side_effect=[_completed(1, "not found"), _completed(0), _completed(0)],
        ) as mock_cli,
    ):
        assert asyncio.run(bridge.ensure_conductor_running("ops", "default")) is True

    add_calls = _calls_for(mock_cli, "add")
    assert len(add_calls) == 1
    runtime_index = add_calls[0].index("-c") + 1
    assert add_calls[0][runtime_index] == "codex"


@pytest.mark.parametrize(
    ("agent", "expected"),
    [
        ("claude", "claude"),
        ("codex", "codex"),
        ("hermes", "hermes"),
        (None, "claude"),
        ("", "claude"),
        ("unsupported-runtime", "claude"),
    ],
)
def test_conductor_runtime_resolution_is_allowlisted(
    agent: str | None, expected: str
) -> None:
    resolver = getattr(bridge, "conductor_agent_command", None)
    assert callable(resolver), "bridge must resolve recreation runtime from meta.json"
    meta = {"name": "ops"}
    if agent is not None:
        meta["agent"] = agent
    with mock.patch("bridge.discover_conductors", return_value=[meta]):
        assert resolver("ops") == expected


def test_codex_long_turn_delivers_late_output_once_without_resend() -> None:
    delivered: list[str] = []

    async def callback(text: str) -> None:
        delivered.append(text)

    async def no_sleep(_seconds: float) -> None:
        return None

    async def scenario() -> None:
        with (
            mock.patch("bridge.asyncio.sleep", new=no_sleep),
            mock.patch(
                "bridge.run_cli",
                return_value=_completed(
                    1,
                    "Codex output freshness timeout (5m0s): "
                    "no fresh assistant response",
                ),
            ) as run_cli,
            mock.patch(
                "bridge.get_session_status", side_effect=["running", "waiting"]
            ),
            mock.patch("bridge.get_session_output", return_value="late Codex answer"),
        ):
            ok, response, still_running = bridge.send_to_conductor(
                "conductor-ops",
                "investigate",
                profile="default",
                wait_for_reply=True,
            )
            assert (ok, response, still_running) == (False, "", True)
            bridge._register_pending_reply(
                "conductor-ops", "default", callback
            )
            pending = [
                task
                for task in asyncio.all_tasks()
                if task is not asyncio.current_task()
            ]
            await asyncio.gather(*pending)

            # One CLI invocation delivered the original message.  The pending
            # watcher only polled status/output and never submitted it again.
            assert run_cli.call_count == 1

    asyncio.run(scenario())
    assert delivered == ["late Codex answer"]
