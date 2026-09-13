"""Regression coverage for Slack env references in the canonical bridge."""

from __future__ import annotations

from unittest import mock

import pytest

import bridge


def _write_slack_config(tmp_path, channel_id: str, allowed_users: list[str]):
    config = tmp_path / "config.toml"
    allowed = ", ".join(repr(value) for value in allowed_users)
    config.write_text(
        "[conductor.slack]\n"
        "bot_token = 'xoxb-test'\n"
        "app_token = 'xapp-test'\n"
        f"channel_id = {channel_id!r}\n"
        f"allowed_user_ids = [{allowed}]\n"
    )
    return config


def test_slack_channel_and_allowed_users_resolve_environment(tmp_path, monkeypatch):
    config = _write_slack_config(
        tmp_path, "$SLACK_CHANNEL", ["$SLACK_USER", "${SLACK_USER_2}"]
    )
    monkeypatch.setenv("SLACK_CHANNEL", "C012345")
    monkeypatch.setenv("SLACK_USER", "U111111")
    monkeypatch.setenv("SLACK_USER_2", "U222222")

    with mock.patch.object(bridge, "CONFIG_PATH", config), mock.patch(
        "bridge.discover_conductors", return_value=[{"name": "ops"}]
    ):
        loaded = bridge.load_config()["slack"]

    assert loaded["configured"] is True
    assert loaded["channel_id"] == "C012345"
    assert loaded["allowed_user_ids"] == ["U111111", "U222222"]


def test_unresolved_slack_channel_reference_disables_slack(tmp_path, monkeypatch):
    config = _write_slack_config(tmp_path, "$MISSING_SLACK_CHANNEL", [])
    monkeypatch.delenv("MISSING_SLACK_CHANNEL", raising=False)

    with mock.patch.object(bridge, "CONFIG_PATH", config), mock.patch(
        "bridge.discover_conductors", return_value=[{"name": "ops"}]
    ), pytest.raises(SystemExit):
        bridge.load_config()


def test_unresolved_allowed_user_reference_fails_closed(tmp_path, monkeypatch):
    config = _write_slack_config(
        tmp_path, "C012345", ["$MISSING_SLACK_ALLOWED_USER"]
    )
    monkeypatch.delenv("MISSING_SLACK_ALLOWED_USER", raising=False)

    with mock.patch.object(bridge, "CONFIG_PATH", config), mock.patch(
        "bridge.discover_conductors", return_value=[{"name": "ops"}]
    ):
        allowed_users = bridge.load_config()["slack"]["allowed_user_ids"]

    # The authorization contract is: an actually empty list means allow-all,
    # while every configured entry is compared literally. Keeping the failed
    # resolution as "" therefore denies all real Slack user IDs.
    assert allowed_users == [""]
    assert bool(allowed_users) is True
    assert "U111111" not in allowed_users
