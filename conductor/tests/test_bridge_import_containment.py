"""Import-time containment regression for the embedded conductor bridge."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import bridge


def _is_beneath(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def test_bridge_import_log_path_is_inside_private_test_root() -> None:
    root = Path(os.environ["AGENT_DECK_ROOT"]).resolve().parent
    log_path = Path(bridge.LOG_PATH).resolve()

    assert _is_beneath(log_path, root)
    assert log_path == (
        Path(os.environ["AGENT_DECK_CONDUCTOR_DIR"]).resolve() / "bridge.log"
    )


def test_all_import_time_bridge_paths_are_private() -> None:
    private_root = Path(os.environ["AGENT_DECK_ROOT"]).resolve().parent
    for name in (
        "HOME",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "XDG_STATE_HOME",
        "XDG_CACHE_HOME",
        "XDG_RUNTIME_DIR",
        "AGENT_DECK_ROOT",
        "AGENT_DECK_CONDUCTOR_DIR",
        "CODEX_HOME",
        "CLAUDE_CONFIG_DIR",
        "TMPDIR",
        "TMUX_TMPDIR",
    ):
        assert _is_beneath(Path(os.environ[name]).resolve(), private_root), name

    assert Path(tempfile.gettempdir()).resolve() == Path(os.environ["TMPDIR"]).resolve()

    assert "TMUX" not in os.environ
    assert "TMUX_PANE" not in os.environ

    dbus_prefix = "unix:path="
    dbus_address = os.environ["DBUS_SESSION_BUS_ADDRESS"]
    assert dbus_address.startswith(dbus_prefix)
    dbus_socket = Path(dbus_address[len(dbus_prefix) :]).resolve()
    assert _is_beneath(dbus_socket, private_root)
    assert not dbus_socket.exists()


def test_containment_is_established_before_bridge_import() -> None:
    conftest_source = (Path(__file__).with_name("conftest.py")).read_text(
        encoding="utf-8"
    )
    bridge_import_call = conftest_source.rindex("\n_load_canonical_bridge()")

    for boundary in (
        'os.environ[_name] = str(_path)',
        'os.environ.pop("TMUX", None)',
        'os.environ.pop("TMUX_PANE", None)',
        'os.environ["DBUS_SESSION_BUS_ADDRESS"]',
    ):
        assert conftest_source.index(boundary) < bridge_import_call, boundary
