"""Test fixtures for the conductor bridge.

The canonical bridge script lives at ``internal/session/conductor_bridge.py``
(embedded into the binary via ``//go:embed``); there is no ``conductor/bridge.py``
checked into the repo. To keep these tests running against the one canonical
file — and to preserve the existing ``from bridge import ...`` /
``mock.patch("bridge.<attr>")`` usage in the test bodies — load that file under
the module name ``bridge`` before any test module is imported.
"""

from __future__ import annotations

import atexit
import importlib.util
import os
import shutil
import sys
import tempfile
from pathlib import Path


# The bridge configures its log path and file handler at import time.  Establish
# every path boundary before executing the module so collection itself cannot
# open an operator's bridge.log or discover live conductor state.
BRIDGE_TEST_ROOT = Path(
    tempfile.mkdtemp(prefix="agent-deck-conductor-tests-")
).resolve()
atexit.register(shutil.rmtree, BRIDGE_TEST_ROOT, ignore_errors=True)

_ISOLATED_ENV = {
    "HOME": BRIDGE_TEST_ROOT / "home",
    "XDG_CONFIG_HOME": BRIDGE_TEST_ROOT / "xdg-config",
    "XDG_DATA_HOME": BRIDGE_TEST_ROOT / "xdg-data",
    "XDG_STATE_HOME": BRIDGE_TEST_ROOT / "xdg-state",
    "XDG_CACHE_HOME": BRIDGE_TEST_ROOT / "xdg-cache",
    "XDG_RUNTIME_DIR": BRIDGE_TEST_ROOT / "xdg-runtime",
    "AGENT_DECK_ROOT": BRIDGE_TEST_ROOT / "agent-deck-root",
    "AGENT_DECK_CONDUCTOR_DIR": BRIDGE_TEST_ROOT / "agent-deck-root" / "conductor",
    "CODEX_HOME": BRIDGE_TEST_ROOT / "codex-home",
    "CLAUDE_CONFIG_DIR": BRIDGE_TEST_ROOT / "claude-config",
    "TMPDIR": BRIDGE_TEST_ROOT / "tmp",
    "TMUX_TMPDIR": BRIDGE_TEST_ROOT / "tmux-tmp",
}
for _name, _path in _ISOLATED_ENV.items():
    _path.mkdir(parents=True, exist_ok=True)
    os.environ[_name] = str(_path)
# mkdtemp() above initializes tempfile's process-global cache before TMPDIR is
# replaced, so reset it as well as the environment variable.
tempfile.tempdir = str(_ISOLATED_ENV["TMPDIR"])

# An inherited tmux client context can override TMUX_TMPDIR.  The private,
# deliberately nonexistent D-Bus socket likewise prevents subprocesses from
# reaching the real user service manager even if a test misses a mock.
os.environ.pop("TMUX", None)
os.environ.pop("TMUX_PANE", None)
_PRIVATE_DBUS_SOCKET = BRIDGE_TEST_ROOT / "nonexistent-session-bus.sock"
os.environ["DBUS_SESSION_BUS_ADDRESS"] = f"unix:path={_PRIVATE_DBUS_SOCKET}"

# repo_root/conductor/tests/conftest.py -> repo_root/internal/session/conductor_bridge.py
_CANONICAL = (
    Path(__file__).resolve().parents[2]
    / "internal"
    / "session"
    / "conductor_bridge.py"
)


def _load_canonical_bridge() -> None:
    if "bridge" in sys.modules:
        raise RuntimeError(
            "bridge test containment failure: bridge was imported before "
            "conductor/tests/conftest.py established its private environment"
        )
    if not _CANONICAL.is_file():
        raise FileNotFoundError(
            f"canonical bridge source not found at {_CANONICAL}; "
            "it should live at internal/session/conductor_bridge.py"
        )
    spec = importlib.util.spec_from_file_location("bridge", _CANONICAL)
    module = importlib.util.module_from_spec(spec)
    # Register before exec so the module can be patched/imported as "bridge".
    sys.modules["bridge"] = module
    spec.loader.exec_module(module)

    log_path = Path(module.LOG_PATH).resolve()
    try:
        log_path.relative_to(BRIDGE_TEST_ROOT)
    except ValueError as exc:
        # This is a collection failure by design: continuing would allow even
        # otherwise mocked tests to append synthetic records to a live log.
        raise RuntimeError(
            f"bridge test containment failure: LOG_PATH {log_path} is outside "
            f"private root {BRIDGE_TEST_ROOT}"
        ) from exc


_load_canonical_bridge()
