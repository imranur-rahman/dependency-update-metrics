"""Shared setup for the Host tests.

The tests build the app with ``create_app`` and inject a fake analysis function and a
fake Turnstile check. The queue and the worker run in the same process, so the tests
need no Docker and no network.
"""

import socket
import sys
from pathlib import Path

import pytest

HOST_DIR = Path(__file__).resolve().parents[1]

if str(HOST_DIR) not in sys.path:
    sys.path.insert(0, str(HOST_DIR))


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    def _blocked(*args, **kwargs):
        raise RuntimeError("network access is not allowed in Host tests")

    monkeypatch.setattr(socket.socket, "connect", _blocked)
