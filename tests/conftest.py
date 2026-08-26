from __future__ import annotations

import socket
import threading
import time
from collections.abc import Callable, Iterator

import pytest

from tcpconn.sync import ServerConnection, TcpServer


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
        return port


def wait_until(predicate: Callable[[], bool], timeout: float = 5.0, interval: float = 0.01) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    if not predicate():
        raise AssertionError(f"condition not met within {timeout}s")


@pytest.fixture(autouse=True)
def no_thread_leaks() -> Iterator[None]:
    before = set(threading.enumerate())
    yield
    wait_until(lambda: set(threading.enumerate()) <= before, timeout=2.0)
    leaked = set(threading.enumerate()) - before
    assert not leaked, f"leaked threads: {[t.name for t in leaked]}"


def _echo_handler(conn: ServerConnection) -> None:
    for message in conn:
        conn.send(message)


@pytest.fixture
def echo_server() -> Iterator[TcpServer]:
    server = TcpServer("127.0.0.1", 0, handler=_echo_handler)
    server.start()
    try:
        yield server
    finally:
        server.stop()
