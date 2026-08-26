from __future__ import annotations

import threading

import pytest
from conftest import free_port, wait_until

from tcpconn.backoff import ExponentialBackoff, ReconnectPolicy
from tcpconn.errors import ReconnectFailed
from tcpconn.sync import ServerConnection, TcpClient, TcpServer


def _echo(conn: ServerConnection) -> None:
    for message in conn:
        conn.send(message)


def test_client_reconnects_after_server_restart_on_same_port() -> None:
    port = free_port()
    server = TcpServer("127.0.0.1", port, handler=_echo)
    server.start()

    attempts: list[int] = []
    lock = threading.Lock()

    def on_reconnect(attempt: int) -> None:
        with lock:
            attempts.append(attempt)

    policy = ReconnectPolicy(
        backoff=ExponentialBackoff(initial=0.05, multiplier=1.5, maximum=0.3, jitter=0.1),
        on_reconnect=on_reconnect,
    )
    client = TcpClient("127.0.0.1", port, reconnect=policy)
    client.connect()
    try:
        client.send(b"before")
        assert client.receive(timeout=2.0) == b"before"

        server.stop()
        wait_until(lambda: not client.is_connected, timeout=5.0)

        server = TcpServer("127.0.0.1", port, handler=_echo)
        server.start()
        wait_until(lambda: client.is_connected, timeout=5.0)

        client.send(b"after")
        assert client.receive(timeout=2.0) == b"after"
        with lock:
            assert attempts, "on_reconnect never fired"
            assert attempts == sorted(attempts)
    finally:
        client.close()
        server.stop()


def test_reconnect_failed_after_exhausting_max_attempts() -> None:
    port = free_port()
    server = TcpServer("127.0.0.1", port, handler=_echo)
    server.start()
    policy = ReconnectPolicy(
        backoff=ExponentialBackoff(initial=0.02, maximum=0.05, max_attempts=2),
    )
    client = TcpClient("127.0.0.1", port, reconnect=policy)
    client.connect()
    try:
        server.stop()
        with pytest.raises(ReconnectFailed):
            client.receive(timeout=5.0)
    finally:
        client.close()
