from __future__ import annotations

import threading
import time

from conftest import wait_until

from tcpconn.errors import ConnectionLost
from tcpconn.sync import ServerConnection, TcpClient, TcpServer


def _silent_handler(conn: ServerConnection) -> None:
    list(conn)


def test_server_stop_closes_all_clients_promptly() -> None:
    server = TcpServer("127.0.0.1", 0, handler=_silent_handler)
    server.start()
    clients = [TcpClient(*server.address) for _ in range(3)]
    for client in clients:
        client.connect()
    wait_until(lambda: len(server.connections) == 3)

    started = time.monotonic()
    server.stop(timeout=2.0)
    elapsed = time.monotonic() - started
    assert elapsed < 2.5

    for client in clients:
        deadline = time.monotonic() + 2.0
        while client.is_connected and time.monotonic() < deadline:
            time.sleep(0.01)
        assert not client.is_connected
        client.close()


def test_close_is_idempotent(echo_server: TcpServer) -> None:
    client = TcpClient(*echo_server.address)
    client.connect()
    client.close()
    client.close()
    assert not client.is_connected


def test_closing_client_mid_receive_unblocks_with_connection_lost(
    echo_server: TcpServer,
) -> None:
    client = TcpClient(*echo_server.address)
    client.connect()
    outcome: list[BaseException | object] = []

    def blocked_receive() -> None:
        try:
            outcome.append(client.receive(timeout=10.0))
        except Exception as exc:
            outcome.append(exc)

    thread = threading.Thread(target=blocked_receive, daemon=True)
    thread.start()
    wait_until(thread.is_alive)
    time.sleep(0.05)
    client.close()
    thread.join(timeout=5.0)
    assert not thread.is_alive()
    assert len(outcome) == 1
    assert isinstance(outcome[0], ConnectionLost)
