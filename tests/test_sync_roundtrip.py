from __future__ import annotations

import socket

import pytest
from conftest import free_port, wait_until

from tcpconn.errors import NotConnected, ReceiveTimeout
from tcpconn.framing import LengthPrefixFramer
from tcpconn.serializers import JsonCodec, RawCodec
from tcpconn.sync import ServerConnection, TcpClient, TcpServer


def test_echo_round_trip(echo_server: TcpServer) -> None:
    with TcpClient(*echo_server.address) as client:
        client.send(b"hello")
        assert client.receive(timeout=2.0) == b"hello"


def test_large_payload_proves_short_read_handling(echo_server: TcpServer) -> None:
    payload = bytes(range(256)) * (4 * 1024 * 1024 // 256)
    with TcpClient(*echo_server.address) as client:
        client.send(payload)
        assert client.receive(timeout=10.0) == payload


def test_many_small_messages_preserve_order_and_boundaries(echo_server: TcpServer) -> None:
    messages = [str(i).encode() for i in range(500)]
    with TcpClient(*echo_server.address) as client:
        for message in messages:
            client.send(message)
        received = [client.receive(timeout=5.0) for _ in messages]
    assert received == messages


def test_receive_timeout_on_silent_server(echo_server: TcpServer) -> None:
    with TcpClient(*echo_server.address) as client, pytest.raises(ReceiveTimeout):
        client.receive(timeout=0.2)


def test_send_before_connect_raises_not_connected() -> None:
    client = TcpClient("127.0.0.1", 1)
    with pytest.raises(NotConnected):
        client.send(b"x")


def test_context_manager_closes_socket_on_exit(echo_server: TcpServer) -> None:
    with TcpClient(*echo_server.address) as client:
        assert client.is_connected
    assert not client.is_connected


def test_server_broadcast_reaches_all_clients() -> None:
    def handler(conn: ServerConnection) -> None:
        list(conn)

    server = TcpServer("127.0.0.1", 0, handler=handler)
    server.start()
    try:
        clients = [TcpClient(*server.address) for _ in range(3)]
        for client in clients:
            client.connect()
        wait_until(lambda: len(server.connections) == 3)
        count = server.broadcast(b"hi everyone")
        assert count == 3
        for client in clients:
            assert client.receive(timeout=2.0) == b"hi everyone"
        for client in clients:
            client.close()
    finally:
        server.stop()


def test_json_codec_carries_dict_unchanged() -> None:
    def handler(conn: ServerConnection) -> None:
        for message in conn:
            conn.send(message)

    server = TcpServer("127.0.0.1", 0, handler=handler, codec=JsonCodec())
    server.start()
    try:
        with TcpClient(*server.address, codec=JsonCodec()) as client:
            obj = {"a": 1, "b": [1, 2, 3], "c": {"nested": True}}
            client.send(obj)
            assert client.receive(timeout=2.0) == obj
    finally:
        server.stop()


def test_request_sends_and_waits_for_reply(echo_server: TcpServer) -> None:
    with TcpClient(*echo_server.address) as client:
        assert client.request(b"ping", timeout=2.0) == b"ping"


def test_client_iter_yields_messages_and_stops_on_connection_lost(
    echo_server: TcpServer,
) -> None:
    client = TcpClient(*echo_server.address)
    client.connect()
    client.send(b"one")
    client.send(b"two")
    iterator = iter(client)
    assert next(iterator) == b"one"
    assert next(iterator) == b"two"
    client.close()
    with pytest.raises(StopIteration):
        next(iterator)


def test_local_address_reports_bound_port(echo_server: TcpServer) -> None:
    with TcpClient(*echo_server.address) as client:
        host, port = client.local_address
        assert host
        assert port > 0


def test_local_address_before_connect_raises_not_connected() -> None:
    client = TcpClient("127.0.0.1", 1)
    with pytest.raises(NotConnected):
        _ = client.local_address


def test_control_channel_false_round_trips_raw_payload() -> None:
    def handler(conn: ServerConnection) -> None:
        for message in conn:
            conn.send(message)

    server = TcpServer("127.0.0.1", 0, handler=handler, control_channel=False)
    server.start()
    try:
        with TcpClient(*server.address, control_channel=False) as client:
            client.send(b"raw-payload")
            assert client.receive(timeout=2.0) == b"raw-payload"
    finally:
        server.stop()


def test_server_context_manager_stops_on_exit() -> None:
    def handler(conn: ServerConnection) -> None:
        list(conn)

    with TcpServer("127.0.0.1", 0, handler=handler) as server, TcpClient(*server.address) as client:
        assert client.is_connected
    assert server.connections == ()


def test_server_address_before_start_raises_not_connected() -> None:
    server = TcpServer("127.0.0.1", 0)
    with pytest.raises(NotConnected):
        _ = server.address


def test_server_without_reuse_address_still_binds() -> None:
    def handler(conn: ServerConnection) -> None:
        list(conn)

    server = TcpServer("127.0.0.1", free_port(), handler=handler, reuse_address=False)
    server.start()
    try:
        wait_until(lambda: server.address[1] > 0)
    finally:
        server.stop()


def test_server_rejects_connections_beyond_max_connections() -> None:
    def handler(conn: ServerConnection) -> None:
        list(conn)

    server = TcpServer("127.0.0.1", 0, handler=handler, max_connections=1)
    server.start()
    try:
        first = TcpClient(*server.address)
        first.connect()
        wait_until(lambda: len(server.connections) == 1)

        second = TcpClient(*server.address)
        second.connect()
        wait_until(lambda: not second.is_connected)

        first.close()
        second.close()
    finally:
        server.stop()


def test_broadcast_skips_connections_that_fail_to_send() -> None:
    server = TcpServer("127.0.0.1", 0)
    left, right = socket.socketpair()
    conn = ServerConnection(
        left, framer=LengthPrefixFramer(), codec=RawCodec(), control_channel=True
    )
    right.close()
    left.close()
    server._connections.append(conn)
    assert server.broadcast(b"x") == 0
