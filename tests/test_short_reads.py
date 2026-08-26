from __future__ import annotations

import socket
import threading
import time

import pytest
from conftest import free_port, wait_until

from tcpconn._socketio import recv_exactly, recv_some, send_all
from tcpconn.errors import ConnectionLost, ReceiveTimeout
from tcpconn.framing import LengthPrefixFramer
from tcpconn.protocol import MessageKind, wrap_kind
from tcpconn.sync import ServerConnection, TcpClient, TcpServer


def test_client_reassembles_a_frame_written_one_byte_at_a_time() -> None:
    port = free_port()
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", port))
    listener.listen(1)

    framer = LengthPrefixFramer()
    body = wrap_kind(MessageKind.DATA, b"trickled payload")
    wire = framer.encode(body)

    def hostile_server() -> None:
        conn, _ = listener.accept()
        with conn:
            for i in range(len(wire)):
                conn.sendall(wire[i : i + 1])
                time.sleep(0.001)

    thread = threading.Thread(target=hostile_server, daemon=True)
    thread.start()
    try:
        with TcpClient("127.0.0.1", port) as client:
            assert client.receive(timeout=5.0) == b"trickled payload"
    finally:
        thread.join(timeout=5.0)
        listener.close()


def test_server_raises_connection_lost_on_truncated_header_close() -> None:
    captured: list[BaseException] = []

    def handler(conn: ServerConnection) -> None:
        try:
            conn.receive(timeout=5.0)
        except Exception as exc:
            captured.append(exc)

    server = TcpServer("127.0.0.1", 0, handler=handler)
    server.start()
    try:
        raw = socket.create_connection(server.address)
        raw.sendall((100).to_bytes(4, "big"))
        raw.close()
        wait_until(lambda: len(captured) == 1, timeout=5.0)
    finally:
        server.stop()
    assert len(captured) == 1
    assert isinstance(captured[0], ConnectionLost)


def test_recv_exactly_reassembles_short_writes() -> None:
    a, b = socket.socketpair()
    try:

        def writer() -> None:
            for chunk in (b"a", b"bc", b"def"):
                b.sendall(chunk)
                time.sleep(0.001)

        thread = threading.Thread(target=writer, daemon=True)
        thread.start()
        assert recv_exactly(a, 6) == b"abcdef"
        thread.join(timeout=5.0)
    finally:
        a.close()
        b.close()


def test_recv_exactly_raises_connection_lost_on_early_close() -> None:
    a, b = socket.socketpair()
    try:
        b.sendall(b"ab")
        b.close()
        with pytest.raises(ConnectionLost):
            recv_exactly(a, 10)
    finally:
        a.close()


def test_recv_exactly_raises_receive_timeout() -> None:
    a, b = socket.socketpair()
    try:
        with pytest.raises(ReceiveTimeout):
            recv_exactly(a, 10, deadline=time.monotonic() + 0.1)
    finally:
        a.close()
        b.close()


def test_send_all_and_recv_some_round_trip() -> None:
    a, b = socket.socketpair()
    try:
        send_all(a, b"payload")
        assert recv_some(b) == b"payload"
    finally:
        a.close()
        b.close()


def test_recv_some_raises_connection_lost_on_peer_close() -> None:
    a, b = socket.socketpair()
    try:
        b.close()
        with pytest.raises(ConnectionLost):
            recv_some(a)
    finally:
        a.close()
