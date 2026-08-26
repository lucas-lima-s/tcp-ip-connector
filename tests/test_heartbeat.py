from __future__ import annotations

import asyncio
import socket
import threading

import pytest
from conftest import free_port

from tcpconn.aio import AsyncServerConnection, AsyncTcpClient, AsyncTcpServer
from tcpconn.errors import ConnectionLost, ReceiveTimeout
from tcpconn.heartbeat import HeartbeatConfig
from tcpconn.sync import ServerConnection, TcpClient, TcpServer


def _silent_handler(conn: ServerConnection) -> None:
    list(conn)


def test_heartbeat_pings_are_answered_and_never_reach_the_inbox() -> None:
    server = TcpServer("127.0.0.1", 0, handler=_silent_handler)
    server.start()
    try:
        client = TcpClient(
            *server.address,
            heartbeat=HeartbeatConfig(interval=0.1, timeout=0.5),
        )
        client.connect()
        try:
            with pytest.raises(ReceiveTimeout):
                client.receive(timeout=0.4)
            assert client.is_connected
        finally:
            client.close()
    finally:
        server.stop()


def test_heartbeat_timeout_raises_connection_lost_when_peer_stops_answering() -> None:
    port = free_port()
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", port))
    listener.listen(1)

    def hostile_server() -> None:
        conn, _ = listener.accept()
        with conn:
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    return

    thread = threading.Thread(target=hostile_server, daemon=True)
    thread.start()
    try:
        client = TcpClient("127.0.0.1", port, heartbeat=HeartbeatConfig(interval=0.05, timeout=0.2))
        client.connect()
        try:
            with pytest.raises(ConnectionLost):
                client.receive(timeout=2.0)
        finally:
            client.close()
    finally:
        thread.join(timeout=5.0)
        listener.close()


def test_control_channel_false_with_heartbeat_raises_value_error() -> None:
    with pytest.raises(ValueError, match="heartbeat"):
        TcpClient(
            "127.0.0.1",
            1,
            control_channel=False,
            heartbeat=HeartbeatConfig(),
        )


def test_heartbeat_config_requires_timeout_greater_than_interval() -> None:
    with pytest.raises(ValueError, match="timeout"):
        HeartbeatConfig(interval=1.0, timeout=1.0)
    with pytest.raises(ValueError, match="timeout"):
        HeartbeatConfig(interval=1.0, timeout=0.5)


async def _async_silent_handler(conn: AsyncServerConnection) -> None:
    async for _ in conn:
        pass


async def test_async_heartbeat_pings_are_answered_and_never_reach_the_inbox() -> None:
    server = AsyncTcpServer("127.0.0.1", 0, handler=_async_silent_handler)
    await server.start()
    try:
        client = AsyncTcpClient(
            *server.address,
            heartbeat=HeartbeatConfig(interval=0.1, timeout=0.5),
        )
        await client.connect()
        try:
            with pytest.raises(ReceiveTimeout):
                await client.receive(timeout=0.4)
            assert client.is_connected
        finally:
            await client.aclose()
    finally:
        await server.stop()


async def test_async_heartbeat_timeout_raises_connection_lost_when_peer_stops_answering() -> None:
    async def hostile_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while True:
            chunk = await reader.read(4096)
            if not chunk:
                writer.close()
                return

    raw_server = await asyncio.start_server(hostile_handler, "127.0.0.1", 0)
    try:
        host, port = raw_server.sockets[0].getsockname()[:2]
        client = AsyncTcpClient(host, port, heartbeat=HeartbeatConfig(interval=0.05, timeout=0.2))
        await client.connect()
        try:
            with pytest.raises(ConnectionLost):
                await client.receive(timeout=2.0)
        finally:
            await client.aclose()
    finally:
        raw_server.close()
        await raw_server.wait_closed()
