from __future__ import annotations

import asyncio

import pytest

from tcpconn.aio import AsyncServerConnection, AsyncTcpClient, AsyncTcpServer
from tcpconn.errors import FrameError, NotConnected, ReceiveTimeout
from tcpconn.framing import LengthPrefixFramer
from tcpconn.heartbeat import HeartbeatConfig
from tcpconn.serializers import JsonCodec


async def _echo(conn: AsyncServerConnection) -> None:
    async for message in conn:
        await conn.send(message)


async def test_async_echo_round_trip() -> None:
    server = AsyncTcpServer("127.0.0.1", 0, handler=_echo)
    await server.start()
    try:
        async with AsyncTcpClient(*server.address) as client:
            await client.send(b"hello")
            assert await client.receive(timeout=2.0) == b"hello"
    finally:
        await server.stop()


async def test_async_large_payload() -> None:
    server = AsyncTcpServer("127.0.0.1", 0, handler=_echo)
    await server.start()
    payload = bytes(range(256)) * (2 * 1024 * 1024 // 256)
    try:
        async with AsyncTcpClient(*server.address) as client:
            await client.send(payload)
            assert await client.receive(timeout=10.0) == payload
    finally:
        await server.stop()


async def test_async_receive_timeout() -> None:
    async def silent(conn: AsyncServerConnection) -> None:
        async for _ in conn:
            pass

    server = AsyncTcpServer("127.0.0.1", 0, handler=silent)
    await server.start()
    try:
        async with AsyncTcpClient(*server.address) as client:
            with pytest.raises(ReceiveTimeout):
                await client.receive(timeout=0.2)
    finally:
        await server.stop()


async def test_async_send_before_connect_raises_not_connected() -> None:
    client = AsyncTcpClient("127.0.0.1", 1)
    with pytest.raises(NotConnected):
        await client.send(b"x")


async def test_async_context_manager_closes_on_exit() -> None:
    server = AsyncTcpServer("127.0.0.1", 0, handler=_echo)
    await server.start()
    try:
        async with AsyncTcpClient(*server.address) as client:
            assert client.is_connected
        assert not client.is_connected
    finally:
        await server.stop()


async def test_async_broadcast_reaches_all_clients() -> None:
    async def silent(conn: AsyncServerConnection) -> None:
        async for _ in conn:
            pass

    server = AsyncTcpServer("127.0.0.1", 0, handler=silent)
    await server.start()
    try:
        clients = [AsyncTcpClient(*server.address) for _ in range(3)]
        for client in clients:
            await client.connect()
        for _ in range(50):
            if len(server.connections) == 3:
                break
            await asyncio.sleep(0.05)
        assert len(server.connections) == 3
        count = await server.broadcast(b"hi everyone")
        assert count == 3
        for client in clients:
            assert await client.receive(timeout=2.0) == b"hi everyone"
        for client in clients:
            await client.aclose()
    finally:
        await server.stop()


async def test_async_json_codec_round_trip() -> None:
    server = AsyncTcpServer("127.0.0.1", 0, handler=_echo, codec=JsonCodec())
    await server.start()
    try:
        async with AsyncTcpClient(*server.address, codec=JsonCodec()) as client:
            obj = {"a": 1, "b": [1, 2, 3]}
            await client.send(obj)
            assert await client.receive(timeout=2.0) == obj
    finally:
        await server.stop()


async def test_async_request_round_trip() -> None:
    server = AsyncTcpServer("127.0.0.1", 0, handler=_echo)
    await server.start()
    try:
        async with AsyncTcpClient(*server.address) as client:
            assert await client.request(b"ping", timeout=2.0) == b"ping"
    finally:
        await server.stop()


async def test_async_client_iter_yields_messages() -> None:
    server = AsyncTcpServer("127.0.0.1", 0, handler=_echo)
    await server.start()
    try:
        client = AsyncTcpClient(*server.address)
        await client.connect()
        await client.send(b"one")
        await client.send(b"two")
        received = []
        async for message in client:
            received.append(message)
            if len(received) == 2:
                break
        assert received == [b"one", b"two"]
        await client.aclose()
    finally:
        await server.stop()


async def test_async_local_address_reports_bound_port() -> None:
    server = AsyncTcpServer("127.0.0.1", 0, handler=_echo)
    await server.start()
    try:
        async with AsyncTcpClient(*server.address) as client:
            host, port = client.local_address
            assert host
            assert port > 0
    finally:
        await server.stop()


async def test_async_local_address_before_connect_raises_not_connected() -> None:
    client = AsyncTcpClient("127.0.0.1", 1)
    with pytest.raises(NotConnected):
        _ = client.local_address


def test_async_control_channel_false_with_heartbeat_raises_value_error() -> None:
    with pytest.raises(ValueError, match="heartbeat"):
        AsyncTcpClient(
            "127.0.0.1",
            1,
            control_channel=False,
            heartbeat=HeartbeatConfig(),
        )


async def test_async_double_aclose_is_noop() -> None:
    server = AsyncTcpServer("127.0.0.1", 0, handler=_echo)
    await server.start()
    try:
        client = AsyncTcpClient(*server.address)
        await client.connect()
        await client.aclose()
        await client.aclose()
        assert not client.is_connected
    finally:
        await server.stop()


async def test_async_control_channel_false_round_trip() -> None:
    server = AsyncTcpServer("127.0.0.1", 0, handler=_echo, control_channel=False)
    await server.start()
    try:
        async with AsyncTcpClient(*server.address, control_channel=False) as client:
            await client.send(b"raw-payload")
            assert await client.receive(timeout=2.0) == b"raw-payload"
    finally:
        await server.stop()


def test_async_server_address_before_start_raises_not_connected() -> None:
    server = AsyncTcpServer("127.0.0.1", 0)
    with pytest.raises(NotConnected):
        _ = server.address


async def test_async_server_context_manager_stops_on_exit() -> None:
    async with (
        AsyncTcpServer("127.0.0.1", 0, handler=_echo) as server,
        AsyncTcpClient(*server.address) as client,
    ):
        assert client.is_connected
    assert server.connections == ()


async def test_async_server_rejects_connections_beyond_max_connections() -> None:
    async def silent(conn: AsyncServerConnection) -> None:
        async for _ in conn:
            pass

    server = AsyncTcpServer("127.0.0.1", 0, handler=silent, max_connections=1)
    await server.start()
    try:
        first = AsyncTcpClient(*server.address)
        await first.connect()
        for _ in range(50):
            if len(server.connections) == 1:
                break
            await asyncio.sleep(0.05)
        assert len(server.connections) == 1

        second = AsyncTcpClient(*server.address)
        await second.connect()
        for _ in range(50):
            if not second.is_connected:
                break
            await asyncio.sleep(0.05)
        assert not second.is_connected

        await first.aclose()
        await second.aclose()
    finally:
        await server.stop()


async def test_async_server_connection_receive_timeout() -> None:
    received_timeout = asyncio.Event()

    async def handler(conn: AsyncServerConnection) -> None:
        try:
            await conn.receive(timeout=0.2)
        except ReceiveTimeout:
            received_timeout.set()

    server = AsyncTcpServer("127.0.0.1", 0, handler=handler)
    await server.start()
    try:
        async with AsyncTcpClient(*server.address):
            await asyncio.wait_for(received_timeout.wait(), timeout=2.0)
    finally:
        await server.stop()


async def test_async_unknown_control_kind_raises_frame_error() -> None:
    framer = LengthPrefixFramer()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        del reader
        writer.write(framer.encode(bytes([0xFF]) + b"data"))
        await writer.drain()
        await asyncio.sleep(1.0)
        writer.close()

    raw_server = await asyncio.start_server(handle, "127.0.0.1", 0)
    try:
        host, port = raw_server.sockets[0].getsockname()[:2]
        client = AsyncTcpClient(host, port)
        await client.connect()
        try:
            with pytest.raises(FrameError):
                await client.receive(timeout=2.0)
        finally:
            await client.aclose()
    finally:
        raw_server.close()
        await raw_server.wait_closed()
