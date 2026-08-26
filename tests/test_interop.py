from __future__ import annotations

import asyncio

from tcpconn.aio import AsyncServerConnection, AsyncTcpClient, AsyncTcpServer
from tcpconn.sync import ServerConnection, TcpClient, TcpServer


def _sync_echo(conn: ServerConnection) -> None:
    for message in conn:
        conn.send(message)


async def _async_echo(conn: AsyncServerConnection) -> None:
    async for message in conn:
        await conn.send(message)


async def test_async_client_against_sync_server() -> None:
    server = TcpServer("127.0.0.1", 0, handler=_sync_echo)
    server.start()
    try:
        async with AsyncTcpClient(*server.address) as client:
            await client.send(b"async client, sync server")
            assert await client.receive(timeout=2.0) == b"async client, sync server"
    finally:
        server.stop()


async def test_sync_client_against_async_server() -> None:
    server = AsyncTcpServer("127.0.0.1", 0, handler=_async_echo)
    await server.start()

    def _run_sync_client(address: tuple[str, int]) -> bytes:
        with TcpClient(*address) as client:
            client.send(b"sync client, async server")
            result: bytes = client.receive(timeout=2.0)
            return result

    try:
        received = await asyncio.to_thread(_run_sync_client, server.address)
        assert received == b"sync client, async server"
    finally:
        await server.stop()
