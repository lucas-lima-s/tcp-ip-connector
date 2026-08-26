from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, Self

from tcpconn.errors import ConnectionLost, NotConnected, ReceiveTimeout
from tcpconn.framing import LengthPrefixFramer
from tcpconn.protocol import Codec, Framer, MessageKind, unwrap_kind, wrap_kind
from tcpconn.serializers import RawCodec


class AsyncServerConnection:
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        framer: Framer,
        codec: Codec,
        control_channel: bool,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._framer = framer
        self._codec = codec
        self._control_channel = control_channel
        self._decoder = framer.decoder()
        self._pending: list[bytes] = []
        self._write_lock = asyncio.Lock()
        self._closed = False
        self.peer: tuple[str, int] = writer.get_extra_info("peername")

    async def send(self, message: Any) -> None:
        payload = self._codec.dumps(message)
        body = wrap_kind(MessageKind.DATA, payload) if self._control_channel else payload
        wire = self._framer.encode(body)
        async with self._write_lock:
            try:
                self._writer.write(wire)
                await self._writer.drain()
            except (ConnectionResetError, OSError) as exc:
                raise ConnectionLost(f"send to {self.peer} failed: {exc}") from exc

    async def receive(self, timeout: float | None = None) -> Any:
        deadline = None
        loop = asyncio.get_running_loop()
        if timeout is not None:
            deadline = loop.time() + timeout
        while True:
            if self._pending:
                frame = self._pending.pop(0)
            else:
                remaining = None
                if deadline is not None:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        raise ReceiveTimeout(f"no message received from {self.peer} in time")
                frame = await self._read_frame(remaining)
            if self._control_channel:
                kind, payload = unwrap_kind(frame)
            else:
                kind, payload = MessageKind.DATA, frame
            if kind is MessageKind.PING:
                await self._reply_pong()
                continue
            if kind is MessageKind.PONG:
                continue
            return self._codec.loads(payload)

    async def _read_frame(self, remaining: float | None) -> bytes:
        async def _once() -> bytes:
            while True:
                try:
                    chunk = await self._reader.read(65536)
                except (ConnectionResetError, OSError) as exc:
                    raise ConnectionLost(f"connection to {self.peer} lost: {exc}") from exc
                if chunk == b"":
                    raise ConnectionLost(f"peer {self.peer} closed the connection")
                frames = self._decoder.feed(chunk)
                if frames:
                    self._pending.extend(frames)
                    return self._pending.pop(0)

        if remaining is None:
            return await _once()
        try:
            async with asyncio.timeout(remaining):
                return await _once()
        except TimeoutError:
            raise ReceiveTimeout(f"no message received from {self.peer} in time") from None

    async def _reply_pong(self) -> None:
        body = wrap_kind(MessageKind.PONG, b"") if self._control_channel else b""
        wire = self._framer.encode(body)
        async with self._write_lock:
            self._writer.write(wire)
            await self._writer.drain()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._writer.close()
        with contextlib.suppress(OSError):
            await self._writer.wait_closed()

    async def __aiter__(self) -> AsyncIterator[Any]:
        while True:
            try:
                yield await self.receive()
            except ConnectionLost:
                return


class AsyncTcpServer:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 0,
        *,
        handler: Callable[[AsyncServerConnection], Awaitable[None]] | None = None,
        framer: Framer | None = None,
        codec: Codec | None = None,
        control_channel: bool = True,
        backlog: int = 50,
        max_connections: int | None = None,
        reuse_address: bool = True,
        logger: logging.Logger | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self._handler = handler
        self._framer: Framer = framer if framer is not None else LengthPrefixFramer()
        self._codec: Codec = codec if codec is not None else RawCodec()
        self._control_channel = control_channel
        self._backlog = backlog
        self._max_connections = max_connections
        self._reuse_address = reuse_address
        self._logger = logger if logger is not None else logging.getLogger(__name__)

        self._server: asyncio.Server | None = None
        self._run_task: asyncio.Task[None] | None = None
        self._tg: asyncio.TaskGroup | None = None
        self._started_event: asyncio.Event | None = None
        self._stop_event: asyncio.Event | None = None
        self._connections: list[AsyncServerConnection] = []

    async def start(self) -> None:
        self._started_event = asyncio.Event()
        self._stop_event = asyncio.Event()
        self._run_task = asyncio.create_task(self._run())
        await self._started_event.wait()

    async def _run(self) -> None:
        started_event = self._started_event
        if started_event is None:
            return
        async with asyncio.TaskGroup() as tg:
            self._tg = tg
            server = await asyncio.start_server(
                self._on_client,
                self.host,
                self.port,
                backlog=self._backlog,
                reuse_address=self._reuse_address,
            )
            self._server = server
            started_event.set()
            async with server:
                await server.serve_forever()

    async def _on_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self._max_connections is not None and len(self._connections) >= self._max_connections:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()
            return
        conn = AsyncServerConnection(
            reader,
            writer,
            framer=self._framer,
            codec=self._codec,
            control_channel=self._control_channel,
        )
        self._connections.append(conn)
        if self._tg is not None:
            self._tg.create_task(self._run_connection(conn))

    async def _run_connection(self, conn: AsyncServerConnection) -> None:
        try:
            if self._handler is not None:
                await self._handler(conn)
        except ConnectionLost:
            pass
        finally:
            await conn.aclose()
            if conn in self._connections:
                self._connections.remove(conn)

    async def serve_forever(self) -> None:
        if self._stop_event is not None:
            await self._stop_event.wait()

    async def stop(self, timeout: float = 5.0) -> None:
        if self._stop_event is not None:
            self._stop_event.set()
        task = self._run_task
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, TimeoutError):
            await asyncio.wait_for(task, timeout=timeout)
        for conn in list(self._connections):
            await conn.aclose()
        self._connections.clear()

    async def broadcast(self, message: Any) -> int:
        count = 0
        for conn in tuple(self._connections):
            try:
                await conn.send(message)
            except (ConnectionLost, NotConnected, OSError):
                continue
            count += 1
        return count

    @property
    def address(self) -> tuple[str, int]:
        if self._server is None or not self._server.sockets:
            raise NotConnected("server has not been started")
        addr = self._server.sockets[0].getsockname()
        return addr[0], addr[1]

    @property
    def connections(self) -> tuple[AsyncServerConnection, ...]:
        return tuple(self._connections)

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()
