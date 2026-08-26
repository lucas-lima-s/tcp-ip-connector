from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
import time
from collections.abc import AsyncIterator
from typing import Any, Self

from tcpconn.backoff import ReconnectPolicy
from tcpconn.errors import (
    ConnectionLost,
    FrameError,
    NotConnected,
    ReceiveTimeout,
    ReconnectFailed,
    SerializationError,
)
from tcpconn.framing import LengthPrefixFramer
from tcpconn.heartbeat import HeartbeatConfig
from tcpconn.protocol import Codec, Framer, MessageKind, unwrap_kind, wrap_kind
from tcpconn.serializers import RawCodec

_HEARTBEAT_TICK = 0.02


class AsyncTcpClient:
    def __init__(
        self,
        host: str,
        port: int,
        *,
        framer: Framer | None = None,
        codec: Codec | None = None,
        control_channel: bool = True,
        connect_timeout: float = 10.0,
        recv_timeout: float | None = None,
        reconnect: ReconnectPolicy | None = None,
        heartbeat: HeartbeatConfig | None = None,
        tcp_nodelay: bool = True,
        logger: logging.Logger | None = None,
    ) -> None:
        if not control_channel and heartbeat is not None:
            raise ValueError("heartbeat requires control_channel=True")
        self.host = host
        self.port = port
        self._framer: Framer = framer if framer is not None else LengthPrefixFramer()
        self._codec: Codec = codec if codec is not None else RawCodec()
        self._control_channel = control_channel
        self._connect_timeout = connect_timeout
        self._recv_timeout = recv_timeout
        self._reconnect_policy = reconnect
        self._heartbeat = heartbeat
        self._tcp_nodelay = tcp_nodelay
        self._logger = logger if logger is not None else logging.getLogger(__name__)

        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._decoder = self._framer.decoder()
        self._inbox: asyncio.Queue[Any] = asyncio.Queue()
        self._write_lock = asyncio.Lock()
        self._connected = False
        self._failure: BaseException | None = None
        self._closing = False
        self._last_write_time = time.monotonic()
        self._last_liveness = time.monotonic()
        self._reader_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._force_disconnect_requested = False

    async def connect(self) -> None:
        await self._do_connect()
        self._closing = False
        self._reader_task = asyncio.create_task(self._read_loop())
        if self._heartbeat is not None:
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _do_connect(self) -> None:
        async with asyncio.timeout(self._connect_timeout):
            reader, writer = await asyncio.open_connection(self.host, self.port)
        if self._tcp_nodelay:
            sock = writer.get_extra_info("socket")
            if sock is not None:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._reader = reader
        self._writer = writer
        self._decoder = self._framer.decoder()
        now = time.monotonic()
        self._last_write_time = now
        self._last_liveness = now
        self._connected = True
        self._failure = None

    async def aclose(self) -> None:
        self._closing = True
        writer = self._writer
        if writer is not None:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()
        self._connected = False
        current = asyncio.current_task()
        for task in (self._reader_task, self._heartbeat_task):
            if task is not None and task is not current:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def send(self, message: Any) -> None:
        if not self.is_connected:
            raise NotConnected(f"not connected to {self.host}:{self.port}")
        payload = self._codec.dumps(message)
        await self._send_kind(MessageKind.DATA, payload)

    async def _send_kind(self, kind: MessageKind, payload: bytes = b"") -> None:
        body = wrap_kind(kind, payload) if self._control_channel else payload
        wire = self._framer.encode(body)
        writer = self._writer
        if writer is None:
            raise NotConnected(f"not connected to {self.host}:{self.port}")
        async with self._write_lock:
            try:
                writer.write(wire)
                await writer.drain()
            except (ConnectionResetError, OSError) as exc:
                raise ConnectionLost(f"send to {self.host}:{self.port} failed: {exc}") from exc
        self._last_write_time = time.monotonic()

    async def receive(self, timeout: float | None = None) -> Any:
        effective = timeout if timeout is not None else self._recv_timeout
        try:
            if effective is None:
                return await self._inbox.get()
            async with asyncio.timeout(effective):
                return await self._inbox.get()
        except TimeoutError:
            if self._failure is not None and self._inbox.empty():
                failure = self._failure
                raise failure from None
            raise ReceiveTimeout(
                f"no message received from {self.host}:{self.port} within {effective!r}s"
            ) from None

    async def request(self, message: Any, timeout: float | None = None) -> Any:
        await self.send(message)
        return await self.receive(timeout)

    async def __aiter__(self) -> AsyncIterator[Any]:
        while True:
            try:
                yield await self.receive()
            except ReceiveTimeout:
                continue
            except ConnectionLost:
                return

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def local_address(self) -> tuple[str, int]:
        if self._writer is None:
            raise NotConnected(f"not connected to {self.host}:{self.port}")
        addr = self._writer.get_extra_info("sockname")
        return addr[0], addr[1]

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def _read_loop(self) -> None:
        while not self._closing:
            try:
                await self._read_until_broken()
                last_error: BaseException = ConnectionLost("socket was closed")
            except (ConnectionLost, FrameError, SerializationError) as exc:
                last_error = exc
            except asyncio.CancelledError:
                if not self._force_disconnect_requested:
                    raise
                self._force_disconnect_requested = False
                last_error = ConnectionLost(
                    f"connection to {self.host}:{self.port} forcibly closed (heartbeat timeout)"
                )
            self._connected = False
            if self._closing:
                self._failure = last_error
                return
            if self._reconnect_policy is None:
                self._failure = last_error
                return
            if not await self._reconnect():
                return

    async def _read_until_broken(self) -> None:
        reader = self._reader
        if reader is None:
            raise ConnectionLost("socket was closed")
        while True:
            try:
                chunk = await reader.read(65536)
            except (ConnectionResetError, OSError) as exc:
                raise ConnectionLost(f"connection to {self.host}:{self.port} lost: {exc}") from exc
            if chunk == b"":
                raise ConnectionLost(f"peer {self.host}:{self.port} closed the connection")
            for frame in self._decoder.feed(chunk):
                await self._handle_frame(frame)

    async def _handle_frame(self, frame: bytes) -> None:
        self._last_liveness = time.monotonic()
        if self._control_channel:
            kind, payload = unwrap_kind(frame)
        else:
            kind, payload = MessageKind.DATA, frame
        if kind is MessageKind.PING:
            await self._send_kind(MessageKind.PONG)
            return
        if kind is MessageKind.PONG:
            return
        message = self._codec.loads(payload)
        await self._inbox.put(message)

    async def _reconnect(self) -> bool:
        policy = self._reconnect_policy
        if policy is None:
            return False
        for attempt, delay in enumerate(policy.backoff.delays(), start=1):
            await asyncio.sleep(delay)
            if self._closing:
                return False
            try:
                await self._do_connect()
            except OSError:
                continue
            else:
                if policy.on_reconnect is not None:
                    policy.on_reconnect(attempt)
                return True
        self._failure = ReconnectFailed(f"failed to reconnect to {self.host}:{self.port}")
        return False

    async def _heartbeat_loop(self) -> None:
        config = self._heartbeat
        if config is None:
            return
        while not self._closing:
            await asyncio.sleep(_HEARTBEAT_TICK)
            if not self.is_connected:
                continue
            now = time.monotonic()
            if now - self._last_liveness > config.timeout:
                await self._force_disconnect()
                continue
            if now - self._last_write_time >= config.interval:
                with contextlib.suppress(ConnectionLost, NotConnected):
                    await self._send_kind(MessageKind.PING)

    async def _force_disconnect(self) -> None:
        writer = self._writer
        if writer is None:
            return
        current = asyncio.current_task()
        reader_task = self._reader_task
        if reader_task is not None and reader_task is not current and not reader_task.done():
            self._force_disconnect_requested = True
            reader_task.cancel()
        writer.close()
        with contextlib.suppress(OSError):
            await writer.wait_closed()
