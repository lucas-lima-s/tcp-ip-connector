from __future__ import annotations

import contextlib
import logging
import queue
import socket
import threading
import time
from collections.abc import Iterator
from typing import Any, Self

from tcpconn._socketio import send_all
from tcpconn.backoff import ReconnectPolicy
from tcpconn.errors import ConnectionLost, NotConnected, ReceiveTimeout
from tcpconn.framing import LengthPrefixFramer
from tcpconn.heartbeat import HeartbeatConfig
from tcpconn.protocol import Codec, Framer, MessageKind, wrap_kind
from tcpconn.serializers import RawCodec
from tcpconn.sync._reader import ReaderThread

_HEARTBEAT_TICK = 0.02
_POLL_INTERVAL = 0.05


class TcpClient:
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

        self._sock: socket.socket | None = None
        self._decoder = self._framer.decoder()
        self._inbox: queue.Queue[Any] = queue.Queue()
        self._stop_event = threading.Event()
        self._write_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._connected = False
        self._failure: BaseException | None = None
        self._last_write_time = time.monotonic()
        self._last_liveness = time.monotonic()
        self._reader_thread: ReaderThread | None = None
        self._heartbeat_thread: threading.Thread | None = None

    def connect(self) -> None:
        self._do_connect()
        self._stop_event.clear()
        self._reader_thread = ReaderThread(self)
        self._reader_thread.start()
        if self._heartbeat is not None:
            self._heartbeat_thread = threading.Thread(
                target=self._run_heartbeat, daemon=True, name="tcpconn-heartbeat"
            )
            self._heartbeat_thread.start()

    def _do_connect(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(self._connect_timeout)
            sock.connect((self.host, self.port))
            sock.settimeout(None)
            if self._tcp_nodelay:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            sock.close()
            raise
        old_sock = self._sock
        self._sock = sock
        if old_sock is not None:
            with contextlib.suppress(OSError):
                old_sock.close()
        self._decoder = self._framer.decoder()
        now = time.monotonic()
        self._last_write_time = now
        self._last_liveness = now
        with self._state_lock:
            self._connected = True
            self._failure = None

    def close(self) -> None:
        self._stop_event.set()
        sock = self._sock
        if sock is not None:
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)
            sock.close()
        with self._state_lock:
            self._connected = False
        current = threading.current_thread()
        if self._reader_thread is not None and self._reader_thread is not current:
            self._reader_thread.join(timeout=5.0)
        if self._heartbeat_thread is not None and self._heartbeat_thread is not current:
            self._heartbeat_thread.join(timeout=5.0)

    def send(self, message: Any) -> None:
        if not self.is_connected:
            raise NotConnected(f"not connected to {self.host}:{self.port}")
        payload = self._codec.dumps(message)
        self._send_kind(MessageKind.DATA, payload)

    def _send_kind(self, kind: MessageKind, payload: bytes = b"") -> None:
        body = wrap_kind(kind, payload) if self._control_channel else payload
        wire = self._framer.encode(body)
        sock = self._sock
        if sock is None:
            raise NotConnected(f"not connected to {self.host}:{self.port}")
        with self._write_lock:
            send_all(sock, wire)
        self._last_write_time = time.monotonic()

    def receive(self, timeout: float | None = None) -> Any:
        effective = timeout if timeout is not None else self._recv_timeout
        deadline = time.monotonic() + effective if effective is not None else None
        while True:
            wait = _POLL_INTERVAL
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._raise_receive_failure(effective)
                wait = min(_POLL_INTERVAL, remaining)
            try:
                return self._inbox.get(timeout=wait)
            except queue.Empty:
                with self._state_lock:
                    failure = self._failure
                if failure is not None and self._inbox.empty():
                    raise failure from None
                if deadline is None:
                    continue

    def _raise_receive_failure(self, effective: float | None) -> None:
        with self._state_lock:
            failure = self._failure
        if failure is not None and self._inbox.empty():
            raise failure
        raise ReceiveTimeout(
            f"no message received from {self.host}:{self.port} within {effective!r}s"
        )

    def request(self, message: Any, timeout: float | None = None) -> Any:
        self.send(message)
        return self.receive(timeout)

    def __iter__(self) -> Iterator[Any]:
        while True:
            try:
                yield self.receive()
            except ReceiveTimeout:
                continue
            except ConnectionLost:
                return

    @property
    def is_connected(self) -> bool:
        with self._state_lock:
            return self._connected

    @property
    def local_address(self) -> tuple[str, int]:
        if self._sock is None:
            raise NotConnected(f"not connected to {self.host}:{self.port}")
        addr = self._sock.getsockname()
        return addr[0], addr[1]

    def __enter__(self) -> Self:
        self.connect()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _run_heartbeat(self) -> None:
        config = self._heartbeat
        if config is None:
            return
        while not self._stop_event.wait(_HEARTBEAT_TICK):
            if not self.is_connected:
                continue
            now = time.monotonic()
            if now - self._last_liveness > config.timeout:
                self._force_disconnect()
                continue
            if now - self._last_write_time >= config.interval:
                try:
                    self._send_kind(MessageKind.PING)
                except (ConnectionLost, NotConnected, OSError):
                    self._force_disconnect()

    def _force_disconnect(self) -> None:
        sock = self._sock
        if sock is None:
            return
        with contextlib.suppress(OSError):
            sock.shutdown(socket.SHUT_RDWR)
