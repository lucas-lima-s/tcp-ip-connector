from __future__ import annotations

import contextlib
import logging
import socket
import threading
import time
from collections.abc import Callable, Iterator
from typing import Any, Self

from tcpconn._socketio import recv_some, send_all
from tcpconn.errors import ConnectionLost, NotConnected, ReceiveTimeout
from tcpconn.framing import LengthPrefixFramer
from tcpconn.protocol import Codec, Framer, MessageKind, unwrap_kind, wrap_kind
from tcpconn.serializers import RawCodec


class ServerConnection:
    def __init__(
        self,
        sock: socket.socket,
        *,
        framer: Framer,
        codec: Codec,
        control_channel: bool,
    ) -> None:
        self._sock = sock
        self._framer = framer
        self._codec = codec
        self._control_channel = control_channel
        self._decoder = framer.decoder()
        self._pending: list[bytes] = []
        self._write_lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._closed = False
        self.peer: tuple[str, int] = sock.getpeername()

    def send(self, message: Any) -> None:
        payload = self._codec.dumps(message)
        body = wrap_kind(MessageKind.DATA, payload) if self._control_channel else payload
        wire = self._framer.encode(body)
        with self._write_lock:
            send_all(self._sock, wire)

    def receive(self, timeout: float | None = None) -> Any:
        deadline = time.monotonic() + timeout if timeout is not None else None
        while True:
            if self._pending:
                frame = self._pending.pop(0)
            else:
                remaining = None
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise ReceiveTimeout(f"no message received from {self.peer} in time")
                self._sock.settimeout(remaining)
                chunk = recv_some(self._sock)
                frames = self._decoder.feed(chunk)
                if not frames:
                    continue
                self._pending.extend(frames)
                frame = self._pending.pop(0)
            if self._control_channel:
                kind, payload = unwrap_kind(frame)
            else:
                kind, payload = MessageKind.DATA, frame
            if kind is MessageKind.PING:
                self._reply_pong()
                continue
            if kind is MessageKind.PONG:
                continue
            return self._codec.loads(payload)

    def _reply_pong(self) -> None:
        body = wrap_kind(MessageKind.PONG, b"") if self._control_channel else b""
        wire = self._framer.encode(body)
        with self._write_lock:
            send_all(self._sock, wire)

    def close(self) -> None:
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
        with contextlib.suppress(OSError):
            self._sock.shutdown(socket.SHUT_RDWR)
        self._sock.close()

    def __iter__(self) -> Iterator[Any]:
        while True:
            try:
                yield self.receive()
            except ConnectionLost:
                return


class TcpServer:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 0,
        *,
        handler: Callable[[ServerConnection], None] | None = None,
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

        self._listen_sock: socket.socket | None = None
        self._accept_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._connections: list[ServerConnection] = []
        self._conn_threads: list[threading.Thread] = []
        self._connections_lock = threading.Lock()

    def start(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self._reuse_address:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port))
        sock.listen(self._backlog)
        sock.settimeout(0.5)
        self._listen_sock = sock
        self._stop_event.clear()
        self._accept_thread = threading.Thread(
            target=self._accept_loop, daemon=True, name="tcpconn-accept"
        )
        self._accept_thread.start()

    def _accept_loop(self) -> None:
        listen_sock = self._listen_sock
        if listen_sock is None:
            return
        while not self._stop_event.is_set():
            try:
                client_sock, _ = listen_sock.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            if self._max_connections is not None and len(self.connections) >= self._max_connections:
                self._reject(client_sock)
                continue
            conn = ServerConnection(
                client_sock,
                framer=self._framer,
                codec=self._codec,
                control_channel=self._control_channel,
            )
            thread = threading.Thread(
                target=self._run_handler, args=(conn,), daemon=True, name="tcpconn-conn"
            )
            with self._connections_lock:
                self._connections.append(conn)
                self._conn_threads.append(thread)
            thread.start()

    @staticmethod
    def _reject(client_sock: socket.socket) -> None:
        with contextlib.suppress(OSError):
            client_sock.shutdown(socket.SHUT_RDWR)
        client_sock.close()

    def _run_handler(self, conn: ServerConnection) -> None:
        try:
            if self._handler is not None:
                self._handler(conn)
        except ConnectionLost:
            pass
        finally:
            conn.close()
            with self._connections_lock:
                if conn in self._connections:
                    self._connections.remove(conn)

    def serve_forever(self) -> None:
        self._stop_event.wait()

    def stop(self, timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        self._stop_event.set()
        if self._listen_sock is not None:
            with contextlib.suppress(OSError):
                self._listen_sock.close()
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=max(0.0, deadline - time.monotonic()))
        with self._connections_lock:
            conns = list(self._connections)
            threads = list(self._conn_threads)
        for conn in conns:
            conn.close()
        for thread in threads:
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
        with self._connections_lock:
            self._connections.clear()
            self._conn_threads.clear()

    def broadcast(self, message: Any) -> int:
        count = 0
        for conn in self.connections:
            try:
                conn.send(message)
            except (ConnectionLost, NotConnected, OSError):
                continue
            count += 1
        return count

    @property
    def address(self) -> tuple[str, int]:
        if self._listen_sock is None:
            raise NotConnected("server has not been started")
        addr = self._listen_sock.getsockname()
        return addr[0], addr[1]

    @property
    def connections(self) -> tuple[ServerConnection, ...]:
        with self._connections_lock:
            return tuple(self._connections)

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()
