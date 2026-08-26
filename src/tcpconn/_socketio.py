from __future__ import annotations

import errno
import socket
import time

from tcpconn.errors import ConnectionLost, ReceiveTimeout

_CONNECTION_LOST_ERRNOS = {
    errno.ECONNRESET,
    errno.ECONNABORTED,
    errno.EPIPE,
    errno.EBADF,
    getattr(errno, "WSAECONNRESET", 10054),
    getattr(errno, "WSAECONNABORTED", 10053),
    getattr(errno, "WSAENOTSOCK", 10038),
}


def _is_connection_lost(exc: OSError) -> bool:
    if isinstance(exc, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
        return True
    return exc.errno in _CONNECTION_LOST_ERRNOS


def send_all(sock: socket.socket, data: bytes) -> None:
    try:
        sock.sendall(data)
    except OSError as exc:
        if _is_connection_lost(exc):
            raise ConnectionLost(f"send to {_peer(sock)} failed: {exc}") from exc
        raise


def recv_exactly(sock: socket.socket, n: int, deadline: float | None = None) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ReceiveTimeout(
                    f"timed out waiting for {n - len(buf)} more bytes from {_peer(sock)}"
                )
            sock.settimeout(remaining)
        try:
            chunk = sock.recv(n - len(buf))
        except TimeoutError as exc:
            raise ReceiveTimeout(f"timed out reading from {_peer(sock)}") from exc
        except OSError as exc:
            if _is_connection_lost(exc):
                raise ConnectionLost(f"connection to {_peer(sock)} lost: {exc}") from exc
            raise
        if chunk == b"":
            raise ConnectionLost(f"peer {_peer(sock)} closed the connection")
        buf.extend(chunk)
    return bytes(buf)


def recv_some(sock: socket.socket, bufsize: int = 65536) -> bytes:
    try:
        data = sock.recv(bufsize)
    except TimeoutError as exc:
        raise ReceiveTimeout(f"timed out reading from {_peer(sock)}") from exc
    except OSError as exc:
        if _is_connection_lost(exc):
            raise ConnectionLost(f"connection to {_peer(sock)} lost: {exc}") from exc
        raise
    if data == b"":
        raise ConnectionLost(f"peer {_peer(sock)} closed the connection")
    return data


def _peer(sock: socket.socket) -> str:
    try:
        return str(sock.getpeername())
    except OSError:
        return "<unknown peer>"
