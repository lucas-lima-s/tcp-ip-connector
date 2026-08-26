from __future__ import annotations


class TcpConnError(Exception):
    """Base class for every exception raised by tcpconn."""


class NotConnected(TcpConnError):
    """Raised when an operation requires a live connection that does not exist."""


class ConnectionLost(TcpConnError):
    """Raised when the peer closed the connection or a socket error tore it down."""


class ReceiveTimeout(TcpConnError):
    """Raised when no data (or no complete message) arrived within the deadline."""


class FrameError(TcpConnError):
    """Raised when the byte stream cannot be parsed into a well-formed frame."""


class ChecksumError(FrameError):
    """Raised when a frame's trailing checksum does not match its payload."""


class SerializationError(TcpConnError):
    """Raised when a message cannot be encoded or decoded by a codec."""


class ReconnectFailed(ConnectionLost):
    """Raised when the reconnect policy exhausts its attempts without success."""
