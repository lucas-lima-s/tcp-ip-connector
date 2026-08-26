from __future__ import annotations

from tcpconn.aio.client import AsyncTcpClient
from tcpconn.aio.server import AsyncServerConnection, AsyncTcpServer
from tcpconn.backoff import ExponentialBackoff, ReconnectPolicy
from tcpconn.errors import (
    ChecksumError,
    ConnectionLost,
    FrameError,
    NotConnected,
    ReceiveTimeout,
    ReconnectFailed,
    SerializationError,
    TcpConnError,
)
from tcpconn.framing import ChecksumFramer, Crc32, LengthPrefixFramer, XorLrc
from tcpconn.heartbeat import HeartbeatConfig
from tcpconn.protocol import Checksum, Codec, FrameDecoder, Framer, MessageKind
from tcpconn.serializers import JsonCodec, MsgpackCodec, RawCodec
from tcpconn.sync.client import TcpClient
from tcpconn.sync.server import ServerConnection, TcpServer

__version__ = "0.1.0"

__all__ = [
    "AsyncServerConnection",
    "AsyncTcpClient",
    "AsyncTcpServer",
    "Checksum",
    "ChecksumError",
    "ChecksumFramer",
    "Codec",
    "ConnectionLost",
    "Crc32",
    "ExponentialBackoff",
    "FrameDecoder",
    "FrameError",
    "Framer",
    "HeartbeatConfig",
    "JsonCodec",
    "LengthPrefixFramer",
    "MessageKind",
    "MsgpackCodec",
    "NotConnected",
    "RawCodec",
    "ReceiveTimeout",
    "ReconnectFailed",
    "ReconnectPolicy",
    "SerializationError",
    "ServerConnection",
    "TcpClient",
    "TcpConnError",
    "TcpServer",
    "XorLrc",
    "__version__",
]
