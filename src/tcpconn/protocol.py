from __future__ import annotations

from enum import IntEnum
from typing import Any, Protocol, runtime_checkable

from tcpconn.errors import FrameError


class MessageKind(IntEnum):
    DATA = 0x00
    PING = 0x01
    PONG = 0x02


@runtime_checkable
class Checksum(Protocol):
    @property
    def digest_size(self) -> int: ...

    def compute(self, data: bytes, /) -> bytes: ...


@runtime_checkable
class FrameDecoder(Protocol):
    def feed(self, data: bytes, /) -> list[bytes]: ...


@runtime_checkable
class Framer(Protocol):
    def encode(self, payload: bytes, /) -> bytes: ...

    def decoder(self) -> FrameDecoder: ...


@runtime_checkable
class Codec(Protocol):
    def dumps(self, obj: Any, /) -> bytes: ...

    def loads(self, data: bytes, /) -> Any: ...


def wrap_kind(kind: MessageKind, payload: bytes) -> bytes:
    return bytes((kind,)) + payload


def unwrap_kind(frame: bytes) -> tuple[MessageKind, bytes]:
    if not frame:
        raise FrameError("frame is empty, missing control-channel kind byte")
    kind_byte = frame[0]
    try:
        kind = MessageKind(kind_byte)
    except ValueError as exc:
        raise FrameError(f"unknown control-channel kind byte 0x{kind_byte:02x}") from exc
    return kind, frame[1:]
