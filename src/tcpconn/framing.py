from __future__ import annotations

import functools
import operator
import zlib
from dataclasses import dataclass, field
from typing import Literal

from tcpconn.errors import ChecksumError, FrameError
from tcpconn.protocol import Checksum, FrameDecoder, Framer


class _LengthPrefixDecoder:
    def __init__(
        self,
        prefix_bytes: int,
        byteorder: Literal["big", "little"],
        max_frame_size: int,
    ) -> None:
        self._prefix_bytes = prefix_bytes
        self._byteorder = byteorder
        self._max_frame_size = max_frame_size
        self._buffer = bytearray()
        self._broken = False

    def feed(self, data: bytes, /) -> list[bytes]:
        if self._broken:
            raise FrameError("decoder is desynchronised after a previous framing error")
        self._buffer.extend(data)
        frames: list[bytes] = []
        while True:
            if len(self._buffer) < self._prefix_bytes:
                return frames
            length = int.from_bytes(self._buffer[: self._prefix_bytes], self._byteorder)
            if length > self._max_frame_size:
                self._broken = True
                raise FrameError(
                    f"declared frame length {length} exceeds max_frame_size {self._max_frame_size}"
                )
            total = self._prefix_bytes + length
            if len(self._buffer) < total:
                return frames
            frame = bytes(self._buffer[self._prefix_bytes : total])
            del self._buffer[:total]
            frames.append(frame)


@dataclass(frozen=True, slots=True)
class LengthPrefixFramer:
    prefix_bytes: int = 4
    byteorder: Literal["big", "little"] = "big"
    max_frame_size: int = 8 * 1024 * 1024

    def encode(self, payload: bytes, /) -> bytes:
        header = len(payload).to_bytes(self.prefix_bytes, self.byteorder)
        return header + payload

    def decoder(self) -> FrameDecoder:
        return _LengthPrefixDecoder(self.prefix_bytes, self.byteorder, self.max_frame_size)


class _ChecksumDecoder:
    def __init__(self, inner: FrameDecoder, checksum: Checksum) -> None:
        self._inner = inner
        self._checksum = checksum

    def feed(self, data: bytes, /) -> list[bytes]:
        payloads: list[bytes] = []
        for frame in self._inner.feed(data):
            digest_size = self._checksum.digest_size
            if len(frame) < digest_size:
                raise FrameError(
                    f"frame of {len(frame)} bytes is shorter than digest_size {digest_size}"
                )
            split_at = len(frame) - digest_size
            body, received = frame[:split_at], frame[split_at:]
            expected = self._checksum.compute(body)
            if expected != received:
                raise ChecksumError(
                    f"checksum mismatch: expected {expected.hex()}, received {received.hex()}"
                )
            payloads.append(body)
        return payloads


@dataclass(frozen=True, slots=True)
class ChecksumFramer:
    inner: Framer
    checksum: Checksum

    def encode(self, payload: bytes, /) -> bytes:
        return self.inner.encode(payload + self.checksum.compute(payload))

    def decoder(self) -> FrameDecoder:
        return _ChecksumDecoder(self.inner.decoder(), self.checksum)


@dataclass(frozen=True, slots=True)
class XorLrc:
    digest_size: int = field(default=1, init=False)

    def compute(self, data: bytes, /) -> bytes:
        return bytes([functools.reduce(operator.xor, data, 0)])


@dataclass(frozen=True, slots=True)
class Crc32:
    digest_size: int = field(default=4, init=False)

    def compute(self, data: bytes, /) -> bytes:
        return zlib.crc32(data).to_bytes(4, "big")
