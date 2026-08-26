from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from tcpconn.errors import SerializationError


@dataclass(frozen=True, slots=True)
class RawCodec:
    def dumps(self, obj: Any, /) -> bytes:
        if not isinstance(obj, (bytes, bytearray, memoryview)):
            raise SerializationError(
                f"RawCodec requires bytes-like objects, got {type(obj).__name__}"
            )
        return bytes(obj)

    def loads(self, data: bytes, /) -> Any:
        return data


@dataclass(frozen=True, slots=True)
class JsonCodec:
    encoding: str = "utf-8"
    ensure_ascii: bool = False

    def dumps(self, obj: Any, /) -> bytes:
        try:
            return json.dumps(obj, ensure_ascii=self.ensure_ascii).encode(self.encoding)
        except (TypeError, ValueError) as exc:
            raise SerializationError(f"failed to encode object as JSON: {exc}") from exc

    def loads(self, data: bytes, /) -> Any:
        try:
            return json.loads(data.decode(self.encoding))
        except (TypeError, ValueError) as exc:
            raise SerializationError(f"failed to decode JSON payload: {exc}") from exc


class MsgpackCodec:
    __slots__ = ("_msgpack",)

    def __init__(self) -> None:
        try:
            import msgpack  # noqa: PLC0415
        except ImportError as exc:
            raise SerializationError(
                "msgpack extra not installed: pip install tcpconn[msgpack]"
            ) from exc
        self._msgpack = msgpack

    def dumps(self, obj: Any, /) -> bytes:
        try:
            result: bytes = self._msgpack.packb(obj, use_bin_type=True)
        except Exception as exc:
            raise SerializationError(f"failed to encode object as msgpack: {exc}") from exc
        return result

    def loads(self, data: bytes, /) -> Any:
        try:
            return self._msgpack.unpackb(data, raw=False)
        except Exception as exc:
            raise SerializationError(f"failed to decode msgpack payload: {exc}") from exc
