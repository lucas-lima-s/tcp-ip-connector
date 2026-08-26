from __future__ import annotations

import pytest

from tcpconn.errors import SerializationError
from tcpconn.serializers import JsonCodec, RawCodec


def test_raw_codec_round_trip() -> None:
    codec = RawCodec()
    assert codec.loads(codec.dumps(b"hello")) == b"hello"


def test_raw_codec_dumps_rejects_non_bytes() -> None:
    with pytest.raises(SerializationError):
        RawCodec().dumps("str")


def test_json_codec_round_trip() -> None:
    codec = JsonCodec()
    obj = {"a": 1, "b": [1, 2, 3], "c": None}
    assert codec.loads(codec.dumps(obj)) == obj


def test_json_codec_dumps_rejects_unserializable_object() -> None:
    with pytest.raises(SerializationError):
        JsonCodec().dumps(object())


def test_json_codec_loads_rejects_malformed_json() -> None:
    with pytest.raises(SerializationError):
        JsonCodec().loads(b"{bad")


def test_msgpack_codec_round_trip() -> None:
    msgpack = pytest.importorskip("msgpack")
    del msgpack
    from tcpconn.serializers import MsgpackCodec

    codec = MsgpackCodec()
    obj = {"a": 1, "b": [1, 2, 3]}
    assert codec.loads(codec.dumps(obj)) == obj


def test_msgpack_codec_import_does_not_require_msgpack_at_module_level() -> None:
    from tcpconn import serializers

    assert hasattr(serializers, "MsgpackCodec")
