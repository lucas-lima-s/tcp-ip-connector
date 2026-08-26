from __future__ import annotations

import pytest

from tcpconn.errors import ChecksumError, FrameError
from tcpconn.framing import ChecksumFramer, Crc32, LengthPrefixFramer, XorLrc

PAYLOAD_SIZES = [0, 1, 2, 255, 256, 65535, 65536, 1_048_576]


@pytest.mark.parametrize("size", PAYLOAD_SIZES)
def test_round_trip_sizes(size: int) -> None:
    framer = LengthPrefixFramer()
    payload = bytes(range(256)) * (size // 256 + 1)
    payload = payload[:size]
    wire = framer.encode(payload)
    decoder = framer.decoder()
    assert decoder.feed(wire) == [payload]


def test_feed_one_byte_at_a_time_yields_one_frame_at_the_right_moment() -> None:
    framer = LengthPrefixFramer()
    payload = b"x" * 1000
    wire = framer.encode(payload)
    decoder = framer.decoder()
    seen: list[bytes] = []
    for i in range(len(wire) - 1):
        seen.extend(decoder.feed(wire[i : i + 1]))
        assert seen == []
    seen.extend(decoder.feed(wire[-1:]))
    assert seen == [payload]


def test_three_frames_in_three_arbitrary_splits() -> None:
    framer = LengthPrefixFramer()
    payloads = [b"first", b"second-payload", b"3"]
    stream = b"".join(framer.encode(p) for p in payloads)
    split_a, split_b = 3, len(stream) - 2
    chunks = [stream[:split_a], stream[split_a:split_b], stream[split_b:]]
    decoder = framer.decoder()
    out: list[bytes] = []
    for chunk in chunks:
        out.extend(decoder.feed(chunk))
    assert out == payloads


def test_length_above_max_frame_size_raises_frame_error() -> None:
    framer = LengthPrefixFramer(max_frame_size=10)
    header = (100).to_bytes(4, "big")
    decoder = framer.decoder()
    with pytest.raises(FrameError):
        decoder.feed(header)


def test_truncated_stream_yields_empty_list_without_raising() -> None:
    framer = LengthPrefixFramer()
    wire = framer.encode(b"hello world")
    decoder = framer.decoder()
    assert decoder.feed(wire[:-3]) == []


def test_little_byteorder_and_two_byte_prefix() -> None:
    framer = LengthPrefixFramer(prefix_bytes=2, byteorder="little")
    payload = b"abc"
    wire = framer.encode(payload)
    assert wire[:2] == len(payload).to_bytes(2, "little")
    decoder = framer.decoder()
    assert decoder.feed(wire) == [payload]


@pytest.mark.parametrize("checksum", [XorLrc(), Crc32()])
def test_checksum_framer_round_trip(checksum: XorLrc | Crc32) -> None:
    framer = ChecksumFramer(LengthPrefixFramer(), checksum)
    payload = b"the quick brown fox"
    wire = framer.encode(payload)
    decoder = framer.decoder()
    assert decoder.feed(wire) == [payload]


@pytest.mark.parametrize("checksum", [XorLrc(), Crc32()])
def test_checksum_framer_flipped_bit_raises_checksum_error(checksum: XorLrc | Crc32) -> None:
    framer = ChecksumFramer(LengthPrefixFramer(), checksum)
    wire = bytearray(framer.encode(b"hello"))
    wire[4] ^= 0xFF
    decoder = framer.decoder()
    with pytest.raises(ChecksumError):
        decoder.feed(bytes(wire))


def test_checksum_framer_short_frame_raises_frame_error() -> None:
    framer = ChecksumFramer(LengthPrefixFramer(), Crc32())
    inner = LengthPrefixFramer().encode(b"ab")
    decoder = framer.decoder()
    with pytest.raises(FrameError):
        decoder.feed(inner)


def test_xor_lrc_empty_input_yields_zero_byte() -> None:
    assert XorLrc().compute(b"") == b"\x00"


def test_length_prefix_decoder_is_broken_after_overflow() -> None:
    framer = LengthPrefixFramer(max_frame_size=1)
    decoder = framer.decoder()
    with pytest.raises(FrameError):
        decoder.feed((10).to_bytes(4, "big"))
    with pytest.raises(FrameError):
        decoder.feed(b"more data")
