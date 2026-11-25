import dataclasses
import json
import struct
import zlib
from pathlib import Path

import numpy as np
import pytest

from eegpipe.protocol import (
    ChecksumError, ConnectionValidator, DimensionMismatchError, Frame, Header,
    Hello, InvalidHelloError, LengthMismatchError, LimitError, MessageOrderError,
    ProtocolError, ReservedFieldError, UnknownMessageTypeError,
    UnsupportedVersionError, BadMagicError, decode_header, decode_message,
    encode_frame, encode_hello,
)

REPO_ROOT = Path(__file__).resolve().parents[2]

def _read_hex(name: str) -> bytes:
    text = (REPO_ROOT / "testdata" / "protocol" / name).read_text(encoding="ascii")
    return bytes.fromhex("".join(line.split("#", 1)[0] for line in text.splitlines()))

def _header(msg_type, body_length, *, magic=b"EEGP", version=1, reserved=0) -> bytes:
    return struct.pack("<4sBBHI", magic, version, msg_type, reserved, body_length)

def _wrap(msg_type, body, *, magic=b"EEGP", version=1, reserved=0, body_length=None) -> bytes:
    h = _header(msg_type, len(body) if body_length is None else body_length,
                magic=magic, version=version, reserved=reserved)
    return h + body + struct.pack("<I", zlib.crc32(h + body) & 0xFFFFFFFF)

def _frame_body(seq, ts, c, s, payload) -> bytes:
    return struct.pack("<QqHH", seq, ts, c, s) + payload

def _hello_dict() -> dict:
    return {"stream_id": "9f3a5c0e12b47d68", "device_name": "virtual-synthetic",
            "n_channels": 3, "samples_per_frame": 2, "sample_rate_hz": 160.0,
            "units": "uV", "channel_names": ["Fp1", "Cz", "O2"]}

def _hello_msg(d: dict) -> bytes:
    return _wrap(1, json.dumps(d).encode("utf-8"))

def _hello_text_msg(text: str) -> bytes:
    return _wrap(1, text.encode("utf-8"))

GOLDEN_HELLO = Hello(stream_id=0x9F3A5C0E12B47D68, device_name="virtual-synthetic",
                     n_channels=3, samples_per_frame=2, sample_rate_hz=160.0,
                     channel_names=("Fp1", "Cz", "O2"))
GOLDEN_HELLO_JSON = ('{"stream_id":"9f3a5c0e12b47d68","device_name":"virtual-synthetic",'
                     '"n_channels":3,"samples_per_frame":2,"sample_rate_hz":160.0,'
                     '"units":"uV","channel_names":["Fp1","Cz","O2"]}')
GOLDEN_FRAME = Frame(seq=41, timestamp_ns=1_700_000_000_123_456_789,
                     samples=np.array([[12.5, 13.25], [-3.0, 0.0], [1.5, -0.5]], dtype="<f4"))
GOLDEN_FRAME_BYTES = _wrap(2, _frame_body(41, 1_700_000_000_123_456_789, 3, 2,
                                          GOLDEN_FRAME.samples.tobytes()))


# 7.1 Round trips

def test_hello_roundtrip():
    h = GOLDEN_HELLO
    assert decode_message(encode_hello(h)) == h


@pytest.mark.parametrize("h", [
    dataclasses.replace(GOLDEN_HELLO, stream_id=0),
    dataclasses.replace(GOLDEN_HELLO, stream_id=2**64 - 1),
    dataclasses.replace(GOLDEN_HELLO, sample_rate_hz=100_000.0),
    dataclasses.replace(GOLDEN_HELLO, n_channels=1, channel_names=("A",)),
    dataclasses.replace(GOLDEN_HELLO, n_channels=4096, channel_names=("x" * 32,) * 4096),
    dataclasses.replace(GOLDEN_HELLO, n_channels=2, channel_names=("T8-P8", "T8-P8")),
    dataclasses.replace(GOLDEN_HELLO, channel_names=("Fp1", "EEG Fp1", "O2")),
])
def test_hello_roundtrip_edges(h):
    assert decode_message(encode_hello(h)) == h


def test_frame_roundtrip():
    original = Frame(7, 123, np.random.default_rng(0).standard_normal((4, 16)).astype("<f4"))
    decoded = decode_message(encode_frame(original))
    assert decoded.seq == 7
    assert decoded.timestamp_ns == 123
    assert np.array_equal(decoded.samples, original.samples)
    assert decoded.samples.dtype == np.dtype("<f4")
    assert decoded.samples.shape == (4, 16)


def test_frame_roundtrip_min_dims():
    encoded = encode_frame(Frame(0, 0, np.array([[1.5]], dtype="<f4")))
    assert len(encoded) == 40
    decoded = decode_message(encoded)
    assert np.array_equal(decoded.samples, np.array([[1.5]], dtype="<f4"))


def test_frame_roundtrip_max_dims():
    encoded = encode_frame(Frame(0, 0, np.zeros((4096, 1024), dtype="<f4")))
    assert len(encoded) == 12 + 20 + 16_777_216 + 4
    decoded = decode_message(encoded)
    assert decoded.samples.shape == (4096, 1024)
    assert not decoded.samples.any()


@pytest.mark.parametrize("seq, ts", [(2**64 - 1, -2**63), (0, 2**63 - 1)])
def test_frame_roundtrip_extreme_ints(seq, ts):
    decoded = decode_message(encode_frame(Frame(seq, ts, np.zeros((1, 1), "<f4"))))
    assert decoded.seq == seq
    assert decoded.timestamp_ns == ts


def test_frame_nan_inf_preserved():
    f = Frame(0, 0, np.array([[np.nan, np.inf, -np.inf]], dtype="<f4"))
    s = decode_message(encode_frame(f)).samples
    assert np.isnan(s[0, 0])
    assert s[0, 1] == np.inf
    assert s[0, 2] == -np.inf


def test_frame_noncontiguous_input():
    base = np.random.default_rng(0).standard_normal((3, 8)).astype("<f4")
    view = base[:, ::2]
    assert encode_frame(Frame(0, 0, view)) == encode_frame(Frame(0, 0, np.ascontiguousarray(view)))


def test_decoded_samples_readonly():
    samples = decode_message(GOLDEN_FRAME_BYTES).samples
    assert samples.flags.writeable is False
    with pytest.raises(ValueError):
        samples[0, 0] = 1.0


# 7.2 Golden vectors

def test_golden_frame_bytes_match_file():
    assert GOLDEN_FRAME_BYTES == _read_hex("frame_valid.hex")


def test_encode_hello_matches_golden():
    assert encode_hello(GOLDEN_HELLO) == _read_hex("hello_valid.hex")


def test_encode_frame_matches_golden():
    assert encode_frame(GOLDEN_FRAME) == _read_hex("frame_valid.hex")


def test_decode_golden_hello():
    assert decode_message(_read_hex("hello_valid.hex")) == GOLDEN_HELLO


def test_decode_golden_frame():
    f = decode_message(_read_hex("frame_valid.hex"))
    assert f.seq == 41
    assert f.timestamp_ns == 1_700_000_000_123_456_789
    assert np.array_equal(f.samples, GOLDEN_FRAME.samples)


@pytest.mark.parametrize("f, X", [
    ("frame_bad_magic.hex", BadMagicError),
    ("frame_bad_version.hex", UnsupportedVersionError),
    ("frame_unknown_type.hex", UnknownMessageTypeError),
    ("frame_reserved_nonzero.hex", ReservedFieldError),
    ("frame_bad_crc.hex", ChecksumError),
    ("frame_zero_channels.hex", LimitError),
    ("frame_length_mismatch.hex", LengthMismatchError),
    ("hello_missing_key.hex", InvalidHelloError),
    ("hello_extra_key.hex", InvalidHelloError),
    ("hello_duplicate_key.hex", InvalidHelloError),
    ("hello_wrong_case_key.hex", InvalidHelloError),
    ("hello_float_n_channels.hex", InvalidHelloError),
])
def test_decode_golden_invalid(f, X):
    with pytest.raises(X):
        decode_message(_read_hex(f))


# 7.3 decode_header

def test_header_valid():
    assert decode_header(_header(2, 44)) == Header(2, 44)


def test_header_bad_magic():
    with pytest.raises(BadMagicError):
        decode_header(_header(2, 44, magic=b"EEGX"))


@pytest.mark.parametrize("version", [0, 2])
def test_header_bad_version(version):
    with pytest.raises(UnsupportedVersionError):
        decode_header(_header(2, 44, version=version))


@pytest.mark.parametrize("data", [_header(0, 44), _header(3, 44)])
def test_header_unknown_type(data):
    with pytest.raises(UnknownMessageTypeError):
        decode_header(data)


def test_header_reserved_nonzero():
    with pytest.raises(ReservedFieldError):
        decode_header(_header(2, 44, reserved=1))


def test_header_hello_limit_ok():
    assert decode_header(_header(1, 262_144)) == Header(1, 262_144)


def test_header_hello_limit_exceeded():
    with pytest.raises(LimitError):
        decode_header(_header(1, 262_145))


def test_header_frame_limit_ok():
    assert decode_header(_header(2, 16_777_236)) == Header(2, 16_777_236)


def test_header_frame_limit_exceeded():
    with pytest.raises(LimitError):
        decode_header(_header(2, 16_777_237))


def test_header_check_order():
    with pytest.raises(BadMagicError):
        decode_header(_header(2, 44, magic=b"EEGX", version=2))


@pytest.mark.parametrize("data", [_header(2, 44)[:11], _header(2, 44) + b"\x00"])
def test_header_wrong_size(data):
    with pytest.raises(ValueError):
        decode_header(data)


def test_header_not_bytes():
    with pytest.raises(TypeError):
        decode_header(bytearray(_header(2, 44)))


# 7.4 decode_message: framing and FRAME bodies

def test_message_shorter_than_header():
    with pytest.raises(LengthMismatchError):
        decode_message(b"EEGP")


def test_message_truncated():
    with pytest.raises(LengthMismatchError):
        decode_message(GOLDEN_FRAME_BYTES[:-1])


def test_message_extra_byte():
    with pytest.raises(LengthMismatchError):
        decode_message(GOLDEN_FRAME_BYTES + b"\x00")


def test_message_corrupted_byte():
    b = bytearray(GOLDEN_FRAME_BYTES)
    b[32] ^= 0xFF
    with pytest.raises(ChecksumError):
        decode_message(bytes(b))


def test_message_not_bytes():
    with pytest.raises(TypeError):
        decode_message(bytearray(GOLDEN_FRAME_BYTES))


def test_frame_body_too_short():
    with pytest.raises(LengthMismatchError):
        decode_message(_wrap(2, b"\x00" * 19))


@pytest.mark.parametrize("c, s", [(0, 1), (4097, 1), (1, 0), (1, 1025)])
def test_frame_dimension_limits(c, s):
    with pytest.raises(LimitError):
        decode_message(_wrap(2, _frame_body(0, 0, c, s, b"")))


def test_frame_length_vs_dims():
    with pytest.raises(LengthMismatchError):
        decode_message(_wrap(2, _frame_body(0, 0, 2, 2, b"\x00" * 12)))


# 7.5 decode_message: HELLO bodies

def test_hello_not_utf8():
    with pytest.raises(InvalidHelloError):
        decode_message(_wrap(1, b"\xff"))


def test_hello_not_json():
    with pytest.raises(InvalidHelloError):
        decode_message(_wrap(1, b"{not json"))


def test_hello_empty_body():
    with pytest.raises(InvalidHelloError):
        decode_message(_wrap(1, b""))


def test_hello_not_object():
    with pytest.raises(InvalidHelloError):
        decode_message(_wrap(1, b"[1, 2]"))


def test_hello_huge_integer_literal():
    with pytest.raises(InvalidHelloError):
        decode_message(_wrap(1, b'{"n_channels":' + b"1" * 5000 + b"}"))


def test_hello_deep_nesting():
    with pytest.raises(InvalidHelloError):
        decode_message(_wrap(1, b"[" * 100_000))


@pytest.mark.parametrize("key", list(_hello_dict()))
def test_hello_missing_key(key):
    d = _hello_dict()
    del d[key]
    with pytest.raises(InvalidHelloError):
        decode_message(_hello_msg(d))


def test_hello_extra_key():
    d = _hello_dict()
    d["extra"] = 1
    with pytest.raises(InvalidHelloError):
        decode_message(_hello_msg(d))


def test_hello_wrong_case_key():
    d = _hello_dict()
    d["Units"] = d.pop("units")
    with pytest.raises(InvalidHelloError):
        decode_message(_hello_msg(d))


def test_hello_duplicate_key():
    with pytest.raises(InvalidHelloError):
        decode_message(_hello_text_msg(GOLDEN_HELLO_JSON[:-1] + ',"units":"uV"}'))


def test_hello_nan_literal():
    with pytest.raises(InvalidHelloError):
        decode_message(_hello_text_msg(GOLDEN_HELLO_JSON.replace("160.0", "NaN")))


def test_hello_infinite_number():
    with pytest.raises(InvalidHelloError):
        decode_message(_hello_text_msg(GOLDEN_HELLO_JSON.replace("160.0", "1e999")))


def test_hello_huge_sample_rate():
    with pytest.raises(InvalidHelloError):
        decode_message(_hello_text_msg(GOLDEN_HELLO_JSON.replace("160.0", "1" + "0" * 399)))


@pytest.mark.parametrize("k, v", [
    ("stream_id", "9F3A5C0E12B47D68"),
    ("stream_id", "9f3a5c0e12b47d6"),
    ("stream_id", "9f3a5c0e12b47d6g"),
    ("stream_id", 123),
    ("device_name", ""),
    ("device_name", "x" * 65),
    ("device_name", "é"),
    ("device_name", "a\nb"),
    ("device_name", 5),
    ("n_channels", 0),
    ("n_channels", 4097),
    ("n_channels", 3.0),
    ("n_channels", True),
    ("n_channels", "3"),
    ("samples_per_frame", 0),
    ("samples_per_frame", 1025),
    ("sample_rate_hz", 0),
    ("sample_rate_hz", -1),
    ("sample_rate_hz", 100000.5),
    ("sample_rate_hz", True),
    ("sample_rate_hz", "160"),
    ("units", "mV"),
    ("channel_names", ["Fp1", "Cz"]),
    ("channel_names", "Fp1"),
    ("channel_names", ["Fp1", "", "O2"]),
    ("channel_names", ["Fp1", "x" * 33, "O2"]),
    ("channel_names", ["Fp1", 2, "O2"]),
    ("channel_names", ["Fp1", "C\tz", "O2"]),
])
def test_hello_bad_values(k, v):
    d = _hello_dict()
    d[k] = v
    with pytest.raises(InvalidHelloError):
        decode_message(_hello_msg(d))


def test_hello_accepts_variants():
    d = _hello_dict()
    d["sample_rate_hz"] = 160
    result = decode_message(_hello_msg(d))
    assert result == GOLDEN_HELLO
    assert type(result.sample_rate_hz) is float

    rev = dict(reversed(list(_hello_dict().items())))
    result = decode_message(_wrap(1, json.dumps(rev, indent=2).encode("utf-8")))
    assert result == GOLDEN_HELLO

    d = _hello_dict()
    d["sample_rate_hz"] = 100000
    result = decode_message(_hello_msg(d))
    assert result.sample_rate_hz == 100000.0


# 7.6 Encoder validation

@pytest.mark.parametrize("make", [
    lambda: encode_frame(dataclasses.replace(GOLDEN_FRAME, samples=[[1.0]])),
    lambda: encode_frame(dataclasses.replace(GOLDEN_FRAME, samples=np.zeros((1, 1), dtype="<f8"))),
    lambda: encode_frame(dataclasses.replace(GOLDEN_FRAME, samples=np.zeros((1, 1), dtype=">f4"))),
    lambda: encode_frame(dataclasses.replace(GOLDEN_FRAME, seq=True)),
    lambda: encode_frame(dataclasses.replace(GOLDEN_FRAME, seq=1.0)),
    lambda: encode_frame(dataclasses.replace(GOLDEN_FRAME, timestamp_ns=1.0)),
    lambda: encode_frame(GOLDEN_HELLO),
])
def test_encode_frame_type_errors(make):
    with pytest.raises(TypeError):
        make()


@pytest.mark.parametrize("make", [
    lambda: encode_frame(Frame(0, 0, np.zeros((4,), dtype="<f4"))),
    lambda: encode_frame(Frame(0, 0, np.zeros((1, 1, 1), dtype="<f4"))),
    lambda: encode_frame(Frame(0, 0, np.zeros((0, 5), dtype="<f4"))),
    lambda: encode_frame(Frame(0, 0, np.zeros((5, 0), dtype="<f4"))),
    lambda: encode_frame(Frame(0, 0, np.zeros((4097, 1), dtype="<f4"))),
    lambda: encode_frame(Frame(0, 0, np.zeros((1, 1025), dtype="<f4"))),
    lambda: encode_frame(dataclasses.replace(GOLDEN_FRAME, seq=-1)),
    lambda: encode_frame(dataclasses.replace(GOLDEN_FRAME, seq=2**64)),
    lambda: encode_frame(dataclasses.replace(GOLDEN_FRAME, timestamp_ns=2**63)),
    lambda: encode_frame(dataclasses.replace(GOLDEN_FRAME, timestamp_ns=-2**63 - 1)),
])
def test_encode_frame_value_errors(make):
    with pytest.raises(ValueError):
        make()


@pytest.mark.parametrize("make", [
    lambda: encode_hello(dataclasses.replace(GOLDEN_HELLO, stream_id="1")),
    lambda: encode_hello(dataclasses.replace(GOLDEN_HELLO, stream_id=True)),
    lambda: encode_hello(dataclasses.replace(GOLDEN_HELLO, device_name=5)),
    lambda: encode_hello(dataclasses.replace(GOLDEN_HELLO, n_channels=3.0)),
    lambda: encode_hello(dataclasses.replace(GOLDEN_HELLO, samples_per_frame="2")),
    lambda: encode_hello(dataclasses.replace(GOLDEN_HELLO, sample_rate_hz="160")),
    lambda: encode_hello(dataclasses.replace(GOLDEN_HELLO, sample_rate_hz=True)),
    lambda: encode_hello(dataclasses.replace(GOLDEN_HELLO, channel_names=["Fp1", "Cz", "O2"])),
    lambda: encode_hello(dataclasses.replace(GOLDEN_HELLO, channel_names=("Fp1", 2, "O2"))),
    lambda: encode_hello(GOLDEN_FRAME),
])
def test_encode_hello_type_errors(make):
    with pytest.raises(TypeError):
        make()


@pytest.mark.parametrize("kwargs", [
    dict(stream_id=-1),
    dict(stream_id=2**64),
    dict(device_name=""),
    dict(device_name="x" * 65),
    dict(device_name="é"),
    dict(n_channels=0),
    dict(n_channels=4097),
    dict(samples_per_frame=0),
    dict(samples_per_frame=1025),
    dict(sample_rate_hz=0.0),
    dict(sample_rate_hz=-1.0),
    dict(sample_rate_hz=100_000.5),
    dict(sample_rate_hz=float("nan")),
    dict(sample_rate_hz=float("inf")),
    dict(sample_rate_hz=10**400),
    dict(channel_names=("Fp1", "Cz")),
    dict(channel_names=("Fp1", "", "O2")),
    dict(channel_names=("Fp1", "x" * 33, "O2")),
    dict(channel_names=("Fp1", "C\tz", "O2")),
])
def test_encode_hello_value_errors(kwargs):
    with pytest.raises(ValueError):
        encode_hello(dataclasses.replace(GOLDEN_HELLO, **kwargs))


def test_encode_hello_oversized_body():
    h = dataclasses.replace(GOLDEN_HELLO, n_channels=4096, channel_names=('"' * 32,) * 4096)
    with pytest.raises(ValueError):
        encode_hello(h)


# 7.7 ConnectionValidator

def test_validator_initial_state():
    v = ConnectionValidator()
    assert v.hello is None


def test_validator_happy_path():
    v = ConnectionValidator()
    v.check_header(Header(1, 173))
    v.check(GOLDEN_HELLO)
    for _ in range(2):
        v.check_header(Header(2, 44))
        v.check(GOLDEN_FRAME)
    assert v.hello == GOLDEN_HELLO


def test_validator_check_header_does_not_store():
    v = ConnectionValidator()
    v.check_header(Header(1, 173))
    assert v.hello is None


def test_validator_frame_header_before_hello():
    v = ConnectionValidator()
    with pytest.raises(MessageOrderError):
        v.check_header(Header(2, 44))


def test_validator_hello_header_after_hello():
    v = ConnectionValidator()
    v.check(GOLDEN_HELLO)
    with pytest.raises(MessageOrderError):
        v.check_header(Header(1, 173))


@pytest.mark.parametrize("n", [45, 52])
def test_validator_frame_header_wrong_length(n):
    v = ConnectionValidator()
    v.check(GOLDEN_HELLO)
    with pytest.raises(DimensionMismatchError):
        v.check_header(Header(2, n))


def test_validator_check_header_bad_args():
    v = ConnectionValidator()
    with pytest.raises(TypeError):
        v.check_header("x")
    with pytest.raises(ValueError):
        v.check_header(Header(7, 0))


def test_validator_frame_before_hello():
    v = ConnectionValidator()
    with pytest.raises(MessageOrderError):
        v.check(GOLDEN_FRAME)


def test_validator_second_hello():
    v = ConnectionValidator()
    v.check(GOLDEN_HELLO)
    with pytest.raises(MessageOrderError):
        v.check(GOLDEN_HELLO)


@pytest.mark.parametrize("shape", [(4, 2), (3, 3)])
def test_validator_dimension_mismatch(shape):
    v = ConnectionValidator()
    v.check(GOLDEN_HELLO)
    with pytest.raises(DimensionMismatchError):
        v.check(Frame(0, 0, np.zeros(shape, "<f4")))


def test_validator_ignores_sequence():
    v = ConnectionValidator()
    v.check(GOLDEN_HELLO)
    for n in (5, 3, 3, 100):
        v.check(dataclasses.replace(GOLDEN_FRAME, seq=n))


def test_validator_rejects_other_types():
    v = ConnectionValidator()
    with pytest.raises(TypeError):
        v.check("hello")
    with pytest.raises(TypeError):
        v.check(Frame(0, 0, [[1.0]]))


# 7.8 Exception hierarchy

def test_exception_hierarchy():
    subclasses = [BadMagicError, UnsupportedVersionError, UnknownMessageTypeError,
                  ReservedFieldError, LimitError, LengthMismatchError, ChecksumError,
                  InvalidHelloError, MessageOrderError, DimensionMismatchError]
    assert len(subclasses) == 10
    for X in subclasses:
        assert issubclass(X, ProtocolError)
    assert issubclass(ProtocolError, Exception)
    for X in subclasses + [ProtocolError]:
        assert not issubclass(X, (ValueError, TypeError))
