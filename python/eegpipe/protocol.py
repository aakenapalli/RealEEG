import json
import re
import struct
import zlib
from dataclasses import dataclass

import numpy as np

MAGIC = b"EEGP"
VERSION = 1
MSG_HELLO = 1
MSG_FRAME = 2

HEADER_SIZE = 12          # common header
CRC_SIZE = 4              # trailer
FRAME_FIXED_SIZE = 20     # seq + timestamp_ns + n_channels + samples_per_frame

MAX_CHANNELS = 4096
MAX_SAMPLES_PER_FRAME = 1024
MAX_SAMPLE_RATE_HZ = 100_000.0
MAX_HELLO_BODY = 262_144                                                      # 256 KiB
MAX_FRAME_BODY = FRAME_FIXED_SIZE + MAX_CHANNELS * MAX_SAMPLES_PER_FRAME * 4  # 16_777_236
MAX_DEVICE_NAME_LEN = 64
MAX_CHANNEL_NAME_LEN = 32
UNITS = "uV"

_HEADER_STRUCT = "<4sBBHI"
_FRAME_FIXED_STRUCT = "<QqHH"
_CRC_STRUCT = "<I"

_HELLO_KEYS = {"stream_id", "device_name", "n_channels", "samples_per_frame",
               "sample_rate_hz", "units", "channel_names"}


def _crc_trailer(b: bytes) -> bytes:
    return struct.pack("<I", zlib.crc32(b) & 0xFFFFFFFF)


class ProtocolError(Exception):
    """Base class for all bad-incoming-bytes errors."""


class BadMagicError(ProtocolError):
    """The header magic is not EEGP."""


class UnsupportedVersionError(ProtocolError):
    """The header version is not supported."""


class UnknownMessageTypeError(ProtocolError):
    """The header message type is unknown."""


class ReservedFieldError(ProtocolError):
    """The header reserved field is not zero."""


class LimitError(ProtocolError):
    """A length or dimension exceeds its allowed limit."""


class LengthMismatchError(ProtocolError):
    """A message or body length does not match what the fields require."""


class ChecksumError(ProtocolError):
    """The CRC-32 trailer does not match the message."""


class InvalidHelloError(ProtocolError):
    """The HELLO body is invalid."""


class MessageOrderError(ProtocolError):
    """A message arrived in the wrong order."""


class DimensionMismatchError(ProtocolError):
    """A FRAME does not match the dimensions in the HELLO."""


@dataclass(frozen=True)
class Header:
    msg_type: int        # MSG_HELLO or MSG_FRAME
    body_length: int     # body bytes, excluding header and CRC


@dataclass(frozen=True)
class Hello:
    stream_id: int                 # 0..2**64 - 1
    device_name: str
    n_channels: int
    samples_per_frame: int
    sample_rate_hz: float
    channel_names: tuple[str, ...]


@dataclass(frozen=True, eq=False)
class Frame:
    seq: int                       # 0..2**64 - 1
    timestamp_ns: int              # -2**63..2**63 - 1
    samples: np.ndarray            # dtype '<f4', shape (n_channels, samples_per_frame)


def _is_int(x) -> bool:
    return type(x) is int


def _is_number(x) -> bool:
    return type(x) in (int, float)


def _is_printable_ascii(s: str) -> bool:
    return all(" " <= ch <= "~" for ch in s)


def _valid_sample_rate(x) -> bool:
    return _is_number(x) and 0 < x <= MAX_SAMPLE_RATE_HZ


def encode_hello(hello: Hello) -> bytes:
    if not isinstance(hello, Hello):
        raise TypeError("hello must be a Hello")
    if not _is_int(hello.stream_id):
        raise TypeError("stream_id must be an int")
    if not 0 <= hello.stream_id <= 2**64 - 1:
        raise ValueError("stream_id outside 0..2**64 - 1")
    if not isinstance(hello.device_name, str):
        raise TypeError("device_name must be a str")
    if not 1 <= len(hello.device_name) <= MAX_DEVICE_NAME_LEN:
        raise ValueError(f"device_name length outside 1..{MAX_DEVICE_NAME_LEN}")
    if not _is_printable_ascii(hello.device_name):
        raise ValueError("device_name must be printable ASCII")
    if not _is_int(hello.n_channels):
        raise TypeError("n_channels must be an int")
    if not 1 <= hello.n_channels <= MAX_CHANNELS:
        raise ValueError(f"n_channels outside 1..{MAX_CHANNELS}")
    if not _is_int(hello.samples_per_frame):
        raise TypeError("samples_per_frame must be an int")
    if not 1 <= hello.samples_per_frame <= MAX_SAMPLES_PER_FRAME:
        raise ValueError(f"samples_per_frame outside 1..{MAX_SAMPLES_PER_FRAME}")
    if not _is_number(hello.sample_rate_hz):
        raise TypeError("sample_rate_hz must be an int or float")
    if not _valid_sample_rate(hello.sample_rate_hz):
        raise ValueError(f"sample_rate_hz must be > 0 and <= {MAX_SAMPLE_RATE_HZ}")
    if type(hello.channel_names) is not tuple:
        raise TypeError("channel_names must be a tuple")
    if not all(isinstance(n, str) for n in hello.channel_names):
        raise TypeError("every channel name must be a str")
    if len(hello.channel_names) != hello.n_channels:
        raise ValueError("channel_names length must equal n_channels")
    for name in hello.channel_names:
        if not 1 <= len(name) <= MAX_CHANNEL_NAME_LEN:
            raise ValueError(f"channel name length outside 1..{MAX_CHANNEL_NAME_LEN}")
        if not _is_printable_ascii(name):
            raise ValueError("channel name must be printable ASCII")
    obj = {
        "stream_id": format(hello.stream_id, "016x"),
        "device_name": hello.device_name,
        "n_channels": hello.n_channels,
        "samples_per_frame": hello.samples_per_frame,
        "sample_rate_hz": float(hello.sample_rate_hz),
        "units": UNITS,
        "channel_names": list(hello.channel_names),
    }
    body = json.dumps(obj, separators=(",", ":")).encode("ascii")
    if len(body) > MAX_HELLO_BODY:
        raise ValueError(f"HELLO body {len(body)} bytes exceeds maximum {MAX_HELLO_BODY}")
    header = struct.pack(_HEADER_STRUCT, MAGIC, VERSION, MSG_HELLO, 0, len(body))
    return header + body + _crc_trailer(header + body)


def encode_frame(frame: Frame) -> bytes:
    if not isinstance(frame, Frame):
        raise TypeError("frame must be a Frame")
    if not _is_int(frame.seq):
        raise TypeError("seq must be an int")
    if not 0 <= frame.seq <= 2**64 - 1:
        raise ValueError("seq outside 0..2**64 - 1")
    if not _is_int(frame.timestamp_ns):
        raise TypeError("timestamp_ns must be an int")
    if not -(2**63) <= frame.timestamp_ns <= 2**63 - 1:
        raise ValueError("timestamp_ns outside -2**63..2**63 - 1")
    if not isinstance(frame.samples, np.ndarray):
        raise TypeError("samples must be a numpy array")
    if frame.samples.dtype != np.dtype("<f4"):
        raise TypeError("samples dtype must be '<f4'")
    if frame.samples.ndim != 2:
        raise ValueError("samples must be 2-dimensional")
    if not 1 <= frame.samples.shape[0] <= MAX_CHANNELS:
        raise ValueError(f"samples channels outside 1..{MAX_CHANNELS}")
    if not 1 <= frame.samples.shape[1] <= MAX_SAMPLES_PER_FRAME:
        raise ValueError(f"samples per frame outside 1..{MAX_SAMPLES_PER_FRAME}")
    c, s = frame.samples.shape
    payload = frame.samples.tobytes(order="C")
    body = struct.pack(_FRAME_FIXED_STRUCT, frame.seq, frame.timestamp_ns, c, s) + payload
    header = struct.pack(_HEADER_STRUCT, MAGIC, VERSION, MSG_FRAME, 0, len(body))
    return header + body + _crc_trailer(header + body)


def decode_header(data: bytes) -> Header:
    if type(data) is not bytes:
        raise TypeError("data must be bytes")
    if len(data) != HEADER_SIZE:
        raise ValueError(f"header must be {HEADER_SIZE} bytes, got {len(data)}")
    magic, version, msg_type, reserved, body_length = struct.unpack(_HEADER_STRUCT, data)
    if magic != MAGIC:
        raise BadMagicError(f"bad magic {magic!r}, expected {MAGIC!r}")
    if version != VERSION:
        raise UnsupportedVersionError(f"unsupported version {version}, expected {VERSION}")
    if msg_type not in (MSG_HELLO, MSG_FRAME):
        raise UnknownMessageTypeError(f"unknown message type {msg_type}")
    if reserved != 0:
        raise ReservedFieldError(f"reserved field is {reserved}, expected 0")
    limit = MAX_HELLO_BODY if msg_type == MSG_HELLO else MAX_FRAME_BODY
    if body_length > limit:
        raise LimitError(
            f"body length {body_length} exceeds maximum {limit} for message type {msg_type}")
    return Header(msg_type, body_length)


def _reject_constant(name):
    raise InvalidHelloError(f"invalid HELLO: non-standard JSON literal {name}")


def _reject_duplicate_keys(pairs):
    keys = set()
    for k, _ in pairs:
        if k in keys:
            raise InvalidHelloError(f"invalid HELLO: duplicate key {k!r}")
        keys.add(k)
    return dict(pairs)


def _bad_hello(reason: str) -> InvalidHelloError:
    return InvalidHelloError(f"invalid HELLO: {reason}")


def _decode_hello_body(data: bytes, end: int) -> Hello:
    try:
        text = data[HEADER_SIZE:end].decode("utf-8")
        obj = json.loads(text, parse_constant=_reject_constant,
                         object_pairs_hook=_reject_duplicate_keys)
    except (ValueError, RecursionError) as e:
        raise InvalidHelloError(f"invalid HELLO: cannot parse body ({e})") from e
    if type(obj) is not dict:
        raise _bad_hello("body is not a JSON object")
    if set(obj) != _HELLO_KEYS:
        raise _bad_hello(f"keys are {sorted(obj)}, expected {sorted(_HELLO_KEYS)}")
    v = obj["stream_id"]
    if not (isinstance(v, str) and re.fullmatch(r"[0-9a-f]{16}", v)):
        raise _bad_hello("stream_id must be 16 lowercase hex digits")
    v = obj["device_name"]
    if not (isinstance(v, str) and 1 <= len(v) <= MAX_DEVICE_NAME_LEN
            and _is_printable_ascii(v)):
        raise _bad_hello("device_name invalid")
    v = obj["n_channels"]
    if not (_is_int(v) and 1 <= v <= MAX_CHANNELS):
        raise _bad_hello("n_channels invalid")
    v = obj["samples_per_frame"]
    if not (_is_int(v) and 1 <= v <= MAX_SAMPLES_PER_FRAME):
        raise _bad_hello("samples_per_frame invalid")
    if not _valid_sample_rate(obj["sample_rate_hz"]):
        raise _bad_hello("sample_rate_hz invalid")
    v = obj["units"]
    if not (isinstance(v, str) and v == UNITS):
        raise _bad_hello(f"units must be {UNITS!r}")
    v = obj["channel_names"]
    if type(v) is not list:
        raise _bad_hello("channel_names must be an array")
    if len(v) != obj["n_channels"]:
        raise _bad_hello("channel_names length must equal n_channels")
    for name in v:
        if not (isinstance(name, str) and 1 <= len(name) <= MAX_CHANNEL_NAME_LEN
                and _is_printable_ascii(name)):
            raise _bad_hello("channel name invalid")
    return Hello(stream_id=int(obj["stream_id"], 16), device_name=obj["device_name"],
                 n_channels=obj["n_channels"], samples_per_frame=obj["samples_per_frame"],
                 sample_rate_hz=float(obj["sample_rate_hz"]),
                 channel_names=tuple(obj["channel_names"]))


def _decode_frame_body(data: bytes, header: Header) -> Frame:
    if header.body_length < FRAME_FIXED_SIZE:
        raise LengthMismatchError(
            f"FRAME body length {header.body_length} is shorter than the 20-byte fixed part")
    seq, timestamp_ns, c, s = struct.unpack_from(_FRAME_FIXED_STRUCT, data, HEADER_SIZE)
    if not 1 <= c <= MAX_CHANNELS:
        raise LimitError(f"n_channels {c} outside 1..4096")
    if not 1 <= s <= MAX_SAMPLES_PER_FRAME:
        raise LimitError(f"samples_per_frame {s} outside 1..1024")
    exp = FRAME_FIXED_SIZE + c * s * 4
    if header.body_length != exp:
        raise LengthMismatchError(
            f"FRAME body length {header.body_length} does not match "
            f"20 + {c} x {s} x 4 = {exp}")
    samples = np.frombuffer(data, dtype="<f4", count=c * s,
                            offset=HEADER_SIZE + FRAME_FIXED_SIZE).reshape(c, s)
    samples.flags.writeable = False
    return Frame(seq, timestamp_ns, samples)


def decode_message(data: bytes) -> Hello | Frame:
    if type(data) is not bytes:
        raise TypeError("data must be bytes")
    if len(data) < HEADER_SIZE:
        raise LengthMismatchError(
            f"message is {len(data)} bytes, shorter than the 12-byte header")
    header = decode_header(data[:HEADER_SIZE])
    exp = HEADER_SIZE + header.body_length + CRC_SIZE
    if len(data) != exp:
        raise LengthMismatchError(f"message is {len(data)} bytes, expected {exp}")
    end = HEADER_SIZE + header.body_length
    calc = zlib.crc32(memoryview(data)[:end]) & 0xFFFFFFFF
    stored = struct.unpack_from(_CRC_STRUCT, data, end)[0]
    if calc != stored:
        raise ChecksumError(f"CRC mismatch: computed {calc:08x}, message has {stored:08x}")
    if header.msg_type == MSG_HELLO:
        return _decode_hello_body(data, end)
    return _decode_frame_body(data, header)


class ConnectionValidator:
    hello: Hello | None

    def __init__(self) -> None:
        self.hello = None

    def check_header(self, header: Header) -> None:
        if not isinstance(header, Header):
            raise TypeError("header must be a Header")
        if header.msg_type not in (MSG_HELLO, MSG_FRAME):
            raise ValueError(f"unknown msg_type {header.msg_type}")
        if header.msg_type == MSG_HELLO:
            if self.hello is not None:
                raise MessageOrderError("second HELLO received on this connection")
            return
        if self.hello is None:
            raise MessageOrderError("FRAME received before HELLO")
        c, s = self.hello.n_channels, self.hello.samples_per_frame
        exp = FRAME_FIXED_SIZE + c * s * 4
        if header.body_length != exp:
            raise DimensionMismatchError(
                f"FRAME body length {header.body_length} does not match "
                f"HELLO dimensions ({c}, {s}), expected {exp}")

    def check(self, message: Hello | Frame) -> None:
        if isinstance(message, Hello):
            if self.hello is not None:
                raise MessageOrderError("second HELLO received on this connection")
            self.hello = message
        elif isinstance(message, Frame):
            if not isinstance(message.samples, np.ndarray):
                raise TypeError("samples must be a numpy array")
            if self.hello is None:
                raise MessageOrderError("FRAME received before HELLO")
            c, s = self.hello.n_channels, self.hello.samples_per_frame
            if message.samples.shape != (c, s):
                raise DimensionMismatchError(
                    f"FRAME shape {message.samples.shape} does not match HELLO ({c}, {s})")
        else:
            raise TypeError("message must be a Hello or Frame")
