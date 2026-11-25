# EEGP Wire Protocol

**Version:** 1

EEGP carries multichannel signal samples from a device to an ingest service
over a single TCP connection. This document is the source of truth for the
format. Any change to the bytes or their meaning requires a new version number.

## Conventions

- The key words MUST, MUST NOT and SHOULD are used as described in RFC 2119
  and RFC 8174.
- The **sender** is the device; the **receiver** is the ingest service.
- Every multi-byte integer and float is **little-endian**.
- Ranges such as 1–4096 are inclusive.
- **Printable ASCII** means characters 0x20–0x7E (space included). For JSON
  strings, this applies to the string value after JSON unescaping.
- Field names in `code` are the identifiers used throughout this document.

## Overview

- **Transport:** TCP. The sender connects to the receiver. Senders SHOULD
  disable Nagle's algorithm (`TCP_NODELAY`).
- **Direction:** one-way. The receiver MUST NOT send bytes to the sender.
- **Messages:** the sender MUST send exactly one `HELLO` as the first message
  of each connection, followed only by `FRAME`s.

## Message structure

Every message has three parts:

| Part | Size |
|---|---|
| Common header | 12 bytes |
| Body | `body_length` bytes |
| CRC-32 trailer | 4 bytes |

### Common header

| Offset | Size | Type | Field | Rule |
|---|---|---|---|---|
| 0 | 4 | bytes | `magic` | MUST be ASCII `EEGP` (`45 45 47 50`) |
| 4 | 1 | uint8 | `version` | MUST be `1` |
| 5 | 1 | uint8 | `msg_type` | MUST be `1` (HELLO) or `2` (FRAME) |
| 6 | 2 | uint16 | `reserved` | MUST be `0` |
| 8 | 4 | uint32 | `body_length` | number of body bytes (excludes header and CRC) |

`body_length` MUST NOT exceed 262,144 for HELLO or 16,777,236 for FRAME. A
receiver MUST check this before allocating memory for the body.

### CRC-32 trailer

A uint32 holding the CRC-32 (IEEE 802.3 polynomial, as computed by zlib's
`crc32` and Go's `hash/crc32.ChecksumIEEE`) of every byte of the message before
the trailer: the header and the body.

## HELLO (`msg_type` 1)

The body is a UTF-8 JSON object with exactly these keys. Key names are
case-sensitive. Key order and whitespace do not matter.

| Key | JSON type | Rule |
|---|---|---|
| `stream_id` | string | the stream's 64-bit ID as exactly 16 lowercase hex digits, most significant digit first, zero-padded (ID 42 is `000000000000002a`) |
| `device_name` | string | 1–64 printable ASCII characters |
| `n_channels` | integer | 1–4096 |
| `samples_per_frame` | integer | 1–1024 |
| `sample_rate_hz` | number | greater than 0 and at most 100,000 |
| `units` | string | exactly `"uV"` (microvolts) |
| `channel_names` | array of strings | exactly `n_channels` entries; each 1–32 printable ASCII characters; duplicates allowed |

- An **integer** is a JSON number written without a fraction or exponent
  (`3`, not `3.0` or `3e0`).
- A **number** is any JSON number whose value, as an IEEE-754 double, is finite
  (`1e999` is invalid). `true` and `false` are not numbers.
- A receiver MUST reject a HELLO with missing keys, extra keys, a key that
  appears twice, a wrong type, a value outside its rule, or the non-standard
  literals `NaN`, `Infinity` and `-Infinity`.
- A receiver MUST treat any body it cannot parse within its implementation
  limits (for example extremely long number literals or very deep nesting) as
  an invalid HELLO, never as a crash. A conforming sender never produces such
  a body.
- Within these rules, receivers MUST accept any valid JSON encoding of a
  correct HELLO: key order, whitespace and string escapes do not matter, and
  `sample_rate_hz` may use any valid number form (`160`, `160.0` and `1.6e2`
  are equal).

## FRAME (`msg_type` 2)

| Body offset | Size | Type | Field |
|---|---|---|---|
| 0 | 8 | uint64 | `seq` |
| 8 | 8 | int64 | `timestamp_ns` |
| 16 | 2 | uint16 | `n_channels` |
| 18 | 2 | uint16 | `samples_per_frame` |
| 20 | C × S × 4 | float32[] | `samples`, in microvolts |

Here C = `n_channels` and S = `samples_per_frame`.

- `body_length` MUST equal exactly 20 + C × S × 4.
- `n_channels` MUST be 1–4096 and `samples_per_frame` MUST be 1–1024, and both
  MUST equal the values in the connection's HELLO.
- `samples` are channel-major: all S samples of channel 0 in time order, then
  all of channel 1, and so on. Any float32 value is allowed, including NaN and
  infinities. Samples start at message offset 32, so they are 4-byte aligned
  relative to the start of the message.
- `seq`: the first FRAME of a stream has `seq` 0, and each following frame adds
  1. A frame the sender deliberately skips (for example during fault testing)
  still uses up its `seq`, so a jump in `seq` marks lost frames. Numbering
  continues across reconnects of the same stream. The frame with `seq` n begins
  at sample index n × S. A 64-bit `seq` does not wrap in practice; a sender
  MUST start a new stream rather than wrap.
- `timestamp_ns`: when the frame's first sample was acquired, as nanoseconds
  since the Unix epoch, by the sender's clock. It is for latency measurement
  and drift detection. The authoritative sample timing is `seq` together with
  `sample_rate_hz`.

## Streams

A **stream** is identified by the `stream_id` in the HELLO. Every FRAME belongs
to the stream named by its connection's HELLO. A sender SHOULD choose
`stream_id` uniformly at random when it starts a stream, and MUST keep it,
together with its `seq` counter, across reconnects of that stream.
`n_channels`, `samples_per_frame` and `sample_rate_hz` MUST NOT change within a
stream; to change them, a sender starts a new stream with a new `stream_id`.
How a receiver tracks a stream across connections (including a reconnect HELLO
whose dimensions differ from the stream's earlier HELLO) is defined by the
receiver.

## Receiver rules

**Validation order.** Before reading the body, a receiver MUST check the
common header and, once it has received the HELLO, that a FRAME's
`body_length` equals 20 + C × S × 4 for the HELLO's dimensions. Before
interpreting the body, it SHOULD verify the total message length
(12 + `body_length` + 4 bytes) and the CRC. Beyond that, the order of checks is
not specified.

**Protocol errors.** Each of the following is a protocol error. On any of them
the receiver MUST close the connection and MUST NOT try to resynchronize,
because after a bad header it cannot know where the next message starts.
- `magic` is not `EEGP`
- unsupported `version`
- unknown `msg_type`
- `reserved` is not 0
- `body_length` above the maximum for its type
- total message length is not 12 + `body_length` + 4 bytes
- CRC mismatch
- invalid HELLO body
- a FRAME before the HELLO, or a second HELLO
- FRAME `body_length` less than 20, or not equal to 20 + C × S × 4
- FRAME `n_channels` or `samples_per_frame` outside its range, or different
  from the HELLO

**Stream anomalies.** These are not protocol errors; the connection stays open.
Receivers SHOULD count and log them.
- a gap in `seq` (lost frames)
- a `seq` equal to or lower than the highest `seq` already received on the
  connection (duplicate or stale frame)

A receiver MUST discard a duplicate or stale frame.

**Resource limits.** Receivers SHOULD enforce a deadline for receiving the
HELLO, an idle timeout, and a maximum number of connections. The values are
chosen by each receiver.

## Example

A FRAME with `seq` 41, `timestamp_ns` 1,700,000,000,123,456,789, 3 channels with
2 samples per channel, and samples `[[12.5, 13.25], [-3.0, 0.0], [1.5, -0.5]]`
is these 60 bytes (hex):

```
45 45 47 50 01 02 00 00 2c 00 00 00    header: EEGP, version 1, FRAME, reserved 0, body_length 44
29 00 00 00 00 00 00 00                seq 41
15 cd 85 3d fe 9c 97 17                timestamp_ns
03 00 02 00                            n_channels 3, samples_per_frame 2
00 00 48 41 00 00 54 41                channel 0: 12.5, 13.25
00 00 40 c0 00 00 00 00                channel 1: -3.0, 0.0
00 00 c0 3f 00 00 00 bf                channel 2: 1.5, -0.5
fa 73 63 b8                            CRC-32
```

Test vectors for this and other messages are in `testdata/protocol/`.
