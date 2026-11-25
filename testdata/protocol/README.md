# Protocol test vectors

Golden messages for the EEGP wire protocol (`docs/PROTOCOL.md`), shared by the
Python and Go test suites.

**File format:** ASCII hex text. Everything from `#` to the end of a line is a
comment. All whitespace is ignored. The remaining characters are pairs of hex
digits, one pair per byte.

**Golden HELLO:** `stream_id` `9f3a5c0e12b47d68`, `device_name`
`virtual-synthetic`, 3 channels, 2 samples per frame, 160.0 Hz, units `uV`,
channel names `Fp1`, `Cz`, `O2`.

**Golden FRAME:** `seq` 41, `timestamp_ns` 1,700,000,000,123,456,789, samples
`[[12.5, 13.25], [-3.0, 0.0], [1.5, -0.5]]`.

Each vector is decoded on its own, with no connection context (no prior HELLO).

`hello_valid.hex` is the exact output of the reference (Python) encoder, which
writes compact JSON with keys in the order shown in the body comment of
`hello_valid.hex`. Other senders may encode the same HELLO differently (see the
HELLO rules in `docs/PROTOCOL.md`).

| File | Expected result | Python exception |
|---|---|---|
| `hello_valid.hex` | decodes to the golden HELLO | — |
| `frame_valid.hex` | decodes to the golden FRAME | — |
| `frame_bad_magic.hex` | rejected: `magic` is `EEGX` | `BadMagicError` |
| `frame_bad_version.hex` | rejected: `version` 2 | `UnsupportedVersionError` |
| `frame_unknown_type.hex` | rejected: `msg_type` 3 | `UnknownMessageTypeError` |
| `frame_reserved_nonzero.hex` | rejected: `reserved` is 1 | `ReservedFieldError` |
| `frame_bad_crc.hex` | rejected: CRC mismatch | `ChecksumError` |
| `frame_zero_channels.hex` | rejected: `n_channels` 0 | `LimitError` |
| `frame_length_mismatch.hex` | rejected: `body_length` 44 does not match 20 + 3 × 3 × 4 = 56 | `LengthMismatchError` |
| `hello_missing_key.hex` | rejected: key `units` missing | `InvalidHelloError` |
| `hello_extra_key.hex` | rejected: extra key `extra` | `InvalidHelloError` |
| `hello_duplicate_key.hex` | rejected: key `units` appears twice | `InvalidHelloError` |
| `hello_wrong_case_key.hex` | rejected: key `Units` (keys are case-sensitive) | `InvalidHelloError` |
| `hello_float_n_channels.hex` | rejected: `n_channels` written as `3.0` | `InvalidHelloError` |
