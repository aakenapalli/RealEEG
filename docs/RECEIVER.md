# Test receiver

`eegpipe.receiver` listens for EEGP devices, validates every message with the
[protocol](PROTOCOL.md) library, tracks each stream across reconnects, and
reports what arrived: gaps, duplicates, stale frames, protocol errors, latency,
clock drift and, optionally, whether every sample is exactly what the
[virtual device](DEVICE.md) sent. Its stream rules and definitions are the
reference for any other receiver, including the Go ingest service.

## Quick start

```bash
# Terminal 1: receive, verify samples, exit 2 s after the device disconnects
python -m eegpipe.receiver --verify-synthetic --exit-after-idle 2

# Terminal 2: 10 s of synthetic data, each frame dropped with probability 0.1
python -m eegpipe.device synthetic --duration 10 --drop-prob 0.1
```

## Options

| Option | Default | Meaning |
|---|---|---|
| `--host` | `127.0.0.1` | Address to listen on |
| `--port` | `7700` | Port to listen on |
| `--duration SECONDS` | none | Stop after this long, closing any open connection |
| `--exit-after-idle SECONDS` | none | Once a connection has ended, stop if no new connection arrives within this long. It never fires before the first connection |
| `--verify-synthetic` | off | Check every sample against the synthetic signal the device generates |
| `--seed N` | 0 | Seed for `--verify-synthetic` (requires it); must match the device's `--seed` |
| `--verify-replay FILE` | off | Check every sample against this EDF file (the one the device replays) |

Without `--duration` or `--exit-after-idle`, it runs until Ctrl-C. The exit code
is 0 after a normal stop or Ctrl-C (after startup the summary is still printed; during
startup it prints `eegpipe.receiver: interrupted`), and 2 for bad
arguments, an unreadable replay file or a port already in use.

## Connections

- One connection is served at a time. Others queue in the operating system's
  backlog and are served in order. Their data is buffered meanwhile, so a
  queued device may hit its 1 s send timeout and reconnect. Connections still
  queued when the receiver stops are never served.
- A connection must deliver a complete HELLO within 5 s of the receiver starting
  to serve it (the HELLO timeout), and is closed after 5 s without any bytes
  (the idle timeout). The HELLO deadline is absolute, so trickling the HELLO
  does not extend it. The idle timeout restarts with every byte, so after the
  HELLO a peer that keeps sending, however slowly, keeps its connection while
  queued connections wait. ("Idle" here is per connection; `--exit-after-idle`
  is about the whole receiver having no connection.)
- Any protocol error (bad magic, unsupported version, CRC mismatch, wrong frame
  size, frame before HELLO, second HELLO, HELLO mismatch, and so on) closes the
  connection and is counted by type. The receiver keeps listening.
- A connection that ends in the middle of a message is counted as `truncated`.

## Stream rules

These are the rules any receiver of this protocol must apply.

1. A stream is identified by its `stream_id`. Its state survives reconnects.
2. A reconnect's HELLO must match the stream's first HELLO in `n_channels`,
   `samples_per_frame`, `sample_rate_hz` and `channel_names`. If it doesn't,
   it is a protocol error (`HelloMismatchError`), the connection is closed, and
   the stream's state is unchanged. A different `device_name` alone is allowed.
3. The first frame of a stream sets the baseline (`initial_seq`). It is not a gap,
   even if it is above 0.
4. After that, with `last` the highest accepted sequence number:
   - `seq > last`: accepted; `seq − last − 1` frames are counted as `missing` (a gap).
   - `seq == last`: a duplicate; discarded.
   - `seq < last`: stale; discarded.
5. Frames lost before the first frame received, or after the last, can't be seen.
6. Use a new `stream_id` for each run of a device. A device restarted with the same
   `stream_id` (and `seq` back at 0) has its frames classified as stale (or
   duplicate at `seq == last`) until `seq` exceeds `last`.

## Definitions

Times are integer nanoseconds unless stated otherwise. S is samples per frame and `rate` the sample
rate in Hz, both from the stream's HELLO.

- **Arrival time:** the receiver's wall clock (Unix ns) as soon as the last byte
  of a message has been read, before the message is decoded.
- **Latency** of an accepted frame: arrival − (`timestamp_ns` + round(S × 10⁹ / rate)).
  `round` rounds halves to even. It compares two wall clocks, so it is only
  meaningful when device and receiver share a clock (same host) or have
  synchronized clocks.
- **Percentiles** are nearest-rank: the q-quantile of n values (q = 0.5 for p50,
  0.99 for p99) is the value at 1-based rank ⌈q × n⌉ in ascending order.
  Reported p50, p99 and the maximum, in milliseconds to 0.1 ms.
- **Drift** is fitted per stream over accepted frames only. For each, x =
  (`timestamp_ns` − first accepted `timestamp_ns`) / 10⁹ and y = ((arrival −
  `timestamp_ns`) − the first accepted frame's (arrival − `timestamp_ns`)) / 10⁹.
  With n points, d = n·Σx² − (Σx)²; drift is `n/a` if n < 2 or d ≤ 0, otherwise
  −(n·Σxy − Σx·Σy) / d × 10⁶ ppm. Positive means the device's sample clock runs
  fast. A device run with `--drift-ppm 10000` reports about 9900 (10000 / 1.01).

## Verification

With `--verify-synthetic` or `--verify-replay`, each accepted frame with sequence
number k is compared byte for byte with samples k × S to k × S + S − 1 of the
source, wrapping at the end of the source. A frame that differs is counted as a
`mismatch`. Corrupted frames never get this far, because the CRC check rejects
them first. If a stream's HELLO doesn't fit the source (sample rate, channel
count or channel names), or the source can't be built, its verification is
reported as `unavailable`.

The synthetic signal is built once per channel count and rate, on the order of
1 s at full scale. While that happens the device may time out and reconnect once.
Verification keeps each built signal in memory (about 123 MB at 1,024 channels
and 30 kHz), so it's meant for test runs, not for unbounded streams of every shape.

## Output

All output goes to stderr. For example, from a run without faults with the
device started right after the receiver (interval lines after the first omitted):

```text
start: listen=127.0.0.1:7700 verify=synthetic hello_timeout_s=5 idle_timeout_s=5
t=1.0s conns=1 accepted=50 missing=0 dup=0 stale=0 mismatch=0 errors=0 lat_p50_ms=0.2 lat_p99_ms=0.4 lat_max_ms=0.5 MB/s=0.26
summary: connections=1 accepted=500 missing=0 dup=0 stale=0 mismatch=0 eof_closes=1 truncated=0 idle_closes=0 hello_timeouts=0 conn_errors=0 accept_errors=0 errors=none streams=1 bytes=2578752 elapsed_s=12.004 MB/s=0.21
stream: id=3f2a9c0d41b7e865 device=eegpipe-synthetic channels=64 rate_hz=1000 samples_per_frame=20 initial_seq=0 last_seq=499 accepted=500 missing=0 dup=0 stale=0 mismatch=0 verify=on drift_ppm=0.3 lat_p50_ms=0.2 lat_p99_ms=0.4 lat_max_ms=0.6
```

- Interval lines are at least 1 s apart, including while waiting for a
  connection, and count events since the previous line. `conns` there is the
  same counter as `connections` in the summary.
- `errors` is a count in interval lines and a list by type in the summary, for
  example `errors=ChecksumError:3`, or `none`.
- `elapsed_s` and the summary's `MB/s` run from startup to shutdown, so they
  include time spent waiting for a device.
- A connection closed because `--duration` ran out is not counted in any of the
  close counters.
- Latency is printed in milliseconds and drift in ppm, both to one decimal
  place; MB means 10⁶ bytes.
- When the receiver never closes a connection itself (no protocol error, timeout
  or `--duration` stop) and runs until the device has finished, compare with the
  device's own counts: `accepted` equals the device's `sent`,
  `dup` equals its `dup`, and `initial_seq + missing + (frames − 1 − last_seq)`
  equals its `drop + overflow + disc`, where `frames` is the device's summary
  value. With `--corrupt-prob`, the receiver also loses frames the device had
  already sent when the connection closed, so only `accepted ≤ sent` and
  `ChecksumError ≤ corrupt` hold.

## Notes for other implementations

- `seq` is an unsigned 64-bit integer and times are signed 64-bit nanoseconds.
  Use unsigned 64-bit counters, compute `seq − last − 1` only when `seq > last`,
  and compute a sample offset as `((seq mod period) × S) mod period` to avoid overflow.
  Compute `arrival − timestamp_ns` and the drift-fit offsets in 64-bit floats or with
  overflow checks, since a CRC-valid frame can carry an extreme timestamp.
- Look samples up with a pure function of k rather than shared mutable state if
  streams are handled concurrently.
- This receiver keeps every latency value in memory (about 6.5 MB per stream per
  hour at 50 frames/s, as Python ints); a long-running service should use a histogram.
- The drift fit uses wall-clock arrival times, so a clock step or slew (NTP) or a
  delivery delay (backlog, building a verification signal) during a run skews it.
