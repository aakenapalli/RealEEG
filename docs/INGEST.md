# Ingest service

`eegpipe` is the Go pipeline core. Its first stage, ingest, accepts
[EEGP](PROTOCOL.md) connections from several devices at once, validates every
message, tracks each stream with the stream rules in [RECEIVER.md](RECEIVER.md),
and keeps the most recent frames of each stream in a ring buffer for the later
stages (processing, fanout, storage) to read. It reports the same counts as the
Python test receiver, so a [virtual device](DEVICE.md) run reconciles against
either one (see [Output](#output) for when the match is exact).

## Quick start

From the repository root, with Go 1.27 and the Python package installed:

```bash
# Build the binary (bin/ is ignored by git)
(cd go && go build -o ../bin/eegpipe ./cmd/eegpipe)

# Terminal 1: listen on 127.0.0.1:7700, exit 2 s after the last device disconnects
./bin/eegpipe --exit-after-idle 2

# Terminal 2: 10 s of synthetic data, each frame dropped with probability 0.1
python -m eegpipe.device synthetic --duration 10 --drop-prob 0.1
```

## Options

| Option | Default | Meaning |
|---|---|---|
| `--host` | `127.0.0.1` | Address to listen on |
| `--port` | `7700` | Port to listen on (1 to 65535) |
| `--duration SECONDS` | none | Stop after this long, closing any open connections |
| `--exit-after-idle SECONDS` | none | Stop once no connection has been open for this long. The countdown starts when the last open connection ends, so it never fires before the first connection |
| `--max-conns N` | 16 | Connections served at once (1 to 1024). Extra connections are accepted and closed immediately. Set it to at least the number of devices plus the number that may reconnect at the same time (twice the device count is always enough), so that a reconnecting device finds a free slot |
| `--ring-seconds S` | 2 | How much of each stream the ring buffer holds (up to 60 s) |
| `--ring-budget-mb MiB` | 1024 | Memory for all streams together, in MiB (1 to 65536): each live (not evicted) stream's ring plus 256 KiB |

Without `--duration` or `--exit-after-idle` it runs until Ctrl-C or SIGTERM,
then prints its summary. The exit code is 0, or 2 for bad options or when it
cannot listen on the address (for example, the port is already in use).

## Connections

- Each connection is served by its own goroutine, up to `--max-conns` at once.
  A connection over the limit is closed right away and counted in `rejected_conns`.
- A connection must deliver a complete HELLO within 5 s (the HELLO timeout,
  absolute, so trickling bytes does not extend it) and is closed after 5 s
  without any bytes (the idle timeout, restarted by every byte).
- **Takeover.** A stream is fed by one connection at a time. If a HELLO arrives
  for a stream whose old connection is still open, the new connection waits up
  to 500 ms for the old one to end, so every frame the old connection carried
  is counted. This is what normally happens when a device reconnects: it has
  closed its old socket, and the server finishes reading it within
  milliseconds. If the old connection is still open after 500 ms (for example a
  peer that crashed or lost the network without closing its socket), the server
  closes it and counts it in `superseded`. Bytes it had not yet read are lost;
  they count as `missing` once a later frame of the stream arrives. If several
  connections wait for the same stream, the newest one wins and the others are
  closed and counted in `superseded`.
- Any protocol error (bad magic, unsupported version, CRC mismatch, wrong frame
  size, frame before HELLO, second HELLO, invalid HELLO, and so on) closes the
  connection and is counted by type. So is a HELLO the server rejects
  (`HelloMismatchError`, `ResourceLimitError`, below). The service keeps running.
- A connection that ends in the middle of a message is counted as `truncated`.

## Streams

Streams follow the [stream rules](RECEIVER.md#stream-rules) of the test
receiver: state is keyed by `stream_id` and survives reconnects, a reconnect
whose HELLO changes `n_channels`, `samples_per_frame`, `sample_rate_hz` or
`channel_names` is rejected (`HelloMismatchError`), the first frame sets the
baseline, gaps count as `missing`, and duplicate or stale frames are discarded.
The shared scenarios in `testdata/streams/` are run against both
implementations to keep them identical.

Latency, percentiles and drift use the [definitions](RECEIVER.md#definitions)
of the test receiver, with one difference: latency percentiles come from a
histogram with 0.1 ms buckets between −1 s and +1 s. A printed p50 or p99 is
within 0.1 ms of the exact value, so its last digit may differ from the test
receiver's (values beyond ±1 s print as ±1000.0); the maximum is exact.

## Ring buffer

Each stream gets a ring of `max(2, ⌈ring_seconds × rate / samples_per_frame⌉)`
frames, allocated when the stream's first HELLO arrives. Frame `seq` lives in
slot `seq mod capacity`, so a reader asks for a frame by its sequence number and
gets one of four answers:

| Answer | Meaning |
|---|---|
| `ok` | the frame's samples (channel-major float32 µV) |
| `not_yet` | the frame is newer than anything received |
| `missing` | the frame was lost (a gap), or precedes the first frame this ring holds (the stream's first frame, or its first frame after it came back from eviction) |
| `overwritten` | the frame was received but has already been replaced by a newer one |

Reads never wait for new data, and a reader holds the ring's lock only while
copying one frame. A reader that falls behind gets `overwritten` instead of
slowing the writer down.

Every live (not evicted) stream is charged its ring plus 256 KiB (its latency
histogram and a typical record) against `--ring-budget-mb`. At full scale
(1,024 channels at 30 kHz, 600 samples per frame) a 2 s ring is 100 frames,
about 246 MB (MB = 10⁶ bytes), so the default 1 GiB holds four such streams.

When a new or returning stream does not fit, the server looks at streams that
have had no connection for at least 60 s. If evicting some of them
(longest-detached first) makes room, they are evicted. Otherwise nothing is
evicted, and the new connection is closed and counted as `ResourceLimitError`.
Eviction frees only a stream's ring and latency histogram: the stream keeps its
counts and its line in the summary (`state=evicted`, `ring_frames=0`). If its
device reconnects later with a matching HELLO, the stream gets a new ring if the
budget allows, and continues counting where it left off; its latency
percentiles then cover only the frames since it came back. If the budget does
not allow it, the connection is closed and counted as `ResourceLimitError`, and
the stream stays evicted.

Memory outside the budget:

- Each connection's read buffer grows to its largest message (up to about
  16 MiB), so there are at most `--max-conns` such buffers.
- A stream's record keeps its channel names. A stream with thousands of long
  names uses up to about 200 KB more than the 256 KiB charged.
- Evicted streams are charged nothing, but their records (up to about 200 KB
  each, mostly channel names) are kept until the service stops.

## Output

All output goes to stderr. For example, from the quick start above without
`--drop-prob` (a 10 s run of the default device, `--exit-after-idle 2`;
interval lines after the first omitted):

```text
start: listen=127.0.0.1:7700 max_conns=16 ring_seconds=2 ring_budget_mb=1024 hello_timeout_s=5 idle_timeout_s=5
t=1.0s conns=1 accepted=50 missing=0 dup=0 stale=0 errors=0 superseded=0 lat_p50_ms=0.1 lat_p99_ms=0.3 lat_max_ms=0.4 MB/s=0.26
summary: connections=1 rejected_conns=0 accepted=500 missing=0 dup=0 stale=0 eof_closes=1 truncated=0 idle_closes=0 hello_timeouts=0 conn_errors=0 accept_errors=0 superseded=0 errors=none streams=1 evicted=0 bytes=2578752 elapsed_s=12.004 MB/s=0.21
stream: id=3f2a9c0d41b7e865 device=eegpipe-synthetic channels=64 rate_hz=1000 samples_per_frame=20 initial_seq=0 last_seq=499 accepted=500 missing=0 dup=0 stale=0 ring_frames=100 state=live drift_ppm=0.3 lat_p50_ms=0.1 lat_p99_ms=0.3 lat_max_ms=0.5
```

- Interval lines are at least 1 s apart and count events since the previous
  line. `conns` there counts every accepted socket, including rejected ones,
  like `connections` in the summary.
- `errors` is a count in interval lines and a list by type in the summary, for
  example `errors=ChecksumError:3,HelloMismatchError:1`, or `none`.
- `streams` counts every stream created, and `evicted` the streams that are
  evicted when the service stops.
- `elapsed_s` and the summary's `MB/s` run from startup to shutdown. In `MB/s`,
  MB means 10⁶ bytes; `ring_budget_mb` is in MiB.
- A connection closed because the service is stopping is not counted in any
  close counter.
- When the service never closes a connection itself and runs until the device
  has finished, the counts reconcile with the device's summary exactly as for the
  [test receiver](RECEIVER.md#output): `accepted` equals the device's `sent`,
  `dup` its `dup`, and `initial_seq + missing + (frames − 1 − last_seq)` equals
  its `drop + overflow + disc`.

## Code layout

| Package | Contents |
|---|---|
| `go/internal/protocol` | EEGP decoding and encoding, strict HELLO parsing, the per-connection validator; tested against `testdata/protocol/` |
| `go/internal/ring` | The per-stream frame ring |
| `go/internal/ingest` | The stream registry (stream rules, latency, drift, ring budget) and the TCP server |
| `go/cmd/eegpipe` | The command-line binary |

```bash
cd go
go test ./...                       # unit, scenario and server tests
go test -race ./...                 # the same under the race detector
go test -tags e2e -run E2E ./internal/ingest   # the real Python device against the server (needs .venv)
```

## Limits

- Latency compares the device's and the server's wall clocks, so it is only
  meaningful on one host or with synchronized clocks.
- Stream state lives in memory and is lost when the service stops.
- Stream records are never removed while the service runs, so a client that keeps
  creating new stream IDs makes memory grow outside the budget.
- Readers cannot yet wait for new frames; they poll `Head`. A notification
  mechanism comes with the processing stage.
