# Virtual EEG device

`eegpipe.device` behaves like an EEG amplifier: it streams samples over TCP
using the [EEGP protocol](PROTOCOL.md), at real-time pace, from either a
synthetic signal or a replayed EDF recording. It can also inject faults on
purpose, reproducibly, to test whatever receives the stream.

## Quick start

From the repository root, in a virtual environment with the Python package
installed (`pip install -e ./python`):

```bash
# Anything that accepts a TCP connection works as a receiver.
# macOS/BSD netcat:
nc -l 7700 > /dev/null
# (traditional netcat: nc -l -p 7700 > /dev/null)

# In another terminal: 64 synthetic channels at 1 kHz, for 10 seconds
python -m eegpipe.device synthetic --duration 10
```

The device connects to `127.0.0.1:7700` by default, sends one HELLO, then one
frame every samples-per-frame / rate seconds (20 ms at the defaults), and
prints a stats line to stderr each time at least one second has passed.

## Sources

### `synthetic`

```bash
python -m eegpipe.device synthetic --channels 1024 --rate 30000
```

| Option | Default | Meaning |
|---|---|---|
| `--channels N` | 64 | 1 to 4096 channels, named `ch0000`, `ch0001`, ... |
| `--rate HZ` | 1000 | Integer sample rate, 200 to 100000 Hz |

Each channel `c` is the sum of:

- a 10 Hz alpha wave, 20 µV amplitude, phase 0.5·c radians
- a channel-specific tone at 13 + (c mod 17) Hz, 10 µV amplitude
- 60 Hz mains hum, 5 µV amplitude, same phase on every channel
- Gaussian noise, standard deviation 5 µV, seeded by `--seed`

One second of signal is computed at startup and repeated, so the stream is
exactly periodic and cheap to produce. The known components make processing
checkable: a 60 Hz notch removes the hum, an 8–12 Hz bandpass keeps
the alpha, and each channel's tone identifies it. Because the noise repeats
every second, averaging over many seconds or very long FFTs will not behave
like real EEG.

### `replay`

```bash
mkdir -p data
curl -L -o data/S001R01.edf https://physionet.org/files/eegmmidb/1.0.0/S001/S001R01.edf
python -m eegpipe.device replay --file data/S001R01.edf
```

Replays any EDF or EDF+ file at its recorded rate, looping at the end. All
signals must share one sample rate. Units are converted to µV (unit labels
`uV`, `mV`, `V` and `nV` are supported), and labels are cleaned: leading and
trailing whitespace and trailing dots are removed (`Fc5.` → `Fc5`), and empty
labels become `chNNNN`. Files with mixed sample rates, other units, or labels
that are not 1–32 printable ASCII characters after cleaning are rejected (exit code 2).

The example file is from the EEG Motor Movement/Imagery Dataset (Schalk et
al., 2004) on PhysioNet: 64 channels at 160 Hz, 61 seconds. `data/` is
gitignored.

## Common options

| Option | Default | Meaning |
|---|---|---|
| `--host` | `127.0.0.1` | Receiver address |
| `--port` | `7700` | Receiver port |
| `--samples-per-frame N` | round(rate × 0.02), clamped to 1–1024 | 1 to 1024 |
| `--duration SECONDS` | until Ctrl-C | Stop after round(SECONDS × rate / samples-per-frame) frames (at least 1) |
| `--seed N` | 0 | Seeds the synthetic noise and every fault decision |
| `--stream-id HEX` | random | 16 lowercase hex digits, sent in the HELLO |

## Faults

| Option | Range | Effect |
|---|---|---|
| `--drop-prob P` | 0 to 1 | Skip each frame with probability P (its sequence number is used up, leaving a gap) |
| `--dup-prob P` | 0 to 1 | Send each frame twice, back to back, with probability P |
| `--corrupt-prob P` | 0 to 1 | With probability P, flip one byte of a frame's samples, so its CRC check fails |
| `--jitter-ms MS` | 0 to 1000 | Delay each frame by a random 0 to MS milliseconds past its deadline. A jitter larger than the frame period also delays the following frames, which then count as late |
| `--drift-ppm PPM` | -10000 to 10000 | Sample clock runs fast (positive) or slow (negative): frames are sent faster or slower than nominal, while timestamps keep nominal spacing |
| `--disconnect-every SECONDS` | > 0 | At the first frame boundary at which the connection has been open at least SECONDS, close it, then reconnect on the next frame. No frames are lost; combine with `--drop-prob` for gaps |

Drop, duplicate, corrupt and jitter decisions depend only on `--seed` and the
sequence number, so they repeat exactly from run to run. Overflow and
disconnect losses depend on timing, and the stream ID (unless given) and
timestamps differ per run.

## Behavior

- **Pacing.** Frame k's deadline is when its last sample would exist:
  start + (k+1) × samples-per-frame / rate on a monotonic clock (drift scales
  the rate). It is sent at its deadline plus its jitter, so timing errors never
  accumulate. `timestamp_ns` is the acquisition time of the frame's first
  sample according to the device's own sample clock.
- **Falling behind.** A late frame is sent immediately. A frame more than one
  second past its deadline is discarded and counted as `overflow`, like a real
  amplifier's buffer overflowing. The operating system's socket buffer can hold
  more data before overflow starts. A slow reconnect (up to 2 s to connect plus
  1 s for the HELLO) can make one frame go out later than that.
- **Receiver down or connection lost.** The device keeps acquiring and
  discards frames (counted as `disc`) until it reconnects. Failed connection
  attempts are retried after 0.1 s, doubling up to 2 s. A send that can't finish
  within 1 s counts as a lost connection. After a connection that lasted at
  least 1 s, the device reconnects on the next frame. After a shorter one, it
  keeps backing off, so a receiver that accepts and immediately closes isn't
  flooded. (With `--disconnect-every` below 1 s, no connection counts as long,
  so backoff from earlier failures persists.) Each new connection starts with a
  HELLO carrying the same stream ID, and sequence numbers keep counting.
- **Stopping.** At the end of `--duration` or on Ctrl-C, the device closes the
  connection, prints a summary and exits with code 0 (a Ctrl-C before streaming
  starts prints `eegpipe.device: interrupted` instead). Bad arguments or an
  unreadable replay file exit with code 2.

## Guarantees for receivers

- Frame k carries samples k × S to k × S + S − 1 of the source (S =
  samples-per-frame), whether or not earlier frames were sent. Synthetic data
  repeats every second; replayed data repeats at the end of the file.
- `timestamp_ns` of frame k is the device's wall-clock time (Unix
  nanoseconds) when streaming started, plus round(k × (S × 10⁹ / rate))
  nanoseconds. Drift does not change it.
- Duplicates are byte-identical and immediately follow the original.
- A connection can end with a truncated message: after a Ctrl-C, a send that
  timed out, or a failed send. The next connection starts cleanly with a HELLO.
- The first frame after any HELLO, including the first, may have a sequence
  number above 0.
- The seed is not sent in the stream. To rebuild expected synthetic samples, a
  receiver needs the seed separately.
- A receiver that tracks the stream ID across connections can count missing
  frames from the sequence-number gaps.

## Output

All output goes to stderr, for example (interval lines after the first omitted):

```text
start: device=eegpipe-synthetic channels=64 rate_hz=1000 samples_per_frame=20 stream_id=3f2a9c0d41b7e865 target=127.0.0.1:7700 seed=0
t=1.0s sent=50 drop=0 dup=0 corrupt=0 overflow=0 disc=0 late=0 max_late_ms=0.4 connects=1 MB/s=0.26
summary: frames=500 sent=500 drop=0 dup=0 corrupt=0 overflow=0 disc=0 late=0 max_late_ms=0.6 connects=1 connect_failures=0 forced_disconnects=0 send_errors=0 bytes=2578752 elapsed_s=10.000 MB/s=0.26
```

- Interval lines count events since the previous line. With long frames,
  lines can be further apart than one second.
- `sent` means written to the socket, not proof of delivery.
- `drop` counts frames skipped by `--drop-prob`. `overflow` and `disc` count the
  other two kinds of loss: falling more than a second behind, and having no
  working connection (including a failed send).
- `late` counts frames whose send started more than 5 ms after their scheduled
  time (deadline plus jitter). Operating-system sleep normally wakes a few
  milliseconds late (about 4 ms on macOS), so 5 ms leaves room for that. `max_late_ms` is the largest delay of any frame,
  late or not.
- `connects` counts successful connections, and `MB/s` is megabytes (10⁶ bytes)
  written per second, including HELLOs and duplicates.
- In the summary, `frames` always equals `sent + drop + overflow + disc`, and
  `elapsed_s` is the time from the start of streaming to shutdown.
- A conforming receiver closes the connection on a corrupt frame, so
  `--corrupt-prob` also causes reconnects and extra `disc` losses.
