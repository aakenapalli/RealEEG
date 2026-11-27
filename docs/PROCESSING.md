# Processing stage

The processing stage is the second stage of `eegpipe`, after [ingest](INGEST.md).
For every stream whose rate it supports, it filters the samples, lowers the
sample rate to 250 Hz (or the lowest rate above it that the frame size allows),
and computes the power in five EEG frequency bands about four times a second. It
publishes both into per-stream ring buffers that later stages read at their own
pace. It runs inside the same binary as ingest; there is nothing extra to start.

## The chain

For each channel of each frame, in order:

1. **Anti-alias low-pass** at the device rate: an 8th-order Butterworth filter
   with its cutoff at 0.36 × the output rate (90 Hz at 250 Hz).
2. **Decimation:** keep every D-th sample. D is the largest divisor of the
   stream's `samples_per_frame` that keeps the output rate at 250 Hz or above,
   or 1 if there is none. So every input frame gives the same number of output
   samples.
3. **High-pass** at 1 Hz (2nd-order Butterworth), removing drift and offset.
4. **Notch** at the mains frequency, 60 Hz by default (`--mains-hz 50` for
   50 Hz), with Q = 30.

All filters are cascades of second-order sections, designed when a stream
starts, with float64 coefficients and state. Output samples are float32 µV.

| Device | D | Output rate |
|---|---|---|
| 30 kHz, 600 samples per frame (full scale) | 120 | 250 Hz |
| 1 kHz, 20 samples per frame (the default synthetic device) | 4 | 250 Hz |
| 160 Hz, 3 samples per frame (the replay device at its defaults) | 1 | 160 Hz |

A stream is processed only if its output rate is between 125 Hz and 1000 Hz.
Below that, mains hum would sit above the Nyquist frequency; above it, the 1 s
feature window and its cost would grow with the rate. Other streams are still
ingested, get no output rings, and their `proc:` line says `unprocessed`.

## Band power

About every 0.25 s (a hop of `round(rate / 4)` output samples, 62 at 250 Hz),
once a full second of output has accumulated, the stage takes the last second
(`round(rate)` samples), applies a Hann window, and sums the periodogram over
the DFT bins in each band:

| Band | Range |
|---|---|
| delta | 1 to 4 Hz |
| theta | 4 to 8 Hz |
| alpha | 8 to 13 Hz |
| beta | 13 to 30 Hz |
| gamma | 30 to 45 Hz |

Each range includes its lower edge and excludes its upper edge. Powers are in
µV². A sinusoid of amplitude A at a whole number of Hz, at least 1 Hz inside a
band's edges, contributes A²/2 to that band, so the synthetic device's 20 µV
alpha rhythm shows up as about 200 µV². A tone right at an edge (such as the
synthetic device's 13 Hz tone) leaks a sixth of its power into the next band.

## Gaps and falling behind

- When frames are missing (lost by the device or the network), the filters are
  reset and the 1 s feature window starts again. The stage never invents
  samples. Each run of missing frames counts once in `gaps`, including a run
  longer than the ring holds, as long as processing had caught up when it began.
  (If processing is then starved for longer than the ring holds, frames it never
  read are counted in that gap too, not as an overrun.) Frames lost while a
  stream was evicted are not counted as a gap.
- If processing falls so far behind that ingest has overwritten the next frame
  (the ring holds `--ring-seconds`, 2 s by default), it jumps to the newest
  frame, resets, and counts it in `overruns`. The frames it skips, including
  some still in the ring, are never processed, so after an overrun `frames`
  falls short of `accepted`.
- Before the summary is printed, each processed stream's goroutine reads on to
  the newest frame in its ingest ring (subject to the overrun rule above).

## Reading the rings

Each processed stream has two more rings next to its ingest ring, the output
ring and the feature ring, all with `--ring-seconds` of history and the same
reader API (`Read`, `ReadStamped`, `Head`, `First`, `Changed`; `ReadStamped`
also returns the slot's stamp). A Go reader gets a stream's rings from
`Registry.Streams()` and `Stream.ProcRings()`.

| Ring | One slot holds | Indexed by | Stamp |
|---|---|---|---|
| ingest | one frame, channels × samples per frame | `seq` | arrival time (Unix ns) |
| output | the filtered, decimated frame, channels × (samples per frame / D) | the input `seq` | arrival time of the input frame |
| features | channels × 5 band powers (µV², channel-major, bands in the order above) | a feature counter per stream | the input `seq` that completed the window |

A reader takes `ch := ring.Changed()`, then calls `Read`; if the answer is
`not_yet`, it waits on `ch`, which closes on the next write. `missing` in the
output ring means that slot will never be produced (a gap or an overrun skip);
comparing the output ring's `Head` with the ingest ring's shows how far
processing has got. The feature ring has no holes; after a revival its `First`
is the next feature number.

## Memory

The output and feature rings and the processing state (filter state and the
1 s history per channel, the window and DFT tables per stream) are charged
against `--ring-budget-mb`. They stop being charged when a stream is evicted,
and are freed once its processing goroutine notices (within 0.1 s of reaching
the newest frame). Not charged: each processed stream's frame buffers (one input
frame, 2.46 MB at full scale, plus one output frame and the feature buffers),
the scratch space of each frame's filtering, each processed stream's 160 KB
latency histogram, which stays until the service stops, and two service-wide
latency histograms (320 KB).

## Output

The processing fields appear in the ingest output on stderr. For example, from a
10 s run of the default device (`--exit-after-idle 2`; interval lines after the
first omitted):

```text
start: listen=127.0.0.1:7700 max_conns=16 ring_seconds=2 ring_budget_mb=1024 hello_timeout_s=5 idle_timeout_s=5 mains_hz=60
t=1.0s conns=1 accepted=50 missing=0 dup=0 stale=0 errors=0 superseded=0 lat_p50_ms=0.1 lat_p99_ms=0.3 lat_max_ms=0.4 MB/s=0.26 proc_p99_ms=0.2 overruns=0
summary: connections=1 rejected_conns=0 accepted=500 missing=0 dup=0 stale=0 eof_closes=1 truncated=0 idle_closes=0 hello_timeouts=0 conn_errors=0 accept_errors=0 superseded=0 errors=none streams=1 evicted=0 bytes=2578752 elapsed_s=12.004 MB/s=0.21 processed=500 gaps=0 overruns=0 proc_p99_ms=0.2 proc_max_ms=0.6
stream: id=3f2a9c0d41b7e865 device=eegpipe-synthetic channels=64 rate_hz=1000 samples_per_frame=20 initial_seq=0 last_seq=499 accepted=500 missing=0 dup=0 stale=0 ring_frames=100 state=live drift_ppm=0.3 lat_p50_ms=0.1 lat_p99_ms=0.3 lat_max_ms=0.5
proc: id=3f2a9c0d41b7e865 decim=4 out_rate_hz=250 frames=500 gaps=0 overruns=0 features=37 lat_p50_ms=0.1 lat_p99_ms=0.2 lat_max_ms=0.6 bp_delta=0.126118 bp_theta=0.179127 bp_alpha=200.538 bp_beta=50.3521 bp_gamma=1.21282
```

- `decim` is D, `frames` the frames processed, `features` the band-power
  vectors written; `overruns` in an interval line is the change since the
  previous line.
- `proc_p99_ms` (interval and summary), `proc_max_ms` (summary) and the `proc:`
  line's latencies measure,
  on the wall clock, from ingest's receipt of a frame (before it is decoded) to
  its filtered output being written. `n/a` means no frame was processed in that
  interval (in the summary and the `proc:` line, at all).
- `processed`, `gaps` and `overruns` in the summary are totals over all streams.
- `bp_*` is the mean over channels of the stream's last feature, in µV², or
  `n/a` if the stream never completed a feature (it never had 1 s of
  contiguous frames).
- With `overruns=0`, `frames` equals `accepted`, unless processing was starved
  for longer than the ring holds right after catching up (that loss is counted
  as a gap).

## How it is tested

The Go code in `go/internal/dsp` is checked against a separate float64
reference in Python (`python/eegpipe/dsp.py`, numpy only). Golden files in
`testdata/dsp/` hold the reference's filter coefficients, band powers and the
output of the whole chain, including a gap. Both implementations are also
checked against closed-form answers: −3 dB at each filter's cutoff, at least
40 dB of attenuation at the mains frequency, and A²/2 for known sinusoids.

```bash
.venv/bin/python -m pytest python/tests/test_dsp.py -q   # the reference and its golden files
cd go && go test ./internal/dsp                          # Go against the golden files
```

## Limits

- The filters are causal IIR filters, so their phase response is not linear;
  there is no zero-phase filtering in real time.
- A feature is computed only once 1 s of output has accumulated after a start
  (including a revival after eviction), a gap or an overrun. The filter transients last about that long, so the first
  feature can still include them.
- Bins are `rate / round(rate)` Hz apart (exactly 1 Hz for a whole-number output
  rate), and band edges fall on that grid.
- Unprocessed streams have no output rings; a reader uses their ingest ring.
- Processing that stays slower than real time shows up only as repeated
  overruns.
