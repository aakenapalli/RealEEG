# Stream scenarios

Shared test cases for the EEGP stream rules in `docs/RECEIVER.md`. Each file is
run by the Python test receiver's tests (`python/tests/test_stream_scenarios.py`)
and by the Go ingest's tests (`go/internal/ingest/scenario_test.go`), so both
implementations classify frames and compute statistics the same way.

**Format:** one JSON object per file.

- `description`: what the scenario checks.
- `events`: in order. A hello event is `{"hello": {...}, "expect": ...}` with the
  HELLO fields `stream_id` (16 hex digits), `device_name`, `n_channels`,
  `samples_per_frame`, `sample_rate_hz` and `channel_names`, and `expect` either
  `"ok"` or `"HelloMismatchError"`. A frame event is `{"frame": {...}, "expect": ...}`
  with `stream_id`, `seq`, `timestamp_ns` and `arrival_ns` (the receiver's wall
  clock when the frame arrived), and `expect` one of `"accepted"`, `"duplicate"` or
  `"stale"`. Frame samples are all zero, shaped like the stream's first HELLO.
- `streams`: the expected final state of every stream: `initial_seq` and
  `last_seq` (`null` before any accepted frame), `accepted`, `missing`, `dup`,
  `stale`, `drift_ppm` (`null` when there is no estimate; compared within 0.01),
  and `lat_p50_ms`, `lat_p99_ms`, `lat_max_ms` (strings as printed, `"n/a"` when
  there are no latencies; compared exactly).

Latencies in these files are multiples of 0.1 ms, so an implementation that
buckets latency at 0.1 ms prints exactly the same strings as one that keeps
every value.
