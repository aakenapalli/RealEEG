# eeg-pipeline

A real-time neural signal streaming pipeline: it ingests multichannel EEG (or
synthetic signals at implant-scale rates), filters and analyzes them with a
bounded latency budget, fans the results out to live clients, and records
sessions for replay.

The goal is a systems project with measured numbers (throughput, p50/p99
latency, behavior under faults), using neuroscience as the domain.

> **Status:** Week 0, background lessons. No pipeline code yet.

## Architecture (planned)

```
[Virtual Device] → [Ingest] → [Processing] → [Fanout] → [Dashboard]
  Python: replay     Go          Go             Go          React
  + generator                        ↓
                                 [Storage]
                            raw chunks + features
```

- **Virtual device:** replays public EEG recordings (PhysioNet) in real time,
  or generates synthetic signals at any channel count and rate, with optional
  fault injection (drops, jitter, clock drift, disconnects).
- **Ingest:** receives binary frames over TCP, detects gaps and duplicates, and
  buffers samples.
- **Processing:** bandpass/notch filtering and band-power features, checked
  against scipy reference outputs.
- **Fanout:** WebSocket streams, decimated per client, with a slow-consumer
  policy so one slow client can't stall the pipeline.
- **Storage:** compressed raw chunks plus a feature store; recorded sessions
  replay through the same ingest path.

Scale target: 1,024 channels × 30 kHz × float32 ≈ 123 MB/s.

## Roadmap

| Week | Focus | Status |
|---|---|---|
| 0 | Background: sampling, Nyquist, FFT, filters, Go concurrency | In progress |
| 1 | Virtual device + binary frame protocol + fault injection | — |
| 2 | Ingest service: frame parsing, gap detection, ring buffer | — |
| 3 | Processing: IIR filters, band power, golden-file tests | — |
| 4 | Observability + benchmarks (Prometheus, pprof, p99) | — |
| 5 | Fanout: WebSockets, decimation, slow consumers | — |
| 6 | Storage + replay | — |
| 7 | Dashboard | — |
| 8 | Fault test matrix, clock drift, one-command demo | — |
