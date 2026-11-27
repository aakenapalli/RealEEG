"""Builds the golden files in testdata/dsp/ from the float64 reference in eegpipe.dsp.

Usage: python -m eegpipe.dsp_golden OUTPUT_DIR
"""
import json
import math
import pathlib
import sys

import numpy as np

from eegpipe import dsp

DECIMATION_CASES = [(30000.0, 600), (1000.0, 20), (160.0, 3), (200.0, 4), (2000.0, 40),
                    (1024.0, 20), (512.0, 10), (30000.0, 7), (100000.0, 2000), (100.0, 2)]


def _rows(sections: np.ndarray) -> list[list[float]]:
    return [[float(v) for v in row] for row in sections]


def decimation() -> list[dict]:
    return [{"sample_rate_hz": r, "samples_per_frame": s, "decimation": dsp.decimation_factor(r, s),
             "output_rate_hz": dsp.output_rate(r, s), "processable": dsp.processable(r, s)}
            for r, s in DECIMATION_CASES]


def coefficients() -> list[dict]:
    out = []
    for order, fc, rate in [(8, 90.0, 30000.0), (8, 90.0, 1000.0), (8, 57.6, 160.0)]:
        out.append({"kind": "lowpass", "order": order, "freq_hz": fc, "rate_hz": rate,
                    "sections": _rows(dsp.butter_lowpass(order, fc, rate))})
    for order, fc, rate in [(2, 1.0, 250.0), (2, 1.0, 160.0)]:
        out.append({"kind": "highpass", "order": order, "freq_hz": fc, "rate_hz": rate,
                    "sections": _rows(dsp.butter_highpass(order, fc, rate))})
    for f0, rate in [(60.0, 250.0), (50.0, 250.0), (60.0, 160.0)]:
        out.append({"kind": "notch", "order": 2, "freq_hz": f0, "rate_hz": rate,
                    "sections": _rows(dsp.notch(f0, dsp.NOTCH_Q, rate))})
    return out


def _bandpower_input(n_channels: int, rate: float, n: int) -> np.ndarray:
    t = np.arange(n) / rate
    rows = []
    for c in range(n_channels):
        rows.append(20.0 * np.cos(2 * math.pi * 10 * t + c) + 10.0 * np.sin(2 * math.pi * (14 + c) * t) + 0.5 * c)
    return np.array(rows)


def bandpower() -> list[dict]:
    out = []
    for rate in (250.0, 160.0):
        n = dsp.window_size(rate)
        x = _bandpower_input(2, rate, n)
        out.append({"output_rate_hz": rate, "bins": dsp.band_bins(rate, n),
                    "window": [[float(v) for v in row] for row in x],
                    "powers": [[float(v) for v in row] for row in dsp.band_powers(x, rate)]})
    return out


def _chain_input(n_channels: int, rate: float, spf: int, seq: int) -> np.ndarray:
    i = seq * spf + np.arange(spf)
    t = i / rate
    rows = []
    for c in range(n_channels):
        rows.append(20.0 * np.sin(2 * math.pi * 10 * t + c) + 10.0 * np.sin(2 * math.pi * (14 + c) * t)
                    + 5.0 * np.sin(2 * math.pi * 60 * t) + (c + 1) * ((i % 7) - 3))
    return np.array(rows, dtype=np.float32)


def chain() -> list[dict]:
    out = []
    for rate, spf, n_channels, mains, n_frames, gap in [(1000.0, 20, 3, 60.0, 200, 100),
                                                        (160.0, 3, 2, 50.0, 120, None)]:
        p = dsp.Processor(rate, spf, n_channels, mains)
        frames = []
        for seq in range(n_frames):
            if seq == gap:
                p.reset()
                continue
            o, feats = p.frame(_chain_input(n_channels, rate, spf, seq))
            frames.append({"seq": seq,
                           "output": [[float(v) for v in row] for row in o],
                           "features": [[[float(v) for v in ch] for ch in f] for f in feats]})
        out.append({"sample_rate_hz": rate, "samples_per_frame": spf, "n_channels": n_channels,
                    "mains_hz": mains, "n_frames": n_frames, "gap_seq": gap, "frames": frames})
    return out


def build() -> dict[str, object]:
    return {"decimation.json": decimation(), "coefficients.json": coefficients(),
            "bandpower.json": bandpower(), "chain.json": chain()}


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print("usage: python -m eegpipe.dsp_golden OUTPUT_DIR", file=sys.stderr)
        return 2
    out_dir = pathlib.Path(argv[0])
    for name, data in build().items():
        (out_dir / name).write_text(json.dumps(data, indent=1) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
