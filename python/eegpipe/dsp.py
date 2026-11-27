"""Float64 reference implementation of the processing stage (docs/PROCESSING.md).

The Go stage in go/internal/dsp must match this module within the tolerances in
testdata/dsp/README.md. Only numpy is used.
"""
import math

import numpy as np

TARGET_OUTPUT_RATE_HZ = 250.0
MIN_OUTPUT_RATE_HZ = 125.0
MAX_OUTPUT_RATE_HZ = 1000.0
LOWPASS_ORDER = 8
LOWPASS_FRACTION = 0.36        # anti-alias cutoff as a fraction of the output rate
HIGHPASS_ORDER = 2
HIGHPASS_HZ = 1.0
NOTCH_Q = 30.0
FEATURES_PER_SECOND = 4
BANDS = (
    ("delta", 1.0, 4.0),
    ("theta", 4.0, 8.0),
    ("alpha", 8.0, 13.0),
    ("beta", 13.0, 30.0),
    ("gamma", 30.0, 45.0),
)


def decimation_factor(sample_rate_hz: float, samples_per_frame: int) -> int:
    """The largest divisor d of samples_per_frame with rate / d >= 250 Hz, or 1."""
    best = 1
    for d in range(1, samples_per_frame + 1):
        if samples_per_frame % d == 0 and sample_rate_hz / d >= TARGET_OUTPUT_RATE_HZ:
            best = d
    return best


def output_rate(sample_rate_hz: float, samples_per_frame: int) -> float:
    return sample_rate_hz / decimation_factor(sample_rate_hz, samples_per_frame)


def processable(sample_rate_hz: float, samples_per_frame: int) -> bool:
    r = output_rate(sample_rate_hz, samples_per_frame)
    return MIN_OUTPUT_RATE_HZ <= r <= MAX_OUTPUT_RATE_HZ


def _butter_sections(order: int, cutoff_hz: float, rate_hz: float, highpass: bool) -> np.ndarray:
    if order < 2 or order % 2:
        raise ValueError("order must be even and >= 2")
    k = math.tan(math.pi * cutoff_hz / rate_hz)
    rows = []
    for i in range(order // 2):
        two_zeta = 2.0 * math.sin(math.pi * (2 * i + 1) / (2 * order))
        norm = 1.0 / (1.0 + two_zeta * k + k * k)
        if highpass:
            b0, b1, b2 = norm, -2.0 * norm, norm
        else:
            b0 = k * k * norm
            b1, b2 = 2.0 * b0, b0
        a1 = 2.0 * (k * k - 1.0) * norm
        a2 = (1.0 - two_zeta * k + k * k) * norm
        rows.append((b0, b1, b2, a1, a2))
    return np.array(rows, dtype=np.float64)


def butter_lowpass(order: int, cutoff_hz: float, rate_hz: float) -> np.ndarray:
    """Butterworth low-pass as rows (b0, b1, b2, a1, a2), bilinear transform with prewarping."""
    return _butter_sections(order, cutoff_hz, rate_hz, highpass=False)


def butter_highpass(order: int, cutoff_hz: float, rate_hz: float) -> np.ndarray:
    return _butter_sections(order, cutoff_hz, rate_hz, highpass=True)


def notch(f0_hz: float, q: float, rate_hz: float) -> np.ndarray:
    """A second-order notch (RBJ cookbook) as one row (b0, b1, b2, a1, a2)."""
    w0 = 2.0 * math.pi * f0_hz / rate_hz
    alpha = math.sin(w0) / (2.0 * q)
    a0 = 1.0 + alpha
    c = -2.0 * math.cos(w0)
    return np.array([(1.0 / a0, c / a0, 1.0 / a0, c / a0, (1.0 - alpha) / a0)], dtype=np.float64)


def gain(sections: np.ndarray, f_hz: float, rate_hz: float) -> float:
    """Magnitude of the cascade's frequency response at f_hz."""
    z = np.exp(-1j * 2.0 * math.pi * f_hz / rate_hz)
    h = 1.0 + 0.0j
    for b0, b1, b2, a1, a2 in sections:
        h *= (b0 + b1 * z + b2 * z * z) / (1.0 + a1 * z + a2 * z * z)
    return float(abs(h))


class Cascade:
    """Cascaded biquads (transposed direct form II) over several channels, float64 state."""

    def __init__(self, sections: np.ndarray, n_channels: int) -> None:
        self.sections = sections
        self.state = np.zeros((len(sections), n_channels, 2))

    def reset(self) -> None:
        self.state[:] = 0.0

    def run(self, x: np.ndarray) -> np.ndarray:
        """Filter x (channels x samples, float64) and return a new float64 array."""
        y = np.array(x, dtype=np.float64)
        for s, (b0, b1, b2, a1, a2) in enumerate(self.sections):
            z1 = self.state[s, :, 0]
            z2 = self.state[s, :, 1]
            for n in range(y.shape[1]):
                xn = y[:, n].copy()
                yn = b0 * xn + z1
                z1 = b1 * xn - a1 * yn + z2
                z2 = b2 * xn - a2 * yn
                y[:, n] = yn
            self.state[s, :, 0] = z1
            self.state[s, :, 1] = z2
        return y


def window_size(out_rate_hz: float) -> int:
    return round(out_rate_hz)


def hop_size(out_rate_hz: float) -> int:
    return round(out_rate_hz / FEATURES_PER_SECOND)


def band_bins(out_rate_hz: float, n: int) -> list[list[int]]:
    """For each band, the DFT bins k (1 <= k <= (n - 1) // 2) with lo <= k * rate / n < hi."""
    out = []
    for _, lo, hi in BANDS:
        out.append([k for k in range(1, (n - 1) // 2 + 1) if lo <= k * out_rate_hz / n < hi])
    return out


def band_powers(window: np.ndarray, out_rate_hz: float) -> np.ndarray:
    """Band powers in uV^2 (channels x 5) of a window (channels x n), Hann-windowed periodogram."""
    x = np.asarray(window, dtype=np.float64)
    n = x.shape[1]
    idx = np.arange(n)
    w = 0.5 - 0.5 * np.cos(2.0 * math.pi * idx / n)
    xw = x * w
    scale = 2.0 / (n * float(np.sum(w * w)))
    out = np.zeros((x.shape[0], len(BANDS)))
    for b, bins in enumerate(band_bins(out_rate_hz, n)):
        for k in bins:
            ang = 2.0 * math.pi * ((k * idx) % n) / n
            re = xw @ np.cos(ang)
            im = xw @ np.sin(ang)
            out[:, b] += scale * (re * re + im * im)
    return out


class Processor:
    """The per-stream processing chain of docs/PROCESSING.md, for whole frames."""

    def __init__(self, sample_rate_hz: float, samples_per_frame: int, n_channels: int,
                 mains_hz: float) -> None:
        if not processable(sample_rate_hz, samples_per_frame):
            raise ValueError("stream is not processable")
        self.d = decimation_factor(sample_rate_hz, samples_per_frame)
        self.out_rate = sample_rate_hz / self.d
        self.spf = samples_per_frame
        self.lowpass = Cascade(butter_lowpass(LOWPASS_ORDER, LOWPASS_FRACTION * self.out_rate,
                                              sample_rate_hz), n_channels)
        self.highpass = Cascade(butter_highpass(HIGHPASS_ORDER, HIGHPASS_HZ, self.out_rate), n_channels)
        self.notch = Cascade(notch(mains_hz, NOTCH_Q, self.out_rate), n_channels)
        self.n = window_size(self.out_rate)
        self.hop = hop_size(self.out_rate)
        self.history = np.zeros((n_channels, 0), dtype=np.float32)
        self.count = 0

    def reset(self) -> None:
        for c in (self.lowpass, self.highpass, self.notch):
            c.reset()
        self.history = self.history[:, :0]
        self.count = 0

    def frame(self, samples: np.ndarray) -> tuple[np.ndarray, list[np.ndarray]]:
        """Process one frame (channels x samples_per_frame, float32). Returns the output
        (channels x samples_per_frame / d, float32) and the band powers completed in it."""
        x = np.asarray(samples, dtype=np.float32).astype(np.float64)
        full = self.lowpass.run(x)
        low = full[:, self.d - 1::self.d]
        out = self.notch.run(self.highpass.run(low)).astype(np.float32)
        features = []
        for j in range(out.shape[1]):
            self.history = np.concatenate([self.history, out[:, j:j + 1]], axis=1)[:, -self.n:]
            self.count += 1
            if self.count >= self.n and (self.count - self.n) % self.hop == 0:
                features.append(band_powers(self.history, self.out_rate))
        return out, features
