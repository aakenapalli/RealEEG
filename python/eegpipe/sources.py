import numpy as np
import pyedflib

ALPHA_HZ = 10
ALPHA_UV = 20.0                # amplitude (peak), not RMS; same for the other _UV values
ALPHA_PHASE_STEP_RAD = 0.5     # channel c's alpha phase is 0.5 * c radians
TONE_BASE_HZ = 13
TONE_COUNT = 17                # channel c's tone is TONE_BASE_HZ + c % TONE_COUNT (13..29 Hz)
TONE_UV = 10.0
LINE_HZ = 60
LINE_UV = 5.0
NOISE_UV = 5.0                 # standard deviation of the Gaussian noise
MIN_SYNTH_RATE_HZ = 200
MAX_SYNTH_RATE_HZ = 100_000
UNIT_SCALE = {"uV": 1.0, "mV": 1_000.0, "V": 1_000_000.0, "nV": 0.001}


class CyclicSource:
    """An endless source that serves columns of a fixed buffer, wrapping around at the end."""

    def __init__(self, buffer: np.ndarray, sample_rate_hz: float,
                 channel_names: tuple[str, ...]) -> None:
        if not isinstance(buffer, np.ndarray):
            raise TypeError("buffer must be a numpy array")
        if buffer.dtype != np.dtype("<f4"):
            raise TypeError("buffer dtype must be '<f4'")
        if buffer.ndim != 2:
            raise ValueError("buffer must be 2-dimensional")
        if buffer.shape[0] < 1 or buffer.shape[1] < 1:
            raise ValueError("buffer must have at least one channel and one sample")
        if type(channel_names) is not tuple or len(channel_names) != buffer.shape[0]:
            raise ValueError("channel_names must be a tuple with one name per buffer row")
        self.n_channels = buffer.shape[0]
        self.period = buffer.shape[1]
        self.sample_rate_hz = float(sample_rate_hz)
        self.channel_names = channel_names
        self.position = 0
        buffer.flags.writeable = False
        self._buffer = buffer

    def read(self, n: int) -> np.ndarray:
        if type(n) is not int:
            raise TypeError("n must be an int")
        if n < 1:
            raise ValueError("n must be >= 1")
        if self.position + n <= self.period:
            out = self._buffer[:, self.position:self.position + n]
        else:
            idx = (self.position + np.arange(n)) % self.period
            out = self._buffer[:, idx]
            out.flags.writeable = False
        self.position = (self.position + n) % self.period
        return out


def make_synthetic(n_channels: int, sample_rate_hz: int, seed: int) -> CyclicSource:
    if type(n_channels) is not int:
        raise TypeError("n_channels must be an int")
    if not 1 <= n_channels <= 4096:
        raise ValueError("n_channels outside 1..4096")
    if type(sample_rate_hz) is not int:
        raise TypeError("sample_rate_hz must be an int")
    if not MIN_SYNTH_RATE_HZ <= sample_rate_hz <= MAX_SYNTH_RATE_HZ:
        raise ValueError("sample_rate_hz outside 200..100000")
    if type(seed) is not int:
        raise TypeError("seed must be an int")
    if seed < 0:
        raise ValueError("seed must be >= 0")

    C = n_channels
    fs = sample_rate_hz
    rng = np.random.default_rng([seed, 0])
    buf = rng.standard_normal((C, fs), dtype=np.float32)
    buf *= np.float32(NOISE_UV)
    t = np.arange(fs, dtype=np.float64) / fs
    line = LINE_UV * np.sin(2 * np.pi * LINE_HZ * t)
    for c in range(C):
        wave = (ALPHA_UV * np.sin(2 * np.pi * ALPHA_HZ * t + ALPHA_PHASE_STEP_RAD * c)
                + TONE_UV * np.sin(2 * np.pi * (TONE_BASE_HZ + c % TONE_COUNT) * t)
                + line)
        buf[c] += wave.astype(np.float32)
    names = tuple(f"ch{c:04d}" for c in range(C))
    return CyclicSource(buf, float(fs), names)


def clean_label(raw: str, index: int) -> str:
    s = raw.strip().rstrip(".").rstrip()
    if s == "":
        return f"ch{index:04d}"
    if len(s) > 32 or any(not (" " <= ch <= "~") for ch in s):
        raise ValueError(
            f"channel {index} label {raw!r} is not 1-32 printable ASCII characters after cleaning")
    return s


def load_replay(path: str) -> CyclicSource:
    with pyedflib.EdfReader(path) as r:
        n = r.signals_in_file
        if n < 1 or n > 4096:
            raise ValueError(f"replay file has {n} signals, need 1..4096")
        rates = [r.getSampleFrequency(i) for i in range(n)]
        if len(set(rates)) != 1:
            raise ValueError(f"signals have different sample rates: {sorted(set(rates))}")
        fs = float(rates[0])
        if not 0 < fs <= 100000:
            raise ValueError(f"sample rate {fs} outside (0, 100000]")
        names = []
        rows = []
        for i in range(n):
            dim = r.getPhysicalDimension(i).strip()
            if dim not in UNIT_SCALE:
                raise ValueError(f"signal {i} ({r.getLabel(i)!r}) has unsupported unit {dim!r}")
            names.append(clean_label(r.getLabel(i), i))
            rows.append((r.readSignal(i) * UNIT_SCALE[dim]).astype("<f4"))
    if len({row.shape[0] for row in rows}) != 1:
        raise ValueError("signals have different lengths")
    if rows[0].shape[0] == 0:
        raise ValueError("replay file has no samples")
    buf = np.stack(rows)
    return CyclicSource(buf, fs, tuple(names))
