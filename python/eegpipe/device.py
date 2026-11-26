import argparse
import dataclasses
import math
import re
import secrets
import socket
import sys
import time

import numpy as np

from eegpipe.protocol import (
    CRC_SIZE, FRAME_FIXED_SIZE, HEADER_SIZE, MAX_SAMPLES_PER_FRAME,
    Frame, Hello, encode_frame, encode_hello,
)
from eegpipe.sources import CyclicSource, load_replay, make_synthetic

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 7700
FRAME_DURATION_S = 0.02
CONNECT_TIMEOUT_S = 2.0
SEND_TIMEOUT_S = 1.0                   # a sendall that can't finish in 1 s fails
BACKOFF_INITIAL_NS = 100_000_000       # 0.1 s
BACKOFF_MAX_NS = 2_000_000_000         # 2 s
STABLE_CONNECTION_NS = 1_000_000_000   # a connection up this long resets the backoff
OVERFLOW_NS = 1_000_000_000            # device buffer: 1 s
LATE_THRESHOLD_NS = 5_000_000          # a frame is "late" if its send starts > 5 ms after its scheduled time
STATS_INTERVAL_NS = 1_000_000_000
MAX_DRIFT_PPM = 10_000.0
MAX_JITTER_MS = 1_000.0
SYNTHETIC_DEVICE_NAME = "eegpipe-synthetic"
REPLAY_DEVICE_NAME = "eegpipe-replay"
DEFAULT_SYNTH_CHANNELS = 64
DEFAULT_SYNTH_RATE_HZ = 1000
PAYLOAD_OFFSET = HEADER_SIZE + FRAME_FIXED_SIZE    # 32


def _is_int(x) -> bool:
    return type(x) is int


def _is_number(x) -> bool:
    return type(x) in (int, float)


def _in_range(x, lo, hi) -> bool:
    return _is_number(x) and math.isfinite(x) and lo <= x <= hi


@dataclasses.dataclass(frozen=True)
class FaultConfig:
    drop_prob: float = 0.0
    dup_prob: float = 0.0
    corrupt_prob: float = 0.0
    jitter_ms: float = 0.0
    drift_ppm: float = 0.0
    disconnect_every_s: float | None = None

    def __post_init__(self):
        if not _in_range(self.drop_prob, 0.0, 1.0):
            raise ValueError("drop_prob must be a number in [0, 1]")
        if not _in_range(self.dup_prob, 0.0, 1.0):
            raise ValueError("dup_prob must be a number in [0, 1]")
        if not _in_range(self.corrupt_prob, 0.0, 1.0):
            raise ValueError("corrupt_prob must be a number in [0, 1]")
        if not _in_range(self.jitter_ms, 0.0, MAX_JITTER_MS):
            raise ValueError("jitter_ms must be a number in [0, 1000]")
        if not _in_range(self.drift_ppm, -MAX_DRIFT_PPM, MAX_DRIFT_PPM):
            raise ValueError("drift_ppm must be a number in [-10000, 10000]")
        d = self.disconnect_every_s
        if d is not None and not (_is_number(d) and math.isfinite(d) and d > 0):
            raise ValueError("disconnect_every_s must be None or a finite number > 0")


@dataclasses.dataclass(frozen=True)
class DeviceConfig:
    host: str
    port: int
    samples_per_frame: int
    duration_s: float | None
    seed: int
    stream_id: int
    device_name: str
    faults: FaultConfig = FaultConfig()

    def __post_init__(self):
        if type(self.host) is not str or self.host == "":
            raise ValueError("host must be a non-empty string")
        if not (_is_int(self.port) and 1 <= self.port <= 65535):
            raise ValueError("port must be an int in 1..65535")
        if not (_is_int(self.samples_per_frame)
                and 1 <= self.samples_per_frame <= MAX_SAMPLES_PER_FRAME):
            raise ValueError("samples_per_frame must be an int in 1..1024")
        d = self.duration_s
        if d is not None and not (_is_number(d) and math.isfinite(d) and d > 0):
            raise ValueError("duration_s must be None or a finite number > 0")
        if not (_is_int(self.seed) and self.seed >= 0):
            raise ValueError("seed must be an int >= 0")
        if not (_is_int(self.stream_id) and 0 <= self.stream_id <= 2**64 - 1):
            raise ValueError("stream_id must be an int in 0..2**64 - 1")
        if not isinstance(self.faults, FaultConfig):
            raise ValueError("faults must be a FaultConfig")


class SystemClock:
    def monotonic_ns(self) -> int:
        return time.monotonic_ns()

    def time_ns(self) -> int:
        return time.time_ns()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


@dataclasses.dataclass
class Stats:
    sent: int = 0                # frames whose first copy was fully written to the socket
    dropped: int = 0             # frames skipped by the drop fault
    duplicated: int = 0          # extra copies fully written by the dup fault
    corrupted: int = 0           # frames that reached the send step with a corrupted byte
    overflow: int = 0            # frames discarded because they were > 1 s past their deadline
    disconnected: int = 0        # frames lost because there was no connection or their send failed
    late: int = 0                # frames whose loop-step-11 lateness exceeded LATE_THRESHOLD_NS
    max_late_ns: int = 0         # largest loop-step-11 lateness
    connects: int = 0            # successful connections (HELLO fully written)
    connect_failures: int = 0
    forced_disconnects: int = 0  # closes caused by disconnect_every_s
    send_errors: int = 0         # OSErrors from sendall on an open connection
    bytes_sent: int = 0          # all bytes fully written: HELLOs, frames, duplicates

    @property
    def frames(self) -> int:
        return self.sent + self.dropped + self.overflow + self.disconnected


def default_samples_per_frame(sample_rate_hz: float) -> int:
    return max(1, min(MAX_SAMPLES_PER_FRAME, round(sample_rate_hz * FRAME_DURATION_S)))


def tcp_connect(host: str, port: int) -> socket.socket:
    sock = socket.create_connection((host, port), timeout=CONNECT_TIMEOUT_S)
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(SEND_TIMEOUT_S)
    except BaseException:
        sock.close()
        raise
    return sock


def run_device(source: CyclicSource, config: DeviceConfig, *,
               clock=None, connect=None, err=None) -> Stats:
    if clock is None:
        clock = SystemClock()
    if connect is None:
        connect = tcp_connect
    if err is None:
        err = sys.stderr
    C, fs, S = source.n_channels, source.sample_rate_hz, config.samples_per_frame
    f = config.faults
    hello_bytes = encode_hello(
        Hello(config.stream_id, config.device_name, C, S, fs, source.channel_names))
    rng = np.random.default_rng([config.seed, 1])
    period_ns = S * 1e9 / (fs * (1 + f.drift_ppm * 1e-6))
    ts_step_ns = S * 1e9 / fs
    max_frames = None if config.duration_s is None else max(1, round(config.duration_s * fs / S))
    stats = Stats()
    print(f"start: device={config.device_name} channels={C} rate_hz={fs:g} samples_per_frame={S} "
          f"stream_id={config.stream_id:016x} target={config.host}:{config.port} seed={config.seed}",
          file=err, flush=True)
    t0 = clock.monotonic_ns()
    t0_wall = clock.time_ns()
    conn = None
    next_connect_ns = t0
    backoff_ns = BACKOFF_INITIAL_NS
    connected_at = 0
    send_start = 0
    last_emit = t0
    snapshot = Stats()
    interval_max_late_ns = 0
    k = 0

    def close_conn():
        nonlocal conn
        try:
            conn.close()
        except OSError:
            pass
        conn = None

    def send_failed():
        nonlocal backoff_ns, next_connect_ns
        now = clock.monotonic_ns()
        lasted = send_start - connected_at
        close_conn()
        stats.send_errors += 1
        if lasted >= STABLE_CONNECTION_NS:
            backoff_ns = BACKOFF_INITIAL_NS
            next_connect_ns = now
        else:
            next_connect_ns = now + backoff_ns
            backoff_ns = min(2 * backoff_ns, BACKOFF_MAX_NS)

    def frame_step(k):
        nonlocal conn, next_connect_ns, backoff_ns, connected_at, send_start
        nonlocal interval_max_late_ns
        u = rng.random(5)
        samples = source.read(S)
        deadline = t0 + round((k + 1) * period_ns)
        scheduled = deadline + round(u[3] * f.jitter_ms * 1e6)
        now = clock.monotonic_ns()
        if now - deadline > OVERFLOW_NS:
            stats.overflow += 1
            return
        msg = encode_frame(Frame(seq=k, timestamp_ns=t0_wall + round(k * ts_step_ns),
                                 samples=samples))
        corrupt = u[2] < f.corrupt_prob
        if corrupt:
            b = bytearray(msg)
            i = PAYLOAD_OFFSET + int(u[4] * (len(msg) - PAYLOAD_OFFSET - CRC_SIZE))
            b[i] ^= 0xFF
            msg = bytes(b)
        now = clock.monotonic_ns()
        if scheduled > now:
            clock.sleep((scheduled - now) / 1e9)
        now = clock.monotonic_ns()
        if conn is None and now >= next_connect_ns:
            try:
                conn = connect(config.host, config.port)
                conn.sendall(hello_bytes)
            except (OSError, UnicodeError):
                if conn is not None:
                    close_conn()
                stats.connect_failures += 1
                next_connect_ns = clock.monotonic_ns() + backoff_ns
                backoff_ns = min(2 * backoff_ns, BACKOFF_MAX_NS)
            else:
                stats.connects += 1
                stats.bytes_sent += len(hello_bytes)
                connected_at = clock.monotonic_ns()
        if conn is None:
            stats.disconnected += 1
            return
        if u[0] < f.drop_prob:
            stats.dropped += 1
            return
        if corrupt:
            stats.corrupted += 1
        late_ns = max(0, clock.monotonic_ns() - scheduled)
        stats.max_late_ns = max(stats.max_late_ns, late_ns)
        interval_max_late_ns = max(interval_max_late_ns, late_ns)
        if late_ns > LATE_THRESHOLD_NS:
            stats.late += 1
        send_start = clock.monotonic_ns()
        try:
            conn.sendall(msg)
        except OSError:
            send_failed()
            stats.disconnected += 1
            return
        stats.sent += 1
        stats.bytes_sent += len(msg)
        if u[1] < f.dup_prob:
            send_start = clock.monotonic_ns()
            try:
                conn.sendall(msg)
            except OSError:
                send_failed()
                return
            stats.duplicated += 1
            stats.bytes_sent += len(msg)

    def end_of_iteration():
        nonlocal backoff_ns, next_connect_ns, snapshot, interval_max_late_ns, last_emit
        now = clock.monotonic_ns()
        if (conn is not None and f.disconnect_every_s is not None
                and now - connected_at >= round(f.disconnect_every_s * 1e9)):
            if now - connected_at >= STABLE_CONNECTION_NS:
                backoff_ns = BACKOFF_INITIAL_NS
            close_conn()
            stats.forced_disconnects += 1
            next_connect_ns = now
        now = clock.monotonic_ns()
        if now - last_emit >= STATS_INTERVAL_NS:
            d = Stats(**{fl.name: getattr(stats, fl.name) - getattr(snapshot, fl.name)
                         for fl in dataclasses.fields(Stats)})
            elapsed_s = (now - t0) / 1e9
            interval_s = (now - last_emit) / 1e9
            print(f"t={elapsed_s:.1f}s sent={d.sent} drop={d.dropped} dup={d.duplicated} "
                  f"corrupt={d.corrupted} overflow={d.overflow} disc={d.disconnected} late={d.late} "
                  f"max_late_ms={interval_max_late_ns / 1e6:.1f} connects={d.connects} "
                  f"MB/s={d.bytes_sent / interval_s / 1e6:.2f}",
                  file=err, flush=True)
            snapshot = dataclasses.replace(stats)
            interval_max_late_ns = 0
            last_emit = now

    try:
        try:
            while max_frames is None or k < max_frames:
                frame_step(k)
                end_of_iteration()
                k += 1
        except KeyboardInterrupt:
            pass
    finally:
        if conn is not None:
            close_conn()
        end = clock.monotonic_ns()
        s = stats
        elapsed_s = (end - t0) / 1e9
        mbps = s.bytes_sent / elapsed_s / 1e6 if elapsed_s > 0 else 0.0
        print(f"summary: frames={s.frames} sent={s.sent} drop={s.dropped} dup={s.duplicated} "
              f"corrupt={s.corrupted} overflow={s.overflow} disc={s.disconnected} late={s.late} "
              f"max_late_ms={s.max_late_ns / 1e6:.1f} connects={s.connects} "
              f"connect_failures={s.connect_failures} forced_disconnects={s.forced_disconnects} "
              f"send_errors={s.send_errors} bytes={s.bytes_sent} elapsed_s={elapsed_s:.3f} MB/s={mbps:.2f}",
              file=err, flush=True)
    return stats


def _non_empty(x: str) -> str:
    if x == "":
        raise argparse.ArgumentTypeError("must be non-empty")
    return x


def _int_in_range(lo: int, hi: int, message: str):
    def parse(x: str) -> int:
        try:
            v = int(x)
        except ValueError:
            raise argparse.ArgumentTypeError(message) from None
        if not lo <= v <= hi:
            raise argparse.ArgumentTypeError(message)
        return v
    return parse


def _float_in_range(lo: float, hi: float, message: str):
    def parse(x: str) -> float:
        try:
            v = float(x)
        except ValueError:
            raise argparse.ArgumentTypeError(message) from None
        if not (math.isfinite(v) and lo <= v <= hi):
            raise argparse.ArgumentTypeError(message)
        return v
    return parse


def _positive_float(x: str) -> float:
    try:
        v = float(x)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a finite number > 0") from None
    if not (math.isfinite(v) and v > 0):
        raise argparse.ArgumentTypeError("must be a finite number > 0")
    return v


def _seed(x: str) -> int:
    try:
        v = int(x)
    except ValueError:
        raise argparse.ArgumentTypeError("must be an integer >= 0") from None
    if v < 0:
        raise argparse.ArgumentTypeError("must be an integer >= 0")
    return v


def _stream_id(x: str) -> int:
    if re.fullmatch(r"[0-9a-f]{16}", x) is None:
        raise argparse.ArgumentTypeError("must be 16 lowercase hex digits")
    return int(x, 16)


_port = _int_in_range(1, 65535, "must be an integer in 1..65535")
_samples_per_frame = _int_in_range(1, 1024, "must be an integer in 1..1024")
_probability = _float_in_range(0.0, 1.0, "must be a number in [0, 1]")
_jitter_ms = _float_in_range(0.0, MAX_JITTER_MS, "must be a number in [0, 1000]")
_drift_ppm = _float_in_range(-MAX_DRIFT_PPM, MAX_DRIFT_PPM, "must be a number in [-10000, 10000]")
_channels = _int_in_range(1, 4096, "must be an integer in 1..4096")
_rate = _int_in_range(200, 100000, "must be an integer in 200..100000")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m eegpipe.device",
        description="Stream EEGP frames from a virtual EEG device.")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--host", dest="host", type=_non_empty, default=DEFAULT_HOST)
    common.add_argument("--port", dest="port", type=_port, default=DEFAULT_PORT)
    common.add_argument("--samples-per-frame", dest="samples_per_frame",
                        type=_samples_per_frame, default=None)
    common.add_argument("--duration", dest="duration", type=_positive_float, default=None)
    common.add_argument("--seed", dest="seed", type=_seed, default=0)
    common.add_argument("--stream-id", dest="stream_id", type=_stream_id, default=None)
    common.add_argument("--drop-prob", dest="drop_prob", type=_probability, default=0.0)
    common.add_argument("--dup-prob", dest="dup_prob", type=_probability, default=0.0)
    common.add_argument("--corrupt-prob", dest="corrupt_prob", type=_probability, default=0.0)
    common.add_argument("--jitter-ms", dest="jitter_ms", type=_jitter_ms, default=0.0)
    common.add_argument("--drift-ppm", dest="drift_ppm", type=_drift_ppm, default=0.0)
    common.add_argument("--disconnect-every", dest="disconnect_every",
                        type=_positive_float, default=None)
    sub = parser.add_subparsers(dest="source", required=True)
    synthetic = sub.add_parser("synthetic", parents=[common])
    synthetic.add_argument("--channels", dest="channels", type=_channels,
                           default=DEFAULT_SYNTH_CHANNELS)
    synthetic.add_argument("--rate", dest="rate", type=_rate, default=DEFAULT_SYNTH_RATE_HZ)
    replay = sub.add_parser("replay", parents=[common])
    replay.add_argument("--file", dest="file", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.source == "synthetic":
            source = make_synthetic(args.channels, args.rate, args.seed)
            name = SYNTHETIC_DEVICE_NAME
        else:
            try:
                source = load_replay(args.file)
            except (OSError, ValueError) as e:
                print(f"eegpipe.device: error: {e}", file=sys.stderr)
                return 2
            name = REPLAY_DEVICE_NAME
        spf = (args.samples_per_frame if args.samples_per_frame is not None
               else default_samples_per_frame(source.sample_rate_hz))
        stream_id = args.stream_id if args.stream_id is not None else secrets.randbits(64)
        faults = FaultConfig(args.drop_prob, args.dup_prob, args.corrupt_prob,
                             args.jitter_ms, args.drift_ppm, args.disconnect_every)
        config = DeviceConfig(args.host, args.port, spf, args.duration, args.seed,
                              stream_id, name, faults)
        run_device(source, config)
        return 0
    except KeyboardInterrupt:
        print("eegpipe.device: interrupted", file=sys.stderr)
        return 0


if __name__ == "__main__":
    sys.exit(main())
