import argparse
import dataclasses
import math
import socket
import sys
import time

from eegpipe.protocol import (
    CRC_SIZE, HEADER_SIZE, MAX_HELLO_BODY, ConnectionValidator, Frame, Hello,
    ProtocolError, decode_header, decode_message,
)
from eegpipe.sources import (
    MAX_SYNTH_RATE_HZ, MIN_SYNTH_RATE_HZ, CyclicSource, load_replay, make_synthetic,
)

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 7700
HELLO_TIMEOUT_S = 5.0
IDLE_TIMEOUT_S = 5.0
POLL_S = 0.2                         # socket timeout used to wake up and check deadlines
STATS_INTERVAL_NS = 1_000_000_000


class HelloMismatchError(ProtocolError):
    """A reconnect's HELLO differs from the stream's first HELLO."""


def percentile(values: list[int], q: float) -> int:
    if len(values) == 0:
        raise ValueError("values must be non-empty")
    if not 0 < q <= 1:
        raise ValueError("q must be in (0, 1]")
    return sorted(values)[max(0, math.ceil(q * len(values)) - 1)]


class SyntheticVerifier:
    mode = "synthetic"

    def __init__(self, seed: int) -> None:
        if type(seed) is not int or seed < 0:
            raise ValueError("seed must be an int >= 0")
        self.seed = seed
        self._cache = {}

    def source_for(self, hello: Hello) -> CyclicSource | None:
        fs = float(hello.sample_rate_hz)
        if not fs.is_integer() or not MIN_SYNTH_RATE_HZ <= fs <= MAX_SYNTH_RATE_HZ:
            return None
        if hello.channel_names != tuple(f"ch{c:04d}" for c in range(hello.n_channels)):
            return None
        key = (hello.n_channels, int(fs))
        if key not in self._cache:
            self._cache[key] = make_synthetic(hello.n_channels, int(fs), self.seed)
        return self._cache[key]


class ReplayVerifier:
    mode = "replay"

    def __init__(self, source: CyclicSource) -> None:
        if not isinstance(source, CyclicSource):
            raise TypeError("source must be a CyclicSource")
        self.source = source

    def source_for(self, hello: Hello) -> CyclicSource | None:
        if (self.source.n_channels, self.source.sample_rate_hz, self.source.channel_names) == (
                hello.n_channels, float(hello.sample_rate_hz), hello.channel_names):
            return self.source
        return None


@dataclasses.dataclass
class StreamState:
    hello: Hello                         # the stream's first HELLO
    source: CyclicSource | None          # verification source, or None
    verify: str                          # "off", "on" or "unavailable"
    initial_seq: int | None = None       # seq of the first accepted frame
    last_seq: int | None = None          # seq of the latest accepted frame
    accepted: int = 0
    missing: int = 0
    duplicates: int = 0
    stale: int = 0
    mismatches: int = 0
    latencies_ns: list[int] = dataclasses.field(default_factory=list)
    n: int = 0                           # drift regression: number of points
    x0: int | None = None                # first accepted frame's timestamp_ns
    y0: int | None = None                # first accepted frame's arrival_ns - timestamp_ns
    sx: float = 0.0
    sy: float = 0.0
    sxx: float = 0.0
    sxy: float = 0.0

    def drift_ppm(self) -> float | None:
        if self.n < 2:
            return None
        d = self.n * self.sxx - self.sx * self.sx
        if d <= 0:
            return None
        slope = (self.n * self.sxy - self.sx * self.sy) / d
        return -slope * 1e6


class StreamTracker:
    def __init__(self, verifier=None) -> None:
        self.verifier = verifier
        self.streams = {}
        self.interval_latencies_ns = []

    def on_hello(self, hello: Hello) -> StreamState:
        if hello.stream_id not in self.streams:
            source = None
            if self.verifier is not None:
                try:
                    source = self.verifier.source_for(hello)
                except (MemoryError, ValueError):
                    source = None
            if self.verifier is None:
                verify = "off"
            elif source is not None:
                verify = "on"
            else:
                verify = "unavailable"
            state = StreamState(hello, source, verify)
            self.streams[hello.stream_id] = state
            return state
        old = self.streams[hello.stream_id].hello
        if ((old.n_channels, old.samples_per_frame, old.sample_rate_hz, old.channel_names) !=
                (hello.n_channels, hello.samples_per_frame, hello.sample_rate_hz, hello.channel_names)):
            raise HelloMismatchError(
                f"stream {hello.stream_id:016x} reconnected with different metadata")
        return self.streams[hello.stream_id]

    def on_frame(self, stream_id: int, frame: Frame, arrival_ns: int) -> str:
        st = self.streams[stream_id]
        seq = frame.seq
        S = st.hello.samples_per_frame
        fs = st.hello.sample_rate_hz
        if st.last_seq is None:
            st.initial_seq = seq
        elif seq == st.last_seq:
            st.duplicates += 1
            return "duplicate"
        elif seq < st.last_seq:
            st.stale += 1
            return "stale"
        else:
            st.missing += seq - st.last_seq - 1
        st.last_seq = seq
        st.accepted += 1
        if st.source is not None:
            st.source.position = (seq * S) % st.source.period
            if frame.samples.tobytes() != st.source.read(S).tobytes():
                st.mismatches += 1
        latency = arrival_ns - (frame.timestamp_ns + round(S * 1e9 / fs))
        st.latencies_ns.append(latency)
        self.interval_latencies_ns.append(latency)
        if st.x0 is None:
            st.x0 = frame.timestamp_ns
            st.y0 = arrival_ns - frame.timestamp_ns
        x = (frame.timestamp_ns - st.x0) / 1e9
        y = ((arrival_ns - frame.timestamp_ns) - st.y0) / 1e9
        st.n += 1
        st.sx += x
        st.sy += y
        st.sxx += x * x
        st.sxy += x * y
        return "accepted"

    def totals(self) -> dict[str, int]:
        return {
            "accepted": sum(s.accepted for s in self.streams.values()),
            "missing": sum(s.missing for s in self.streams.values()),
            "duplicates": sum(s.duplicates for s in self.streams.values()),
            "stale": sum(s.stale for s in self.streams.values()),
            "mismatches": sum(s.mismatches for s in self.streams.values()),
        }


def _lat(values, q):
    if not values:
        return "n/a"
    return f"{percentile(values, q) / 1e6:.1f}"


def _lat_max(values):
    if not values:
        return "n/a"
    return f"{max(values) / 1e6:.1f}"


def _finite_number(x):
    return type(x) in (int, float) and math.isfinite(x)


@dataclasses.dataclass(frozen=True)
class ReceiverConfig:
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT             # 0 means any free port
    duration_s: float | None = None
    exit_after_idle_s: float | None = None
    hello_timeout_s: float = HELLO_TIMEOUT_S
    idle_timeout_s: float = IDLE_TIMEOUT_S

    def __post_init__(self):
        if type(self.host) is not str or self.host == "":
            raise ValueError("host must be a non-empty string")
        if type(self.port) is not int or not 0 <= self.port <= 65535:
            raise ValueError("port must be an int in 0..65535")
        if self.duration_s is not None and not (_finite_number(self.duration_s) and self.duration_s > 0):
            raise ValueError("duration_s must be None or a finite number > 0")
        if self.exit_after_idle_s is not None and not (
                _finite_number(self.exit_after_idle_s) and self.exit_after_idle_s > 0):
            raise ValueError("exit_after_idle_s must be None or a finite number > 0")
        if not (_finite_number(self.hello_timeout_s) and self.hello_timeout_s > 0):
            raise ValueError("hello_timeout_s must be a finite number > 0")
        if not (_finite_number(self.idle_timeout_s) and self.idle_timeout_s > 0):
            raise ValueError("idle_timeout_s must be a finite number > 0")


@dataclasses.dataclass
class ReceiverStats:
    connections: int = 0                 # connections returned by accept()
    accept_errors: int = 0               # OSErrors from accept() (e.g. too many open files)
    eof_closes: int = 0                  # peer closed cleanly between messages
    truncated: int = 0                   # peer closed in the middle of a message
    idle_closes: int = 0                 # closed after idle_timeout_s without bytes
    hello_timeouts: int = 0              # closed because no complete HELLO arrived within hello_timeout_s
    conn_errors: int = 0                 # OSError while receiving (e.g. connection reset)
    bytes_received: int = 0
    errors: dict[str, int] = dataclasses.field(default_factory=dict)   # ProtocolError class name -> count


class Receiver:
    def __init__(self, config: ReceiverConfig, verifier=None) -> None:
        if not isinstance(config, ReceiverConfig):
            raise TypeError("config must be a ReceiverConfig")
        self._server = socket.create_server((config.host, config.port))
        self._server.settimeout(POLL_S)
        self.port = self._server.getsockname()[1]
        self.tracker = StreamTracker(verifier)
        self.stats = ReceiverStats()
        self.config = config
        self.verifier = verifier

    def close(self) -> None:
        try:
            self._server.close()
        except OSError:
            pass

    def _counts(self):
        c = self.tracker.totals()
        c["connections"] = self.stats.connections
        c["bytes"] = self.stats.bytes_received
        c["errors"] = sum(self.stats.errors.values())
        return c

    def _duration_over(self, now):
        return (self.config.duration_s is not None
                and now - self._t0 >= round(self.config.duration_s * 1e9))

    def _tick(self, err):
        now = time.monotonic_ns()
        if now - self._last_emit >= STATS_INTERVAL_NS:
            c = self._counts()
            p = self._snapshot
            L = self.tracker.interval_latencies_ns
            elapsed_s = (now - self._t0) / 1e9
            interval_s = (now - self._last_emit) / 1e9

            def D(key):
                return c[key] - p[key]

            print(
                f"t={elapsed_s:.1f}s conns={D('connections')} accepted={D('accepted')} missing={D('missing')} "
                f"dup={D('duplicates')} stale={D('stale')} mismatch={D('mismatches')} errors={D('errors')} "
                f"lat_p50_ms={_lat(L, 0.5)} lat_p99_ms={_lat(L, 0.99)} lat_max_ms={_lat_max(L)} "
                f"MB/s={D('bytes') / interval_s / 1e6:.2f}",
                file=err, flush=True)
            self._snapshot = self._counts()
            self.tracker.interval_latencies_ns.clear()
            self._last_emit = now

    def run(self, err=None):
        if err is None:
            err = sys.stderr
        mode = self.verifier.mode if self.verifier is not None else "off"
        print(
            f"start: listen={self.config.host}:{self.port} verify={mode} "
            f"hello_timeout_s={self.config.hello_timeout_s:g} idle_timeout_s={self.config.idle_timeout_s:g}",
            file=err, flush=True)
        self._t0 = time.monotonic_ns()
        self._last_emit = self._t0
        self._snapshot = self._counts()
        idle_since = None
        try:
            try:
                while True:
                    now = time.monotonic_ns()
                    if self._duration_over(now):
                        break
                    if (idle_since is not None and self.config.exit_after_idle_s is not None
                            and now - idle_since >= round(self.config.exit_after_idle_s * 1e9)):
                        break
                    try:
                        conn, _ = self._server.accept()
                    except TimeoutError:
                        self._tick(err)
                        continue
                    except OSError:
                        self.stats.accept_errors += 1
                        time.sleep(POLL_S)
                        self._tick(err)
                        continue
                    self.stats.connections += 1
                    stop = self._serve(conn, err)
                    idle_since = time.monotonic_ns()
                    self._tick(err)
                    if stop:
                        break
            except KeyboardInterrupt:
                pass
        finally:
            self.close()
            t = self.tracker.totals()
            s = self.stats
            elapsed_s = (time.monotonic_ns() - self._t0) / 1e9
            mbps = s.bytes_received / elapsed_s / 1e6 if elapsed_s > 0 else 0.0
            errs = ",".join(f"{k}:{v}" for k, v in sorted(s.errors.items())) or "none"
            print(
                f"summary: connections={s.connections} accepted={t['accepted']} missing={t['missing']} "
                f"dup={t['duplicates']} stale={t['stale']} mismatch={t['mismatches']} "
                f"eof_closes={s.eof_closes} truncated={s.truncated} idle_closes={s.idle_closes} "
                f"hello_timeouts={s.hello_timeouts} conn_errors={s.conn_errors} accept_errors={s.accept_errors} errors={errs} "
                f"streams={len(self.tracker.streams)} bytes={s.bytes_received} elapsed_s={elapsed_s:.3f} MB/s={mbps:.2f}",
                file=err, flush=True)
            for st in self.tracker.streams.values():
                h = st.hello
                L = st.latencies_ns

                def opt(x):
                    return "n/a" if x is None else x

                drift = "n/a" if st.drift_ppm() is None else f"{st.drift_ppm():.1f}"
                print(
                    f"stream: id={h.stream_id:016x} device={h.device_name} channels={h.n_channels} "
                    f"rate_hz={h.sample_rate_hz:g} samples_per_frame={h.samples_per_frame} "
                    f"initial_seq={opt(st.initial_seq)} last_seq={opt(st.last_seq)} accepted={st.accepted} "
                    f"missing={st.missing} dup={st.duplicates} stale={st.stale} mismatch={st.mismatches} "
                    f"verify={st.verify} drift_ppm={drift} lat_p50_ms={_lat(L, 0.5)} lat_p99_ms={_lat(L, 0.99)} "
                    f"lat_max_ms={_lat_max(L)}",
                    file=err, flush=True)
        return self.stats

    def _serve(self, conn, err):
        conn.settimeout(POLL_S)
        validator = ConnectionValidator()
        connected_at = last_data = time.monotonic_ns()
        buf = bytearray(HEADER_SIZE + MAX_HELLO_BODY + CRC_SIZE)
        view = memoryview(buf)

        def read(start, n, boundary):
            nonlocal last_data
            got = 0
            while got < n:
                try:
                    k = conn.recv_into(view[start + got:start + n])
                except TimeoutError:
                    k = None
                now = time.monotonic_ns()
                if k == 0:
                    return "eof" if (boundary and got == 0) else "truncated"
                if k is not None:
                    got += k
                    last_data = now
                    self.stats.bytes_received += k
                self._tick(err)
                if self._duration_over(now):
                    return "stop"
                if validator.hello is None and now - connected_at >= round(self.config.hello_timeout_s * 1e9):
                    return "hello_timeout"
                if now - last_data >= round(self.config.idle_timeout_s * 1e9):
                    return "idle"
            return "ok"

        def finish(status):
            if status == "eof":
                self.stats.eof_closes += 1
                return False
            if status == "truncated":
                self.stats.truncated += 1
                return False
            if status == "idle":
                self.stats.idle_closes += 1
                return False
            if status == "hello_timeout":
                self.stats.hello_timeouts += 1
                return False
            return True  # "stop"

        try:
            try:
                while True:
                    status = read(0, HEADER_SIZE, boundary=True)
                    if status != "ok":
                        return finish(status)
                    try:
                        header = decode_header(bytes(view[:HEADER_SIZE]))
                        validator.check_header(header)
                        total = HEADER_SIZE + header.body_length + CRC_SIZE
                        if len(buf) < total:
                            new = bytearray(total)
                            new[:HEADER_SIZE] = view[:HEADER_SIZE]
                            view.release()
                            buf = new
                            view = memoryview(buf)
                        status = read(HEADER_SIZE, total - HEADER_SIZE, boundary=False)
                        if status != "ok":
                            return finish(status)
                        arrival = time.time_ns()
                        message = decode_message(bytes(view[:total]))
                        validator.check(message)
                        if isinstance(message, Hello):
                            self.tracker.on_hello(message)
                        else:
                            self.tracker.on_frame(validator.hello.stream_id, message, arrival)
                    except ProtocolError as e:
                        name = type(e).__name__
                        self.stats.errors[name] = self.stats.errors.get(name, 0) + 1
                        return False
                    self._tick(err)
                    if self._duration_over(time.monotonic_ns()):
                        return True
            except OSError:
                self.stats.conn_errors += 1
                return False
        finally:
            try:
                conn.close()
            except OSError:
                pass


def _host(x: str):
    if x == "":
        raise argparse.ArgumentTypeError("must be non-empty")
    return x


def _port(x: str):
    try:
        v = int(x)
    except ValueError:
        raise argparse.ArgumentTypeError("must be an integer in 1..65535")
    if not 1 <= v <= 65535:
        raise argparse.ArgumentTypeError("must be an integer in 1..65535")
    return v


def _positive_seconds(x: str):
    try:
        v = float(x)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a finite number > 0")
    if not math.isfinite(v) or v <= 0:
        raise argparse.ArgumentTypeError("must be a finite number > 0")
    return v


def _seed(x: str):
    try:
        v = int(x)
    except ValueError:
        raise argparse.ArgumentTypeError("must be an integer >= 0")
    if v < 0:
        raise argparse.ArgumentTypeError("must be an integer >= 0")
    return v


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m eegpipe.receiver",
                                     description="Receive and verify EEGP streams.")
    parser.add_argument("--host", dest="host", type=_host, default=DEFAULT_HOST)
    parser.add_argument("--port", dest="port", type=_port, default=DEFAULT_PORT)
    parser.add_argument("--duration", dest="duration", type=_positive_seconds, default=None)
    parser.add_argument("--exit-after-idle", dest="exit_after_idle", type=_positive_seconds, default=None)
    parser.add_argument("--seed", dest="seed", type=_seed, default=None)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--verify-synthetic", dest="verify_synthetic", action="store_true")
    group.add_argument("--verify-replay", dest="verify_replay", metavar="FILE", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.seed is not None and not args.verify_synthetic:
        parser.error("--seed requires --verify-synthetic")
    try:
        if args.verify_synthetic:
            verifier = SyntheticVerifier(args.seed if args.seed is not None else 0)
        elif args.verify_replay is not None:
            try:
                verifier = ReplayVerifier(load_replay(args.verify_replay))
            except (OSError, ValueError) as e:
                print(f"eegpipe.receiver: error: {e}", file=sys.stderr)
                return 2
        else:
            verifier = None
        config = ReceiverConfig(args.host, args.port, args.duration, args.exit_after_idle)
        try:
            receiver = Receiver(config, verifier)
        except OSError as e:
            print(f"eegpipe.receiver: error: {e}", file=sys.stderr)
            return 2
        receiver.run()
        return 0
    except KeyboardInterrupt:
        print("eegpipe.receiver: interrupted", file=sys.stderr)
        return 0


if __name__ == "__main__":
    sys.exit(main())
