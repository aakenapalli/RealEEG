import dataclasses
import io
import math
import socket
import struct
import threading

import numpy as np
import pyedflib
import pytest

from eegpipe.protocol import HEADER_SIZE, Frame, Hello, ProtocolError, encode_frame, encode_hello
from eegpipe.sources import CyclicSource, load_replay, make_synthetic
from eegpipe.device import DeviceConfig, FaultConfig, run_device
from eegpipe.device import main as device_main
from eegpipe.receiver import (
    HelloMismatchError, Receiver, ReceiverConfig, ReplayVerifier, StreamTracker,
    SyntheticVerifier, main, percentile,
)

MS = 1_000_000
T0 = 1_000_000_000                       # FakeClock's starting monotonic time
WALL0 = 1_700_000_000_000_000_000        # FakeClock's wall time at T0


def make_hello(stream_id=1, n_channels=2, samples_per_frame=4, sample_rate_hz=200.0,
               channel_names=None, device_name="dev"):
    names = channel_names if channel_names is not None else tuple(f"ch{c:04d}" for c in range(n_channels))
    return Hello(stream_id, device_name, n_channels, samples_per_frame, sample_rate_hz, names)


def make_frame(seq, timestamp_ns=0, n_channels=2, samples_per_frame=4, samples=None):
    if samples is None:
        samples = np.zeros((n_channels, samples_per_frame), dtype="<f4")
    return Frame(seq, timestamp_ns, samples)


def feed(tracker, seqs, stream_id=1, arrival_ns=0):
    return [tracker.on_frame(stream_id, make_frame(s), arrival_ns) for s in seqs]


H = make_hello()                         # stream 1: 2 channels, S = 4, 200 Hz


def frame_bytes(k):
    return encode_frame(make_frame(k))   # zeros, timestamp 0; 68 bytes


CORRUPT = bytes(b ^ 0xFF if i == 40 else b for i, b in enumerate(frame_bytes(0)))


class FakeClock:
    """Same behavior as Plan 02's test clock: time only moves when sleep() is called."""

    def __init__(self):
        self.mono = T0

    def monotonic_ns(self):
        return self.mono

    def time_ns(self):
        return WALL0 + (self.mono - T0)

    def sleep(self, seconds):
        self.mono += round(seconds * 1e9)


def small_source():
    return make_synthetic(4, 1000, 0)    # the device's signal; its seed (0) is fixed here


def device_config(**overrides):
    """A Plan 02 DeviceConfig: 4 ch at 1 kHz (from small_source), S = 20, 1.0 s (50 frames).
    Its `seed` only seeds the device's faults, never the signal."""
    fault_fields = {f.name for f in dataclasses.fields(FaultConfig)}
    faults = FaultConfig(**{k: v for k, v in overrides.items() if k in fault_fields})
    base = dict(host="127.0.0.1", port=7700, samples_per_frame=20, duration_s=1.0, seed=0,
                stream_id=0x0123456789ABCDEF, device_name="test-device", faults=faults)
    base.update({k: v for k, v in overrides.items() if k not in fault_fields})
    return DeviceConfig(**base)


def tracker_with(hello=None, verifier=None):
    tracker = StreamTracker(verifier) if verifier is not None else StreamTracker()
    st = tracker.on_hello(hello if hello is not None else H)
    return tracker, st


@pytest.mark.parametrize("values, q, expected", [
    (list(range(1, 11)), 0.5, 5),
    (list(range(1, 11)), 0.99, 10),
    (list(range(1, 11)), 1.0, 10),
    ([7], 0.5, 7),
    ([3, 1, 2], 0.5, 2),
])
def test_percentile(values, q, expected):
    assert percentile(values, q) == expected


@pytest.mark.parametrize("values, q", [([], 0.5), ([1], 0), ([1], 1.5)])
def test_percentile_rejects(values, q):
    with pytest.raises(ValueError):
        percentile(values, q)


def test_verifier_constructors_reject():
    with pytest.raises(ValueError):
        SyntheticVerifier(-1)
    with pytest.raises(ValueError):
        SyntheticVerifier(True)
    with pytest.raises(TypeError):
        ReplayVerifier("x")


def test_first_frame_sets_baseline():
    tracker, st = tracker_with()
    assert feed(tracker, [5]) == ["accepted"]
    assert st.initial_seq == 5
    assert st.last_seq == 5
    assert st.accepted == 1
    assert st.missing == 0
    assert st.verify == "off"


def test_gaps_counted():
    tracker, st = tracker_with()
    assert feed(tracker, [0, 1, 4, 5, 9]) == ["accepted"] * 5
    assert st.missing == 5
    assert st.accepted == 5
    assert st.last_seq == 9


def test_duplicate_discarded():
    tracker, st = tracker_with()
    assert feed(tracker, [0, 1, 1]) == ["accepted", "accepted", "duplicate"]
    assert st.duplicates == 1
    assert st.accepted == 2


def test_stale_discarded():
    tracker, st = tracker_with()
    assert feed(tracker, [0, 1, 2, 1]) == ["accepted"] * 3 + ["stale"]
    assert st.stale == 1
    assert st.last_seq == 2


def test_reconnect_continues_stream():
    tracker, st = tracker_with()
    feed(tracker, [0, 1])
    assert tracker.on_hello(make_hello()) is st
    feed(tracker, [5])
    assert st.missing == 3
    assert len(tracker.streams) == 1


def test_reconnect_device_name_change_allowed():
    tracker, st = tracker_with()
    tracker.on_hello(make_hello(device_name="other"))
    assert st.hello.device_name == "dev"


@pytest.mark.parametrize("overrides", [
    {"samples_per_frame": 8},
    {"n_channels": 3},
    {"sample_rate_hz": 250.0},
    {"channel_names": ("x", "y")},
])
def test_hello_mismatch(overrides):
    tracker, st = tracker_with()
    with pytest.raises(HelloMismatchError) as e:
        tracker.on_hello(make_hello(**overrides))
    assert isinstance(e.value, ProtocolError)
    assert st.hello == H


def test_streams_independent():
    tracker = StreamTracker()
    tracker.on_hello(make_hello(stream_id=1))
    tracker.on_hello(make_hello(stream_id=2))
    tracker.on_frame(1, make_frame(0), 0)
    tracker.on_frame(2, make_frame(10), 0)
    tracker.on_frame(1, make_frame(2), 0)
    tracker.on_frame(2, make_frame(11), 0)
    s1, s2 = tracker.streams[1], tracker.streams[2]
    assert s1.missing == 1
    assert s1.initial_seq == 0
    assert s2.missing == 0
    assert s2.initial_seq == 10
    assert tracker.totals() == {"accepted": 4, "missing": 1, "duplicates": 0, "stale": 0, "mismatches": 0}


def test_latency():
    tracker, st = tracker_with()
    tracker.on_frame(1, make_frame(0, timestamp_ns=1_000 * MS), 1_030 * MS)
    assert st.latencies_ns == [10 * MS]
    assert tracker.interval_latencies_ns == [10 * MS]


def test_only_accepted_frames_measured():
    tracker, st = tracker_with()
    feed(tracker, [0, 1, 1, 0])
    assert len(st.latencies_ns) == 2
    assert st.n == 2


def test_drift_estimate():
    tracker, st = tracker_with()
    for k in range(100):
        ts = WALL0 + k * 20 * MS
        arrival = ts + 25 * MS - k * 20_000
        tracker.on_frame(1, make_frame(k, timestamp_ns=ts), arrival)
    assert abs(st.drift_ppm() - 1000.0) < 0.01


def test_drift_needs_two_points():
    tracker, st = tracker_with()
    tracker.on_frame(1, make_frame(0, timestamp_ns=5), 7)
    assert st.drift_ppm() is None
    tracker.on_frame(1, make_frame(1, timestamp_ns=5), 9)
    assert st.n == 2
    assert st.drift_ppm() is None


def test_synthetic_verification():
    tracker, st = tracker_with(H, SyntheticVerifier(3))
    assert st.verify == "on"
    src = make_synthetic(2, 200, 3)
    for k in range(60):
        tracker.on_frame(1, make_frame(k, samples=src.read(4).copy()), 0)
    assert st.mismatches == 0
    tracker.on_frame(1, make_frame(60, samples=np.ones((2, 4), dtype="<f4")), 0)
    assert st.mismatches == 1
    src2 = make_synthetic(2, 200, 3)
    src2.position = (125 * 4) % 200
    tracker.on_frame(1, make_frame(125, samples=src2.read(4).copy()), 0)
    assert st.mismatches == 1
    assert st.missing == 64


@pytest.mark.parametrize("overrides", [
    {"sample_rate_hz": 160.5},
    {"sample_rate_hz": 100.0},
    {"channel_names": ("a", "b")},
])
def test_synthetic_verification_unavailable(overrides):
    tracker, st = tracker_with(make_hello(**overrides), SyntheticVerifier(0))
    assert st.verify == "unavailable"
    assert feed(tracker, [0, 1]) == ["accepted"] * 2
    assert st.mismatches == 0


def test_synthetic_verifier_caches_by_shape():
    v = SyntheticVerifier(0)
    assert v.source_for(make_hello(stream_id=1)) is v.source_for(make_hello(stream_id=2))


def test_verification_compares_bytes():
    buf = np.array([[0.0, 1.0]], dtype="<f4")
    v = ReplayVerifier(CyclicSource(buf, 200.0, ("a",)))
    tracker, st = tracker_with(
        make_hello(n_channels=1, samples_per_frame=2, channel_names=("a",)), v)
    tracker.on_frame(1, make_frame(0, n_channels=1, samples_per_frame=2,
                                   samples=np.array([[-0.0, 1.0]], dtype="<f4")), 0)
    assert st.mismatches == 1


def test_source_failure_marks_unavailable():
    class Boom:
        mode = "synthetic"

        def source_for(self, hello):
            raise MemoryError

    assert StreamTracker(Boom()).on_hello(H).verify == "unavailable"


def test_replay_verification():
    buf = np.arange(80, dtype="<f4").reshape(2, 40)
    v = ReplayVerifier(CyclicSource(buf, 100.0, ("a", "b")))
    tracker, st = tracker_with(make_hello(sample_rate_hz=100.0, channel_names=("a", "b")), v)
    assert st.verify == "on"
    for k in range(12):
        tracker.on_frame(1, make_frame(k, samples=buf[:, [(4 * k + j) % 40 for j in range(4)]]), 0)
    assert st.mismatches == 0
    tracker.on_frame(1, make_frame(12), 0)
    assert st.mismatches == 1
    t2 = StreamTracker(v)
    assert t2.on_hello(make_hello(sample_rate_hz=100.0, channel_names=("a", "c"))).verify == "unavailable"


FAST = dict(port=0, exit_after_idle_s=0.3, hello_timeout_s=0.3, idle_timeout_s=0.3)


class RunningReceiver:
    """A Receiver running in a daemon thread. Config fields default to FAST."""

    def __init__(self, verifier=None, **overrides):
        self.rx = Receiver(ReceiverConfig(**{**FAST, **overrides}), verifier)
        self.port = self.rx.port
        self.tracker = self.rx.tracker
        self.err = io.StringIO()
        self.stats = None
        self.error = None
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        try:
            self.stats = self.rx.run(err=self.err)
        except BaseException as e:
            self.error = e

    def join(self):
        self.thread.join(timeout=15)
        assert not self.thread.is_alive()
        if self.error is not None:
            raise self.error
        return self.stats

    def lines(self):
        return self.err.getvalue().splitlines()


def client(port):
    return socket.create_connection(("127.0.0.1", port), timeout=5)


def wait_closed(sock):
    """Block until the receiver closes the connection. Returns b"" when closed
    (a reset also counts as closed)."""
    try:
        return sock.recv(1)
    except ConnectionResetError:
        return b""


def run_pair(source=None, verifier=None, rx_overrides=None, **device_overrides):
    """Run the Plan 02 device (FakeClock, real TCP) against a RunningReceiver.
    Returns (device_stats, receiver_stats, tracker)."""
    rx = RunningReceiver(verifier=verifier, **{"exit_after_idle_s": 1.0, "hello_timeout_s": 2.0,
                                               "idle_timeout_s": 2.0, **(rx_overrides or {})})
    dev = run_device(source if source is not None else small_source(),
                     device_config(port=rx.port, **device_overrides),
                     clock=FakeClock(), err=io.StringIO())
    return dev, rx.join(), rx.tracker


def assert_reconciled(dev, tracker):
    (st,) = tracker.streams.values()
    assert st.accepted == dev.sent
    assert st.duplicates == dev.duplicated
    assert st.stale == 0
    assert st.mismatches == 0
    trailing = dev.frames - 1 - st.last_seq
    assert st.initial_seq + st.missing + trailing == dev.dropped + dev.overflow + dev.disconnected


def write_edf(path, labels, dims, rates, seconds=2):
    """Identical copy of the write_edf helper in python/tests/test_sources.py."""
    hdrs, data = [], []
    for i, (label, dim, fs) in enumerate(zip(labels, dims, rates)):
        hdrs.append(dict(label=label, dimension=dim, sample_frequency=fs,
                         physical_max=1000.0, physical_min=-1000.0,
                         digital_max=32767, digital_min=-32768,
                         transducer="", prefilter=""))
        t = np.arange(seconds * fs) / fs
        data.append(100.0 * (i + 1) * np.sin(2 * np.pi * 10 * t))
    w = pyedflib.EdfWriter(str(path), len(labels), file_type=pyedflib.FILETYPE_EDFPLUS)
    w.setSignalHeaders(hdrs)
    w.writeSamples(data)
    w.close()
    return str(path)


@pytest.mark.parametrize("overrides", [
    {"host": ""},
    {"port": -1},
    {"port": 65536},
    {"port": True},
    {"duration_s": 0},
    {"duration_s": math.nan},
    {"exit_after_idle_s": -1},
    {"hello_timeout_s": 0},
    {"idle_timeout_s": math.inf},
])
def test_receiver_config_rejects(overrides):
    with pytest.raises(ValueError):
        ReceiverConfig(**overrides)


def test_receiver_rejects_bad_config_type():
    with pytest.raises(TypeError):
        Receiver("x")


def test_receives_valid_stream():
    rx = RunningReceiver()
    c = client(rx.port)
    c.sendall(encode_hello(H) + frame_bytes(0) + frame_bytes(1) + frame_bytes(2))
    c.close()
    stats = rx.join()
    assert stats.connections == 1
    assert stats.eof_closes == 1
    assert stats.errors == {}
    assert stats.bytes_received == len(encode_hello(H)) + 3 * len(frame_bytes(0))
    assert rx.tracker.totals()["accepted"] == 3


def test_output_lines():
    rx = RunningReceiver()
    c = client(rx.port)
    c.sendall(encode_hello(H) + frame_bytes(0) + frame_bytes(1) + frame_bytes(2))
    c.close()
    stats = rx.join()
    lines = rx.lines()
    assert lines[0] == f"start: listen=127.0.0.1:{rx.port} verify=off hello_timeout_s=0.3 idle_timeout_s=0.3"
    assert lines[-2].startswith(
        "summary: connections=1 accepted=3 missing=0 dup=0 stale=0 mismatch=0 eof_closes=1 truncated=0 "
        "idle_closes=0 hello_timeouts=0 conn_errors=0 accept_errors=0 errors=none streams=1 "
        f"bytes={stats.bytes_received} elapsed_s=")
    assert lines[-1].startswith(
        "stream: id=0000000000000001 device=dev channels=2 rate_hz=200 samples_per_frame=4 "
        "initial_seq=0 last_seq=2 accepted=3 missing=0 dup=0 stale=0 mismatch=0 verify=off drift_ppm=n/a ")
    assert all(line.startswith("t=") for line in lines[1:-2])


@pytest.mark.parametrize("payload, error_name", [
    (b"XXXX" + bytes(8), "BadMagicError"),
    (frame_bytes(0)[:HEADER_SIZE], "MessageOrderError"),
    (encode_hello(H) + struct.pack("<4sBBHI", b"EEGP", 1, 2, 0, 60), "DimensionMismatchError"),
    (encode_hello(H) + CORRUPT, "ChecksumError"),
])
def test_protocol_error_closes_and_counts(payload, error_name):
    rx = RunningReceiver()
    c = client(rx.port)
    c.sendall(payload)
    assert wait_closed(c) == b""
    c.close()
    stats = rx.join()
    assert stats.errors == {error_name: 1}
    assert stats.connections == 1


def test_hello_timeout():
    rx = RunningReceiver()
    c = client(rx.port)
    assert wait_closed(c) == b""
    c.close()
    stats = rx.join()
    assert stats.hello_timeouts == 1
    assert stats.idle_closes == 0


def test_trickled_hello_times_out():
    import time
    rx = RunningReceiver()
    c = client(rx.port)
    hb = encode_hello(H)
    closed_early = False
    for i in range(20):
        try:
            c.sendall(hb[i:i + 1])
        except OSError:
            closed_early = True
            break
        time.sleep(0.05)
    assert closed_early is True
    assert wait_closed(c) == b""
    c.close()
    stats = rx.join()
    assert stats.hello_timeouts == 1


def test_idle_timeout():
    rx = RunningReceiver()
    c = client(rx.port)
    c.sendall(encode_hello(H))
    assert wait_closed(c) == b""
    c.close()
    stats = rx.join()
    assert stats.idle_closes == 1
    assert stats.hello_timeouts == 0
    assert len(rx.tracker.streams) == 1


def test_truncated_message():
    rx = RunningReceiver()
    c = client(rx.port)
    c.sendall(encode_hello(H) + frame_bytes(0)[:20])
    c.close()
    stats = rx.join()
    assert stats.truncated == 1
    assert stats.eof_closes == 0
    assert rx.tracker.totals()["accepted"] == 0


def test_hello_mismatch_on_reconnect():
    rx = RunningReceiver()
    c1 = client(rx.port)
    c1.sendall(encode_hello(H) + frame_bytes(0))
    c2 = client(rx.port)
    c1.close()
    c2.sendall(encode_hello(make_hello(samples_per_frame=8)))
    assert wait_closed(c2) == b""
    c2.close()
    stats = rx.join()
    assert stats.errors == {"HelloMismatchError": 1}
    assert stats.connections == 2


def test_reconnect_gap_counted():
    rx = RunningReceiver()
    c1 = client(rx.port)
    c1.sendall(encode_hello(H) + frame_bytes(0) + frame_bytes(1))
    c2 = client(rx.port)
    c1.close()
    c2.sendall(encode_hello(H) + frame_bytes(5))
    c2.close()
    stats = rx.join()
    st = rx.tracker.streams[1]
    assert st.missing == 3
    assert st.accepted == 3
    assert stats.connections == 2


def test_duration_without_clients():
    rx = RunningReceiver(duration_s=0.3, exit_after_idle_s=None)
    stats = rx.join()
    assert stats.connections == 0
    assert rx.lines()[-1].startswith("summary: connections=0 ")


def test_duration_closes_active_connection():
    rx = RunningReceiver(duration_s=0.5, exit_after_idle_s=None, hello_timeout_s=5.0, idle_timeout_s=5.0)
    c = client(rx.port)
    c.sendall(encode_hello(H))
    assert wait_closed(c) == b""
    c.close()
    stats = rx.join()
    assert stats.connections == 1
    assert stats.idle_closes == 0
    assert stats.eof_closes == 0


def test_e2e_clean_verified():
    dev, rx_stats, tracker = run_pair(verifier=SyntheticVerifier(0))
    (st,) = tracker.streams.values()
    assert_reconciled(dev, tracker)
    assert st.verify == "on"
    assert st.accepted == 50
    assert st.initial_seq == 0
    assert st.missing == 0
    assert rx_stats.connections == 1
    assert rx_stats.errors == {}


def test_e2e_drop():
    dev, rx_stats, tracker = run_pair(verifier=SyntheticVerifier(0), drop_prob=0.3, seed=7, duration_s=2.0)
    assert_reconciled(dev, tracker)
    assert dev.dropped > 0


def test_e2e_duplicate():
    dev, rx_stats, tracker = run_pair(verifier=SyntheticVerifier(0), dup_prob=0.3, seed=7, duration_s=2.0)
    (st,) = tracker.streams.values()
    assert_reconciled(dev, tracker)
    assert st.duplicates == dev.duplicated > 0


def test_e2e_jitter_and_drift():
    dev, rx_stats, tracker = run_pair(verifier=SyntheticVerifier(0), jitter_ms=5.0, drift_ppm=500,
                                      duration_s=2.0)
    assert_reconciled(dev, tracker)


def test_e2e_disconnects():
    dev, rx_stats, tracker = run_pair(verifier=SyntheticVerifier(0), disconnect_every_s=0.3,
                                      duration_s=2.0)
    (st,) = tracker.streams.values()
    assert_reconciled(dev, tracker)
    assert rx_stats.connections == dev.connects > 1
    assert rx_stats.eof_closes == rx_stats.connections
    assert st.missing == 0


def test_e2e_all_faults_but_corruption():
    dev, rx_stats, tracker = run_pair(verifier=SyntheticVerifier(0), seed=11, drop_prob=0.2, dup_prob=0.2,
                                      jitter_ms=3.0, drift_ppm=-300, disconnect_every_s=0.25,
                                      duration_s=2.0)
    assert_reconciled(dev, tracker)


def test_e2e_corruption_bounds():
    dev, rx_stats, tracker = run_pair(verifier=SyntheticVerifier(0), rx_overrides={"exit_after_idle_s": 1.5},
                                      corrupt_prob=0.1, duration_s=2.0)
    (st,) = tracker.streams.values()
    assert 1 <= rx_stats.errors.get("ChecksumError", 0) <= dev.corrupted
    assert st.mismatches == 0
    assert st.stale == 0
    assert st.accepted <= dev.sent


def test_e2e_replay_verified(tmp_path):
    path = write_edf(tmp_path / "r.edf", ["Fc5.", "C3.."], ["uV", "uV"], [160, 160])
    dev, rx_stats, tracker = run_pair(source=load_replay(path), verifier=ReplayVerifier(load_replay(path)),
                                      samples_per_frame=3)
    (st,) = tracker.streams.values()
    assert_reconciled(dev, tracker)
    assert st.verify == "on"
    assert st.accepted == 53


def test_e2e_drift_real_time():
    rx = RunningReceiver(exit_after_idle_s=1.0, hello_timeout_s=2.0, idle_timeout_s=2.0)
    dev = run_device(small_source(), device_config(port=rx.port, duration_s=2.0, drift_ppm=10000),
                     err=io.StringIO())
    rx.join()
    (st,) = rx.tracker.streams.values()
    assert abs(st.drift_ppm() - 10000 / 1.01) < 1500
    assert st.accepted == dev.sent


def test_cli_end_to_end_verified(capsys):
    s = socket.create_server(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    result = []
    t = threading.Thread(target=lambda: result.append(main(
        ["--port", str(port), "--exit-after-idle", "0.5", "--duration", "10",
         "--verify-synthetic", "--seed", "4"])), daemon=True)
    t.start()
    assert device_main(["synthetic", "--channels", "2", "--rate", "200", "--duration", "1.0",
                        "--port", str(port), "--seed", "4"]) == 0
    t.join(timeout=15)
    assert not t.is_alive()
    assert result == [0]
    err = capsys.readouterr().err
    stream_lines = [line for line in err.splitlines() if line.startswith("stream: ")]
    assert len(stream_lines) == 1
    line = stream_lines[0]
    assert " missing=0 " in line
    assert " mismatch=0 " in line
    assert " verify=on " in line
    accepted = int(line.split(" accepted=")[1].split(" ")[0])
    assert accepted >= 1


@pytest.mark.parametrize("argv", [
    ["--port", "0"],
    ["--seed", "1"],
    ["--seed", "-1", "--verify-synthetic"],
    ["--verify-synthetic", "--verify-replay", "x.edf"],
    ["--verify-replay"],
    ["--duration", "0"],
    ["--exit-after-idle", "nan"],
])
def test_cli_rejects_bad_arguments(argv):
    with pytest.raises(SystemExit) as e:
        main(argv)
    assert e.value.code == 2


def test_cli_bad_replay_file(tmp_path, capsys):
    assert main(["--verify-replay", str(tmp_path / "missing.edf")]) == 2
    assert capsys.readouterr().err.startswith("eegpipe.receiver: error: ")


def test_cli_port_in_use(capsys):
    blocker = socket.create_server(("127.0.0.1", 0))
    assert main(["--port", str(blocker.getsockname()[1]), "--duration", "0.1"]) == 2
    assert capsys.readouterr().err.startswith("eegpipe.receiver: error: ")
    blocker.close()


def test_cli_interrupt_during_startup(monkeypatch, capsys):
    def f(*args):
        raise KeyboardInterrupt

    monkeypatch.setattr("eegpipe.receiver.load_replay", f)
    assert main(["--verify-replay", "x.edf"]) == 0
    assert "eegpipe.receiver: interrupted" in capsys.readouterr().err


def test_module_help():
    import subprocess
    import sys
    r = subprocess.run([sys.executable, "-m", "eegpipe.receiver", "--help"],
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0
    assert "--verify-synthetic" in r.stdout
