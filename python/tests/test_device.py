import dataclasses
import io
import math
import socket
import subprocess
import sys
import threading

import numpy as np
import pyedflib
import pytest

from eegpipe.protocol import (
    CRC_SIZE, HEADER_SIZE, ChecksumError, ConnectionValidator, Frame, Hello,
    decode_header, decode_message, encode_frame, encode_hello,
)
from eegpipe.sources import make_synthetic
from eegpipe.device import (
    DeviceConfig, FaultConfig, PAYLOAD_OFFSET, SEND_TIMEOUT_S, Stats,
    default_samples_per_frame, main, run_device, tcp_connect,
)

T0 = 1_000_000_000                      # FakeClock's starting monotonic time
WALL0 = 1_700_000_000_000_000_000       # FakeClock's wall time at T0
MS = 1_000_000
HELLO = Hello(0x0123456789ABCDEF, "test-device", 4, 20, 1000.0,
              ("ch0000", "ch0001", "ch0002", "ch0003"))


class FakeClock:
    def __init__(self, interrupt_on_sleep=None):
        self.mono = T0
        self.sleeps = 0
        self.interrupt_on_sleep = interrupt_on_sleep   # 1-based sleep call that raises KeyboardInterrupt

    def monotonic_ns(self):
        return self.mono

    def time_ns(self):
        return WALL0 + (self.mono - T0)

    def sleep(self, seconds):
        self.sleeps += 1
        if self.sleeps == self.interrupt_on_sleep:
            raise KeyboardInterrupt
        self.mono += round(seconds * 1e9)

    def advance(self, ns):
        self.mono += ns


class FakeSocket:
    """Records each sendall call. costs maps a 0-based call index to nanoseconds the
    clock advances after that call is recorded. fail_on is the 0-based call index that
    raises BrokenPipeError instead (nothing recorded). Call 0 is always the HELLO."""

    def __init__(self, clock, costs=None, fail_on=None):
        self.clock = clock
        self.costs = costs or {}
        self.fail_on = fail_on
        self.calls = 0
        self.chunks = []        # bytes of each recorded call
        self.times = []         # clock.mono when each recorded call started
        self.closed = False

    def sendall(self, data):
        index = self.calls
        self.calls += 1
        if self.closed or index == self.fail_on:
            raise BrokenPipeError("fake broken pipe")
        self.times.append(self.clock.mono)
        self.chunks.append(bytes(data))
        self.clock.advance(self.costs.get(index, 0))

    def close(self):
        self.closed = True


class FakeConnector:
    """Returns, or raises, the given outcomes in order; raises ConnectionRefusedError when exhausted."""

    def __init__(self, clock, outcomes):
        self.clock = clock
        self.outcomes = list(outcomes)
        self.calls = []         # (host, port, clock.mono) per call

    def __call__(self, host, port):
        self.calls.append((host, port, self.clock.mono))
        if not self.outcomes:
            raise ConnectionRefusedError("fake refused")
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def make_config(**overrides):
    fault_fields = {f.name for f in dataclasses.fields(FaultConfig)}
    faults = FaultConfig(**{k: v for k, v in overrides.items() if k in fault_fields})
    base = dict(host="127.0.0.1", port=7700, samples_per_frame=20, duration_s=0.1, seed=0,
                stream_id=0x0123456789ABCDEF, device_name="test-device", faults=faults)
    base.update({k: v for k, v in overrides.items() if k not in fault_fields})
    return DeviceConfig(**base)


def small_source():
    return make_synthetic(4, 1000, 0)      # 4 channels, 1 kHz; with S = 20 a frame is 20 ms


def run_fake(config, sockets=None, clock=None):
    """Run the device with a FakeClock and a FakeConnector.
    `sockets` lists the connector's outcomes in order: FakeSocket instances, or
    exception instances that the connector raises. Default: one FakeSocket.
    Returns (stats, clock, fake_sockets, connector, output_text), where
    fake_sockets is the FakeSocket outcomes in order."""
    clock = clock or FakeClock()
    sockets = sockets if sockets is not None else [FakeSocket(clock)]
    connector = FakeConnector(clock, sockets)
    err = io.StringIO()
    stats = run_device(small_source(), config, clock=clock, connect=connector, err=err)
    return stats, clock, [s for s in sockets if isinstance(s, FakeSocket)], connector, err.getvalue()


def all_chunks(sockets):
    return [c for s in sockets for c in s.chunks]


def sent_frames(sockets):
    """[(frame, start_time)] for every recorded chunk that decodes to a Frame, in order."""
    out = []
    for s in sockets:
        for chunk, t in zip(s.chunks, s.times):
            m = decode_message(chunk)
            if isinstance(m, Frame):
                out.append((m, t))
    return out


def seqs(sockets):
    return [m.seq for m, _ in sent_frames(sockets)]


def send_times(sockets):
    return [t for _, t in sent_frames(sockets)]


def expected_draws(seed, n):
    rng = np.random.default_rng([seed, 1])
    return [rng.random(5) for _ in range(n)]


def expected_frame_samples(n_frames, S=20):
    src = small_source()
    return [src.read(S).copy() for _ in range(n_frames)]


def write_edf(path, labels, dims, rates, seconds=2):
    """Identical copy of the test_sources.py helper."""
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


def recv_exact(conn, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            break
        buf += chunk
    return bytes(buf)


class OneShotReceiver:
    """Accepts one TCP connection on 127.0.0.1 and decodes every message until EOF."""

    def __init__(self):
        self.server = socket.create_server(("127.0.0.1", 0))
        self.server.settimeout(10)
        self.port = self.server.getsockname()[1]
        self.messages = []
        self.error = None
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        try:
            conn, _ = self.server.accept()
            with conn:
                conn.settimeout(10)
                validator = ConnectionValidator()
                while True:
                    head = recv_exact(conn, HEADER_SIZE)
                    if len(head) < HEADER_SIZE:
                        break
                    header = decode_header(head)
                    validator.check_header(header)
                    rest = recv_exact(conn, header.body_length + CRC_SIZE)
                    message = decode_message(head + rest)
                    validator.check(message)
                    self.messages.append(message)
        except BaseException as e:
            self.error = e
        finally:
            self.server.close()

    def join(self):
        self.thread.join(timeout=15)
        assert not self.thread.is_alive()
        if self.error is not None:
            raise self.error
        return self.messages


@pytest.mark.parametrize("rate,expected", [
    (160, 3), (125, 2), (1000, 20), (30000, 600), (100000, 1024), (10, 1),
])
def test_default_samples_per_frame(rate, expected):
    assert default_samples_per_frame(rate) == expected


@pytest.mark.parametrize("overrides", [
    {"drop_prob": 1.5},
    {"dup_prob": -0.1},
    {"corrupt_prob": math.nan},
    {"drop_prob": "0.5"},
    {"drop_prob": True},
    {"jitter_ms": -1},
    {"jitter_ms": 1001},
    {"jitter_ms": math.inf},
    {"drift_ppm": 10001},
    {"drift_ppm": -10001},
    {"disconnect_every_s": 0},
    {"disconnect_every_s": -1},
])
def test_fault_config_rejects(overrides):
    with pytest.raises(ValueError):
        FaultConfig(**overrides)


@pytest.mark.parametrize("overrides", [
    {"host": ""},
    {"port": 0},
    {"port": 65536},
    {"port": True},
    {"samples_per_frame": 0},
    {"samples_per_frame": 1025},
    {"duration_s": 0},
    {"duration_s": -1},
    {"duration_s": math.nan},
    {"duration_s": math.inf},
    {"seed": -1},
    {"stream_id": 2**64},
    {"stream_id": -1},
    {"faults": None},
])
def test_device_config_rejects(overrides):
    with pytest.raises(ValueError):
        make_config(**overrides)


def test_stats_frames_property():
    assert Stats(sent=1, dropped=2, overflow=3, disconnected=4, duplicated=9).frames == 10


def test_tcp_connect_sets_options():
    server = socket.create_server(("127.0.0.1", 0))
    sock = tcp_connect("127.0.0.1", server.getsockname()[1])
    try:
        assert sock.gettimeout() == SEND_TIMEOUT_S
        assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY) != 0
    finally:
        sock.close()
        server.close()


def test_bad_device_name_fails_before_output():
    clock = FakeClock()
    connector = FakeConnector(clock, [FakeSocket(clock)])
    err = io.StringIO()
    with pytest.raises(ValueError):
        run_device(small_source(), make_config(device_name=""), clock=clock,
                   connect=connector, err=err)
    assert connector.calls == []
    assert err.getvalue() == ""


ZERO_COUNTERS = ("dropped", "duplicated", "corrupted", "overflow", "disconnected", "late",
                 "max_late_ns", "connect_failures", "forced_disconnects", "send_errors")


def assert_zero_counters(stats):
    for name in ZERO_COUNTERS:
        assert getattr(stats, name) == 0, name


def test_default_run_hello_and_frames():
    stats, clock, sockets, connector, out = run_fake(make_config())
    assert sockets[0].chunks[0] == encode_hello(HELLO)
    assert seqs(sockets) == [0, 1, 2, 3, 4]
    expected = expected_frame_samples(5)
    for m, _ in sent_frames(sockets):
        assert np.array_equal(m.samples, expected[m.seq])
    assert stats.sent == 5
    assert stats.frames == 5
    assert stats.connects == 1
    assert_zero_counters(stats)
    assert stats.bytes_sent == sum(len(c) for c in all_chunks(sockets))
    assert sockets[0].closed


def test_timestamps_follow_sample_clock():
    stats, clock, sockets, connector, out = run_fake(make_config())
    for m, _ in sent_frames(sockets):
        assert m.timestamp_ns == WALL0 + m.seq * 20 * MS
    assert seqs(sockets) == [0, 1, 2, 3, 4]


def test_pacing_absolute_deadlines():
    stats, clock, sockets, connector, out = run_fake(make_config())
    assert send_times(sockets) == [T0 + (k + 1) * 20 * MS for k in range(5)]
    assert stats.late == 0
    assert stats.max_late_ns == 0


def test_late_frames_catch_up():
    clock = FakeClock()
    stats, clock, sockets, connector, out = run_fake(
        make_config(), sockets=[FakeSocket(clock, costs={2: 50 * MS})], clock=clock)
    assert send_times(sockets) == [T0 + t * MS for t in (20, 40, 90, 90, 100)]
    assert stats.late == 2
    assert stats.max_late_ns == 30 * MS
    assert stats.sent == 5


def test_overflow_discards_beyond_one_second():
    clock = FakeClock()
    stats, clock, sockets, connector, out = run_fake(
        make_config(duration_s=1.6), sockets=[FakeSocket(clock, costs={1: 1500 * MS})],
        clock=clock)
    assert seqs(sockets) == [0] + list(range(25, 80))
    assert stats.overflow == 24
    assert stats.sent == 56
    assert stats.late == 50
    assert stats.max_late_ns == 1000 * MS
    assert stats.frames == 80


def test_drift_changes_pacing_not_timestamps():
    stats, clock, sockets, connector, out = run_fake(make_config(drift_ppm=10000))
    frames = sent_frames(sockets)
    assert len(frames) == 5
    for k, (m, t) in enumerate(frames):
        assert m.seq == k
        assert t == T0 + round((k + 1) * (20 * 1e9 / (1000.0 * (1 + 10000 * 1e-6))))
        assert m.timestamp_ns == WALL0 + k * 20 * MS


def test_jitter_is_bounded_and_seeded():
    stats, clock, sockets, connector, out = run_fake(make_config(jitter_ms=5.0))
    u = expected_draws(0, 5)
    assert send_times(sockets) == [
        T0 + (k + 1) * 20 * MS + round(u[k][3] * 5.0 * 1e6) for k in range(5)]
    assert stats.late == 0


def test_drop_all():
    stats, clock, sockets, connector, out = run_fake(make_config(drop_prob=1.0))
    assert sockets[0].chunks == [encode_hello(HELLO)]
    assert stats.dropped == 5
    assert stats.sent == 0
    assert stats.frames == 5


def test_drop_some_matches_draws():
    stats, clock, sockets, connector, out = run_fake(make_config(drop_prob=0.5, duration_s=1.0))
    u = expected_draws(0, 50)
    assert seqs(sockets) == [k for k in range(50) if u[k][0] >= 0.5]
    assert stats.dropped + stats.sent == 50


def test_duplicate_all():
    stats, clock, sockets, connector, out = run_fake(make_config(dup_prob=1.0))
    assert seqs(sockets) == [0, 0, 1, 1, 2, 2, 3, 3, 4, 4]
    for j in range(5):
        assert sockets[0].chunks[1 + 2 * j] == sockets[0].chunks[2 + 2 * j]
    assert stats.duplicated == 5
    assert stats.sent == 5


def test_corrupt_flips_one_payload_byte():
    stats, clock, sockets, connector, out = run_fake(
        make_config(corrupt_prob=1.0, duration_s=0.02))
    good = encode_frame(Frame(0, WALL0, expected_frame_samples(1)[0]))
    bad = sockets[0].chunks[1]
    i = PAYLOAD_OFFSET + int(expected_draws(0, 1)[0][4] * (len(good) - PAYLOAD_OFFSET - CRC_SIZE))
    assert [j for j in range(len(good)) if good[j] != bad[j]] == [i]
    assert bad[i] == good[i] ^ 0xFF
    with pytest.raises(ChecksumError):
        decode_message(bad)
    assert stats.corrupted == 1


def test_faults_reproducible_and_independent():
    a = run_fake(make_config(drop_prob=0.3, seed=7, duration_s=1.0))
    b = run_fake(make_config(drop_prob=0.3, seed=7, duration_s=1.0))
    assert all_chunks(a[2]) == all_chunks(b[2])
    c = run_fake(make_config(drop_prob=0.3, dup_prob=0.3, seed=7, duration_s=1.0))
    dropped_a = set(range(50)) - set(seqs(a[2]))
    dropped_c = set(range(50)) - set(seqs(c[2]))
    assert dropped_c == dropped_a


def test_receiver_down_backoff():
    clock = FakeClock()
    refused = [ConnectionRefusedError(), ConnectionRefusedError(), ConnectionRefusedError()]
    stats, clock, sockets, connector, out = run_fake(
        make_config(duration_s=0.8), sockets=refused + [FakeSocket(clock)], clock=clock)
    assert [c[2] for c in connector.calls] == [T0 + t * MS for t in (20, 120, 320, 720)]
    assert seqs(sockets) == [35, 36, 37, 38, 39]
    assert stats.connect_failures == 3
    assert stats.connects == 1
    assert stats.disconnected == 35
    assert stats.sent == 5


def test_send_error_on_young_connection_backs_off():
    clock = FakeClock()
    stats, clock, sockets, connector, out = run_fake(
        make_config(duration_s=0.2),
        sockets=[FakeSocket(clock, fail_on=3), FakeSocket(clock)], clock=clock)
    assert decode_message(sockets[0].chunks[0]) == HELLO
    assert seqs([sockets[0]]) == [0, 1]
    assert sockets[0].closed
    assert [c[2] for c in connector.calls] == [T0 + 20 * MS, T0 + 160 * MS]
    assert sockets[1].chunks[0] == encode_hello(HELLO)
    assert seqs([sockets[1]]) == [7, 8, 9]
    assert stats.send_errors == 1
    assert stats.disconnected == 5
    assert stats.connects == 2
    assert stats.sent == 5


def test_send_error_on_stable_connection_retries_next_frame():
    clock = FakeClock()
    stats, clock, sockets, connector, out = run_fake(
        make_config(duration_s=1.2),
        sockets=[FakeSocket(clock, fail_on=52), FakeSocket(clock)], clock=clock)
    assert [c[2] for c in connector.calls] == [T0 + 20 * MS, T0 + 1060 * MS]
    assert seqs([sockets[0]]) == list(range(51))
    assert seqs([sockets[1]]) == list(range(52, 60))
    assert stats.send_errors == 1
    assert stats.disconnected == 1
    assert stats.sent == 59


def test_flapping_receiver_backs_off():
    clock = FakeClock()
    stats, clock, sockets, connector, out = run_fake(
        make_config(duration_s=1.0),
        sockets=[FakeSocket(clock, fail_on=1) for _ in range(10)], clock=clock)
    assert [c[2] for c in connector.calls] == [T0 + t * MS for t in (20, 120, 320, 720)]
    assert stats.connects == 4
    assert stats.send_errors == 4
    assert stats.sent == 0
    assert stats.disconnected == 50


def test_disconnect_every():
    clock = FakeClock()
    stats, clock, sockets, connector, out = run_fake(
        make_config(disconnect_every_s=0.05, duration_s=0.2),
        sockets=[FakeSocket(clock) for _ in range(3)], clock=clock)
    assert [seqs([s]) for s in sockets] == [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]]
    for s in sockets:
        assert s.chunks[0] == encode_hello(HELLO)
    assert stats.forced_disconnects == 2
    assert stats.connects == 3
    assert stats.sent == 10
    assert stats.disconnected == 0


def test_disconnect_every_fires_without_sends():
    clock = FakeClock()
    stats, clock, sockets, connector, out = run_fake(
        make_config(drop_prob=1.0, disconnect_every_s=0.05, duration_s=0.2),
        sockets=[FakeSocket(clock) for _ in range(3)], clock=clock)
    assert stats.forced_disconnects == 2
    assert stats.connects == 3
    assert stats.dropped == 10


def test_keyboard_interrupt_is_graceful():
    clock = FakeClock(interrupt_on_sleep=3)
    stats, clock, sockets, connector, out = run_fake(
        make_config(duration_s=None), sockets=[FakeSocket(clock)], clock=clock)
    assert seqs(sockets) == [0, 1]
    assert stats.frames == 2
    assert sockets[0].closed
    assert out.splitlines()[-1].startswith("summary: frames=2 ")


def test_unexpected_error_propagates_after_summary():
    clock = FakeClock()
    err = io.StringIO()

    def boom(host, port):
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        run_device(small_source(), make_config(), clock=clock, connect=boom, err=err)
    assert err.getvalue().splitlines()[-1].startswith("summary: frames=0 ")


def test_output_lines():
    stats, clock, sockets, connector, out = run_fake(make_config(duration_s=2.5))
    H = len(encode_hello(HELLO))
    F = 356
    assert out.splitlines() == [
        "start: device=test-device channels=4 rate_hz=1000 samples_per_frame=20 stream_id=0123456789abcdef target=127.0.0.1:7700 seed=0",
        f"t=1.0s sent=50 drop=0 dup=0 corrupt=0 overflow=0 disc=0 late=0 max_late_ms=0.0 connects=1 MB/s={(H + 50 * F) / 1e6:.2f}",
        f"t=2.0s sent=50 drop=0 dup=0 corrupt=0 overflow=0 disc=0 late=0 max_late_ms=0.0 connects=0 MB/s={50 * F / 1e6:.2f}",
        f"summary: frames=125 sent=125 drop=0 dup=0 corrupt=0 overflow=0 disc=0 late=0 max_late_ms=0.0 connects=1 connect_failures=0 forced_disconnects=0 send_errors=0 bytes={H + 125 * F} elapsed_s=2.500 MB/s={(H + 125 * F) / 2.5 / 1e6:.2f}",
    ]


def test_counter_invariant_under_all_faults():
    clock = FakeClock()
    stats, clock, sockets, connector, out = run_fake(
        make_config(drop_prob=0.2, dup_prob=0.2, corrupt_prob=0.2, jitter_ms=3.0,
                    drift_ppm=500, disconnect_every_s=0.3, duration_s=2.0),
        sockets=[FakeSocket(clock) for _ in range(20)], clock=clock)
    assert stats.frames == stats.sent + stats.dropped + stats.overflow + stats.disconnected == 100


def test_end_to_end_real_socket():
    rx = OneShotReceiver()
    run_device(small_source(), make_config(port=rx.port, duration_s=0.2),
               clock=FakeClock(), err=io.StringIO())
    msgs = rx.join()
    assert msgs[0] == HELLO
    assert [m.seq for m in msgs[1:]] == list(range(10))
    expected = expected_frame_samples(10)
    for m in msgs[1:]:
        assert np.array_equal(m.samples, expected[m.seq])


def test_cli_synthetic(capsys):
    rx = OneShotReceiver()
    assert main(["synthetic", "--channels", "2", "--rate", "200", "--duration", "0.1",
                 "--port", str(rx.port), "--stream-id", "00000000000000ff"]) == 0
    msgs = rx.join()
    assert msgs[0].stream_id == 255
    assert msgs[0].device_name == "eegpipe-synthetic"
    assert msgs[0].n_channels == 2
    assert msgs[0].samples_per_frame == 4
    assert msgs[0].sample_rate_hz == 200.0
    assert [m.seq for m in msgs[1:]] == [0, 1, 2, 3, 4]
    assert "summary: frames=5 sent=5 " in capsys.readouterr().err


def test_cli_replay(tmp_path):
    path = write_edf(tmp_path / "r.edf", ["Fc5.", "C3.."], ["uV", "mV"], [160, 160])
    rx = OneShotReceiver()
    assert main(["replay", "--file", path, "--duration", "0.1", "--port", str(rx.port)]) == 0
    msgs = rx.join()
    assert msgs[0].device_name == "eegpipe-replay"
    assert msgs[0].channel_names == ("Fc5", "C3")
    assert msgs[0].sample_rate_hz == 160.0
    assert msgs[0].samples_per_frame == 3
    assert [m.seq for m in msgs[1:]] == [0, 1, 2, 3, 4]


@pytest.mark.parametrize("argv", [
    [],
    ["synthetic", "--drop-prob", "2"],
    ["synthetic", "--rate", "199"],
    ["synthetic", "--channels", "0"],
    ["synthetic", "--stream-id", "XYZ"],
    ["synthetic", "--stream-id", "00000000000000FF"],
    ["synthetic", "--port", "0"],
    ["synthetic", "--duration", "nan"],
    ["synthetic", "--drift-ppm", "20000"],
    ["synthetic", "--jitter-ms", "1001"],
    ["replay"],
])
def test_cli_rejects_bad_arguments(argv):
    with pytest.raises(SystemExit) as e:
        main(argv)
    assert e.value.code == 2


def test_cli_replay_bad_file(tmp_path, capsys):
    assert main(["replay", "--file", str(tmp_path / "missing.edf")]) == 2
    assert capsys.readouterr().err.startswith("eegpipe.device: error: ")


def test_cli_interrupt_during_startup(monkeypatch, capsys):
    def f(*args):
        raise KeyboardInterrupt

    monkeypatch.setattr("eegpipe.device.make_synthetic", f)
    assert main(["synthetic"]) == 0
    assert "eegpipe.device: interrupted" in capsys.readouterr().err


def test_module_help():
    r = subprocess.run([sys.executable, "-m", "eegpipe.device", "--help"],
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0
    assert "synthetic" in r.stdout
    assert "replay" in r.stdout
