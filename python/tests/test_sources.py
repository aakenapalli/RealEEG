import numpy as np
import pyedflib
import pytest

from eegpipe.sources import (
    ALPHA_HZ, ALPHA_UV, LINE_HZ, LINE_UV, TONE_BASE_HZ, TONE_COUNT, TONE_UV, UNIT_SCALE,
    CyclicSource, clean_label, load_replay, make_synthetic,
)


def write_edf(path, labels, dims, rates, seconds=2):
    """Write an EDF+ file whose signal i is a 10 Hz sine of amplitude 100 * (i + 1)
    in units dims[i], sampled at rates[i] Hz, lasting `seconds` (an int)."""
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


def edf_expected(path, i, scale):
    """What load_replay should produce for signal i: pyedflib's physical values times scale, as '<f4'."""
    with pyedflib.EdfReader(path) as r:
        return (r.readSignal(i) * scale).astype("<f4")


def grid():
    """2 x 10 buffer whose value at (row, col) is 10 * row + col."""
    return np.arange(20, dtype="<f4").reshape(2, 10)


def test_read_without_wrap():
    src = CyclicSource(grid(), 100.0, ("a", "b"))
    assert np.array_equal(src.read(4), grid()[:, 0:4])
    assert src.position == 4
    assert np.array_equal(src.read(4), grid()[:, 4:8])


def test_read_wraps():
    src = CyclicSource(grid(), 100.0, ("a", "b"))
    src.read(8)
    assert np.array_equal(src.read(4), grid()[:, [8, 9, 0, 1]])
    assert src.position == 2


def test_read_longer_than_period():
    src = CyclicSource(np.arange(3, dtype="<f4").reshape(1, 3), 100.0, ("a",))
    assert np.array_equal(src.read(7), np.array([[0, 1, 2, 0, 1, 2, 0]]))
    assert src.position == 1


def test_read_result_is_readonly_f4():
    src = CyclicSource(grid(), 100.0, ("a", "b"))
    first = src.read(4)
    src.read(4)
    wrapped = src.read(4)
    for out in (first, wrapped):
        assert out.dtype == np.dtype("<f4")
        assert out.shape == (2, 4)
        assert out.flags.writeable is False


def test_constructor_takes_ownership():
    buf = grid()
    src = CyclicSource(buf, 100, ("a", "b"))
    assert buf.flags.writeable is False
    assert src.n_channels == 2
    assert src.sample_rate_hz == 100.0
    assert isinstance(src.sample_rate_hz, float)
    assert src.channel_names == ("a", "b")
    assert src.period == 10
    assert src.position == 0


@pytest.mark.parametrize("buffer,names,exception", [
    ([[1.0]], ("a",), TypeError),
    (np.zeros((1, 3)), ("a",), TypeError),
    (np.zeros(5, "<f4"), ("a",), ValueError),
    (np.zeros((0, 5), "<f4"), (), ValueError),
    (np.zeros((2, 0), "<f4"), ("a", "b"), ValueError),
    (np.zeros((2, 3), "<f4"), ["a", "b"], ValueError),
    (np.zeros((2, 3), "<f4"), ("a",), ValueError),
])
def test_constructor_rejects(buffer, names, exception):
    with pytest.raises(exception):
        CyclicSource(buffer, 100.0, names)


@pytest.mark.parametrize("n,exception", [
    (0, ValueError), (-1, ValueError), (2.0, TypeError), (True, TypeError),
])
def test_read_rejects(n, exception):
    src = CyclicSource(grid(), 100.0, ("a", "b"))
    with pytest.raises(exception):
        src.read(n)


def test_synthetic_shape_names_rate():
    src = make_synthetic(3, 1000, 0)
    assert src.n_channels == 3
    assert src.channel_names == ("ch0000", "ch0001", "ch0002")
    assert src.sample_rate_hz == 1000.0
    assert src.period == 1000


def test_synthetic_deterministic():
    a = make_synthetic(4, 1000, 5).read(1000)
    b = make_synthetic(4, 1000, 5).read(1000)
    c = make_synthetic(4, 1000, 6).read(1000)
    assert np.array_equal(a, b)
    assert not np.array_equal(a, c)


def test_synthetic_spectrum():
    x = make_synthetic(20, 1000, 0).read(1000)
    for c in range(20):
        amp = np.abs(np.fft.rfft(x[c].astype(np.float64))) * 2 / 1000
        tone = TONE_BASE_HZ + c % TONE_COUNT
        assert abs(amp[ALPHA_HZ] - ALPHA_UV) <= 1.0
        assert abs(amp[tone] - TONE_UV) <= 1.0
        assert abs(amp[LINE_HZ] - LINE_UV) <= 1.0
        for i in range(1, 500):
            if i not in (ALPHA_HZ, tone, LINE_HZ):
                assert amp[i] < 2.0


def test_synthetic_periodic_seamless():
    src = make_synthetic(2, 200, 1)
    a = src.read(200).copy()
    b = src.read(200).copy()
    assert np.array_equal(a, b)
    src.read(150)
    assert np.array_equal(src.read(100), np.concatenate([a[:, 150:], a[:, :50]], axis=1))


@pytest.mark.parametrize("args,exception", [
    ((0, 1000, 0), ValueError),
    ((4097, 1000, 0), ValueError),
    ((True, 1000, 0), TypeError),
    ((4, 199, 0), ValueError),
    ((4, 100001, 0), ValueError),
    ((4, 1000.0, 0), TypeError),
    ((4, 1000, -1), ValueError),
    ((4, 1000, 1.5), TypeError),
])
def test_synthetic_rejects(args, exception):
    with pytest.raises(exception):
        make_synthetic(*args)


@pytest.mark.parametrize("raw,expected", [
    ("Fc5.", "Fc5"),
    ("C3..", "C3"),
    ("T10.", "T10"),
    (" Cz ", "Cz"),
    ("Fc5 .", "Fc5"),
    ("EEG Fp1-Ref", "EEG Fp1-Ref"),
    ("...", "ch0007"),
    ("", "ch0007"),
])
def test_clean_label(raw, expected):
    assert clean_label(raw, 7) == expected


@pytest.mark.parametrize("raw", ["Fp1\u00e9", "x" * 33])
def test_clean_label_rejects(raw):
    with pytest.raises(ValueError):
        clean_label(raw, 0)


def test_replay_loads_and_cleans(tmp_path):
    path = write_edf(tmp_path / "r.edf", ["Fc5.", "C3..", "Cz"], ["uV", "uV", "uV"], [160, 160, 160])
    src = load_replay(path)
    assert src.n_channels == 3
    assert src.channel_names == ("Fc5", "C3", "Cz")
    assert src.sample_rate_hz == 160.0
    assert src.period == 320
    out = src.read(320)
    for i in range(3):
        assert np.array_equal(out[i], edf_expected(path, i, 1.0))


@pytest.mark.parametrize("dim,scale", list(UNIT_SCALE.items()))
def test_replay_unit_scale(tmp_path, dim, scale):
    path = write_edf(tmp_path / "u.edf", ["Cz"], [dim], [160])
    assert np.array_equal(load_replay(path).read(320)[0], edf_expected(path, 0, scale))


def test_replay_loops(tmp_path):
    path = write_edf(tmp_path / "r.edf", ["Fc5.", "C3..", "Cz"], ["uV", "uV", "uV"], [160, 160, 160])
    src = load_replay(path)
    a = src.read(320).copy()
    assert np.array_equal(src.read(5), a[:, :5])


def test_replay_rejects_mixed_rates(tmp_path):
    path = write_edf(tmp_path / "m.edf", ["A", "B"], ["uV", "uV"], [160, 80])
    with pytest.raises(ValueError, match="different sample rates"):
        load_replay(path)


def test_replay_rejects_unknown_unit(tmp_path):
    path = write_edf(tmp_path / "t.edf", ["T"], ["degC"], [160])
    with pytest.raises(ValueError, match="unsupported unit"):
        load_replay(path)


def test_replay_missing_file(tmp_path):
    with pytest.raises(OSError):
        load_replay(str(tmp_path / "nope.edf"))
