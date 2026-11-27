import json
import math
import pathlib

import numpy as np
import pytest

from eegpipe import dsp, dsp_golden

GOLDEN_DIR = pathlib.Path(__file__).resolve().parents[2] / "testdata" / "dsp"


def db(x: float) -> float:
    return 20.0 * math.log10(x)


@pytest.mark.parametrize("name", ["decimation.json", "coefficients.json", "bandpower.json", "chain.json"])
def test_golden_file_matches_reference(name):
    assert json.loads((GOLDEN_DIR / name).read_text()) == json.loads(json.dumps(dsp_golden.build()[name]))


@pytest.mark.parametrize("rate,spf,d", [(30000.0, 600, 120), (1000.0, 20, 4), (160.0, 3, 1),
                                        (2000.0, 40, 8), (1024.0, 20, 4)])
def test_decimation_factor(rate, spf, d):
    assert dsp.decimation_factor(rate, spf) == d


def test_processable_bounds():
    assert dsp.processable(160.0, 3)
    assert not dsp.processable(100.0, 2)
    assert not dsp.processable(30000.0, 7)


@pytest.mark.parametrize("rate", [30000.0, 1000.0])
def test_lowpass_minus_3db_at_cutoff_and_unity_at_dc(rate):
    sections = dsp.butter_lowpass(8, 90.0, rate)
    assert abs(db(dsp.gain(sections, 90.0, rate)) + 3.0103) < 0.001
    assert abs(dsp.gain(sections, 0.0, rate) - 1.0) < 1e-9


def test_lowpass_rolloff_is_8th_order():
    sections = dsp.butter_lowpass(8, 90.0, 30000.0)
    assert db(dsp.gain(sections, 180.0, 30000.0)) < -47.0


def test_highpass_minus_3db_at_1hz():
    sections = dsp.butter_highpass(2, 1.0, 250.0)
    assert abs(db(dsp.gain(sections, 1.0, 250.0)) + 3.0103) < 0.001
    assert dsp.gain(sections, 0.0, 250.0) < 1e-9
    assert abs(dsp.gain(sections, 10.0, 250.0) - 1.0) < 1e-4


@pytest.mark.parametrize("mains", [50.0, 60.0])
def test_notch_removes_mains_and_keeps_alpha(mains):
    sections = dsp.notch(mains, dsp.NOTCH_Q, 250.0)
    assert db(dsp.gain(sections, mains, 250.0) + 1e-300) < -40.0
    assert abs(dsp.gain(sections, 10.0, 250.0) - 1.0) < 1e-3


def test_notch_attenuates_a_filtered_mains_tone_by_40db():
    n = 2500
    x = 5.0 * np.sin(2 * math.pi * 60.0 * np.arange(n) / 250.0)
    y = dsp.Cascade(dsp.notch(60.0, dsp.NOTCH_Q, 250.0), 1).run(x[None, :])[0]
    assert np.max(np.abs(y[-250:])) < 5.0 * 0.01


def test_band_power_of_known_sinusoids():
    rate = 250.0
    n = dsp.window_size(rate)
    t = np.arange(n) / rate
    x = 20.0 * np.sin(2 * math.pi * 10 * t + 0.3) + 10.0 * np.sin(2 * math.pi * 20 * t)
    p = dsp.band_powers(x[None, :], rate)[0]
    assert abs(p[2] - 200.0) < 0.01          # alpha: A^2 / 2
    assert abs(p[3] - 50.0) < 0.01           # beta
    assert p[0] < 1e-9 and p[1] < 1e-9 and p[4] < 1e-9


def test_processed_synthetic_signal_has_expected_band_powers():
    rate, spf = 1000.0, 20
    p = dsp.Processor(rate, spf, 1, 60.0)
    feats = []
    for seq in range(150):
        t = (seq * spf + np.arange(spf)) / rate
        x = (20.0 * np.sin(2 * math.pi * 10 * t) + 10.0 * np.sin(2 * math.pi * 20 * t)
             + 5.0 * np.sin(2 * math.pi * 60 * t))
        _, f = p.frame(x[None, :].astype(np.float32))
        feats += f
    last = feats[-1][0]
    assert abs(last[2] - 200.0) / 200.0 < 0.02
    assert abs(last[3] - 50.0) / 50.0 < 0.02
    assert last[4] < 0.5


def test_processor_output_shape_and_feature_timing():
    p = dsp.Processor(1000.0, 20, 2, 60.0)
    counts = []
    for _ in range(100):
        out, f = p.frame(np.zeros((2, 20), dtype=np.float32))
        assert out.shape == (2, 5) and out.dtype == np.float32
        counts.append(len(f))
    assert sum(counts) == 5                 # at output samples 250, 312, 374, 436, 498
    assert counts.index(1) == 49            # sample 250 is the last sample of frame 49


def test_reset_restarts_the_feature_window():
    p = dsp.Processor(1000.0, 20, 1, 60.0)
    for _ in range(49):
        p.frame(np.zeros((1, 20), dtype=np.float32))
    p.reset()
    assert sum(len(p.frame(np.zeros((1, 20), dtype=np.float32))[1]) for _ in range(49)) == 0


def test_unprocessable_stream_is_rejected():
    with pytest.raises(ValueError):
        dsp.Processor(100.0, 2, 1, 60.0)
