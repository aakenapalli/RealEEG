import json
import pathlib

import numpy as np
import pytest

from eegpipe.protocol import Frame, Hello
# _lat and _lat_max are private, but they are the reference for the printed latency format.
from eegpipe.receiver import HelloMismatchError, StreamTracker, _lat, _lat_max

SCENARIO_DIR = pathlib.Path(__file__).resolve().parents[2] / "testdata" / "streams"
SCENARIOS = sorted(SCENARIO_DIR.glob("*.json"))
DRIFT_TOLERANCE_PPM = 0.01


def test_scenario_files_present():
    assert len(SCENARIOS) == 14


@pytest.mark.parametrize("path", SCENARIOS, ids=lambda p: p.stem)
def test_scenario(path):
    scenario = json.loads(path.read_text())
    tracker = StreamTracker()
    for event in scenario["events"]:
        if "hello" in event:
            h = event["hello"]
            hello = Hello(int(h["stream_id"], 16), h["device_name"], h["n_channels"],
                          h["samples_per_frame"], float(h["sample_rate_hz"]), tuple(h["channel_names"]))
            try:
                tracker.on_hello(hello)
                result = "ok"
            except HelloMismatchError:
                result = "HelloMismatchError"
        else:
            f = event["frame"]
            sid = int(f["stream_id"], 16)
            first = tracker.streams[sid].hello
            samples = np.zeros((first.n_channels, first.samples_per_frame), dtype="<f4")
            result = tracker.on_frame(sid, Frame(f["seq"], f["timestamp_ns"], samples), f["arrival_ns"])
        assert result == event["expect"], event
    assert len(tracker.streams) == len(scenario["streams"])
    for exp in scenario["streams"]:
        st = tracker.streams[int(exp["stream_id"], 16)]
        assert st.initial_seq == exp["initial_seq"]
        assert st.last_seq == exp["last_seq"]
        assert st.accepted == exp["accepted"]
        assert st.missing == exp["missing"]
        assert st.duplicates == exp["dup"]
        assert st.stale == exp["stale"]
        if exp["drift_ppm"] is None:
            assert st.drift_ppm() is None
        else:
            assert abs(st.drift_ppm() - exp["drift_ppm"]) <= DRIFT_TOLERANCE_PPM
        assert _lat(st.latencies_ns, 0.5) == exp["lat_p50_ms"]
        assert _lat(st.latencies_ns, 0.99) == exp["lat_p99_ms"]
        assert _lat_max(st.latencies_ns) == exp["lat_max_ms"]
