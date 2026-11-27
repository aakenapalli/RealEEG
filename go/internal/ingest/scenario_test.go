package ingest

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aakenapalli/RealEEG/go/internal/protocol"
)

type scenarioHello struct {
	StreamID        string   `json:"stream_id"`
	DeviceName      string   `json:"device_name"`
	NChannels       int      `json:"n_channels"`
	SamplesPerFrame int      `json:"samples_per_frame"`
	SampleRateHz    float64  `json:"sample_rate_hz"`
	ChannelNames    []string `json:"channel_names"`
}

type scenarioFrame struct {
	StreamID    string `json:"stream_id"`
	Seq         uint64 `json:"seq"`
	TimestampNs int64  `json:"timestamp_ns"`
	ArrivalNs   int64  `json:"arrival_ns"`
}

type scenarioEvent struct {
	Hello  *scenarioHello `json:"hello"`
	Frame  *scenarioFrame `json:"frame"`
	Expect string         `json:"expect"`
}

type scenarioStream struct {
	StreamID   string   `json:"stream_id"`
	InitialSeq *uint64  `json:"initial_seq"`
	LastSeq    *uint64  `json:"last_seq"`
	Accepted   uint64   `json:"accepted"`
	Missing    uint64   `json:"missing"`
	Dup        uint64   `json:"dup"`
	Stale      uint64   `json:"stale"`
	DriftPPM   *float64 `json:"drift_ppm"`
	LatP50     string   `json:"lat_p50_ms"`
	LatP99     string   `json:"lat_p99_ms"`
	LatMax     string   `json:"lat_max_ms"`
}

type scenario struct {
	Description string           `json:"description"`
	Events      []scenarioEvent  `json:"events"`
	Streams     []scenarioStream `json:"streams"`
}

const scenarioGlob = "../../../testdata/streams/*.json"

func TestScenarioFilesPresent(t *testing.T) {
	files, err := filepath.Glob(scenarioGlob)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 14 {
		t.Fatalf("%d scenario files, want 14", len(files))
	}
}

func parseID(t *testing.T, s string) uint64 {
	t.Helper()
	id, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestScenarios(t *testing.T) {
	files, err := filepath.Glob(scenarioGlob)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range files {
		name := strings.TrimSuffix(filepath.Base(path), ".json")
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var sc scenario
			if err := json.Unmarshal(raw, &sc); err != nil {
				t.Fatal(err)
			}
			reg := NewRegistry(RegistryConfig{RingSeconds: 2, RingBudgetBytes: 1 << 30, EvictAfter: 60 * time.Second})
			conn := map[uint64]uint64{}
			for i, ev := range sc.Events {
				if ev.Hello != nil {
					h := protocol.Hello{
						StreamID:        parseID(t, ev.Hello.StreamID),
						DeviceName:      ev.Hello.DeviceName,
						NChannels:       ev.Hello.NChannels,
						SamplesPerFrame: ev.Hello.SamplesPerFrame,
						SampleRateHz:    ev.Hello.SampleRateHz,
						ChannelNames:    ev.Hello.ChannelNames,
					}
					id := uint64(i + 1)
					_, err := reg.Attach(h, id)
					var be *BusyError
					if errors.As(err, &be) {
						old, _ := reg.Lookup(h.StreamID)
						reg.Detach(old, be.ConnID)
						_, err = reg.Attach(h, id)
						if errors.As(err, &be) {
							t.Fatalf("event %d: still busy", i)
						}
					}
					result := "ok"
					if err != nil {
						var re *RejectError
						if !errors.As(err, &re) {
							t.Fatalf("event %d: %v", i, err)
						}
						result = re.Name
					} else {
						conn[h.StreamID] = id
					}
					if result != ev.Expect {
						t.Fatalf("event %d: hello result %q, want %q", i, result, ev.Expect)
					}
					continue
				}
				f := ev.Frame
				sid := parseID(t, f.StreamID)
				s, ok := reg.Lookup(sid)
				if !ok {
					t.Fatalf("event %d: unknown stream", i)
				}
				c, spf := s.hello.NChannels, s.hello.SamplesPerFrame
				fr := testFrame(f.Seq, f.TimestampNs, c, spf, 0)
				res := s.OnFrame(conn[sid], fr, f.ArrivalNs)
				if res.String() != ev.Expect {
					t.Fatalf("event %d: frame result %q, want %q", i, res.String(), ev.Expect)
				}
			}
			snaps := reg.Snapshots()
			if len(snaps) != len(sc.Streams) {
				t.Fatalf("%d streams, want %d", len(snaps), len(sc.Streams))
			}
			for _, exp := range sc.Streams {
				s, ok := reg.Lookup(parseID(t, exp.StreamID))
				if !ok {
					t.Fatalf("stream %s missing", exp.StreamID)
				}
				sn := s.Snapshot()
				if sn.HasFrames != (exp.InitialSeq != nil) || sn.HasFrames != (exp.LastSeq != nil) {
					t.Errorf("stream %s: HasFrames %v", exp.StreamID, sn.HasFrames)
				}
				if sn.HasFrames && exp.InitialSeq != nil && exp.LastSeq != nil &&
					(sn.InitialSeq != *exp.InitialSeq || sn.LastSeq != *exp.LastSeq) {
					t.Errorf("stream %s: seqs %d..%d, want %d..%d", exp.StreamID, sn.InitialSeq, sn.LastSeq, *exp.InitialSeq, *exp.LastSeq)
				}
				if sn.Accepted != exp.Accepted || sn.Missing != exp.Missing || sn.Dup != exp.Dup || sn.Stale != exp.Stale {
					t.Errorf("stream %s: counts %d/%d/%d/%d, want %d/%d/%d/%d", exp.StreamID,
						sn.Accepted, sn.Missing, sn.Dup, sn.Stale, exp.Accepted, exp.Missing, exp.Dup, exp.Stale)
				}
				if sn.HasDrift != (exp.DriftPPM != nil) {
					t.Errorf("stream %s: HasDrift %v", exp.StreamID, sn.HasDrift)
				} else if exp.DriftPPM != nil && math.Abs(sn.DriftPPM-*exp.DriftPPM) > 0.01 {
					t.Errorf("stream %s: drift %v, want %v", exp.StreamID, sn.DriftPPM, *exp.DriftPPM)
				}
				if sn.LatP50 != exp.LatP50 || sn.LatP99 != exp.LatP99 || sn.LatMax != exp.LatMax {
					t.Errorf("stream %s: latency %s/%s/%s, want %s/%s/%s", exp.StreamID,
						sn.LatP50, sn.LatP99, sn.LatMax, exp.LatP50, exp.LatP99, exp.LatMax)
				}
			}
		})
	}
}
