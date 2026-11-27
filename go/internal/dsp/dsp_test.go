package dsp

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func loadGolden(t testing.TB, name string, v any) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "testdata", "dsp", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}

func near(a, b, abs, rel float64) bool {
	return math.Abs(a-b) <= abs+rel*math.Abs(b)
}

func chainInput(c, i int, fs float64) float32 {
	twoPi := 2 * math.Pi
	t := float64(i) / fs
	return float32(20*math.Sin(twoPi*10*t+float64(c)) + 10*math.Sin(twoPi*float64(14+c)*t) + 5*math.Sin(twoPi*60*t) + float64(c+1)*float64(i%7-3))
}

func chainFrame(nCh, spf int, fs float64, seq int) []float32 {
	in := make([]float32, nCh*spf)
	for c := 0; c < nCh; c++ {
		for j := 0; j < spf; j++ {
			in[c*spf+j] = chainInput(c, seq*spf+j, fs)
		}
	}
	return in
}

// expectPanic runs f and checks that it panics with the message want.
func expectPanic(t *testing.T, want string, f func()) {
	t.Helper()
	defer func() {
		t.Helper()
		r := recover()
		if r == nil {
			t.Errorf("no panic, want %q", want)
			return
		}
		if got := fmt.Sprint(r); got != want {
			t.Errorf("panic %q, want %q", got, want)
		}
	}()
	f()
}

func TestDecimationGolden(t *testing.T) {
	var cases []struct {
		SampleRateHz    float64 `json:"sample_rate_hz"`
		SamplesPerFrame int     `json:"samples_per_frame"`
		Decimation      int     `json:"decimation"`
		OutputRateHz    float64 `json:"output_rate_hz"`
		Processable     bool    `json:"processable"`
	}
	loadGolden(t, "decimation.json", &cases)
	if len(cases) == 0 {
		t.Fatal("no cases")
	}
	for _, c := range cases {
		if got := DecimationFactor(c.SampleRateHz, c.SamplesPerFrame); got != c.Decimation {
			t.Errorf("DecimationFactor(%g, %d) = %d, want %d", c.SampleRateHz, c.SamplesPerFrame, got, c.Decimation)
		}
		if got := OutputRate(c.SampleRateHz, c.SamplesPerFrame); got != c.OutputRateHz {
			t.Errorf("OutputRate(%g, %d) = %v, want %v", c.SampleRateHz, c.SamplesPerFrame, got, c.OutputRateHz)
		}
		if got := Processable(c.SampleRateHz, c.SamplesPerFrame); got != c.Processable {
			t.Errorf("Processable(%g, %d) = %v, want %v", c.SampleRateHz, c.SamplesPerFrame, got, c.Processable)
		}
	}
}

func TestCoefficientsGolden(t *testing.T) {
	var cases []struct {
		Kind     string       `json:"kind"`
		Order    int          `json:"order"`
		FreqHz   float64      `json:"freq_hz"`
		RateHz   float64      `json:"rate_hz"`
		Sections [][5]float64 `json:"sections"`
	}
	loadGolden(t, "coefficients.json", &cases)
	if len(cases) == 0 {
		t.Fatal("no cases")
	}
	for _, c := range cases {
		var got []Section
		switch c.Kind {
		case "lowpass":
			got = ButterLowpass(c.Order, c.FreqHz, c.RateHz)
		case "highpass":
			got = ButterHighpass(c.Order, c.FreqHz, c.RateHz)
		case "notch":
			got = []Section{Notch(c.FreqHz, NotchQ, c.RateHz)}
		default:
			t.Fatalf("unknown kind %q", c.Kind)
		}
		if len(got) != len(c.Sections) {
			t.Errorf("%s %g/%g: %d sections, want %d", c.Kind, c.FreqHz, c.RateHz, len(got), len(c.Sections))
			continue
		}
		for i, s := range got {
			v := [5]float64{s.B0, s.B1, s.B2, s.A1, s.A2}
			for k := range v {
				if !near(v[k], c.Sections[i][k], 1e-15, 1e-12) {
					t.Errorf("%s %g/%g section %d value %d: %v, want %v", c.Kind, c.FreqHz, c.RateHz, i, k, v[k], c.Sections[i][k])
				}
			}
		}
	}
}

type bandCase struct {
	OutputRateHz float64      `json:"output_rate_hz"`
	Bins         [5][]int     `json:"bins"`
	Window       [][]float64  `json:"window"`
	Powers       [][5]float64 `json:"powers"`
}

func TestBandBinsGolden(t *testing.T) {
	var cases []bandCase
	loadGolden(t, "bandpower.json", &cases)
	if len(cases) == 0 {
		t.Fatal("no cases")
	}
	for _, c := range cases {
		got := BandBins(c.OutputRateHz, WindowSize(c.OutputRateHz))
		for b := 0; b < NumBands; b++ {
			if fmt.Sprint(got[b]) != fmt.Sprint(c.Bins[b]) {
				t.Errorf("rate %g band %d: bins %v, want %v", c.OutputRateHz, b, got[b], c.Bins[b])
			}
		}
	}
}

func TestBandPowerGolden(t *testing.T) {
	var cases []bandCase
	loadGolden(t, "bandpower.json", &cases)
	if len(cases) == 0 {
		t.Fatal("no cases")
	}
	for _, c := range cases {
		n := len(c.Window[0])
		bp := NewBandPower(c.OutputRateHz, n)
		scratch := make([]float64, n)
		for ch, row := range c.Window {
			var dst [NumBands]float64
			bp.Compute(row, 0, scratch, &dst)
			for b := 0; b < NumBands; b++ {
				if !near(dst[b], c.Powers[ch][b], 1e-12, 1e-9) {
					t.Errorf("rate %g channel %d band %d: %v, want %v", c.OutputRateHz, ch, b, dst[b], c.Powers[ch][b])
				}
			}
		}
	}
}

func TestChainGolden(t *testing.T) {
	var cases []struct {
		SampleRateHz    float64 `json:"sample_rate_hz"`
		SamplesPerFrame int     `json:"samples_per_frame"`
		NChannels       int     `json:"n_channels"`
		MainsHz         float64 `json:"mains_hz"`
		NFrames         int     `json:"n_frames"`
		GapSeq          *int    `json:"gap_seq"`
		Frames          []struct {
			Seq      int            `json:"seq"`
			Output   [][]float64    `json:"output"`
			Features [][][5]float64 `json:"features"`
		} `json:"frames"`
	}
	loadGolden(t, "chain.json", &cases)
	if len(cases) == 0 {
		t.Fatal("no cases")
	}
	for ci, c := range cases {
		p := NewProcessor(c.SampleRateHz, c.SamplesPerFrame, c.NChannels, c.MainsHz)
		C, S, OP := c.NChannels, c.SamplesPerFrame, p.OutPerFrame()
		next := 0
		for seq := 0; seq < c.NFrames; seq++ {
			if c.GapSeq != nil && seq == *c.GapSeq {
				p.Reset()
				continue
			}
			if next >= len(c.Frames) {
				t.Fatalf("case %d: more processed frames than the file's %d", ci, len(c.Frames))
			}
			want := c.Frames[next]
			next++
			if want.Seq != seq {
				t.Fatalf("case %d: file frame seq %d, processed seq %d", ci, want.Seq, seq)
			}
			in := chainFrame(C, S, c.SampleRateHz, seq)
			out := make([]float32, C*OP)
			due := p.BeginFrame()
			if len(due) != len(want.Features) {
				t.Fatalf("case %d seq %d: %d features, want %d", ci, seq, len(due), len(want.Features))
			}
			feats := make([][]float32, len(due))
			for f := range feats {
				feats[f] = make([]float32, C*NumBands)
			}
			p.Channels(in, out, feats, due, 0, C)
			for ch := 0; ch < C; ch++ {
				for j := 0; j < OP; j++ {
					if !near(float64(out[ch*OP+j]), want.Output[ch][j], 1e-4, 1e-5) {
						t.Errorf("case %d seq %d out[%d][%d] = %v, want %v", ci, seq, ch, j, out[ch*OP+j], want.Output[ch][j])
					}
				}
			}
			for f := range feats {
				for ch := 0; ch < C; ch++ {
					for b := 0; b < NumBands; b++ {
						if !near(float64(feats[f][ch*NumBands+b]), want.Features[f][ch][b], 1e-3, 1e-5) {
							t.Errorf("case %d seq %d feat[%d][%d][%d] = %v, want %v", ci, seq, f, ch, b, feats[f][ch*NumBands+b], want.Features[f][ch][b])
						}
					}
				}
			}
		}
		if next != len(c.Frames) {
			t.Errorf("case %d: processed %d frames, file has %d", ci, next, len(c.Frames))
		}
	}
}

func TestLowpassCutoff(t *testing.T) {
	for _, fs := range []float64{30000, 1000} {
		s := ButterLowpass(8, 90, fs)
		if g := Gain(s, 90, fs); math.Abs(g-1/math.Sqrt2) > 1e-6 {
			t.Errorf("fs %g: gain at 90 Hz = %v", fs, g)
		}
		if g := Gain(s, 0, fs); math.Abs(g-1) > 1e-9 {
			t.Errorf("fs %g: gain at 0 Hz = %v", fs, g)
		}
	}
	if g := Gain(ButterLowpass(8, 90, 30000), 180, 30000); g >= math.Pow(10, -47.0/20) {
		t.Errorf("gain at 180 Hz = %v", g)
	}
}

func TestHighpassCutoff(t *testing.T) {
	s := ButterHighpass(2, 1, 250)
	if g := Gain(s, 1, 250); math.Abs(g-1/math.Sqrt2) > 1e-6 {
		t.Errorf("gain at 1 Hz = %v", g)
	}
	if g := Gain(s, 0, 250); g >= 1e-9 {
		t.Errorf("gain at 0 Hz = %v", g)
	}
	if g := Gain(s, 10, 250); math.Abs(g-1) > 1e-4 {
		t.Errorf("gain at 10 Hz = %v", g)
	}
}

func TestNotchDepth(t *testing.T) {
	for _, m := range []float64{50, 60} {
		s := []Section{Notch(m, 30, 250)}
		if g := Gain(s, m, 250); g >= 0.01 {
			t.Errorf("mains %g: gain at mains = %v", m, g)
		}
		if g := Gain(s, 10, 250); math.Abs(g-1) > 1e-3 {
			t.Errorf("mains %g: gain at 10 Hz = %v", m, g)
		}
	}
}

func TestBandPowerSinusoids(t *testing.T) {
	hist := make([]float64, 250)
	for j := range hist {
		t := float64(j) / 250
		hist[j] = 20*math.Sin(2*math.Pi*10*t+0.3) + 10*math.Sin(2*math.Pi*20*t)
	}
	var dst [NumBands]float64
	NewBandPower(250, 250).Compute(hist, 0, make([]float64, 250), &dst)
	if math.Abs(dst[2]-200) >= 0.01 {
		t.Errorf("alpha = %v", dst[2])
	}
	if math.Abs(dst[3]-50) >= 0.01 {
		t.Errorf("beta = %v", dst[3])
	}
	for _, b := range []int{0, 1, 4} {
		if dst[b] >= 1e-9 {
			t.Errorf("band %d = %v", b, dst[b])
		}
	}
}

func TestProcessorSyntheticBands(t *testing.T) {
	p := NewProcessor(1000, 20, 1, 60)
	out := make([]float32, p.OutPerFrame())
	var last []float32
	for seq := 0; seq < 150; seq++ {
		in := make([]float32, 20)
		for i := range in {
			t := float64(seq*20+i) / 1000
			in[i] = float32(20*math.Sin(2*math.Pi*10*t) + 10*math.Sin(2*math.Pi*20*t) + 5*math.Sin(2*math.Pi*60*t))
		}
		due := p.BeginFrame()
		feats := make([][]float32, len(due))
		for f := range feats {
			feats[f] = make([]float32, NumBands)
		}
		p.Channels(in, out, feats, due, 0, 1)
		if len(feats) > 0 {
			last = feats[len(feats)-1]
		}
	}
	if last == nil {
		t.Fatal("no feature")
	}
	if math.Abs(float64(last[2])-200)/200 >= 0.02 {
		t.Errorf("alpha = %v", last[2])
	}
	if math.Abs(float64(last[3])-50)/50 >= 0.02 {
		t.Errorf("beta = %v", last[3])
	}
	if last[4] >= 0.5 {
		t.Errorf("gamma = %v", last[4])
	}
}

func TestProcessorFeatureTiming(t *testing.T) {
	p := NewProcessor(1000, 20, 2, 60)
	if p.Decimation() != 4 || p.OutRate() != 250 || p.OutPerFrame() != 5 {
		t.Fatalf("D %d R %v OP %d", p.Decimation(), p.OutRate(), p.OutPerFrame())
	}
	in := make([]float32, 40)
	out := make([]float32, 10)
	total, firstFrame, firstPos := 0, -1, -1
	for seq := 0; seq < 100; seq++ {
		due := p.BeginFrame()
		feats := make([][]float32, len(due))
		for f := range feats {
			feats[f] = make([]float32, 10)
		}
		if len(due) > 0 && firstFrame < 0 {
			firstFrame, firstPos = seq, due[0]
		}
		total += len(due)
		p.Channels(in, out, feats, due, 0, 2)
	}
	if total != 5 || firstFrame != 49 || firstPos != 4 {
		t.Errorf("total %d first frame %d position %d", total, firstFrame, firstPos)
	}
}

func TestProcessorReset(t *testing.T) {
	p := NewProcessor(1000, 20, 1, 60)
	in := make([]float32, 20)
	out := make([]float32, 5)
	run := func(n int) int {
		total := 0
		for i := 0; i < n; i++ {
			due := p.BeginFrame()
			feats := make([][]float32, len(due))
			for f := range feats {
				feats[f] = make([]float32, NumBands)
			}
			p.Channels(in, out, feats, due, 0, 1)
			total += len(due)
		}
		return total
	}
	run(49)
	p.Reset()
	if n := run(49); n != 0 {
		t.Errorf("%d features after reset", n)
	}
}

func TestChannelsSplitMatchesWhole(t *testing.T) {
	a := NewProcessor(1000, 20, 3, 60)
	b := NewProcessor(1000, 20, 3, 60)
	for seq := 0; seq < 100; seq++ {
		in := chainFrame(3, 20, 1000, seq)
		oa, ob := make([]float32, 15), make([]float32, 15)
		da, db := a.BeginFrame(), b.BeginFrame()
		if len(da) != len(db) {
			t.Fatalf("seq %d: due differs", seq)
		}
		fa, fb := make([][]float32, len(da)), make([][]float32, len(db))
		for f := range fa {
			fa[f], fb[f] = make([]float32, 15), make([]float32, 15)
		}
		a.Channels(in, oa, fa, da, 0, 3)
		b.Channels(in, ob, fb, db, 0, 1)
		b.Channels(in, ob, fb, db, 1, 3)
		for i := range oa {
			if math.Float32bits(oa[i]) != math.Float32bits(ob[i]) {
				t.Fatalf("seq %d out[%d]: %v vs %v", seq, i, oa[i], ob[i])
			}
		}
		for f := range fa {
			for i := range fa[f] {
				if math.Float32bits(fa[f][i]) != math.Float32bits(fb[f][i]) {
					t.Fatalf("seq %d feat[%d][%d]: %v vs %v", seq, f, i, fa[f][i], fb[f][i])
				}
			}
		}
	}
}

func TestPanics(t *testing.T) {
	expectPanic(t, "dsp: NewProcessor: stream is not processable", func() { NewProcessor(100, 2, 1, 60) })
	expectPanic(t, "dsp: Butterworth order must be even and >= 2", func() { ButterLowpass(3, 1, 100) })
	expectPanic(t, "dsp: Channels: bad buffer sizes", func() {
		p := NewProcessor(1000, 20, 1, 60)
		p.Channels(make([]float32, 20), make([]float32, 4), nil, nil, 0, 1)
	})
	expectPanic(t, "dsp: Compute: hist has 10 samples, want 250", func() {
		var dst [NumBands]float64
		NewBandPower(250, 250).Compute(make([]float64, 10), 0, make([]float64, 250), &dst)
	})
}

func TestStateBytes(t *testing.T) {
	for _, c := range []struct {
		ch   int
		rate float64
		want int64
	}{{2, 200, 9024}, {1024, 250, 2578288}, {2, 250, 11024}} {
		if got := StateBytes(c.ch, c.rate); got != c.want {
			t.Errorf("StateBytes(%d, %g) = %d, want %d", c.ch, c.rate, got, c.want)
		}
	}
}

func newBenchProcessor() (*Processor, []float32, []float32) {
	const C, S = 1024, 600
	p := NewProcessor(30000, S, C, 60)
	return p, chainFrame(C, S, 30000, 0), make([]float32, C*p.OutPerFrame())
}

func BenchmarkProcessorFullScale(b *testing.B) {
	p, in, out := newBenchProcessor()
	b.SetBytes(1024 * 600 * 4)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		due := p.BeginFrame()
		feats := make([][]float32, len(due))
		for f := range feats {
			feats[f] = make([]float32, 1024*NumBands)
		}
		p.Channels(in, out, feats, due, 0, 1024)
	}
}

func BenchmarkProcessorFeatureFrame(b *testing.B) {
	p, in, out := newBenchProcessor()
	for i := 0; i < 49; i++ {
		due := p.BeginFrame()
		feats := make([][]float32, len(due))
		for f := range feats {
			feats[f] = make([]float32, 1024*NumBands)
		}
		p.Channels(in, out, feats, due, 0, 1024)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var due []int
		for {
			due = p.BeginFrame()
			if len(due) > 0 {
				break
			}
			p.Channels(in, out, nil, due, 0, 1024)
		}
		feats := make([][]float32, len(due))
		for f := range feats {
			feats[f] = make([]float32, 1024*NumBands)
		}
		b.StartTimer()
		p.Channels(in, out, feats, due, 0, 1024)
	}
}
