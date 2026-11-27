package ingest

import (
	"context"
	"math"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aakenapalli/RealEEG/go/internal/dsp"
	"github.com/aakenapalli/RealEEG/go/internal/protocol"
	"github.com/aakenapalli/RealEEG/go/internal/ring"
)

func procHello(id uint64) protocol.Hello { return testHello(id, 2, 20, 1000) }

func writeFrameData(c net.Conn, seq uint64, ts int64, nCh, spf int, samples []float32) error {
	b, err := protocol.EncodeFrame(seq, ts, nCh, spf, samples)
	if err != nil {
		return err
	}
	_, err = c.Write(b)
	return err
}

func testSamples(seq uint64, nCh int) []float32 {
	out := make([]float32, nCh*20)
	for c := 0; c < nCh; c++ {
		for i := 0; i < 20; i++ {
			t := float64(seq*20+uint64(i)) / 1000
			out[c*20+i] = float32(20*math.Sin(2*math.Pi*10*t+float64(c)) + float64(c))
		}
	}
	return out
}

func sineFrame(seq uint64) []float32 { return testSamples(seq, 2) }

func startServerWith(t *testing.T, cfg Config, setup func(*Server)) *runningServer {
	t.Helper()
	srv, err := Listen(cfg)
	if err != nil {
		t.Fatal(err)
	}
	setup(srv)
	ctx, cancel := context.WithCancel(context.Background())
	rs := &runningServer{t: t, srv: srv, cancel: cancel, done: make(chan struct{}), waitTimeout: 5 * time.Second}
	go func() {
		rs.sum = srv.Run(ctx, &rs.buf)
		close(rs.done)
	}()
	t.Cleanup(func() {
		if !rs.finished {
			cancel()
			select {
			case <-rs.done:
			case <-time.After(5 * time.Second):
			}
		}
	})
	return rs
}

func activeCount(rs *runningServer) int {
	rs.srv.connsMu.Lock()
	defer rs.srv.connsMu.Unlock()
	return len(rs.srv.active)
}

func procCfg() Config {
	cfg := testConfig()
	cfg.IdleTimeout = 5 * time.Second
	return cfg
}

func sendSine(t *testing.T, c net.Conn, from, to uint64) {
	t.Helper()
	for q := from; q <= to; q++ {
		if err := writeFrameData(c, q, now()-20*int64(time.Millisecond), 2, 20, sineFrame(q)); err != nil {
			t.Fatal(err)
		}
	}
}

// blockingHook returns a hook that waits for release when seq == at, and the
// release function (safe to call more than once).
func blockingHook(at uint64) (func(uint64), func()) {
	release := make(chan struct{})
	var once sync.Once
	closeRelease := func() { once.Do(func() { close(release) }) }
	hook := func(seq uint64) {
		if seq == at {
			<-release
		}
	}
	return hook, closeRelease
}

func TestProcessCleanStream(t *testing.T) {
	rs := startServer(t, procCfg())
	c := dial(t, rs.srv.Port())
	sendHello(t, c, procHello(1))
	sendSine(t, c, 0, 99)
	c.Close()
	waitFor(t, "processing", func() bool { c := counts(rs); return c.EOFCloses == 1 && c.Processed == 100 })
	_, out, feat := rs.srv.reg.Streams()[0].ProcRings()
	if st := out.Read(99, make([]float32, out.FrameLen())); st != ring.OK {
		t.Errorf("out Read(99) = %v", st)
	}
	if h, ok := feat.Head(); h != 4 || !ok {
		t.Errorf("feat Head = %d, %v", h, ok)
	}
	sum, text := rs.stop()
	ps := sum.ProcSnapshots[0]
	if !ps.Processable || ps.Decimation != 4 || ps.OutRateHz != 250 || ps.Frames != 100 || ps.Gaps != 0 || ps.Overruns != 0 || ps.Features != 5 || !ps.HasBands {
		t.Errorf("snapshot = %+v", ps)
	}
	if math.Abs(ps.Bands[2]-200) >= 10 {
		t.Errorf("alpha = %v", ps.Bands[2])
	}
	if ps.LatP50 == "n/a" {
		t.Errorf("LatP50 = n/a")
	}
	if sum.Processed != 100 || sum.Overruns != 0 {
		t.Errorf("summary processed %d overruns %d", sum.Processed, sum.Overruns)
	}
	if !strings.Contains(text, "\nproc: id=0000000000000001 decim=4 out_rate_hz=250 frames=100 gaps=0 overruns=0 features=5 ") {
		t.Errorf("no proc line in:\n%s", text)
	}
}

func TestProcessGap(t *testing.T) {
	rs := startServer(t, procCfg())
	c := dial(t, rs.srv.Port())
	sendHello(t, c, procHello(1))
	sendSine(t, c, 0, 49)
	sendSine(t, c, 60, 99)
	c.Close()
	waitFor(t, "processing", func() bool { c := counts(rs); return c.EOFCloses == 1 && c.Processed == 90 })
	sum, _ := rs.stop()
	ps := sum.ProcSnapshots[0]
	if ps.Frames != 90 || ps.Gaps != 1 || ps.Overruns != 0 || ps.Features != 1 {
		t.Errorf("snapshot = %+v", ps)
	}
}

func TestProcessLongGapIsGap(t *testing.T) {
	cfg := procCfg()
	cfg.RingSeconds = 0.1
	var calls atomic.Int32
	rs := startServerWith(t, cfg, func(s *Server) {
		s.proc.beforeFrame = func(seq uint64) {
			if seq == 5 {
				calls.Add(1)
			}
		}
	})
	c := dial(t, rs.srv.Port())
	sendHello(t, c, procHello(1))
	sendSine(t, c, 0, 4)
	waitFor(t, "five frames", func() bool { return counts(rs).Processed == 5 })
	waitFor(t, "second wait at seq 5", func() bool { return calls.Load() >= 2 })
	sendSine(t, c, 20, 21)
	c.Close()
	waitFor(t, "processing", func() bool { c := counts(rs); return c.EOFCloses == 1 && c.Processed == 7 })
	sum, _ := rs.stop()
	ps := sum.ProcSnapshots[0]
	if ps.Frames != 7 || ps.Gaps != 1 || ps.Overruns != 0 {
		t.Errorf("snapshot = %+v", ps)
	}
}

func TestProcessOverrun(t *testing.T) {
	cfg := procCfg()
	cfg.RingSeconds = 0.1
	hook, release := blockingHook(0)
	rs := startServerWith(t, cfg, func(s *Server) { s.proc.beforeFrame = hook })
	t.Cleanup(release)
	c := dial(t, rs.srv.Port())
	sendHello(t, c, procHello(1))
	sendSine(t, c, 0, 19)
	waitFor(t, "accepted", func() bool { return counts(rs).Accepted == 20 })
	release()
	c.Close()
	waitFor(t, "processing", func() bool { c := counts(rs); return c.EOFCloses == 1 && c.Processed == 1 })
	sum, _ := rs.stop()
	ps := sum.ProcSnapshots[0]
	if ps.Overruns != 1 || ps.Frames != 1 || ps.Gaps != 0 {
		t.Errorf("snapshot = %+v", ps)
	}
}

func TestProcessUnprocessable(t *testing.T) {
	rs := startServer(t, procCfg())
	c := dial(t, rs.srv.Port())
	sendHello(t, c, testHello(1, 2, 2, 100))
	for q := uint64(0); q < 10; q++ {
		if err := writeFrame(c, q, now()-20*ms, 2, 2, 0); err != nil {
			t.Fatal(err)
		}
	}
	c.Close()
	waitFor(t, "accepted", func() bool { c := counts(rs); return c.EOFCloses == 1 && c.Accepted == 10 })
	if _, out, _ := rs.srv.reg.Streams()[0].ProcRings(); out != nil {
		t.Error("unprocessable stream has an output ring")
	}
	sum, text := rs.stop()
	if sum.ProcSnapshots[0].Processable {
		t.Error("snapshot is processable")
	}
	if !strings.Contains(text, "\nproc: id=0000000000000001 unprocessed out_rate_hz=100\n") {
		t.Errorf("no proc line in:\n%s", text)
	}
}

func TestProcessDrainsAtStop(t *testing.T) {
	cfg := procCfg()
	cfg.RingSeconds = 5
	rs := startServerWith(t, cfg, func(s *Server) {
		s.proc.beforeFrame = func(seq uint64) {
			if seq == 0 {
				deadline := time.Now().Add(10 * time.Second)
				for !s.proc.draining.Load() && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
			}
		}
	})
	c := dial(t, rs.srv.Port())
	sendHello(t, c, procHello(1))
	sendSine(t, c, 0, 199)
	c.Close()
	waitFor(t, "accepted", func() bool { c := counts(rs); return c.Accepted == 200 && c.EOFCloses == 1 })
	if n := counts(rs).Processed; n != 0 {
		t.Errorf("Processed = %d before stop", n)
	}
	sum, _ := rs.stop()
	if sum.Processed != 200 {
		t.Errorf("Processed = %d, want 200", sum.Processed)
	}
}

func TestProcessParallelMatchesDirect(t *testing.T) {
	prev := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })
	rs := startServerWith(t, procCfg(), func(s *Server) { s.proc.parallelMin = 1 })
	c := dial(t, rs.srv.Port())
	sendHello(t, c, testHello(1, 3, 20, 1000))
	for q := uint64(0); q < 100; q++ {
		if err := writeFrameData(c, q, now()-20*int64(time.Millisecond), 3, 20, testSamples(q, 3)); err != nil {
			t.Fatal(err)
		}
	}
	c.Close()
	waitFor(t, "processing", func() bool { c := counts(rs); return c.EOFCloses == 1 && c.Processed == 100 })
	_, out, feat := rs.srv.reg.Streams()[0].ProcRings()

	p := dsp.NewProcessor(1000, 20, 3, 60)
	var wantFeats [][]float32
	for q := uint64(0); q < 100; q++ {
		wantOut := make([]float32, 3*5)
		due := p.BeginFrame()
		fb := make([][]float32, len(due))
		for f := range fb {
			fb[f] = make([]float32, 15)
		}
		p.Channels(testSamples(q, 3), wantOut, fb, due, 0, 3)
		wantFeats = append(wantFeats, fb...)
		got := make([]float32, 15)
		if st := out.Read(q, got); st != ring.OK {
			t.Fatalf("out Read(%d) = %v", q, st)
		}
		for i := range got {
			if math.Float32bits(got[i]) != math.Float32bits(wantOut[i]) {
				t.Fatalf("frame %d out[%d] = %v, want %v", q, i, got[i], wantOut[i])
			}
		}
	}
	if len(wantFeats) != 5 {
		t.Fatalf("direct run made %d features", len(wantFeats))
	}
	for f, want := range wantFeats {
		got := make([]float32, 15)
		if st := feat.Read(uint64(f), got); st != ring.OK {
			t.Fatalf("feat Read(%d) = %v", f, st)
		}
		for i := range got {
			if math.Float32bits(got[i]) != math.Float32bits(want[i]) {
				t.Fatalf("feature %d[%d] = %v, want %v", f, i, got[i], want[i])
			}
		}
	}
	rs.stop()
}

func TestProcessConcurrentStreams(t *testing.T) {
	rs := startServer(t, procCfg())
	var conns []net.Conn
	for k := 0; k < 4; k++ {
		conns = append(conns, dial(t, rs.srv.Port()))
	}
	errs := make([]error, 4)
	var wg sync.WaitGroup
	for k := 0; k < 4; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			c := conns[k]
			if err := writeHello(c, procHello(uint64(k+1))); err != nil {
				errs[k] = err
				return
			}
			for q := uint64(0); q < 100; q++ {
				if err := writeFrameData(c, q, now()-20*int64(time.Millisecond), 2, 20, sineFrame(q)); err != nil {
					errs[k] = err
					return
				}
			}
			c.Close()
		}(k)
	}
	wg.Wait()
	for k, err := range errs {
		if err != nil {
			t.Errorf("sender %d: %v", k+1, err)
		}
	}
	waitFor(t, "processing", func() bool { c := counts(rs); return c.Processed == 400 && c.EOFCloses == 4 })
	sum, _ := rs.stop()
	if len(sum.ProcSnapshots) != 4 {
		t.Fatalf("%d snapshots", len(sum.ProcSnapshots))
	}
	for _, ps := range sum.ProcSnapshots {
		if ps.Frames != 100 || ps.Overruns != 0 {
			t.Errorf("snapshot = %+v", ps)
		}
	}
}

func revivalConfig() Config {
	cfg := procCfg()
	cfg.EvictAfter = 0
	cfg.RingBudgetBytes = 293488
	return cfg
}

// revivalRun runs the A, B, C connections of TestProcessRevival and returns
// the server with C's connection closed and EOFCloses == 3.
func revivalRun(t *testing.T, rs *runningServer, waitProcessed bool) {
	t.Helper()
	port := rs.srv.Port()
	send := func(id uint64, from, to uint64) {
		c := dial(t, port)
		sendHello(t, c, procHello(id))
		sendSine(t, c, from, to)
		c.Close()
	}
	send(1, 0, 9)
	waitFor(t, "A", func() bool { c := counts(rs); return c.Processed == 10 && c.EOFCloses == 1 && activeCount(rs) == 0 })
	send(2, 0, 9)
	waitFor(t, "B", func() bool { c := counts(rs); return c.Processed == 20 && c.EOFCloses == 2 && activeCount(rs) == 0 })
	send(1, 10, 19)
	if waitProcessed {
		waitFor(t, "C", func() bool { c := counts(rs); return c.Processed == 30 && c.EOFCloses == 3 })
	} else {
		waitFor(t, "C", func() bool { return counts(rs).EOFCloses == 3 })
	}
}

func TestProcessRevivalDrainsAtStop(t *testing.T) {
	rs := startServer(t, revivalConfig())
	revivalRun(t, rs, false)
	sum, _ := rs.stop()
	if sum.Processed != 30 {
		t.Errorf("Processed = %d, want 30", sum.Processed)
	}
}

func TestProcessRevival(t *testing.T) {
	rs := startServer(t, revivalConfig())
	revivalRun(t, rs, true)
	sum, _ := rs.stop()
	var found bool
	for _, ps := range sum.ProcSnapshots {
		if ps.StreamID == 1 {
			found = true
			if ps.Frames != 20 || ps.Gaps != 0 {
				t.Errorf("stream 1 snapshot = %+v", ps)
			}
		}
	}
	if !found {
		t.Error("no snapshot for stream 1")
	}
	if sum.Evicted != 1 {
		t.Errorf("Evicted = %d, want 1", sum.Evicted)
	}
}
