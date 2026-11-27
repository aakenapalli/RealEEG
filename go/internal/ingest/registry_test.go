package ingest

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aakenapalli/RealEEG/go/internal/protocol"
	"github.com/aakenapalli/RealEEG/go/internal/ring"
)

func testHello(id uint64, c, s int, fs float64) protocol.Hello {
	names := make([]string, c)
	for i := range names {
		names[i] = fmt.Sprintf("c%d", i)
	}
	return protocol.Hello{StreamID: id, DeviceName: "dev", NChannels: c, SamplesPerFrame: s, SampleRateHz: fs, ChannelNames: names}
}

func testFrame(seq uint64, ts int64, c, s int, value float32) protocol.Frame {
	b := make([]byte, c*s*4)
	for i := 0; i < c*s; i++ {
		binary.LittleEndian.PutUint32(b[4*i:], math.Float32bits(value))
	}
	return protocol.Frame{Seq: seq, TimestampNs: ts, NChannels: c, SamplesPerFrame: s, Samples: b}
}

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

func newTestRegistry(budget int64, clk *fakeClock) *Registry {
	return NewRegistry(RegistryConfig{RingSeconds: 2, RingBudgetBytes: budget, EvictAfter: 60 * time.Second, Now: clk.Now})
}

var (
	H1 = testHello(1, 2, 4, 200)
	H2 = testHello(2, 2, 4, 200)
)

const (
	gib  = int64(1 << 30)
	need = int64(3200 + StreamOverheadBytes) // 265344
	ms   = int64(time.Millisecond)
)

var t0 = time.Unix(1_000_000, 0)

func fresh() (*Registry, *fakeClock) {
	clk := &fakeClock{now: t0}
	return newTestRegistry(gib, clk), clk
}

// feed sends "frame k" with the standard 5 ms latency.
func feed(s *Stream, conn, k uint64) Result {
	ts := int64(k) * 20 * ms
	return s.OnFrame(conn, testFrame(k, ts, 2, 4, 0), ts+25*ms)
}

func mustAttach(t *testing.T, r *Registry, h protocol.Hello, conn uint64) *Stream {
	t.Helper()
	s, err := r.Attach(h, conn)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	return s
}

func wantReject(t *testing.T, err error, name string) {
	t.Helper()
	var re *RejectError
	if !errors.As(err, &re) || re.Name != name {
		t.Fatalf("err = %v, want RejectError %s", err, name)
	}
}

func TestRingCapacity(t *testing.T) {
	cases := []struct {
		secs, fs float64
		spf      int
		want     int
	}{{2, 200, 4, 100}, {2, 30000, 600, 100}, {2, 1, 1024, 2}, {0.01, 1000, 20, 2}, {2.5, 1000, 20, 125}, {0.03, 1000, 20, 2}}
	for _, c := range cases {
		if got := RingCapacity(c.secs, c.fs, c.spf); got != c.want {
			t.Errorf("RingCapacity(%v,%v,%d) = %d, want %d", c.secs, c.fs, c.spf, got, c.want)
		}
	}
}

func TestAttachCreatesStream(t *testing.T) {
	r, _ := fresh()
	s := mustAttach(t, r, H1, 1)
	if _, ok := r.Lookup(1); !ok {
		t.Fatal("Lookup")
	}
	if s.Ring().Capacity() != 100 || s.Ring().FrameLen() != 8 || r.Totals().Streams != 1 {
		t.Fatal("wrong ring or totals")
	}
}

func TestFramesGoToRing(t *testing.T) {
	r, _ := fresh()
	s := mustAttach(t, r, H1, 1)
	got := []Result{
		s.OnFrame(1, testFrame(0, 0, 2, 4, 1.5), 0),
		s.OnFrame(1, testFrame(1, 0, 2, 4, 2.5), 0),
		s.OnFrame(1, testFrame(1, 0, 2, 4, 9.0), 0),
	}
	want := []Result{Accepted, Accepted, Duplicate}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("result %d = %v, want %v", i, got[i], want[i])
		}
	}
	dst := make([]float32, 8)
	for seq, v := range []float32{1.5, 2.5} {
		if st := s.Ring().Read(uint64(seq), dst); st.String() != "ok" {
			t.Fatalf("Read(%d) = %v", seq, st)
		}
		for _, x := range dst {
			if x != v {
				t.Fatalf("Read(%d) sample %v, want %v", seq, x, v)
			}
		}
	}
}

func TestAttachBusy(t *testing.T) {
	r, _ := fresh()
	s := mustAttach(t, r, H1, 1)
	s2, err := r.Attach(H1, 2)
	var be *BusyError
	if s2 != nil || !errors.As(err, &be) || be.ConnID != 1 {
		t.Fatalf("got %v, %v", s2, err)
	}
	if feed(s, 1, 0) != Accepted {
		t.Fatal("frame 0")
	}
	r.Detach(s, 1)
	if got := mustAttach(t, r, H1, 2); got != s {
		t.Fatal("different stream")
	}
	if feed(s, 2, 1) != Accepted {
		t.Fatal("frame 1")
	}
}

func TestReattachSameConn(t *testing.T) {
	r, _ := fresh()
	a := mustAttach(t, r, H1, 1)
	b := mustAttach(t, r, H1, 1)
	if a != b {
		t.Fatal("different streams")
	}
}

func TestDetachOnlyOwner(t *testing.T) {
	r, _ := fresh()
	s := mustAttach(t, r, H1, 1)
	r.Detach(s, 2)
	_, err := r.Attach(H1, 3)
	var be *BusyError
	if !errors.As(err, &be) || be.ConnID != 1 {
		t.Fatalf("err = %v", err)
	}
	if feed(s, 1, 0) != Accepted {
		t.Fatal("frame 0")
	}
}

func TestHelloMismatchKeepsState(t *testing.T) {
	r, _ := fresh()
	s := mustAttach(t, r, H1, 1)
	feed(s, 1, 0)
	_, err := r.Attach(testHello(1, 3, 4, 200), 2)
	var be *BusyError
	if errors.As(err, &be) {
		t.Fatal("got BusyError")
	}
	wantReject(t, err, HelloMismatchName)
	if feed(s, 1, 1) != Accepted {
		t.Fatal("frame 1")
	}
}

func mustPanicMsg(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if s, _ := r.(string); s != want {
			t.Fatalf("panic = %v, want %q", r, want)
		}
	}()
	fn()
}

func TestOnFrameNotAttachedPanics(t *testing.T) {
	r, _ := fresh()
	s := mustAttach(t, r, H1, 1)
	mustPanicMsg(t, "ingest: OnFrame from connection 2, stream is attached to 1", func() { feed(s, 2, 0) })
}

func TestBudgetRejectsNewStream(t *testing.T) {
	clk := &fakeClock{now: t0}
	r := newTestRegistry(need-1, clk)
	_, err := r.Attach(H1, 1)
	wantReject(t, err, ResourceLimitName)
	if _, ok := r.Lookup(1); ok || r.Totals().Streams != 0 {
		t.Fatal("stream created")
	}
	r = newTestRegistry(need, clk)
	if _, err := r.Attach(H1, 1); err != nil {
		t.Fatal(err)
	}
}

func TestEvictionMakesRoom(t *testing.T) {
	clk := &fakeClock{now: t0}
	r := newTestRegistry(need, clk)
	s := mustAttach(t, r, H1, 1)
	feed(s, 1, 0)
	feed(s, 1, 1)
	r.Detach(s, 1)
	clk.now = t0.Add(60*time.Second - 1)
	_, err := r.Attach(H2, 2)
	wantReject(t, err, ResourceLimitName)
	if s.Ring() == nil {
		t.Fatal("ring freed early")
	}
	clk.now = t0.Add(60 * time.Second)
	mustAttach(t, r, H2, 2)
	s1, ok := r.Lookup(1)
	if !ok || s1.Ring() != nil {
		t.Fatal("stream 1 not evicted")
	}
	tot := r.Totals()
	if tot.Streams != 2 || tot.Evicted != 1 || tot.Accepted != 2 {
		t.Fatalf("totals = %+v", tot)
	}
	snap := r.Snapshots()[0]
	if !snap.Evicted || snap.Accepted != 2 || snap.RingFrames != 0 || snap.LatP50 != "5.0" {
		t.Fatalf("snapshot = %+v", snap)
	}
	if !strings.Contains(snap.Line(), "ring_frames=0 state=evicted") {
		t.Fatalf("line = %s", snap.Line())
	}
}

func TestEvictedStreamRevives(t *testing.T) {
	clk := &fakeClock{now: t0}
	r := newTestRegistry(need, clk)
	s1 := mustAttach(t, r, H1, 1)
	feed(s1, 1, 0)
	feed(s1, 1, 1)
	r.Detach(s1, 1)
	clk.now = t0.Add(60 * time.Second)
	s2 := mustAttach(t, r, H2, 2)
	r.Detach(s2, 2)
	clk.now = t0.Add(120 * time.Second)
	got := mustAttach(t, r, H1, 3)
	if got != s1 {
		t.Fatal("not the same stream")
	}
	if s1.Ring().Capacity() != 100 || s2.Ring() != nil {
		t.Fatal("rings wrong")
	}
	ts := int64(5) * 20 * ms
	if s1.OnFrame(3, testFrame(5, ts, 2, 4, 0), ts+27*ms) != Accepted {
		t.Fatal("frame 5")
	}
	dst := make([]float32, 8)
	if st := s1.Ring().Read(1, dst); st.String() != "missing" {
		t.Fatalf("Read(1) = %v", st)
	}
	if st := s1.Ring().Read(5, dst); st.String() != "ok" {
		t.Fatalf("Read(5) = %v", st)
	}
	sn := s1.Snapshot()
	if sn.Accepted != 3 || sn.Missing != 3 || sn.LastSeq != 5 || sn.Evicted || sn.RingFrames != 100 || sn.LatP50 != "7.0" || sn.LatMax != "7.0" {
		t.Fatalf("snapshot = %+v", sn)
	}
	if tot := r.Totals(); tot.Streams != 2 || tot.Evicted != 1 {
		t.Fatalf("totals = %+v", tot)
	}
}

func TestRevivalRejected(t *testing.T) {
	clk := &fakeClock{now: t0}
	r := newTestRegistry(need, clk)
	s1 := mustAttach(t, r, H1, 1)
	feed(s1, 1, 0)
	r.Detach(s1, 1)
	clk.now = t0.Add(60 * time.Second)
	mustAttach(t, r, H2, 2)
	clk.now = t0.Add(120 * time.Second)
	_, err := r.Attach(H1, 3)
	wantReject(t, err, ResourceLimitName)
	_, err = r.Attach(testHello(1, 3, 4, 200), 4)
	wantReject(t, err, HelloMismatchName)
	if s1.Ring() != nil || !s1.Snapshot().Evicted {
		t.Fatal("stream 1 not evicted")
	}
}

func TestAttachedStreamNotEvicted(t *testing.T) {
	clk := &fakeClock{now: t0}
	r := newTestRegistry(need, clk)
	mustAttach(t, r, H1, 1)
	clk.now = t0.Add(time.Hour)
	_, err := r.Attach(H2, 2)
	wantReject(t, err, ResourceLimitName)
}

func TestEvictionOrder(t *testing.T) {
	clk := &fakeClock{now: t0}
	r := newTestRegistry(2*need, clk)
	s1 := mustAttach(t, r, H1, 1)
	r.Detach(s1, 1)
	clk.now = t0.Add(time.Second)
	s2 := mustAttach(t, r, H2, 2)
	r.Detach(s2, 2)
	clk.now = t0.Add(2 * time.Minute)
	mustAttach(t, r, testHello(3, 2, 4, 200), 3)
	if s1.Ring() != nil || s2.Ring() == nil {
		t.Fatal("wrong stream evicted")
	}
}

func TestSaturatingLatency(t *testing.T) {
	r, _ := fresh()
	s := mustAttach(t, r, H1, 1)
	s.OnFrame(1, testFrame(0, math.MinInt64, 2, 4, 0), 1_700_000_000_000_000_000)
	sn := s.Snapshot()
	if sn.LatMax != "9223372036854.8" || sn.LatP50 != "1000.0" {
		t.Fatalf("snapshot = %+v", sn)
	}
	r, _ = fresh()
	s = mustAttach(t, r, H1, 1)
	s.OnFrame(1, testFrame(0, math.MaxInt64, 2, 4, 0), 0)
	if sn := s.Snapshot(); sn.LatP50 != "-1000.0" {
		t.Fatalf("snapshot = %+v", sn)
	}
}

func TestTinyRateNoOverflow(t *testing.T) {
	r, _ := fresh()
	s := mustAttach(t, r, testHello(1, 2, 4, 1e-10), 1)
	if s.OnFrame(1, testFrame(0, 0, 2, 4, 0), 1_000_000_000_000_000_000) != Accepted {
		t.Fatal("not accepted")
	}
	sn := s.Snapshot()
	if sn.RingFrames != 2 || sn.LatP50 != "-1000.0" {
		t.Fatalf("snapshot = %+v", sn)
	}
}

func TestTakeIntervalResets(t *testing.T) {
	r, _ := fresh()
	s := mustAttach(t, r, H1, 1)
	for k := uint64(0); k < 3; k++ {
		feed(s, 1, k)
	}
	a, b, c := r.TakeInterval()
	if a != "5.0" || b != "5.0" || c != "5.0" {
		t.Fatalf("first = %s %s %s", a, b, c)
	}
	a, b, c = r.TakeInterval()
	if a != "n/a" || b != "n/a" || c != "n/a" {
		t.Fatalf("second = %s %s %s", a, b, c)
	}
}

func TestSnapshotLine(t *testing.T) {
	r, _ := fresh()
	s := mustAttach(t, r, H1, 1)
	for k := uint64(0); k < 5; k++ {
		s.OnFrame(1, testFrame(k, 1_000_000_000_000+int64(k)*20*ms, 2, 4, 0), 1_000_000_000_000+int64(k)*20*ms+25*ms)
	}
	want := "stream: id=0000000000000001 device=dev channels=2 rate_hz=200 samples_per_frame=4 initial_seq=0 last_seq=4 accepted=5 missing=0 dup=0 stale=0 ring_frames=100 state=live drift_ppm=-0.0 lat_p50_ms=5.0 lat_p99_ms=5.0 lat_max_ms=5.0"
	if got := s.Snapshot().Line(); got != want {
		t.Fatalf("line:\n got %s\nwant %s", got, want)
	}
}

func TestNoFramesLine(t *testing.T) {
	r, _ := fresh()
	s := mustAttach(t, r, H1, 1)
	line := s.Snapshot().Line()
	if !strings.Contains(line, "initial_seq=n/a last_seq=n/a") || !strings.Contains(line, "drift_ppm=n/a lat_p50_ms=n/a lat_p99_ms=n/a lat_max_ms=n/a") {
		t.Fatalf("line = %s", line)
	}
}

func TestConcurrentAccess(t *testing.T) {
	r, _ := fresh()
	streams := make([]*Stream, 5)
	for k := 1; k <= 4; k++ {
		streams[k] = mustAttach(t, r, testHello(uint64(k), 2, 4, 200), uint64(k))
	}
	var writers, readers sync.WaitGroup
	var mu sync.Mutex
	var errs []string
	report := func(s string) { mu.Lock(); errs = append(errs, s); mu.Unlock() }
	stop := make(chan struct{})
	for k := 1; k <= 4; k++ {
		writers.Add(1)
		go func(k int) {
			defer writers.Done()
			for q := uint64(0); q < 2000; q++ {
				ts := int64(q) * 20 * ms
				if streams[k].OnFrame(uint64(k), testFrame(q, ts, 2, 4, float32(q)), ts+25*ms) != Accepted {
					report("not accepted")
					return
				}
			}
		}(k)
	}
	readers.Add(2)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			r.Snapshots()
			r.Totals()
			r.TakeInterval()
		}
	}()
	go func() {
		defer readers.Done()
		dst := make([]float32, 8)
		for {
			select {
			case <-stop:
				return
			default:
			}
			for k := 1; k <= 4; k++ {
				rg := streams[k].Ring()
				h, ok := rg.Head()
				if !ok {
					continue
				}
				if rg.Read(h, dst) == ring.OK {
					for _, v := range dst {
						if v != float32(h) {
							report(fmt.Sprintf("torn read seq %d: %v", h, v))
							return
						}
					}
				}
			}
		}
	}()
	writers.Wait()
	close(stop)
	readers.Wait()
	for _, e := range errs {
		t.Error(e)
	}
	tot := r.Totals()
	if tot.Accepted != 8000 || tot.Missing != 0 {
		t.Fatalf("totals = %+v", tot)
	}
}

func BenchmarkFramePath(b *testing.B) {
	clk := &fakeClock{now: t0}
	r := newTestRegistry(gib, clk)
	h := testHello(1, 1024, 600, 30000)
	s, err := r.Attach(h, 1)
	if err != nil {
		b.Fatal(err)
	}
	msg, err := protocol.EncodeFrame(0, 0, 1024, 600, make([]float32, 1024*600))
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(msg)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m, err := protocol.DecodeMessage(msg)
		if err != nil {
			b.Fatal(err)
		}
		m.Frame.Seq = uint64(i)
		s.OnFrame(1, m.Frame, 0)
	}
}

func newProcRegistry(budget int64, clk *fakeClock) *Registry {
	return NewRegistry(RegistryConfig{RingSeconds: 2, RingBudgetBytes: budget, EvictAfter: 60 * time.Second, Processing: true, Now: clk.Now})
}

func TestFeatureRingCapacity(t *testing.T) {
	for _, c := range []struct {
		sec  float64
		want int
	}{{2, 8}, {0.1, 2}, {60, 240}} {
		if got := FeatureRingCapacity(c.sec); got != c.want {
			t.Errorf("FeatureRingCapacity(%g) = %d, want %d", c.sec, got, c.want)
		}
	}
}

func TestProcRingsAllocated(t *testing.T) {
	r := newProcRegistry(gib, &fakeClock{now: t0})
	s := mustAttach(t, r, H1, 1)
	in, out, feat := s.ProcRings()
	if in == nil || out == nil || feat == nil {
		t.Fatalf("rings: %v %v %v", in, out, feat)
	}
	if in.Capacity() != 100 || in.FrameLen() != 8 {
		t.Errorf("in: %d / %d", in.Capacity(), in.FrameLen())
	}
	if out.Capacity() != 100 || out.FrameLen() != 8 {
		t.Errorf("out: %d / %d", out.Capacity(), out.FrameLen())
	}
	if feat.Capacity() != 8 || feat.FrameLen() != 10 {
		t.Errorf("feat: %d / %d", feat.Capacity(), feat.FrameLen())
	}
}

func TestProcBudget(t *testing.T) {
	r := newProcRegistry(277887, &fakeClock{now: t0})
	_, err := r.Attach(H1, 1)
	wantReject(t, err, ResourceLimitName)
	r = newProcRegistry(277888, &fakeClock{now: t0})
	if _, err := r.Attach(H1, 1); err != nil {
		t.Fatalf("Attach: %v", err)
	}
}

func TestUnprocessableHasNoProcRings(t *testing.T) {
	r := newProcRegistry(263744, &fakeClock{now: t0})
	s, err := r.Attach(testHello(1, 2, 2, 100), 1)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	in, out, feat := s.ProcRings()
	if in == nil || out != nil || feat != nil {
		t.Errorf("rings: %v %v %v", in, out, feat)
	}
}

func TestEvictionFreesProcRings(t *testing.T) {
	clk := &fakeClock{now: t0}
	r := newProcRegistry(277888, clk)
	s := mustAttach(t, r, H1, 1)
	r.Detach(s, 1)
	clk.now = t0.Add(60 * time.Second)
	mustAttach(t, r, H2, 2)
	in, out, feat := s.ProcRings()
	if in != nil || out != nil || feat != nil {
		t.Errorf("rings after eviction: %v %v %v", in, out, feat)
	}
}

func TestOnFrameStampsArrival(t *testing.T) {
	r := newProcRegistry(gib, &fakeClock{now: t0})
	s := mustAttach(t, r, H1, 1)
	if res := s.OnFrame(1, testFrame(0, 0, 2, 4, 0), 12345); res != Accepted {
		t.Fatalf("OnFrame = %v", res)
	}
	st, stamp := s.Ring().ReadStamped(0, make([]float32, 8))
	if st != ring.OK || stamp != 12345 {
		t.Errorf("ReadStamped = %v, %d", st, stamp)
	}
}

func TestStreamsAndHello(t *testing.T) {
	r := newProcRegistry(gib, &fakeClock{now: t0})
	s1 := mustAttach(t, r, H1, 1)
	mustAttach(t, r, H2, 2)
	r.Detach(s1, 1)
	h := H1
	h.DeviceName = "dev-b"
	mustAttach(t, r, h, 3)
	list := r.Streams()
	if len(list) != 2 || list[0].Hello().StreamID != 1 || list[1].Hello().StreamID != 2 {
		t.Fatalf("Streams = %v", list)
	}
	if got := list[0].Hello().DeviceName; got != "dev" {
		t.Errorf("DeviceName = %q", got)
	}
}
