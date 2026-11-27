package ingest

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aakenapalli/RealEEG/go/internal/dsp"
	"github.com/aakenapalli/RealEEG/go/internal/protocol"
	"github.com/aakenapalli/RealEEG/go/internal/ring"
)

// ProcSnapshot is the state of one stream's processing.
type ProcSnapshot struct {
	StreamID                         uint64
	Processable                      bool
	Decimation                       int     // valid when Processable
	OutRateHz                        float64 // dsp.OutputRate, also for unprocessable streams
	Frames, Gaps, Overruns, Features uint64
	LatP50, LatP99, LatMax           string // ms format, or "n/a"
	HasBands                         bool
	Bands                            [dsp.NumBands]float64 // mean over channels of the last feature
}

// Line formats the snapshot as a proc: line.
func (p ProcSnapshot) Line() string {
	if !p.Processable {
		return fmt.Sprintf("proc: id=%016x unprocessed out_rate_hz=%s", p.StreamID, g6(p.OutRateHz))
	}
	var b [dsp.NumBands]string
	for i := range b {
		b[i] = "n/a"
		if p.HasBands {
			b[i] = g6(p.Bands[i])
		}
	}
	return fmt.Sprintf("proc: id=%016x decim=%d out_rate_hz=%s frames=%d gaps=%d overruns=%d features=%d lat_p50_ms=%s lat_p99_ms=%s lat_max_ms=%s bp_delta=%s bp_theta=%s bp_alpha=%s bp_beta=%s bp_gamma=%s",
		p.StreamID, p.Decimation, g6(p.OutRateHz), p.Frames, p.Gaps, p.Overruns, p.Features,
		p.LatP50, p.LatP99, p.LatMax, b[0], b[1], b[2], b[3], b[4])
}

const procRecheck = 100 * time.Millisecond

type procStage struct {
	reg         *Registry
	mainsHz     float64
	parallelMin int

	mu      sync.Mutex
	streams map[uint64]*procStream

	intervalMu sync.Mutex
	interval   *histogram
	total      *histogram

	wg          sync.WaitGroup
	draining    atomic.Bool
	drainCh     chan struct{}
	beforeFrame func(seq uint64)
}

type procStream struct {
	id          uint64
	hello       protocol.Hello
	processable bool
	d           int
	r           float64

	mu         sync.Mutex
	running    bool
	frames     uint64
	gaps       uint64
	overruns   uint64
	features   uint64
	featureSeq uint64
	lat        *histogram
	bands      [dsp.NumBands]float64
	hasBands   bool
}

func newProcStage(reg *Registry, mainsHz float64) *procStage {
	return &procStage{
		reg:         reg,
		mainsHz:     mainsHz,
		parallelMin: 65536,
		streams:     make(map[uint64]*procStream),
		interval:    new(histogram),
		total:       new(histogram),
		drainCh:     make(chan struct{}),
	}
}

func newProcStream(id uint64, h protocol.Hello) *procStream {
	ps := &procStream{id: id, hello: h}
	ps.processable = dsp.Processable(h.SampleRateHz, h.SamplesPerFrame)
	ps.d = dsp.DecimationFactor(h.SampleRateHz, h.SamplesPerFrame)
	ps.r = dsp.OutputRate(h.SampleRateHz, h.SamplesPerFrame)
	if ps.processable {
		ps.lat = new(histogram)
	}
	return ps
}

// ensure starts processing of a stream's current rings if needed.
func (st *procStage) ensure(s *Stream) {
	st.mu.Lock()
	ps := st.streams[s.id]
	if ps == nil {
		ps = newProcStream(s.id, s.Hello())
		st.streams[s.id] = ps
	}
	if !ps.processable {
		st.mu.Unlock()
		return
	}
	ps.mu.Lock()
	if ps.running {
		ps.mu.Unlock()
		st.mu.Unlock()
		return
	}
	in, out, feat := s.ProcRings()
	if in == nil || out == nil {
		ps.mu.Unlock()
		st.mu.Unlock()
		return
	}
	ps.running = true
	st.wg.Add(1)
	ps.mu.Unlock()
	st.mu.Unlock()
	go st.process(ps, s, in, out, feat)
}

func (st *procStage) process(ps *procStream, s *Stream, in, out, feat *ring.Ring) {
	defer func() {
		ps.mu.Lock()
		ps.running = false
		ps.mu.Unlock()
		if cur := s.Ring(); cur != nil && cur != in {
			st.ensure(s)
		}
		st.wg.Done()
	}()

	h := ps.hello
	C, S := h.NChannels, h.SamplesPerFrame
	p := dsp.NewProcessor(h.SampleRateHz, S, C, st.mainsHz)
	inBuf := make([]float32, C*S)
	outBuf := make([]float32, C*p.OutPerFrame())
	var featBufs [][]float32

	timer := time.NewTimer(procRecheck)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	leave := func(ch <-chan struct{}) bool {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}
	wait := func(ch <-chan struct{}) bool {
		if st.draining.Load() {
			return leave(ch)
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(procRecheck)
		select {
		case <-ch:
			return true
		case <-st.drainCh:
			return true
		case <-timer.C:
			if s.Ring() == in {
				return true
			}
			return leave(ch)
		}
	}

	var seq uint64
	for {
		ch := in.Changed()
		if first, ok := in.First(); ok {
			seq = first
			break
		}
		if !wait(ch) {
			return
		}
	}
	inGap, caughtUp := false, false
	for {
		if st.beforeFrame != nil {
			st.beforeFrame(seq)
		}
		ch := in.Changed()
		rs, stamp := in.ReadStamped(seq, inBuf)
		switch rs {
		case ring.OK:
			featBufs = st.handleFrame(ps, p, out, feat, seq, stamp, inBuf, outBuf, featBufs)
			seq++
			inGap = false
			caughtUp = false
		case ring.Missing:
			if !inGap {
				ps.mu.Lock()
				ps.gaps++
				ps.mu.Unlock()
				inGap = true
				p.Reset()
			}
			seq++
		case ring.Overwritten:
			head, _ := in.Head()
			if caughtUp {
				if !inGap {
					ps.mu.Lock()
					ps.gaps++
					ps.mu.Unlock()
					inGap = true
					p.Reset()
				}
				seq = head - uint64(in.Capacity()) + 1
			} else {
				ps.mu.Lock()
				ps.overruns++
				ps.mu.Unlock()
				p.Reset()
				seq = head
				inGap = false
			}
		case ring.NotYet:
			caughtUp = true
			if !wait(ch) {
				return
			}
		}
	}
}

func (st *procStage) handleFrame(ps *procStream, p *dsp.Processor, out, feat *ring.Ring, seq uint64, stamp int64,
	inBuf, outBuf []float32, featBufs [][]float32) [][]float32 {
	C := ps.hello.NChannels
	due := p.BeginFrame()
	for len(featBufs) < len(due) {
		featBufs = append(featBufs, make([]float32, C*dsp.NumBands))
	}
	fb := featBufs[:len(due)]

	if C*ps.hello.SamplesPerFrame >= st.parallelMin && runtime.GOMAXPROCS(0) > 1 {
		g := min(4*runtime.GOMAXPROCS(0), C)
		base, rem := C/g, C%g
		var wg sync.WaitGroup
		lo := 0
		for i := 0; i < g; i++ {
			n := base
			if i < rem {
				n++
			}
			hi := lo + n
			wg.Add(1)
			go func(lo, hi int) {
				defer wg.Done()
				p.Channels(inBuf, outBuf, fb, due, lo, hi)
			}(lo, hi)
			lo = hi
		}
		wg.Wait()
	} else {
		p.Channels(inBuf, outBuf, fb, due, 0, C)
	}

	out.WriteStamped(seq, stamp, func(dst []float32) { copy(dst, outBuf) })
	lat := satSub(time.Now().UnixNano(), stamp)

	ps.mu.Lock()
	ps.lat.record(lat)
	for f := range fb {
		feat.WriteStamped(ps.featureSeq, int64(seq), func(dst []float32) { copy(dst, fb[f]) })
		ps.featureSeq++
		ps.features++
	}
	if len(due) > 0 {
		last := fb[len(due)-1]
		for b := 0; b < dsp.NumBands; b++ {
			sum := 0.0
			for c := 0; c < C; c++ {
				sum += float64(last[c*dsp.NumBands+b])
			}
			ps.bands[b] = sum / float64(C)
		}
		ps.hasBands = true
	}
	ps.frames++
	ps.mu.Unlock()

	st.intervalMu.Lock()
	st.interval.record(lat)
	st.total.record(lat)
	st.intervalMu.Unlock()
	return featBufs
}

// stop is called by Run after every connection goroutine has ended.
func (st *procStage) stop() {
	st.draining.Store(true)
	close(st.drainCh)
	st.wg.Wait()
}

func (st *procStage) totals() (frames, gaps, overruns uint64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, ps := range st.streams {
		ps.mu.Lock()
		frames += ps.frames
		gaps += ps.gaps
		overruns += ps.overruns
		ps.mu.Unlock()
	}
	return
}

func (st *procStage) takeInterval() string {
	st.intervalMu.Lock()
	defer st.intervalMu.Unlock()
	p99 := st.interval.p99()
	st.interval.reset()
	return p99
}

func (st *procStage) totalStrings() (p99, max string) {
	st.intervalMu.Lock()
	defer st.intervalMu.Unlock()
	return st.total.p99(), st.total.maxString()
}

func (st *procStage) snapshots() []ProcSnapshot {
	streams := st.reg.Streams()
	out := make([]ProcSnapshot, 0, len(streams))
	for _, s := range streams {
		st.mu.Lock()
		ps := st.streams[s.id]
		if ps == nil {
			ps = newProcStream(s.id, s.Hello())
			st.streams[s.id] = ps
		}
		ps.mu.Lock()
		snap := ProcSnapshot{StreamID: ps.id, Processable: ps.processable, Decimation: ps.d, OutRateHz: ps.r,
			LatP50: "n/a", LatP99: "n/a", LatMax: "n/a"}
		if ps.processable {
			snap.Frames, snap.Gaps, snap.Overruns, snap.Features = ps.frames, ps.gaps, ps.overruns, ps.features
			snap.LatP50, snap.LatP99, snap.LatMax = ps.lat.p50(), ps.lat.p99(), ps.lat.maxString()
			snap.HasBands, snap.Bands = ps.hasBands, ps.bands
		}
		ps.mu.Unlock()
		st.mu.Unlock()
		out = append(out, snap)
	}
	return out
}
