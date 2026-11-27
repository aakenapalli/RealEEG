package ingest

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/aakenapalli/RealEEG/go/internal/protocol"
	"github.com/aakenapalli/RealEEG/go/internal/ring"
)

// Result is the outcome of Stream.OnFrame.
type Result int

const (
	Accepted Result = iota + 1
	Duplicate
	Stale
)

func (r Result) String() string {
	switch r {
	case Accepted:
		return "accepted"
	case Duplicate:
		return "duplicate"
	case Stale:
		return "stale"
	}
	return fmt.Sprintf("Result(%d)", int(r))
}

const (
	HelloMismatchName = "HelloMismatchError"
	ResourceLimitName = "ResourceLimitError"
)

// StreamOverheadBytes is charged against the budget for every live stream, on top of
// its ring. It covers the latency histogram (160 KB) and a typical stream record; a stream
// with thousands of long channel names uses more (up to about 200 KB of names).
const StreamOverheadBytes = 256 << 10

// RejectError is returned by Attach. Name is HelloMismatchName or ResourceLimitName.
type RejectError struct {
	Name string
	Msg  string
}

func (e *RejectError) Error() string { return e.Msg }

// BusyError is returned by Attach when the stream is attached to another connection.
type BusyError struct {
	ConnID uint64 // the connection the stream is attached to
}

func (e *BusyError) Error() string {
	return fmt.Sprintf("ingest: stream is attached to connection %d", e.ConnID)
}

type RegistryConfig struct {
	RingSeconds     float64
	RingBudgetBytes int64
	EvictAfter      time.Duration
	Now             func() time.Time // nil means time.Now
}

type Totals struct {
	Accepted, Missing, Dup, Stale uint64
	Streams                       int // every stream ever created (all stay in the registry)
	Evicted                       int // streams currently evicted
}

type StreamSnapshot struct {
	StreamID                   uint64
	DeviceName                 string // from the stream's first HELLO
	NChannels, SamplesPerFrame int
	SampleRateHz               float64
	HasFrames                  bool
	InitialSeq, LastSeq        uint64 // valid when HasFrames
	Accepted, Missing          uint64
	Dup, Stale                 uint64
	RingFrames                 int // ring capacity; 0 while evicted
	Evicted                    bool
	DriftPPM                   float64 // valid when HasDrift
	HasDrift                   bool
	LatP50, LatP99, LatMax     string // ms format, or "n/a"
}

func g6(x float64) string { return strconv.FormatFloat(x, 'g', 6, 64) }

func (s StreamSnapshot) Line() string {
	initial, last := "n/a", "n/a"
	if s.HasFrames {
		initial = strconv.FormatUint(s.InitialSeq, 10)
		last = strconv.FormatUint(s.LastSeq, 10)
	}
	state := "live"
	if s.Evicted {
		state = "evicted"
	}
	drift := "n/a"
	if s.HasDrift {
		drift = fmt.Sprintf("%.1f", s.DriftPPM)
	}
	return fmt.Sprintf("stream: id=%016x device=%s channels=%d rate_hz=%s samples_per_frame=%d initial_seq=%s last_seq=%s accepted=%d missing=%d dup=%d stale=%d ring_frames=%d state=%s drift_ppm=%s lat_p50_ms=%s lat_p99_ms=%s lat_max_ms=%s",
		s.StreamID, s.DeviceName, s.NChannels, g6(s.SampleRateHz), s.SamplesPerFrame, initial, last,
		s.Accepted, s.Missing, s.Dup, s.Stale, s.RingFrames, state, drift, s.LatP50, s.LatP99, s.LatMax)
}

// RingCapacity returns the number of frames in a stream's ring.
func RingCapacity(ringSeconds, sampleRateHz float64, samplesPerFrame int) int {
	n := int(math.Ceil(ringSeconds * sampleRateHz / float64(samplesPerFrame)))
	if n < 2 {
		n = 2
	}
	return n
}

// Registry tracks every stream.
type Registry struct {
	cfg RegistryConfig
	now func() time.Time

	mu      sync.Mutex
	streams map[uint64]*Stream
	order   []*Stream
	inUse   int64
	evicted int

	intervalMu sync.Mutex
	interval   *histogram
}

// Stream is the state of one stream_id.
type Stream struct {
	reg   *Registry
	id    uint64
	hello protocol.Hello // the stream's first HELLO
	need  int64

	mu         sync.Mutex
	conn       uint64
	detachedAt time.Time
	evicted    bool
	ring       *ring.Ring
	hist       *histogram
	evP50      string
	evP99      string
	evMax      string

	hasFrames  bool
	initialSeq uint64
	lastSeq    uint64
	accepted   uint64
	missing    uint64
	dup        uint64
	stale      uint64

	n                int
	x0, y0           int64
	sx, sy, sxx, sxy float64
}

func NewRegistry(cfg RegistryConfig) *Registry {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Registry{cfg: cfg, now: now, streams: make(map[uint64]*Stream), interval: new(histogram)}
}

func (r *Registry) ringCapacity(h protocol.Hello) int {
	return RingCapacity(r.cfg.RingSeconds, h.SampleRateHz, h.SamplesPerFrame)
}

func (r *Registry) needFor(h protocol.Hello) int64 {
	return int64(r.ringCapacity(h))*int64(h.NChannels)*int64(h.SamplesPerFrame)*4 + StreamOverheadBytes
}

func sameMetadata(a, b protocol.Hello) bool {
	if a.NChannels != b.NChannels || a.SamplesPerFrame != b.SamplesPerFrame || a.SampleRateHz != b.SampleRateHz ||
		len(a.ChannelNames) != len(b.ChannelNames) {
		return false
	}
	for i := range a.ChannelNames {
		if a.ChannelNames[i] != b.ChannelNames[i] {
			return false
		}
	}
	return true
}

// reserve makes room for need bytes for stream id, evicting if allowed.
// The caller holds r.mu.
func (r *Registry) reserve(id uint64, need int64) *RejectError {
	if r.inUse+need <= r.cfg.RingBudgetBytes {
		return nil
	}
	now := r.now()
	type cand struct {
		s          *Stream
		detachedAt time.Time
		idx        int
	}
	var cands []cand
	var candBytes int64
	for i, s := range r.order {
		s.mu.Lock()
		ev, conn, det := s.evicted, s.conn, s.detachedAt
		s.mu.Unlock()
		if !ev && conn == 0 && now.Sub(det) >= r.cfg.EvictAfter {
			cands = append(cands, cand{s, det, i})
			candBytes += s.need
		}
	}
	if r.inUse-candBytes+need > r.cfg.RingBudgetBytes {
		return &RejectError{ResourceLimitName, fmt.Sprintf("ring budget exceeded: stream %016x needs %d bytes, %d of %d in use",
			id, need, r.inUse, r.cfg.RingBudgetBytes)}
	}
	sort.SliceStable(cands, func(i, j int) bool { return cands[i].detachedAt.Before(cands[j].detachedAt) })
	for _, c := range cands {
		if r.inUse+need <= r.cfg.RingBudgetBytes {
			break
		}
		r.evict(c.s)
	}
	return nil
}

// evict frees a stream's ring and histogram. The caller holds r.mu.
func (r *Registry) evict(s *Stream) {
	s.mu.Lock()
	s.evP50, s.evP99, s.evMax = s.hist.p50(), s.hist.p99(), s.hist.maxString()
	s.ring = nil
	s.hist = nil
	s.evicted = true
	s.mu.Unlock()
	r.inUse -= s.need
	r.evicted++
}

// Attach attaches connection connID (>= 1) to the stream of h.
func (r *Registry) Attach(h protocol.Hello, connID uint64) (*Stream, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.streams[h.StreamID]; ok {
		s.mu.Lock()
		if !sameMetadata(s.hello, h) {
			s.mu.Unlock()
			return nil, &RejectError{HelloMismatchName, fmt.Sprintf("stream %016x reconnected with different metadata", h.StreamID)}
		}
		if s.conn != 0 && s.conn != connID {
			be := &BusyError{s.conn}
			s.mu.Unlock()
			return nil, be
		}
		if s.conn == connID {
			s.mu.Unlock()
			return s, nil
		}
		if !s.evicted {
			s.conn = connID
			s.mu.Unlock()
			return s, nil
		}
		s.mu.Unlock()
		if rej := r.reserve(h.StreamID, s.need); rej != nil {
			return nil, rej
		}
		rg := ring.New(r.ringCapacity(s.hello), s.hello.NChannels, s.hello.SamplesPerFrame)
		hist := new(histogram)
		s.mu.Lock()
		s.ring = rg
		s.hist = hist
		s.evicted = false
		s.conn = connID
		s.mu.Unlock()
		r.inUse += s.need
		r.evicted--
		return s, nil
	}
	need := r.needFor(h)
	if rej := r.reserve(h.StreamID, need); rej != nil {
		return nil, rej
	}
	first := h
	first.ChannelNames = append([]string(nil), h.ChannelNames...)
	s := &Stream{
		reg:   r,
		id:    h.StreamID,
		hello: first,
		need:  need,
		conn:  connID,
		ring:  ring.New(r.ringCapacity(h), h.NChannels, h.SamplesPerFrame),
		hist:  new(histogram),
	}
	r.streams[h.StreamID] = s
	r.order = append(r.order, s)
	r.inUse += need
	return s, nil
}

// Detach releases the stream if connID is the connection attached to it.
func (r *Registry) Detach(s *Stream, connID uint64) {
	s.mu.Lock()
	if s.conn == connID {
		s.conn = 0
		s.detachedAt = r.now()
	}
	s.mu.Unlock()
}

// Lookup returns the stream with that ID, live or evicted.
func (r *Registry) Lookup(streamID uint64) (*Stream, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.streams[streamID]
	return s, ok
}

func (r *Registry) Totals() Totals {
	r.mu.Lock()
	defer r.mu.Unlock()
	var t Totals
	for _, s := range r.order {
		s.mu.Lock()
		t.Accepted += s.accepted
		t.Missing += s.missing
		t.Dup += s.dup
		t.Stale += s.stale
		s.mu.Unlock()
	}
	t.Streams = len(r.order)
	t.Evicted = r.evicted
	return t
}

// TakeInterval returns the interval latency strings and resets the interval histogram.
func (r *Registry) TakeInterval() (p50, p99, max string) {
	r.intervalMu.Lock()
	defer r.intervalMu.Unlock()
	p50, p99, max = r.interval.p50(), r.interval.p99(), r.interval.maxString()
	r.interval.reset()
	return
}

func (r *Registry) Snapshots() []StreamSnapshot {
	r.mu.Lock()
	list := append([]*Stream(nil), r.order...)
	r.mu.Unlock()
	out := make([]StreamSnapshot, len(list))
	for i, s := range list {
		out[i] = s.Snapshot()
	}
	return out
}

// Ring returns the stream's ring, or nil while it is evicted.
func (s *Stream) Ring() *ring.Ring {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ring
}

func satAdd(a, b int64) int64 {
	c := a + b
	if a > 0 && b > 0 && c < 0 {
		return math.MaxInt64
	}
	if a < 0 && b < 0 && c >= 0 {
		return math.MinInt64
	}
	return c
}

func satSub(a, b int64) int64 {
	c := a - b
	if a >= 0 && b < 0 && c < 0 {
		return math.MaxInt64
	}
	if a < 0 && b > 0 && c >= 0 {
		return math.MinInt64
	}
	return c
}

// OnFrame classifies and records one frame from the attached connection.
func (s *Stream) OnFrame(connID uint64, f protocol.Frame, arrivalNs int64) Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != connID {
		panic(fmt.Sprintf("ingest: OnFrame from connection %d, stream is attached to %d", connID, s.conn))
	}
	seq := f.Seq
	if !s.hasFrames {
		s.initialSeq = seq
	} else if seq == s.lastSeq {
		s.dup++
		return Duplicate
	} else if seq < s.lastSeq {
		s.stale++
		return Stale
	} else {
		s.missing += seq - s.lastSeq - 1
	}
	s.lastSeq = seq
	s.hasFrames = true
	s.accepted++
	s.ring.Write(seq, func(dst []float32) { f.DecodeSamples(dst) })

	v := math.RoundToEven(float64(s.hello.SamplesPerFrame) * 1e9 / s.hello.SampleRateHz)
	var dur int64
	if v >= 0x1p63 {
		dur = math.MaxInt64
	} else {
		dur = int64(v)
	}
	lat := satSub(arrivalNs, satAdd(f.TimestampNs, dur))
	s.hist.record(lat)
	s.reg.intervalMu.Lock()
	s.reg.interval.record(lat)
	s.reg.intervalMu.Unlock()

	d := satSub(arrivalNs, f.TimestampNs)
	if s.n == 0 {
		s.x0, s.y0 = f.TimestampNs, d
	}
	x := float64(satSub(f.TimestampNs, s.x0)) / 1e9
	y := float64(satSub(d, s.y0)) / 1e9
	s.n++
	s.sx += x
	s.sy += y
	s.sxx += float64(x * x)
	s.sxy += float64(x * y)
	return Accepted
}

func (s *Stream) snapshotLocked() StreamSnapshot {
	snap := StreamSnapshot{
		StreamID:        s.id,
		DeviceName:      s.hello.DeviceName,
		NChannels:       s.hello.NChannels,
		SamplesPerFrame: s.hello.SamplesPerFrame,
		SampleRateHz:    s.hello.SampleRateHz,
		HasFrames:       s.hasFrames,
		InitialSeq:      s.initialSeq,
		LastSeq:         s.lastSeq,
		Accepted:        s.accepted,
		Missing:         s.missing,
		Dup:             s.dup,
		Stale:           s.stale,
	}
	if s.n >= 2 {
		n := float64(s.n)
		den := float64(n*s.sxx) - float64(s.sx*s.sx)
		if den > 0 {
			slope := (float64(n*s.sxy) - float64(s.sx*s.sy)) / den
			snap.DriftPPM = -slope * 1e6
			snap.HasDrift = true
		}
	}
	if s.evicted {
		snap.RingFrames = 0
		snap.Evicted = true
		snap.LatP50, snap.LatP99, snap.LatMax = s.evP50, s.evP99, s.evMax
	} else {
		snap.RingFrames = s.ring.Capacity()
		snap.LatP50, snap.LatP99, snap.LatMax = s.hist.p50(), s.hist.p99(), s.hist.maxString()
	}
	return snap
}

func (s *Stream) Snapshot() StreamSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}
