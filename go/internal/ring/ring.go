// Package ring implements a per-stream ring of frames indexed by seq.
package ring

import (
	"fmt"
	"sync"
)

// Status is the result of Read.
type Status int

const (
	OK          Status = iota + 1
	NotYet             // seq is above the newest frame written (or nothing written yet)
	Missing            // seq was never written: a gap, or any seq below the first frame
	Overwritten        // seq was written but a newer frame has reused its slot
)

func (s Status) String() string {
	switch s {
	case OK:
		return "ok"
	case NotYet:
		return "not_yet"
	case Missing:
		return "missing"
	case Overwritten:
		return "overwritten"
	}
	return fmt.Sprintf("Status(%d)", int(s))
}

// Ring holds the most recent frames of one stream.
type Ring struct {
	mu       sync.RWMutex
	capacity int
	frameLen int
	data     []float32
	slotSeq  []uint64
	stamps   []int64
	filled   []bool
	changed  chan struct{}
	head     uint64
	first    uint64
	hasHead  bool
}

// New allocates a ring of capacity frames of nChannels x samplesPerFrame samples.
func New(capacity, nChannels, samplesPerFrame int) *Ring {
	if capacity < 1 || nChannels < 1 || samplesPerFrame < 1 {
		panic("ring: New: capacity, nChannels and samplesPerFrame must be >= 1")
	}
	fl := nChannels * samplesPerFrame
	return &Ring{
		capacity: capacity,
		frameLen: fl,
		data:     make([]float32, capacity*fl),
		slotSeq:  make([]uint64, capacity),
		stamps:   make([]int64, capacity),
		filled:   make([]bool, capacity),
	}
}

func (r *Ring) Capacity() int { return r.capacity }
func (r *Ring) FrameLen() int { return r.frameLen }

// Head returns the newest seq written; ok is false if none.
func (r *Ring) Head() (seq uint64, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.head, r.hasHead
}

// Write stores frame seq with stamp 0; see WriteStamped.
func (r *Ring) Write(seq uint64, fill func(dst []float32)) {
	r.WriteStamped(seq, 0, fill)
}

// WriteStamped stores frame seq, which must be above the current head, with
// the given stamp. fill must write exactly FrameLen samples into the slice it
// is given. It wakes the readers waiting on Changed.
func (r *Ring) WriteStamped(seq uint64, stamp int64, fill func(dst []float32)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.hasHead && seq <= r.head {
		panic(fmt.Sprintf("ring: Write seq %d is not above head %d", seq, r.head))
	}
	i := int(seq % uint64(r.capacity))
	fill(r.data[i*r.frameLen : (i+1)*r.frameLen : (i+1)*r.frameLen])
	r.slotSeq[i] = seq
	r.stamps[i] = stamp
	r.filled[i] = true
	r.head = seq
	if !r.hasHead {
		r.first = seq
		r.hasHead = true
	}
	if r.changed != nil {
		close(r.changed)
		r.changed = nil
	}
}

// First returns the seq of the first frame written; ok is false if none.
func (r *Ring) First() (seq uint64, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.first, r.hasHead
}

// Changed returns a channel that the next write closes. A reader takes it
// before Read and waits on it only if Read returned NotYet.
func (r *Ring) Changed() <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.changed == nil {
		r.changed = make(chan struct{})
	}
	return r.changed
}

// Read copies frame seq into dst (which must hold FrameLen elements) and
// returns OK, or says why the frame is unavailable. dst is only written on OK.
func (r *Ring) Read(seq uint64, dst []float32) Status {
	st, _ := r.ReadStamped(seq, dst)
	return st
}

// ReadStamped is Read that also returns the frame's stamp on OK, and 0 with
// every other status.
func (r *Ring) ReadStamped(seq uint64, dst []float32) (Status, int64) {
	if len(dst) != r.frameLen {
		panic(fmt.Sprintf("ring: Read dst has %d elements, want %d", len(dst), r.frameLen))
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.hasHead || seq > r.head {
		return NotYet, 0
	}
	if seq < r.first {
		return Missing, 0
	}
	if r.head-seq >= uint64(r.capacity) {
		return Overwritten, 0
	}
	i := int(seq % uint64(r.capacity))
	if r.filled[i] && r.slotSeq[i] == seq {
		copy(dst, r.data[i*r.frameLen:(i+1)*r.frameLen])
		return OK, r.stamps[i]
	}
	return Missing, 0
}
