package ring

import (
	"math"
	"reflect"
	"sync"
	"testing"
	"time"
)

func pattern(seq uint64, n int) []float32 {
	p := make([]float32, n)
	for i := range p {
		p[i] = float32(seq%1000)*1000 + float32(i)
	}
	return p
}

func writeSeq(r *Ring, seq uint64) {
	r.Write(seq, func(dst []float32) { copy(dst, pattern(seq, r.FrameLen())) })
}

func mustPanic(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("did not panic")
		}
		if s, _ := r.(string); s != want {
			t.Fatalf("panic = %v, want %q", r, want)
		}
	}()
	fn()
}

func wantRead(t *testing.T, r *Ring, seq uint64, want Status) {
	t.Helper()
	dst := make([]float32, r.FrameLen())
	if got := r.Read(seq, dst); got != want {
		t.Fatalf("Read(%d) = %v, want %v", seq, got, want)
	}
	if want == OK && !reflect.DeepEqual(dst, pattern(seq, r.FrameLen())) {
		t.Fatalf("Read(%d) data = %v", seq, dst)
	}
}

func TestNewPanics(t *testing.T) {
	const msg = "ring: New: capacity, nChannels and samplesPerFrame must be >= 1"
	mustPanic(t, msg, func() { New(0, 1, 1) })
	mustPanic(t, msg, func() { New(1, 0, 1) })
	mustPanic(t, msg, func() { New(1, 1, 0) })
}

func TestEmpty(t *testing.T) {
	r := New(4, 2, 3)
	if r.Capacity() != 4 || r.FrameLen() != 6 {
		t.Fatal("dimensions")
	}
	if _, ok := r.Head(); ok {
		t.Fatal("Head ok")
	}
	wantRead(t, r, 0, NotYet)
}

func TestWriteRead(t *testing.T) {
	r := New(4, 2, 3)
	for s := uint64(0); s < 4; s++ {
		writeSeq(r, s)
	}
	for s := uint64(0); s < 4; s++ {
		wantRead(t, r, s, OK)
	}
	if h, ok := r.Head(); !ok || h != 3 {
		t.Fatalf("Head = %d %v", h, ok)
	}
}

func TestNotYet(t *testing.T) {
	r := New(4, 2, 3)
	for s := uint64(0); s < 3; s++ {
		writeSeq(r, s)
	}
	dst := []float32{9, 9, 9, 9, 9, 9}
	if got := r.Read(3, dst); got != NotYet {
		t.Fatalf("got %v", got)
	}
	if !reflect.DeepEqual(dst, []float32{9, 9, 9, 9, 9, 9}) {
		t.Fatal("dst touched")
	}
}

func TestGap(t *testing.T) {
	r := New(8, 1, 2)
	writeSeq(r, 0)
	writeSeq(r, 1)
	writeSeq(r, 5)
	for s := uint64(2); s <= 4; s++ {
		wantRead(t, r, s, Missing)
	}
	wantRead(t, r, 5, OK)
}

func TestOverwritten(t *testing.T) {
	r := New(4, 1, 2)
	for s := uint64(0); s < 10; s++ {
		writeSeq(r, s)
	}
	wantRead(t, r, 5, Overwritten)
	for s := uint64(6); s < 10; s++ {
		wantRead(t, r, s, OK)
	}
}

func TestBeforeFirst(t *testing.T) {
	r := New(4, 1, 2)
	writeSeq(r, 10)
	wantRead(t, r, 9, Missing)
	wantRead(t, r, 3, Missing)
}

func TestGapAfterWrap(t *testing.T) {
	r := New(4, 1, 2)
	for s := uint64(0); s < 4; s++ {
		writeSeq(r, s)
	}
	writeSeq(r, 6)
	wantRead(t, r, 4, Missing)
	wantRead(t, r, 5, Missing)
	wantRead(t, r, 3, OK)
	wantRead(t, r, 2, Overwritten)
}

func TestWriteNotIncreasingPanics(t *testing.T) {
	r := New(4, 1, 2)
	writeSeq(r, 5)
	mustPanic(t, "ring: Write seq 5 is not above head 5", func() { writeSeq(r, 5) })
	mustPanic(t, "ring: Write seq 4 is not above head 5", func() { writeSeq(r, 4) })
}

func TestReadWrongLenPanics(t *testing.T) {
	r := New(4, 2, 3)
	mustPanic(t, "ring: Read dst has 7 elements, want 6", func() { r.Read(0, make([]float32, 7)) })
}

func TestLargeSeq(t *testing.T) {
	r := New(4, 1, 2)
	for _, s := range []uint64{math.MaxUint64 - 2, math.MaxUint64 - 1, math.MaxUint64} {
		writeSeq(r, s)
	}
	for _, s := range []uint64{math.MaxUint64 - 2, math.MaxUint64 - 1, math.MaxUint64} {
		wantRead(t, r, s, OK)
	}
	if h, _ := r.Head(); h != math.MaxUint64 {
		t.Fatalf("Head = %d", h)
	}
}

func TestConcurrentReaders(t *testing.T) {
	r := New(16, 1, 1024)
	var done bool
	var mu sync.Mutex
	isDone := func() bool { mu.Lock(); defer mu.Unlock(); return done }
	var wg sync.WaitGroup
	errs := make(chan string, 16)
	for k := 0; k < 4; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dst := make([]float32, 1024)
			for !isDone() {
				h, ok := r.Head()
				if !ok {
					continue
				}
				for d := uint64(0); d < 8 && d <= h; d++ {
					seq := h - d
					if r.Read(seq, dst) == OK && !reflect.DeepEqual(dst, pattern(seq, 1024)) {
						select {
						case errs <- "torn frame":
						default:
						}
						return
					}
				}
			}
		}()
	}
	for s := uint64(0); s < 2000; s++ {
		writeSeq(r, s)
	}
	mu.Lock()
	done = true
	mu.Unlock()
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}

func TestStamps(t *testing.T) {
	r := New(4, 1, 2)
	dst := make([]float32, 2)
	r.WriteStamped(0, 777, func(d []float32) {})
	writeSeq(r, 1)
	if st, stamp := r.ReadStamped(0, dst); st != OK || stamp != 777 {
		t.Errorf("ReadStamped(0) = %v, %d", st, stamp)
	}
	if st, stamp := r.ReadStamped(1, dst); st != OK || stamp != 0 {
		t.Errorf("ReadStamped(1) = %v, %d", st, stamp)
	}
	if st, stamp := r.ReadStamped(2, dst); st != NotYet || stamp != 0 {
		t.Errorf("ReadStamped(2) = %v, %d", st, stamp)
	}
}

func TestFirst(t *testing.T) {
	r := New(4, 1, 2)
	if _, ok := r.First(); ok {
		t.Error("First ok on an empty ring")
	}
	writeSeq(r, 5)
	writeSeq(r, 6)
	if seq, ok := r.First(); seq != 5 || !ok {
		t.Errorf("First = %d, %v", seq, ok)
	}
}

func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestChangedClosedByWrite(t *testing.T) {
	r := New(4, 1, 2)
	ch := r.Changed()
	if closed(ch) {
		t.Fatal("channel closed before a write")
	}
	writeSeq(r, 0)
	if !closed(ch) {
		t.Fatal("channel not closed by Write")
	}
	if closed(r.Changed()) {
		t.Fatal("new channel is closed")
	}
}

func TestChangedWakesAllWaiters(t *testing.T) {
	r := New(4, 1, 2)
	ch := r.Changed()
	var wg sync.WaitGroup
	done := make(chan struct{}, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ch
			done <- struct{}{}
		}()
	}
	writeSeq(r, 0)
	for i := 0; i < 4; i++ {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("only %d waiters woke", i)
		}
	}
	wg.Wait()
}
