package ingest

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aakenapalli/RealEEG/go/internal/protocol"
	"github.com/aakenapalli/RealEEG/go/internal/ring"
)

func testConfig() Config {
	cfg := DefaultConfig()
	cfg.Port = 0
	cfg.HelloTimeout = 300 * time.Millisecond
	cfg.IdleTimeout = 300 * time.Millisecond
	cfg.StatsInterval = 100 * time.Millisecond
	cfg.PollInterval = 20 * time.Millisecond
	return cfg
}

type runningServer struct {
	t           *testing.T
	srv         *Server
	cancel      context.CancelFunc
	done        chan struct{}
	buf         bytes.Buffer
	sum         Summary
	waitTimeout time.Duration
	finished    bool
}

func startServer(t *testing.T, cfg Config) *runningServer {
	t.Helper()
	srv, err := Listen(cfg)
	if err != nil {
		t.Fatal(err)
	}
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

func (rs *runningServer) collect(d time.Duration) (Summary, string) {
	rs.t.Helper()
	select {
	case <-rs.done:
		rs.finished = true
		return rs.sum, rs.buf.String()
	case <-time.After(d):
		rs.t.Fatal("Run did not return")
		return Summary{}, ""
	}
}

func (rs *runningServer) stop() (Summary, string) {
	rs.t.Helper()
	rs.cancel()
	return rs.collect(5 * time.Second)
}

func (rs *runningServer) wait() (Summary, string) {
	rs.t.Helper()
	return rs.collect(rs.waitTimeout)
}

func counts(rs *runningServer) Summary { return rs.srv.counters() }

func dial(t *testing.T, port int) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func now() int64 { return time.Now().UnixNano() }

func H(id uint64) protocol.Hello { return testHello(id, 2, 4, 200) }

var helloLen = func() int {
	b, err := protocol.EncodeHello(H1)
	if err != nil {
		panic(err)
	}
	return len(b)
}()

func writeHello(c net.Conn, h protocol.Hello) error {
	b, err := protocol.EncodeHello(h)
	if err != nil {
		return err
	}
	_, err = c.Write(b)
	return err
}

func frameBytes(seq uint64, ts int64, ch, s int, value float32) ([]byte, error) {
	samples := make([]float32, ch*s)
	for i := range samples {
		samples[i] = value
	}
	return protocol.EncodeFrame(seq, ts, ch, s, samples)
}

func writeFrame(c net.Conn, seq uint64, ts int64, ch, s int, value float32) error {
	b, err := frameBytes(seq, ts, ch, s, value)
	if err != nil {
		return err
	}
	_, err = c.Write(b)
	return err
}

func sendHello(t *testing.T, c net.Conn, h protocol.Hello) {
	t.Helper()
	if err := writeHello(c, h); err != nil {
		t.Fatal(err)
	}
}

func sendFrame(t *testing.T, c net.Conn, seq uint64, ts int64, ch, s int, value float32) {
	t.Helper()
	if err := writeFrame(c, seq, ts, ch, s, value); err != nil {
		t.Fatal(err)
	}
}

func sendFrames(t *testing.T, c net.Conn, from, to uint64) {
	t.Helper()
	for q := from; q <= to; q++ {
		sendFrame(t, c, q, now()-20*ms, 2, 4, 0)
	}
}

type writer struct {
	stopCh chan struct{}
	doneCh chan struct{}
	n      int64
	mu     sync.Mutex
}

func startWriter(c net.Conn, firstSeq uint64, every time.Duration) *writer {
	w := &writer{stopCh: make(chan struct{}), doneCh: make(chan struct{})}
	go func() {
		defer close(w.doneCh)
		tick := time.NewTicker(every)
		defer tick.Stop()
		for seq := firstSeq; ; seq++ {
			select {
			case <-w.stopCh:
				return
			case <-tick.C:
			}
			if err := writeFrame(c, seq, now()-20*ms, 2, 4, 0); err != nil {
				return
			}
			w.mu.Lock()
			w.n++
			w.mu.Unlock()
		}
	}()
	return w
}

func (w *writer) stop() {
	select {
	case <-w.stopCh:
	default:
		close(w.stopCh)
	}
}

func (w *writer) join() error {
	select {
	case <-w.doneCh:
		return nil
	case <-time.After(2 * time.Second):
		return errors.New("writer did not end")
	}
}

func (w *writer) sent() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.n
}

func waitClosed(t *testing.T, c net.Conn) {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	p := make([]byte, 4096)
	for {
		_, err := c.Read(p)
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatal("not closed")
			}
			return
		}
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func loadVector(t testing.TB, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "testdata", "protocol", name))
	if err != nil {
		t.Fatal(err)
	}
	var sb strings.Builder
	for _, line := range strings.Split(string(raw), "\n") {
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = line[:i]
		}
		sb.WriteString(line)
	}
	b, err := hex.DecodeString(strings.Join(strings.Fields(sb.String()), ""))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func wrapMsg(msgType uint8, body []byte) []byte {
	b := make([]byte, 12, 12+len(body)+4)
	copy(b, "EEGP")
	b[4] = 1
	b[5] = msgType
	binary.LittleEndian.PutUint32(b[8:], uint32(len(body)))
	b = append(b, body...)
	return binary.LittleEndian.AppendUint32(b, crc32.ChecksumIEEE(b))
}

func errTotal(rs *runningServer) uint64 { return errorTotal(counts(rs).Errors) }

func joinErrs(t *testing.T, mu *sync.Mutex, errs *[]error) {
	t.Helper()
	mu.Lock()
	defer mu.Unlock()
	for _, e := range *errs {
		t.Error(e)
	}
}

func TestServesValidStream(t *testing.T) {
	rs := startServer(t, testConfig())
	c := dial(t, rs.srv.Port())
	sendHello(t, c, H1)
	sendFrames(t, c, 0, 9)
	c.Close()
	waitFor(t, "EOFCloses", func() bool { c := counts(rs); return c.EOFCloses == 1 && c.Processed == 10 })
	sum, _ := rs.stop()
	if sum.Connections != 1 || sum.Accepted != 10 || sum.Missing != 0 || sum.EOFCloses != 1 || len(sum.Errors) != 0 || sum.Streams != 1 {
		t.Fatalf("summary = %+v", sum)
	}
}

func TestOutputFormat(t *testing.T) {
	cfg := testConfig()
	cfg.IdleTimeout = 5 * time.Second
	rs := startServer(t, cfg)
	c := dial(t, rs.srv.Port())
	sendHello(t, c, H1)
	sendFrames(t, c, 0, 4)
	time.Sleep(250 * time.Millisecond)
	c.Close()
	waitFor(t, "EOFCloses", func() bool { return counts(rs).EOFCloses == 1 })
	_, out := rs.stop()
	sub := strings.NewReplacer("{D}", `\d+`, "{L}", `(-?\d+\.\d|n/a)`, "{B}", `(\S+)`)
	re := func(s string) *regexp.Regexp { return regexp.MustCompile(sub.Replace(s)) }
	startRe := re(`^start: listen=127\.0\.0\.1:\d+ max_conns=16 ring_seconds=2 ring_budget_mb=1024 hello_timeout_s=0\.3 idle_timeout_s=5 mains_hz=60$`)
	intervalRe := re(`^t=\d+\.\ds conns={D} accepted={D} missing={D} dup={D} stale={D} errors={D} superseded={D} lat_p50_ms={L} lat_p99_ms={L} lat_max_ms={L} MB/s=\d+\.\d\d proc_p99_ms={L} overruns={D}$`)
	summaryRe := re(`^summary: connections={D} rejected_conns={D} accepted={D} missing={D} dup={D} stale={D} eof_closes={D} truncated={D} idle_closes={D} hello_timeouts={D} conn_errors={D} accept_errors={D} superseded={D} errors=(none|[A-Za-z]+:\d+(,[A-Za-z]+:\d+)*) streams={D} evicted={D} bytes={D} elapsed_s=\d+\.\d{3} MB/s=\d+\.\d\d processed={D} gaps={D} overruns={D} proc_p99_ms={L} proc_max_ms={L}$`)
	streamRe := re(`^stream: id=[0-9a-f]{16} device=\S+ channels={D} rate_hz=\S+ samples_per_frame={D} initial_seq=(\d+|n/a) last_seq=(\d+|n/a) accepted={D} missing={D} dup={D} stale={D} ring_frames={D} state=(live|evicted) drift_ppm=(-?\d+\.\d|n/a) lat_p50_ms={L} lat_p99_ms={L} lat_max_ms={L}$`)
	procRe := re(`^proc: id=[0-9a-f]{16} (unprocessed out_rate_hz=\S+|decim={D} out_rate_hz=\S+ frames={D} gaps={D} overruns={D} features={D} lat_p50_ms={L} lat_p99_ms={L} lat_max_ms={L} bp_delta={B} bp_theta={B} bp_alpha={B} bp_beta={B} bp_gamma={B})$`)
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) < 5 {
		t.Fatalf("output:\n%s", out)
	}
	if !startRe.MatchString(lines[0]) {
		t.Errorf("bad start line %q", lines[0])
	}
	for _, l := range lines[1 : len(lines)-3] {
		if !intervalRe.MatchString(l) {
			t.Errorf("bad interval line %q", l)
		}
	}
	if !summaryRe.MatchString(lines[len(lines)-3]) {
		t.Errorf("bad summary line %q", lines[len(lines)-3])
	}
	sl := lines[len(lines)-2]
	if !streamRe.MatchString(sl) || !strings.Contains(sl, "accepted=5") || !strings.Contains(sl, "ring_frames=100") || !strings.Contains(sl, "state=live") {
		t.Errorf("bad stream line %q", sl)
	}
	pl := lines[len(lines)-1]
	if !procRe.MatchString(pl) || !strings.Contains(pl, "frames=5") {
		t.Errorf("bad proc line %q", pl)
	}
}

func TestProtocolErrorsCloseAndCount(t *testing.T) {
	hdr := func(version, typ uint8, reserved uint16) []byte {
		b := make([]byte, 12)
		copy(b, "EEGP")
		b[4], b[5] = version, typ
		binary.LittleEndian.PutUint16(b[6:], reserved)
		return b
	}
	badUnits := `{"stream_id":"9f3a5c0e12b47d68","device_name":"virtual-synthetic","n_channels":3,"samples_per_frame":2,"sample_rate_hz":160.0,"units":"mV","channel_names":["Fp1","Cz","O2"]}`
	cases := []struct {
		name string
		want string
		send func(t *testing.T, c net.Conn)
	}{
		{"bad_magic", "BadMagicError", func(t *testing.T, c net.Conn) { c.Write(loadVector(t, "frame_bad_magic.hex")) }},
		{"bad_version", "UnsupportedVersionError", func(t *testing.T, c net.Conn) { c.Write(hdr(2, 2, 0)) }},
		{"unknown_type", "UnknownMessageTypeError", func(t *testing.T, c net.Conn) { c.Write(hdr(1, 3, 0)) }},
		{"reserved", "ReservedFieldError", func(t *testing.T, c net.Conn) { c.Write(hdr(1, 2, 1)) }},
		{"bad_crc", "ChecksumError", func(t *testing.T, c net.Conn) {
			sendHello(t, c, H1)
			b, err := frameBytes(0, now(), 2, 4, 0)
			if err != nil {
				t.Fatal(err)
			}
			b[len(b)-1] ^= 0xff
			c.Write(b)
		}},
		{"frame_first", "MessageOrderError", func(t *testing.T, c net.Conn) { sendFrame(t, c, 0, now(), 2, 4, 0) }},
		{"two_hellos", "MessageOrderError", func(t *testing.T, c net.Conn) { sendHello(t, c, H1); sendHello(t, c, H1) }},
		{"wrong_shape", "DimensionMismatchError", func(t *testing.T, c net.Conn) { sendHello(t, c, H1); sendFrame(t, c, 0, now(), 3, 4, 0) }},
		{"invalid_hello", "InvalidHelloError", func(t *testing.T, c net.Conn) { c.Write(wrapMsg(1, []byte(badUnits))) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rs := startServer(t, testConfig())
			c := dial(t, rs.srv.Port())
			tc.send(t, c)
			waitClosed(t, c)
			waitFor(t, "one error", func() bool { return errTotal(rs) == 1 })
			sum, _ := rs.stop()
			if len(sum.Errors) != 1 || sum.Errors[tc.want] != 1 {
				t.Fatalf("errors = %v, want %s", sum.Errors, tc.want)
			}
		})
	}
}

func TestHelloMismatchOnReconnect(t *testing.T) {
	rs := startServer(t, testConfig())
	a := dial(t, rs.srv.Port())
	sendHello(t, a, H1)
	sendFrames(t, a, 0, 0)
	a.Close()
	waitFor(t, "A done", func() bool { c := counts(rs); return c.Accepted == 1 && c.EOFCloses == 1 })
	b := dial(t, rs.srv.Port())
	sendHello(t, b, testHello(1, 3, 4, 200))
	waitClosed(t, b)
	waitFor(t, "mismatch", func() bool { return counts(rs).Errors["HelloMismatchError"] == 1 })
	sum, _ := rs.stop()
	if sum.Errors["HelloMismatchError"] != 1 || sum.Accepted != 1 {
		t.Fatalf("summary = %+v", sum)
	}
}

func TestHelloTimeout(t *testing.T) {
	rs := startServer(t, testConfig())
	c := dial(t, rs.srv.Port())
	waitClosed(t, c)
	waitFor(t, "HelloTimeouts", func() bool { return counts(rs).HelloTimeouts == 1 })
	sum, _ := rs.stop()
	if sum.HelloTimeouts != 1 {
		t.Fatalf("summary = %+v", sum)
	}
}

func TestTrickledHelloTimesOut(t *testing.T) {
	rs := startServer(t, testConfig())
	c := dial(t, rs.srv.Port())
	b, err := protocol.EncodeHello(H1)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range b {
			if _, err := c.Write(b[i : i+1]); err != nil {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
	waitClosed(t, c)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("writer did not end")
	}
	waitFor(t, "HelloTimeouts", func() bool { return counts(rs).HelloTimeouts == 1 })
	sum, _ := rs.stop()
	if sum.HelloTimeouts != 1 {
		t.Fatalf("summary = %+v", sum)
	}
}

func TestIdleTimeout(t *testing.T) {
	rs := startServer(t, testConfig())
	c := dial(t, rs.srv.Port())
	sendHello(t, c, H1)
	waitClosed(t, c)
	waitFor(t, "IdleCloses", func() bool { return counts(rs).IdleCloses == 1 })
	sum, _ := rs.stop()
	if sum.IdleCloses != 1 || sum.HelloTimeouts != 0 {
		t.Fatalf("summary = %+v", sum)
	}
}

func TestTruncated(t *testing.T) {
	rs := startServer(t, testConfig())
	c := dial(t, rs.srv.Port())
	sendHello(t, c, H1)
	b, err := frameBytes(0, now(), 2, 4, 0)
	if err != nil {
		t.Fatal(err)
	}
	c.Write(b[:30])
	c.Close()
	waitFor(t, "Truncated", func() bool { return counts(rs).Truncated == 1 })
	sum, _ := rs.stop()
	if sum.Truncated != 1 {
		t.Fatalf("summary = %+v", sum)
	}
}

func TestMaxConns(t *testing.T) {
	cfg := testConfig()
	cfg.MaxConns = 2
	cfg.IdleTimeout = 5 * time.Second
	rs := startServer(t, cfg)
	a := dial(t, rs.srv.Port())
	b := dial(t, rs.srv.Port())
	sendHello(t, a, H(1))
	sendHello(t, b, H(2))
	waitFor(t, "2 streams", func() bool { return counts(rs).Streams == 2 })
	c := dial(t, rs.srv.Port())
	_ = writeHello(c, H(3))
	waitClosed(t, c)
	waitFor(t, "RejectedConns", func() bool { return counts(rs).RejectedConns == 1 })
	sum, _ := rs.stop()
	if sum.Connections != 3 || sum.RejectedConns != 1 || sum.Streams != 2 {
		t.Fatalf("summary = %+v", sum)
	}
}

func TestConcurrentStreams(t *testing.T) {
	rs := startServer(t, testConfig())
	conns := make([]net.Conn, 5)
	for k := 1; k <= 4; k++ {
		conns[k] = dial(t, rs.srv.Port())
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	for k := 1; k <= 4; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			c := conns[k]
			err := writeHello(c, H(uint64(k)))
			for q := uint64(0); err == nil && q < 200; q++ {
				err = writeFrame(c, q, now()-20*ms, 2, 4, 0)
			}
			c.Close()
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(k)
	}
	wg.Wait()
	joinErrs(t, &mu, &errs)
	waitFor(t, "all frames", func() bool { c := counts(rs); return c.Accepted == 800 && c.EOFCloses == 4 })
	sum, _ := rs.stop()
	if sum.Accepted != 800 || len(sum.StreamSnapshots) != 4 {
		t.Fatalf("summary = %+v", sum)
	}
	for _, sn := range sum.StreamSnapshots {
		if sn.Accepted != 200 || sn.Missing != 0 {
			t.Errorf("stream %d: %+v", sn.StreamID, sn)
		}
	}
}

func TestTakeoverAfterDrain(t *testing.T) {
	cfg := testConfig()
	cfg.IdleTimeout = 5 * time.Second
	rs := startServer(t, cfg)
	a := dial(t, rs.srv.Port())
	sendHello(t, a, H1)
	waitFor(t, "A attached", func() bool { return counts(rs).Streams == 1 })
	b := dial(t, rs.srv.Port())
	sendHello(t, b, H1)
	waitFor(t, "B hello read", func() bool { return counts(rs).Bytes == uint64(2*helloLen) })
	sendFrames(t, a, 0, 4)
	a.Close()
	sendFrames(t, b, 5, 9)
	b.Close()
	waitFor(t, "all frames", func() bool { c := counts(rs); return c.Accepted == 10 && c.EOFCloses == 2 })
	sum, _ := rs.stop()
	sn := sum.StreamSnapshots[0]
	if sum.Superseded != 0 || sn.Accepted != 10 || sn.Missing != 0 || sn.Stale != 0 {
		t.Fatalf("summary = %+v, stream %+v", sum, sn)
	}
}

func TestSupersedeAfterTakeoverWait(t *testing.T) {
	cfg := testConfig()
	cfg.IdleTimeout = 5 * time.Second
	cfg.TakeoverWait = 200 * time.Millisecond
	rs := startServer(t, cfg)
	a := dial(t, rs.srv.Port())
	sendHello(t, a, H1)
	sendFrames(t, a, 0, 4)
	waitFor(t, "A frames", func() bool { return counts(rs).Accepted == 5 })
	tB := time.Now()
	b := dial(t, rs.srv.Port())
	sendHello(t, b, H1)
	sendFrames(t, b, 5, 9)
	waitFor(t, "B frames", func() bool { return counts(rs).Accepted == 10 })
	d := time.Since(tB)
	waitClosed(t, a)
	b.Close()
	waitFor(t, "closes", func() bool { c := counts(rs); return c.EOFCloses == 1 && c.Superseded == 1 })
	sum, _ := rs.stop()
	sn := sum.StreamSnapshots[0]
	if d < 200*time.Millisecond || sum.Superseded != 1 || sum.EOFCloses != 1 || sn.Accepted != 10 || sn.Missing != 0 {
		t.Fatalf("d = %v, summary = %+v, stream %+v", d, sum, sn)
	}
}

func TestTwoWritersOneStream(t *testing.T) {
	cfg := testConfig()
	cfg.IdleTimeout = 5 * time.Second
	cfg.TakeoverWait = 100 * time.Millisecond
	rs := startServer(t, cfg)
	a := dial(t, rs.srv.Port())
	sendHello(t, a, H1)
	wA := startWriter(a, 0, 2*time.Millisecond)
	waitFor(t, "A frames", func() bool { return counts(rs).Accepted >= 10 })
	b := dial(t, rs.srv.Port())
	sendHello(t, b, H1)
	wB := startWriter(b, 100000, 2*time.Millisecond)
	time.Sleep(300 * time.Millisecond)
	wB.stop()
	if err := wB.join(); err != nil {
		t.Error(err)
	}
	b.Close()
	if err := wA.join(); err != nil {
		t.Error(err)
	}
	waitFor(t, "closes", func() bool { c := counts(rs); return c.Superseded == 1 && c.EOFCloses == 1 })
	sum, _ := rs.stop()
	sn := sum.StreamSnapshots[0]
	last := 100000 + uint64(wB.sent()) - 1
	if sn.InitialSeq != 0 || sn.LastSeq != last || sn.Dup != 0 || sn.Stale != 0 || sn.Accepted+sn.Missing != last+1 {
		t.Fatalf("stream %+v, want last %d", sn, last)
	}
}

func TestStopWhileStreaming(t *testing.T) {
	cfg := testConfig()
	cfg.IdleTimeout = 5 * time.Second
	rs := startServer(t, cfg)
	var ws []*writer
	for k := 1; k <= 4; k++ {
		c := dial(t, rs.srv.Port())
		sendHello(t, c, H(uint64(k)))
		ws = append(ws, startWriter(c, 0, 2*time.Millisecond))
	}
	waitFor(t, "frames", func() bool { return counts(rs).Accepted >= 40 })
	t1 := time.Now()
	sum, _ := rs.stop()
	if time.Since(t1) > 2*time.Second {
		t.Errorf("stop took %v", time.Since(t1))
	}
	for _, w := range ws {
		if err := w.join(); err != nil {
			t.Error(err)
		}
	}
	if sum.EOFCloses != 0 || sum.Truncated != 0 || sum.IdleCloses != 0 || sum.ConnErrors != 0 || sum.Superseded != 0 {
		t.Fatalf("summary = %+v", sum)
	}
}

func TestStopDuringTakeoverWait(t *testing.T) {
	cfg := testConfig()
	cfg.IdleTimeout = 5 * time.Second
	cfg.TakeoverWait = 10 * time.Second
	rs := startServer(t, cfg)
	a := dial(t, rs.srv.Port())
	sendHello(t, a, H1)
	waitFor(t, "A attached", func() bool { return counts(rs).Streams == 1 })
	b := dial(t, rs.srv.Port())
	sendHello(t, b, H1)
	waitFor(t, "B hello read", func() bool { return counts(rs).Bytes == uint64(2*helloLen) })
	t1 := time.Now()
	sum, _ := rs.stop()
	if time.Since(t1) > 2*time.Second {
		t.Errorf("stop took %v", time.Since(t1))
	}
	if sum.Superseded != 0 || len(sum.Errors) != 0 {
		t.Fatalf("summary = %+v", sum)
	}
}

func TestNewestWaiterWins(t *testing.T) {
	cfg := testConfig()
	cfg.IdleTimeout = 5 * time.Second
	cfg.TakeoverWait = 300 * time.Millisecond
	rs := startServer(t, cfg)
	a := dial(t, rs.srv.Port())
	sendHello(t, a, H1)
	waitFor(t, "A attached", func() bool { return counts(rs).Streams == 1 })
	b := dial(t, rs.srv.Port())
	sendHello(t, b, H1)
	waitFor(t, "B hello read", func() bool { return counts(rs).Bytes == uint64(2*helloLen) })
	c := dial(t, rs.srv.Port())
	sendHello(t, c, H1)
	waitFor(t, "C hello read", func() bool { return counts(rs).Bytes == uint64(3*helloLen) })
	waitFor(t, "2 superseded", func() bool { return counts(rs).Superseded == 2 })
	sendFrames(t, c, 0, 4)
	c.Close()
	waitFor(t, "C done", func() bool { cc := counts(rs); return cc.Accepted == 5 && cc.EOFCloses == 1 })
	waitClosed(t, a)
	waitClosed(t, b)
	sum, _ := rs.stop()
	if sum.Superseded != 2 || sum.EOFCloses != 1 || sum.StreamSnapshots[0].Accepted != 5 {
		t.Fatalf("summary = %+v", sum)
	}
}

func TestDurationStops(t *testing.T) {
	cfg := testConfig()
	cfg.Duration = 300 * time.Millisecond
	rs := startServer(t, cfg)
	t1 := time.Now()
	sum, _ := rs.wait()
	if time.Since(t1) > 2*time.Second || sum.Elapsed < 300*time.Millisecond {
		t.Fatalf("took %v, elapsed %v", time.Since(t1), sum.Elapsed)
	}
}

func TestDurationClosesActiveConnection(t *testing.T) {
	cfg := testConfig()
	cfg.Duration = 300 * time.Millisecond
	cfg.HelloTimeout = 5 * time.Second
	cfg.IdleTimeout = 5 * time.Second
	rs := startServer(t, cfg)
	c := dial(t, rs.srv.Port())
	sendHello(t, c, H1)
	sum, _ := rs.wait()
	waitClosed(t, c)
	if sum.EOFCloses != 0 || sum.IdleCloses != 0 || sum.Truncated != 0 {
		t.Fatalf("summary = %+v", sum)
	}
}

func TestExitAfterIdle(t *testing.T) {
	cfg := testConfig()
	cfg.ExitAfterIdle = 300 * time.Millisecond
	cfg.Duration = 3 * time.Second
	rs := startServer(t, cfg)
	t1 := time.Now()
	rs.wait()
	if d := time.Since(t1); d < 2900*time.Millisecond {
		t.Fatalf("idle exit fired before a connection: %v", d)
	}

	rs = startServer(t, cfg)
	c := dial(t, rs.srv.Port())
	sendHello(t, c, H1)
	c.Close()
	t2 := time.Now()
	rs.wait()
	if d := time.Since(t2); d > 1500*time.Millisecond {
		t.Fatalf("took %v after close", d)
	}
}

func TestResourceLimit(t *testing.T) {
	cfg := testConfig()
	cfg.RingBudgetBytes = 100
	rs := startServer(t, cfg)
	c := dial(t, rs.srv.Port())
	sendHello(t, c, H1)
	waitClosed(t, c)
	waitFor(t, "ResourceLimitError", func() bool { return counts(rs).Errors["ResourceLimitError"] == 1 })
	sum, _ := rs.stop()
	if sum.Errors["ResourceLimitError"] != 1 || sum.Streams != 0 {
		t.Fatalf("summary = %+v", sum)
	}
}

func TestRegistryRingHasSamples(t *testing.T) {
	rs := startServer(t, testConfig())
	c := dial(t, rs.srv.Port())
	sendHello(t, c, H1)
	sendFrame(t, c, 0, now()-20*ms, 2, 4, 7.25)
	waitFor(t, "Accepted", func() bool { return counts(rs).Accepted == 1 })
	s, ok := rs.srv.Registry().Lookup(1)
	if !ok {
		t.Fatal("no stream")
	}
	dst := make([]float32, 8)
	if st := s.Ring().Read(0, dst); st != ring.OK {
		t.Fatalf("Read = %v", st)
	}
	for _, v := range dst {
		if v != 7.25 {
			t.Fatalf("sample %v", v)
		}
	}
	rs.stop()
}

func TestListenPortInUse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	cfg := testConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = ln.Addr().(*net.TCPAddr).Port
	if _, err := Listen(cfg); err == nil {
		t.Fatal("Listen succeeded")
	}
}

func TestConfigValidate(t *testing.T) {
	inf := func() float64 { var z float64; return 1 / z }()
	cases := []struct {
		mut  func(c *Config)
		want string
	}{
		{func(c *Config) { c.Host = "" }, "ingest: Host must be non-empty"},
		{func(c *Config) { c.Port = -1 }, "ingest: Port must be in 0..65535"},
		{func(c *Config) { c.Port = 65536 }, "ingest: Port must be in 0..65535"},
		{func(c *Config) { c.Duration = -1 }, "ingest: Duration must be >= 0"},
		{func(c *Config) { c.ExitAfterIdle = -1 }, "ingest: ExitAfterIdle must be >= 0"},
		{func(c *Config) { c.MaxConns = 0 }, "ingest: MaxConns must be in 1..1024"},
		{func(c *Config) { c.MaxConns = 1025 }, "ingest: MaxConns must be in 1..1024"},
		{func(c *Config) { c.RingSeconds = 0 }, "ingest: RingSeconds must be in (0, 60]"},
		{func(c *Config) { c.RingSeconds = 61 }, "ingest: RingSeconds must be in (0, 60]"},
		{func(c *Config) { c.RingSeconds = inf }, "ingest: RingSeconds must be in (0, 60]"},
		{func(c *Config) { c.RingBudgetBytes = 0 }, "ingest: RingBudgetBytes must be >= 1"},
		{func(c *Config) { c.HelloTimeout = 0 }, "ingest: HelloTimeout must be > 0"},
		{func(c *Config) { c.IdleTimeout = 0 }, "ingest: IdleTimeout must be > 0"},
		{func(c *Config) { c.StatsInterval = 0 }, "ingest: StatsInterval must be > 0"},
		{func(c *Config) { c.PollInterval = 0 }, "ingest: PollInterval must be > 0"},
		{func(c *Config) { c.TakeoverWait = -1 }, "ingest: TakeoverWait must be >= 0"},
		{func(c *Config) { c.EvictAfter = -1 }, "ingest: EvictAfter must be >= 0"},
		{func(c *Config) { c.MainsHz = 55 }, "ingest: MainsHz must be 50 or 60"},
	}
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		cfg := DefaultConfig()
		c.mut(&cfg)
		err := cfg.Validate()
		if err == nil || err.Error() != c.want {
			t.Errorf("got %v, want %q", err, c.want)
		}
	}
}
