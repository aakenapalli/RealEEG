package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aakenapalli/RealEEG/go/internal/protocol"
)

type Config struct {
	Host            string
	Port            int           // 0..65535; 0 picks a free port
	Duration        time.Duration // 0 = run until the context is cancelled
	ExitAfterIdle   time.Duration // 0 = off
	MaxConns        int           // operator requirement, not checked by Validate: see "Takeover"
	RingSeconds     float64
	RingBudgetBytes int64 // rings plus StreamOverheadBytes per live stream
	HelloTimeout    time.Duration
	IdleTimeout     time.Duration
	TakeoverWait    time.Duration // how long a new connection waits for a stream's old one
	EvictAfter      time.Duration
	StatsInterval   time.Duration
	PollInterval    time.Duration
}

func DefaultConfig() Config {
	return Config{
		Host:            "127.0.0.1",
		Port:            7700,
		Duration:        0,
		ExitAfterIdle:   0,
		MaxConns:        16,
		RingSeconds:     2,
		RingBudgetBytes: 1 << 30,
		HelloTimeout:    5 * time.Second,
		IdleTimeout:     5 * time.Second,
		TakeoverWait:    500 * time.Millisecond,
		EvictAfter:      60 * time.Second,
		StatsInterval:   time.Second,
		PollInterval:    200 * time.Millisecond,
	}
}

func (c Config) Validate() error {
	switch {
	case c.Host == "":
		return errors.New("ingest: Host must be non-empty")
	case c.Port < 0 || c.Port > 65535:
		return errors.New("ingest: Port must be in 0..65535")
	case c.Duration < 0:
		return errors.New("ingest: Duration must be >= 0")
	case c.ExitAfterIdle < 0:
		return errors.New("ingest: ExitAfterIdle must be >= 0")
	case c.MaxConns < 1 || c.MaxConns > 1024:
		return errors.New("ingest: MaxConns must be in 1..1024")
	case !(c.RingSeconds > 0 && c.RingSeconds <= 60):
		return errors.New("ingest: RingSeconds must be in (0, 60]")
	case c.RingBudgetBytes < 1:
		return errors.New("ingest: RingBudgetBytes must be >= 1")
	case c.HelloTimeout <= 0:
		return errors.New("ingest: HelloTimeout must be > 0")
	case c.IdleTimeout <= 0:
		return errors.New("ingest: IdleTimeout must be > 0")
	case c.StatsInterval <= 0:
		return errors.New("ingest: StatsInterval must be > 0")
	case c.PollInterval <= 0:
		return errors.New("ingest: PollInterval must be > 0")
	case c.TakeoverWait < 0:
		return errors.New("ingest: TakeoverWait must be >= 0")
	case c.EvictAfter < 0:
		return errors.New("ingest: EvictAfter must be >= 0")
	}
	return nil
}

type Summary struct {
	Connections, RejectedConns              uint64
	Accepted, Missing, Dup, Stale           uint64
	EOFCloses, Truncated, IdleCloses        uint64
	HelloTimeouts, ConnErrors, AcceptErrors uint64
	Superseded                              uint64
	Errors                                  map[string]uint64 // error name -> count; empty map if none
	Streams, Evicted                        int               // Evicted: streams evicted when the Summary was built
	Bytes                                   uint64
	Elapsed                                 time.Duration
	StreamSnapshots                         []StreamSnapshot
}

const takeoverPoll = 5 * time.Millisecond

type status int

const (
	statusOK status = iota
	statusStop
	statusSuperseded
	statusEOF
	statusTruncated
	statusIdle
	statusHelloTimeout
	statusConnError
)

type activeConn struct {
	c          net.Conn
	superseded atomic.Bool
}

type Server struct {
	cfg  Config
	ln   net.Listener
	reg  *Registry
	port int

	ran      atomic.Bool
	stopping atomic.Bool
	bytes    atomic.Uint64
	wg       sync.WaitGroup

	connsMu   sync.Mutex
	active    map[uint64]*activeConn
	nextID    uint64
	idleSet   bool
	idleSince time.Time

	statsMu       sync.Mutex
	connections   uint64
	rejectedConns uint64
	eofCloses     uint64
	truncated     uint64
	idleCloses    uint64
	helloTimeouts uint64
	connErrors    uint64
	acceptErrors  uint64
	superseded    uint64
	errs          map[string]uint64
}

// Listen validates cfg and opens the listening socket.
func Listen(cfg Config) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)))
	if err != nil {
		return nil, err
	}
	return &Server{
		cfg:    cfg,
		ln:     ln,
		port:   ln.Addr().(*net.TCPAddr).Port,
		reg:    NewRegistry(RegistryConfig{RingSeconds: cfg.RingSeconds, RingBudgetBytes: cfg.RingBudgetBytes, EvictAfter: cfg.EvictAfter}),
		active: make(map[uint64]*activeConn),
		errs:   make(map[string]uint64),
	}, nil
}

func (s *Server) Port() int           { return s.port }
func (s *Server) Registry() *Registry { return s.reg }

func (s *Server) counters() Summary {
	s.statsMu.Lock()
	sum := Summary{
		Connections:   s.connections,
		RejectedConns: s.rejectedConns,
		EOFCloses:     s.eofCloses,
		Truncated:     s.truncated,
		IdleCloses:    s.idleCloses,
		HelloTimeouts: s.helloTimeouts,
		ConnErrors:    s.connErrors,
		AcceptErrors:  s.acceptErrors,
		Superseded:    s.superseded,
		Errors:        make(map[string]uint64, len(s.errs)),
	}
	for k, v := range s.errs {
		sum.Errors[k] = v
	}
	s.statsMu.Unlock()
	sum.Bytes = s.bytes.Load()
	t := s.reg.Totals()
	sum.Accepted, sum.Missing, sum.Dup, sum.Stale = t.Accepted, t.Missing, t.Dup, t.Stale
	sum.Streams, sum.Evicted = t.Streams, t.Evicted
	return sum
}

func (s *Server) bump(p *uint64) {
	s.statsMu.Lock()
	*p++
	s.statsMu.Unlock()
}

func (s *Server) count(err error) {
	name := "UnknownError"
	var pe *protocol.Error
	var re *RejectError
	switch {
	case errors.As(err, &pe):
		name = pe.Kind.String()
	case errors.As(err, &re):
		name = re.Name
	}
	s.statsMu.Lock()
	s.errs[name]++
	s.statsMu.Unlock()
}

func (s *Server) finish(st status) {
	switch st {
	case statusEOF:
		s.bump(&s.eofCloses)
	case statusTruncated:
		s.bump(&s.truncated)
	case statusIdle:
		s.bump(&s.idleCloses)
	case statusHelloTimeout:
		s.bump(&s.helloTimeouts)
	case statusConnError:
		s.bump(&s.connErrors)
	case statusSuperseded:
		s.bump(&s.superseded)
	}
}

func (s *Server) supersede(connID uint64) {
	s.connsMu.Lock()
	if ac, ok := s.active[connID]; ok {
		ac.superseded.Store(true)
		ac.c.Close()
	}
	s.connsMu.Unlock()
}

func (s *Server) acceptLoop() {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			if s.stopping.Load() || errors.Is(err, net.ErrClosed) {
				return
			}
			s.bump(&s.acceptErrors)
			time.Sleep(s.cfg.PollInterval)
			continue
		}
		s.bump(&s.connections)
		s.connsMu.Lock()
		if s.stopping.Load() {
			s.connsMu.Unlock()
			c.Close()
			continue
		}
		if len(s.active) >= s.cfg.MaxConns {
			s.connsMu.Unlock()
			s.bump(&s.rejectedConns)
			c.Close()
			continue
		}
		s.nextID++
		id := s.nextID
		ac := &activeConn{c: c}
		s.active[id] = ac
		s.idleSet = false
		s.wg.Add(1)
		s.connsMu.Unlock()
		go s.serve(id, ac)
	}
}

func (s *Server) serve(id uint64, ac *activeConn) {
	conn := ac.c
	var validator protocol.Validator
	buf := make([]byte, protocol.HeaderSize+protocol.MaxHelloBody+protocol.CRCSize)
	var stream *Stream
	start := time.Now()
	lastData := start
	defer func() {
		if stream != nil {
			s.reg.Detach(stream, id)
		}
		conn.Close()
		s.connsMu.Lock()
		delete(s.active, id)
		if len(s.active) == 0 {
			s.idleSet = true
			s.idleSince = time.Now()
		}
		s.connsMu.Unlock()
		s.wg.Done()
	}()

	read := func(p []byte, boundary bool) status {
		got := 0
		for got < len(p) {
			deadline := lastData.Add(s.cfg.IdleTimeout)
			if stream == nil {
				if hd := start.Add(s.cfg.HelloTimeout); hd.Before(deadline) {
					deadline = hd
				}
			}
			conn.SetReadDeadline(deadline)
			n, err := conn.Read(p[got:])
			if n > 0 {
				got += n
				lastData = time.Now()
				s.bytes.Add(uint64(n))
			}
			if got == len(p) {
				return statusOK
			}
			if err != nil {
				switch {
				case s.stopping.Load():
					return statusStop
				case ac.superseded.Load():
					return statusSuperseded
				case errors.Is(err, io.EOF):
					if boundary && got == 0 {
						return statusEOF
					}
					return statusTruncated
				case errors.Is(err, os.ErrDeadlineExceeded):
					if stream == nil && time.Since(start) >= s.cfg.HelloTimeout {
						return statusHelloTimeout
					}
					return statusIdle
				default:
					return statusConnError
				}
			}
		}
		return statusOK
	}

	takeover := func(h protocol.Hello) (*Stream, status, error) {
		var waitingFor uint64
		var deadline time.Time
		requested := false
		for {
			st, err := s.reg.Attach(h, id)
			if err == nil {
				lastData = time.Now()
				return st, statusOK, nil
			}
			var be *BusyError
			if !errors.As(err, &be) {
				return nil, statusOK, err
			}
			if s.stopping.Load() {
				return nil, statusStop, nil
			}
			if be.ConnID > id {
				return nil, statusSuperseded, nil
			}
			now := time.Now()
			if be.ConnID != waitingFor {
				waitingFor = be.ConnID
				deadline = now.Add(s.cfg.TakeoverWait)
				requested = false
			}
			if !requested && !now.Before(deadline) {
				s.supersede(be.ConnID)
				requested = true
			}
			time.Sleep(takeoverPoll)
		}
	}

	for {
		if st := read(buf[:protocol.HeaderSize], true); st != statusOK {
			s.finish(st)
			return
		}
		h, err := protocol.DecodeHeader(buf[:protocol.HeaderSize])
		if err != nil {
			s.count(err)
			return
		}
		if err = validator.CheckHeader(h); err != nil {
			s.count(err)
			return
		}
		total := protocol.HeaderSize + int(h.BodyLength) + protocol.CRCSize
		if total > len(buf) {
			nb := make([]byte, total)
			copy(nb, buf[:protocol.HeaderSize])
			buf = nb
		}
		if st := read(buf[protocol.HeaderSize:total], false); st != statusOK {
			s.finish(st)
			return
		}
		arrival := time.Now().UnixNano()
		msg, err := protocol.DecodeMessage(buf[:total])
		if err != nil {
			s.count(err)
			return
		}
		if err = validator.Check(msg); err != nil {
			s.count(err)
			return
		}
		switch msg.Type {
		case protocol.MsgHello:
			var st status
			stream, st, err = takeover(msg.Hello)
			if err != nil {
				s.count(err)
				return
			}
			if st != statusOK {
				s.finish(st)
				return
			}
		case protocol.MsgFrame:
			stream.OnFrame(id, msg.Frame, arrival)
		}
	}
}

func (s *Server) idleExpired() bool {
	if s.cfg.ExitAfterIdle <= 0 {
		return false
	}
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	return s.idleSet && time.Since(s.idleSince) >= s.cfg.ExitAfterIdle
}

func errorTotal(m map[string]uint64) uint64 {
	var n uint64
	for _, v := range m {
		n += v
	}
	return n
}

// Run serves until the context is cancelled or a stop condition holds, writes
// the report to out and returns the summary. It may be called once.
func (s *Server) Run(ctx context.Context, out io.Writer) Summary {
	if s.ran.Swap(true) {
		panic("ingest: Run called twice")
	}
	fmt.Fprintf(out, "start: listen=%s max_conns=%d ring_seconds=%s ring_budget_mb=%d hello_timeout_s=%s idle_timeout_s=%s\n",
		net.JoinHostPort(s.cfg.Host, strconv.Itoa(s.port)), s.cfg.MaxConns, g6(s.cfg.RingSeconds),
		s.cfg.RingBudgetBytes/(1<<20), g6(s.cfg.HelloTimeout.Seconds()), g6(s.cfg.IdleTimeout.Seconds()))
	t0 := time.Now()

	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		s.acceptLoop()
	}()

	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()
	lastLine := t0
	prev := s.counters()
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
		}
		if s.cfg.Duration > 0 && time.Since(t0) >= s.cfg.Duration {
			break
		}
		if s.idleExpired() {
			break
		}
		if now := time.Now(); now.Sub(lastLine) >= s.cfg.StatsInterval {
			cur := s.counters()
			p50, p99, mx := s.reg.TakeInterval()
			secs := now.Sub(lastLine).Seconds()
			fmt.Fprintf(out, "t=%.1fs conns=%d accepted=%d missing=%d dup=%d stale=%d errors=%d superseded=%d lat_p50_ms=%s lat_p99_ms=%s lat_max_ms=%s MB/s=%.2f\n",
				now.Sub(t0).Seconds(), cur.Connections-prev.Connections, cur.Accepted-prev.Accepted,
				cur.Missing-prev.Missing, cur.Dup-prev.Dup, cur.Stale-prev.Stale,
				errorTotal(cur.Errors)-errorTotal(prev.Errors), cur.Superseded-prev.Superseded,
				p50, p99, mx, float64(cur.Bytes-prev.Bytes)/secs/1e6)
			prev = cur
			lastLine = now
		}
	}

	s.stopping.Store(true)
	s.ln.Close()
	s.connsMu.Lock()
	for _, ac := range s.active {
		ac.c.Close()
	}
	s.connsMu.Unlock()
	<-acceptDone
	s.wg.Wait()

	sum := s.counters()
	sum.StreamSnapshots = s.reg.Snapshots()
	sum.Elapsed = time.Since(t0)

	list := "none"
	if len(sum.Errors) > 0 {
		names := make([]string, 0, len(sum.Errors))
		for k := range sum.Errors {
			names = append(names, k)
		}
		sort.Strings(names)
		parts := make([]string, len(names))
		for i, k := range names {
			parts[i] = k + ":" + strconv.FormatUint(sum.Errors[k], 10)
		}
		list = strings.Join(parts, ",")
	}
	mbs := 0.0
	if sum.Elapsed > 0 {
		mbs = float64(sum.Bytes) / sum.Elapsed.Seconds() / 1e6
	}
	fmt.Fprintf(out, "summary: connections=%d rejected_conns=%d accepted=%d missing=%d dup=%d stale=%d eof_closes=%d truncated=%d idle_closes=%d hello_timeouts=%d conn_errors=%d accept_errors=%d superseded=%d errors=%s streams=%d evicted=%d bytes=%d elapsed_s=%.3f MB/s=%.2f\n",
		sum.Connections, sum.RejectedConns, sum.Accepted, sum.Missing, sum.Dup, sum.Stale, sum.EOFCloses, sum.Truncated,
		sum.IdleCloses, sum.HelloTimeouts, sum.ConnErrors, sum.AcceptErrors, sum.Superseded, list, sum.Streams, sum.Evicted,
		sum.Bytes, sum.Elapsed.Seconds(), mbs)
	for _, sn := range sum.StreamSnapshots {
		fmt.Fprintln(out, sn.Line())
	}
	return sum
}
