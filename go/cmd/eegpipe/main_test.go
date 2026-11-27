package main

import (
	"bytes"
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aakenapalli/RealEEG/go/internal/protocol"
)

func runArgs(args ...string) (int, string) {
	var buf bytes.Buffer
	code := run(context.Background(), args, &buf)
	return code, buf.String()
}

func TestRunRejectsBadFlags(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"--port", "0"}, "--port must be an integer in 1..65535"},
		{[]string{"--port", "70000"}, "--port must be an integer in 1..65535"},
		{[]string{"--host", ""}, "--host must be non-empty"},
		{[]string{"--duration", "0"}, "--duration must be a number in (0, 1e9]"},
		{[]string{"--duration", "-1"}, "--duration must be a number in (0, 1e9]"},
		{[]string{"--duration", "2e9"}, "--duration must be a number in (0, 1e9]"},
		{[]string{"--duration", "NaN"}, "--duration must be a number in (0, 1e9]"},
		{[]string{"--exit-after-idle", "0"}, "--exit-after-idle must be a number in (0, 1e9]"},
		{[]string{"--max-conns", "0"}, "--max-conns must be an integer in 1..1024"},
		{[]string{"--max-conns", "1025"}, "--max-conns must be an integer in 1..1024"},
		{[]string{"--ring-seconds", "0"}, "--ring-seconds must be a number in (0, 60]"},
		{[]string{"--ring-seconds", "61"}, "--ring-seconds must be a number in (0, 60]"},
		{[]string{"--ring-budget-mb", "0"}, "--ring-budget-mb must be an integer in 1..65536"},
		{[]string{"--ring-budget-mb", "65537"}, "--ring-budget-mb must be an integer in 1..65536"},
		{[]string{"--mains-hz", "55"}, "--mains-hz must be 50 or 60"},
		{[]string{"--mains-hz", "0"}, "--mains-hz must be 50 or 60"},
		{[]string{"extra"}, `unexpected argument "extra"`},
	}
	for _, c := range cases {
		t.Run(strings.Join(c.args, "_"), func(t *testing.T) {
			code, out := runArgs(c.args...)
			if code != 2 || !strings.Contains(out, c.want) {
				t.Fatalf("code %d, stderr %q, want %q", code, out, c.want)
			}
		})
	}
}

func TestRunUnknownFlag(t *testing.T) {
	if code, _ := runArgs("--nope"); code != 2 {
		t.Fatalf("code = %d", code)
	}
}

func TestRunHelp(t *testing.T) {
	code, out := runArgs("--help")
	if code != 0 || !strings.Contains(out, "-ring-budget-mb") {
		t.Fatalf("code %d, stderr %q", code, out)
	}
}

func TestRunPortInUse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	code, out := runArgs("--port", strconv.Itoa(ln.Addr().(*net.TCPAddr).Port))
	if code != 2 || !strings.HasPrefix(out, "eegpipe: error: ") {
		t.Fatalf("code %d, stderr %q", code, out)
	}
}

func TestRunEndToEnd(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	var (
		mu   sync.Mutex
		buf  bytes.Buffer
		code = -1
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		var local bytes.Buffer
		c := run(context.Background(), []string{"--port", strconv.Itoa(port), "--exit-after-idle", "0.3", "--duration", "5"}, &local)
		mu.Lock()
		code = c
		buf = local
		mu.Unlock()
	}()

	var conn net.Conn
	deadline := time.Now().Add(2 * time.Second)
	for {
		conn, err = net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	hello, err := protocol.EncodeHello(protocol.Hello{StreamID: 1, DeviceName: "dev", NChannels: 2, SamplesPerFrame: 4, SampleRateHz: 200, ChannelNames: []string{"c0", "c1"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(hello); err != nil {
		t.Fatal(err)
	}
	for q := uint64(0); q < 3; q++ {
		f, err := protocol.EncodeFrame(q, time.Now().UnixNano()-int64(20*time.Millisecond), 2, 4, make([]float32, 8))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write(f); err != nil {
			t.Fatal(err)
		}
	}
	conn.Close()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return")
	}
	mu.Lock()
	defer mu.Unlock()
	out := buf.String()
	if code != 0 {
		t.Fatalf("code = %d", code)
	}
	if !strings.Contains(out, "start: ") || !strings.Contains(out, "summary: ") || !strings.Contains(out, "accepted=3") ||
		strings.Count(out, "\nstream: ")+boolInt(strings.HasPrefix(out, "stream: ")) != 1 ||
		strings.Count(out, "\nproc: ") != 1 {
		t.Fatalf("stderr:\n%s", out)
	}
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
