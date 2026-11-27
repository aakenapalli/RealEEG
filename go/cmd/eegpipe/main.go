// Command eegpipe is the EEG pipeline core: it ingests EEGP streams.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aakenapalli/RealEEG/go/internal/ingest"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("eegpipe", flag.ContinueOnError)
	fs.SetOutput(stderr)
	host := fs.String("host", "127.0.0.1", "address to listen on")
	port := fs.Int("port", 7700, "port to listen on (1 to 65535)")
	duration := fs.Float64("duration", 0, "stop after this many seconds, closing open connections")
	exitIdle := fs.Float64("exit-after-idle", 0, "stop once no connection has been open for this many seconds")
	maxConns := fs.Int("max-conns", 16, "connections served at once (1 to 1024)")
	ringSeconds := fs.Float64("ring-seconds", 2, "seconds of each stream held in its ring buffer (up to 60)")
	ringBudgetMB := fs.Int("ring-budget-mb", 1024, "memory for all streams together, in MiB (1 to 65536)")
	mainsHz := fs.Int("mains-hz", 60, "mains frequency to notch out, 50 or 60")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "eegpipe: error: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	given := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { given[f.Name] = true })

	fail := func(msg string) int {
		fmt.Fprintln(stderr, "eegpipe: error: "+msg)
		return 2
	}
	okSeconds := func(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) && v > 0 && v <= 1e9 }

	if *host == "" {
		return fail("--host must be non-empty")
	}
	if *port < 1 || *port > 65535 {
		return fail("--port must be an integer in 1..65535")
	}
	if given["duration"] && !okSeconds(*duration) {
		return fail("--duration must be a number in (0, 1e9]")
	}
	if given["exit-after-idle"] && !okSeconds(*exitIdle) {
		return fail("--exit-after-idle must be a number in (0, 1e9]")
	}
	if *maxConns < 1 || *maxConns > 1024 {
		return fail("--max-conns must be an integer in 1..1024")
	}
	if !(*ringSeconds > 0 && *ringSeconds <= 60) {
		return fail("--ring-seconds must be a number in (0, 60]")
	}
	if *ringBudgetMB < 1 || *ringBudgetMB > 65536 {
		return fail("--ring-budget-mb must be an integer in 1..65536")
	}
	if *mainsHz != 50 && *mainsHz != 60 {
		return fail("--mains-hz must be 50 or 60")
	}

	cfg := ingest.DefaultConfig()
	cfg.Host = *host
	cfg.Port = *port
	if given["duration"] {
		cfg.Duration = time.Duration(*duration * float64(time.Second))
	}
	if given["exit-after-idle"] {
		cfg.ExitAfterIdle = time.Duration(*exitIdle * float64(time.Second))
	}
	cfg.MaxConns = *maxConns
	cfg.RingSeconds = *ringSeconds
	cfg.RingBudgetBytes = int64(*ringBudgetMB) << 20
	cfg.MainsHz = float64(*mainsHz)

	srv, err := ingest.Listen(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "eegpipe: error: %v\n", err)
		return 2
	}
	srv.Run(ctx, stderr)
	return 0
}
