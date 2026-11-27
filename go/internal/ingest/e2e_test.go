//go:build e2e

package ingest

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func repoRoot(t testing.TB) string {
	t.Helper()
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func pythonPath(t testing.TB) string {
	t.Helper()
	p := os.Getenv("EEGPIPE_PYTHON")
	if p == "" {
		p = filepath.Join(repoRoot(t), ".venv", "bin", "python")
	}
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("python interpreter not found: %v", err)
	}
	return p
}

func startE2E(t *testing.T, mut func(*Config)) *runningServer {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Port = 0
	cfg.ExitAfterIdle = time.Second
	cfg.Duration = 60 * time.Second
	if mut != nil {
		mut(&cfg)
	}
	rs := startServer(t, cfg)
	rs.waitTimeout = 70 * time.Second
	return rs
}

// deviceRun never calls t, so it is safe in goroutines.
func deviceRun(py, root string, port, channels int, args ...string) (map[string]int64, string, error) {
	full := append([]string{"-m", "eegpipe.device", "synthetic", "--channels", strconv.Itoa(channels),
		"--rate", "1000", "--port", strconv.Itoa(port)}, args...)
	cmd := exec.Command(py, full...)
	cmd.Dir = root
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	runErr := cmd.Run()
	text := stderr.String()
	var line string
	for _, l := range strings.Split(text, "\n") {
		if strings.HasPrefix(l, "summary:") {
			line = l
		}
	}
	if runErr != nil {
		return nil, text, fmt.Errorf("device failed: %w\n%s", runErr, text)
	}
	if line == "" {
		return nil, text, errors.New("device printed no summary line:\n" + text)
	}
	m := map[string]int64{}
	for _, f := range strings.Fields(strings.TrimPrefix(line, "summary:")) {
		k, v, ok := strings.Cut(f, "=")
		if !ok {
			continue
		}
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			m[k] = n
		}
	}
	return m, text, nil
}

func runDevice(t *testing.T, port, channels int, args ...string) map[string]int64 {
	t.Helper()
	m, _, err := deviceRun(pythonPath(t), repoRoot(t), port, channels, args...)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func snapshotFor(t *testing.T, sum Summary, id uint64) StreamSnapshot {
	t.Helper()
	for _, sn := range sum.StreamSnapshots {
		if sn.StreamID == id {
			return sn
		}
	}
	t.Fatalf("no stream %016x in summary", id)
	return StreamSnapshot{}
}

func reconcile(t *testing.T, dev map[string]int64, snap StreamSnapshot) {
	t.Helper()
	if int64(snap.Accepted) != dev["sent"] {
		t.Errorf("Accepted %d != device sent %d", snap.Accepted, dev["sent"])
	}
	if int64(snap.Dup) != dev["dup"] {
		t.Errorf("Dup %d != device dup %d", snap.Dup, dev["dup"])
	}
	if snap.Stale != 0 {
		t.Errorf("Stale = %d", snap.Stale)
	}
	lhs := int64(snap.InitialSeq) + int64(snap.Missing) + (dev["frames"] - 1 - int64(snap.LastSeq))
	rhs := dev["drop"] + dev["overflow"] + dev["disc"]
	if lhs != rhs {
		t.Errorf("reconcile: initial %d + missing %d + (frames %d - 1 - last %d) = %d, want drop+overflow+disc = %d",
			snap.InitialSeq, snap.Missing, dev["frames"], snap.LastSeq, lhs, rhs)
	}
}

func TestE2EClean(t *testing.T) {
	rs := startE2E(t, nil)
	dev := runDevice(t, rs.srv.Port(), 8, "--duration", "2", "--stream-id", "00000000000000e1")
	sum, _ := rs.wait()
	sn := snapshotFor(t, sum, 0xe1)
	reconcile(t, dev, sn)
	if sn.Accepted != 100 || sn.InitialSeq != 0 || sn.Missing != 0 || sum.Connections != 1 || sum.EOFCloses != 1 || len(sum.Errors) != 0 {
		t.Fatalf("summary = %+v, stream %+v", sum, sn)
	}
}

func TestE2EDrop(t *testing.T) {
	rs := startE2E(t, nil)
	dev := runDevice(t, rs.srv.Port(), 8, "--duration", "2", "--seed", "7", "--drop-prob", "0.3", "--stream-id", "00000000000000e2")
	sum, _ := rs.wait()
	reconcile(t, dev, snapshotFor(t, sum, 0xe2))
	if dev["drop"] <= 0 {
		t.Fatalf("device drop = %d", dev["drop"])
	}
}

func TestE2EDuplicate(t *testing.T) {
	rs := startE2E(t, nil)
	dev := runDevice(t, rs.srv.Port(), 8, "--duration", "2", "--seed", "7", "--dup-prob", "0.3", "--stream-id", "00000000000000e3")
	sum, _ := rs.wait()
	sn := snapshotFor(t, sum, 0xe3)
	reconcile(t, dev, sn)
	if int64(sn.Dup) != dev["dup"] || dev["dup"] <= 0 {
		t.Fatalf("Dup %d, device dup %d", sn.Dup, dev["dup"])
	}
}

func TestE2EJitterAndDrift(t *testing.T) {
	rs := startE2E(t, nil)
	dev := runDevice(t, rs.srv.Port(), 8, "--duration", "2", "--jitter-ms", "5", "--drift-ppm", "500", "--stream-id", "00000000000000e4")
	sum, _ := rs.wait()
	reconcile(t, dev, snapshotFor(t, sum, 0xe4))
}

func TestE2EDisconnects(t *testing.T) {
	rs := startE2E(t, nil)
	dev := runDevice(t, rs.srv.Port(), 8, "--duration", "2", "--disconnect-every", "0.3", "--stream-id", "00000000000000e5")
	sum, _ := rs.wait()
	reconcile(t, dev, snapshotFor(t, sum, 0xe5))
	if int64(sum.Connections) != dev["connects"] || dev["connects"] <= 1 || sum.EOFCloses != sum.Connections || sum.Superseded != 0 || sum.Missing != 0 {
		t.Fatalf("summary = %+v, device %v", sum, dev)
	}
}

func TestE2EAllFaultsButCorruption(t *testing.T) {
	rs := startE2E(t, nil)
	dev := runDevice(t, rs.srv.Port(), 8, "--duration", "2", "--seed", "11", "--drop-prob", "0.2", "--dup-prob", "0.2",
		"--jitter-ms", "3", "--drift-ppm", "-300", "--disconnect-every", "0.25", "--stream-id", "00000000000000e6")
	sum, _ := rs.wait()
	reconcile(t, dev, snapshotFor(t, sum, 0xe6))
}

func TestE2ECorruptionBounds(t *testing.T) {
	rs := startE2E(t, nil)
	dev := runDevice(t, rs.srv.Port(), 8, "--duration", "2", "--corrupt-prob", "0.1", "--stream-id", "00000000000000e7")
	sum, _ := rs.wait()
	sn := snapshotFor(t, sum, 0xe7)
	cs := int64(sum.Errors["ChecksumError"])
	if cs < 1 || cs > dev["corrupt"] || int64(sn.Accepted) > dev["sent"] || sn.Stale != 0 {
		t.Fatalf("ChecksumError %d, device %v, stream %+v", cs, dev, sn)
	}
}

func TestE2EConcurrentDevices(t *testing.T) {
	rs := startE2E(t, func(c *Config) { c.ExitAfterIdle = 3 * time.Second })
	py, root := pythonPath(t), repoRoot(t)
	port := rs.srv.Port()
	devs := make([]map[string]int64, 5)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	for k := 1; k <= 4; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			m, _, err := deviceRun(py, root, port, 64, "--duration", "3", "--drop-prob", "0.1",
				"--seed", strconv.Itoa(k), "--stream-id", fmt.Sprintf("00000000000000f%d", k))
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			devs[k] = m
		}(k)
	}
	wg.Wait()
	joinErrs(t, &mu, &errs)
	if t.Failed() {
		return
	}
	sum, _ := rs.wait()
	for k := 1; k <= 4; k++ {
		reconcile(t, devs[k], snapshotFor(t, sum, 0xf0+uint64(k)))
	}
	if sum.Connections != 4 || sum.RejectedConns != 0 || sum.Streams != 4 {
		t.Fatalf("summary = %+v", sum)
	}
}

func TestE2EDriftRealTime(t *testing.T) {
	rs := startE2E(t, nil)
	dev := runDevice(t, rs.srv.Port(), 8, "--duration", "2", "--drift-ppm", "10000", "--stream-id", "00000000000000e8")
	sum, _ := rs.wait()
	sn := snapshotFor(t, sum, 0xe8)
	if !sn.HasDrift || math.Abs(sn.DriftPPM-10000/1.01) >= 1500 || int64(sn.Accepted) != dev["sent"] {
		t.Fatalf("drift %v (has %v), accepted %d, device sent %d", sn.DriftPPM, sn.HasDrift, sn.Accepted, dev["sent"])
	}
}
