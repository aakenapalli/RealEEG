package ingest

import "testing"

func TestHistogramEmpty(t *testing.T) {
	var h histogram
	if h.p50() != "n/a" || h.p99() != "n/a" || h.maxString() != "n/a" {
		t.Fatal("expected n/a")
	}
}

func TestHistogramBucketRounding(t *testing.T) {
	cases := []struct{ v, want int64 }{{149999, 100000}, {150000, 200000}, {-50000, 0}, {-50001, -100000}, {0, 0}}
	for _, c := range cases {
		var h histogram
		h.record(c.v)
		if got := h.quantile(0.5); got != c.want {
			t.Errorf("record(%d): quantile = %d, want %d", c.v, got, c.want)
		}
	}
}

func TestHistogramNearestRank(t *testing.T) {
	var h histogram
	for i := int64(1); i <= 10; i++ {
		h.record(i * 1_000_000)
	}
	if h.p50() != "5.0" || h.p99() != "10.0" || h.maxString() != "10.0" {
		t.Fatalf("got %s %s %s", h.p50(), h.p99(), h.maxString())
	}
}

func TestHistogramClamp(t *testing.T) {
	var h histogram
	h.record(5e9)
	if h.p50() != "1000.0" || h.maxString() != "5000.0" {
		t.Fatalf("got %s %s", h.p50(), h.maxString())
	}
	var g histogram
	g.record(-5e9)
	if g.p50() != "-1000.0" || g.maxString() != "-5000.0" {
		t.Fatalf("got %s %s", g.p50(), g.maxString())
	}
}

func TestHistogramReset(t *testing.T) {
	var h histogram
	h.record(3_000_000)
	h.reset()
	if h.p50() != "n/a" || h.p99() != "n/a" || h.maxString() != "n/a" || h.n != 0 {
		t.Fatal("reset failed")
	}
}
