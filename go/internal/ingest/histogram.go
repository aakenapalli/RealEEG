package ingest

import (
	"fmt"
	"math"
)

const (
	histBucketNs = 100_000       // 0.1 ms
	histLimitNs  = 1_000_000_000 // values are clamped to +-1 s for bucketing
	histOffset   = 10_000        // bucket index of 0 ns
	histBuckets  = 20_001
)

type histogram struct {
	counts [histBuckets]uint64
	n      uint64
	max    int64
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

func (h *histogram) record(v int64) {
	c := v
	if c < -histLimitNs {
		c = -histLimitNs
	}
	if c > histLimitNs {
		c = histLimitNs
	}
	idx := floorDiv(c+histBucketNs/2, histBucketNs) + histOffset
	h.counts[idx]++
	h.n++
	if h.n == 1 || v > h.max {
		h.max = v
	}
}

// quantile returns the nearest-rank quantile, rounded to the bucket. Only
// called with n > 0.
func (h *histogram) quantile(q float64) int64 {
	rank := int(math.Ceil(q * float64(h.n)))
	if rank < 1 {
		rank = 1
	}
	var run uint64
	for idx, c := range h.counts {
		run += c
		if run >= uint64(rank) {
			return int64(idx-histOffset) * histBucketNs
		}
	}
	return int64(histBuckets-1-histOffset) * histBucketNs
}

func (h *histogram) reset() {
	h.counts = [histBuckets]uint64{}
	h.n = 0
	h.max = 0
}

func msFormat(v int64) string { return fmt.Sprintf("%.1f", float64(v)/1e6) }

func (h *histogram) p50() string {
	if h.n == 0 {
		return "n/a"
	}
	return msFormat(h.quantile(0.5))
}

func (h *histogram) p99() string {
	if h.n == 0 {
		return "n/a"
	}
	return msFormat(h.quantile(0.99))
}

func (h *histogram) maxString() string {
	if h.n == 0 {
		return "n/a"
	}
	return msFormat(h.max)
}
