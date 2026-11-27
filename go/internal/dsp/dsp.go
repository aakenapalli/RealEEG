// Package dsp holds the filter design, biquad cascades, decimation and band
// power of the processing stage. It mirrors python/eegpipe/dsp.py.
package dsp

import (
	"fmt"
	"math"
	"math/cmplx"
)

const (
	TargetOutputRateHz = 250.0
	MinOutputRateHz    = 125.0
	MaxOutputRateHz    = 1000.0
	LowpassOrder       = 8
	LowpassFraction    = 0.36
	HighpassOrder      = 2
	HighpassHz         = 1.0
	NotchQ             = 30.0
	FeaturesPerSecond  = 4
	NumBands           = 5
)

// BandNames are the names of the five bands, in order.
var BandNames = [NumBands]string{"delta", "theta", "alpha", "beta", "gamma"}

// BandEdges are the [lo, hi) edges of the bands in Hz.
var BandEdges = [NumBands][2]float64{{1, 4}, {4, 8}, {8, 13}, {13, 30}, {30, 45}}

// Section is a second-order section; a0 is 1.
type Section struct{ B0, B1, B2, A1, A2 float64 }

// DecimationFactor is the largest divisor d of samplesPerFrame with
// sampleRateHz/d >= TargetOutputRateHz, or 1.
func DecimationFactor(sampleRateHz float64, samplesPerFrame int) int {
	best := 1
	for d := 1; d <= samplesPerFrame; d++ {
		if samplesPerFrame%d == 0 && sampleRateHz/float64(d) >= TargetOutputRateHz {
			best = d
		}
	}
	return best
}

// OutputRate is the rate after decimation.
func OutputRate(sampleRateHz float64, samplesPerFrame int) float64 {
	return sampleRateHz / float64(DecimationFactor(sampleRateHz, samplesPerFrame))
}

// Processable reports whether the stream's output rate is supported.
func Processable(sampleRateHz float64, samplesPerFrame int) bool {
	r := OutputRate(sampleRateHz, samplesPerFrame)
	return MinOutputRateHz <= r && r <= MaxOutputRateHz
}

func butter(order int, cutoffHz, rateHz float64, highpass bool) []Section {
	if order < 2 || order%2 != 0 {
		panic("dsp: Butterworth order must be even and >= 2")
	}
	k := math.Tan(math.Pi * cutoffHz / rateHz)
	out := make([]Section, 0, order/2)
	for i := 0; i < order/2; i++ {
		twoZeta := 2 * math.Sin(math.Pi*float64(2*i+1)/float64(2*order))
		norm := 1 / (1 + twoZeta*k + k*k)
		var s Section
		if highpass {
			s.B0, s.B1, s.B2 = norm, -2*norm, norm
		} else {
			s.B0 = k * k * norm
			s.B1 = 2 * s.B0
			s.B2 = s.B0
		}
		s.A1 = 2 * (k*k - 1) * norm
		s.A2 = (1 - twoZeta*k + k*k) * norm
		out = append(out, s)
	}
	return out
}

// ButterLowpass designs a Butterworth low-pass as second-order sections.
func ButterLowpass(order int, cutoffHz, rateHz float64) []Section {
	return butter(order, cutoffHz, rateHz, false)
}

// ButterHighpass designs a Butterworth high-pass as second-order sections.
func ButterHighpass(order int, cutoffHz, rateHz float64) []Section {
	return butter(order, cutoffHz, rateHz, true)
}

// Notch designs a second-order notch.
func Notch(f0Hz, q, rateHz float64) Section {
	w0 := 2 * math.Pi * f0Hz / rateHz
	alpha := math.Sin(w0) / (2 * q)
	a0 := 1 + alpha
	c := -2 * math.Cos(w0)
	return Section{1 / a0, c / a0, 1 / a0, c / a0, (1 - alpha) / a0}
}

// Gain is the magnitude of the cascade's frequency response at fHz.
func Gain(sections []Section, fHz, rateHz float64) float64 {
	z := cmplx.Exp(complex(0, -2*math.Pi*fHz/rateHz))
	h := complex(1, 0)
	for _, s := range sections {
		num := complex(s.B0, 0) + complex(s.B1, 0)*z + complex(s.B2, 0)*z*z
		den := complex(1, 0) + complex(s.A1, 0)*z + complex(s.A2, 0)*z*z
		h *= num / den
	}
	return cmplx.Abs(h)
}

// Cascade is a chain of biquads with float64 state.
type Cascade struct {
	sections []Section
	state    []float64
}

// NewCascade returns a cascade with zero state.
func NewCascade(sections []Section) *Cascade {
	return &Cascade{sections: append([]Section(nil), sections...), state: make([]float64, 2*len(sections))}
}

// Reset zeroes the state.
func (c *Cascade) Reset() {
	for i := range c.state {
		c.state[i] = 0
	}
}

// Run filters x in place, section by section, in transposed direct form II.
func (c *Cascade) Run(x []float64) {
	for s, sec := range c.sections {
		z1, z2 := c.state[2*s], c.state[2*s+1]
		for i, v := range x {
			y := sec.B0*v + z1
			z1 = sec.B1*v - sec.A1*y + z2
			z2 = sec.B2*v - sec.A2*y
			x[i] = y
		}
		c.state[2*s], c.state[2*s+1] = z1, z2
	}
}

// WindowSize is the feature window length in output samples.
func WindowSize(outRateHz float64) int { return int(math.RoundToEven(outRateHz)) }

// HopSize is the number of output samples between features.
func HopSize(outRateHz float64) int {
	return int(math.RoundToEven(outRateHz / FeaturesPerSecond))
}

// BandBins returns, for each band, the DFT bins inside it.
func BandBins(outRateHz float64, n int) [NumBands][]int {
	var out [NumBands][]int
	for b := 0; b < NumBands; b++ {
		for k := 1; k <= (n-1)/2; k++ {
			f := float64(k) * outRateHz / float64(n)
			if BandEdges[b][0] <= f && f < BandEdges[b][1] {
				out[b] = append(out[b], k)
			}
		}
	}
	return out
}

// BandPower computes Hann-windowed periodogram band powers.
type BandPower struct {
	n     int
	w     []float64
	scale float64
	cosT  []float64
	sinT  []float64
	bins  [NumBands][]int
}

// NewBandPower precomputes the window and tables for n samples at outRateHz.
func NewBandPower(outRateHz float64, n int) *BandPower {
	b := &BandPower{n: n, w: make([]float64, n), cosT: make([]float64, n), sinT: make([]float64, n)}
	sum := 0.0
	for j := 0; j < n; j++ {
		b.w[j] = 0.5 - 0.5*math.Cos(2*math.Pi*float64(j)/float64(n))
		sum += b.w[j] * b.w[j]
	}
	b.scale = 2 / (float64(n) * sum)
	for m := 0; m < n; m++ {
		b.cosT[m] = math.Cos(2 * math.Pi * float64(m) / float64(n))
		b.sinT[m] = math.Sin(2 * math.Pi * float64(m) / float64(n))
	}
	b.bins = BandBins(outRateHz, n)
	return b
}

// Compute fills dst with the band powers of the circular buffer hist whose
// oldest sample is at start. scratch must have length >= n.
func (b *BandPower) Compute(hist []float64, start int, scratch []float64, dst *[NumBands]float64) {
	n := b.n
	if len(hist) != n {
		panic(fmt.Sprintf("dsp: Compute: hist has %d samples, want %d", len(hist), n))
	}
	for j := 0; j < n; j++ {
		scratch[j] = hist[(start+j)%n] * b.w[j]
	}
	for band := 0; band < NumBands; band++ {
		dst[band] = 0
		for _, k := range b.bins[band] {
			re, im, m := 0.0, 0.0, 0
			for j := 0; j < n; j++ {
				re += scratch[j] * b.cosT[m]
				im += scratch[j] * b.sinT[m]
				m += k
				if m >= n {
					m -= n
				}
			}
			dst[band] += b.scale * (re*re + im*im)
		}
	}
}

// StateBytes is the memory charged for the processing state of a stream.
func StateBytes(nChannels int, outRateHz float64) int64 {
	n := int64(WindowSize(outRateHz))
	return int64(nChannels)*(8*n+512) + 24*n
}
