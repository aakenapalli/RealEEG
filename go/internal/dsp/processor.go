package dsp

// Processor is the per-stream processing chain. It is not safe for concurrent
// use, except Channels on disjoint channel ranges between two BeginFrame calls.
type Processor struct {
	s, c, d, op, n, h int
	rate              float64
	lp, hp, nt        []*Cascade
	hist              [][]float64
	pos               []int
	bp                *BandPower
	count             int
	due               []int
}

// NewProcessor creates a processor; it panics if the stream is not processable.
func NewProcessor(sampleRateHz float64, samplesPerFrame, nChannels int, mainsHz float64) *Processor {
	if !Processable(sampleRateHz, samplesPerFrame) {
		panic("dsp: NewProcessor: stream is not processable")
	}
	d := DecimationFactor(sampleRateHz, samplesPerFrame)
	r := sampleRateHz / float64(d)
	p := &Processor{s: samplesPerFrame, c: nChannels, d: d, op: samplesPerFrame / d, rate: r,
		n: WindowSize(r), h: HopSize(r)}
	lps := ButterLowpass(LowpassOrder, LowpassFraction*r, sampleRateHz)
	hps := ButterHighpass(HighpassOrder, HighpassHz, r)
	nts := []Section{Notch(mainsHz, NotchQ, r)}
	for c := 0; c < nChannels; c++ {
		p.lp = append(p.lp, NewCascade(lps))
		p.hp = append(p.hp, NewCascade(hps))
		p.nt = append(p.nt, NewCascade(nts))
		p.hist = append(p.hist, make([]float64, p.n))
	}
	p.pos = make([]int, nChannels)
	p.bp = NewBandPower(r, p.n)
	return p
}

// Decimation returns D.
func (p *Processor) Decimation() int { return p.d }

// OutRate returns the output rate.
func (p *Processor) OutRate() float64 { return p.rate }

// OutPerFrame returns the output samples per input frame.
func (p *Processor) OutPerFrame() int { return p.op }

// Reset resets the filters and the sample count.
func (p *Processor) Reset() {
	for c := range p.lp {
		p.lp[c].Reset()
		p.hp[c].Reset()
		p.nt[c].Reset()
	}
	p.count = 0
}

// BeginFrame returns the output positions in the coming frame at which a
// feature is due. The slice is reused and valid until the next call.
func (p *Processor) BeginFrame() []int {
	p.due = p.due[:0]
	for j := 0; j < p.op; j++ {
		p.count++
		if p.count >= p.n && (p.count-p.n)%p.h == 0 {
			p.due = append(p.due, j)
		}
	}
	return p.due
}

// Channels processes channels lo..hi-1 of one frame.
func (p *Processor) Channels(in, out []float32, feats [][]float32, due []int, lo, hi int) {
	bad := len(in) != p.c*p.s || len(out) != p.c*p.op || len(feats) != len(due) || !(0 <= lo && lo <= hi && hi <= p.c)
	for _, f := range feats {
		if len(f) != p.c*NumBands {
			bad = true
		}
	}
	if bad {
		panic("dsp: Channels: bad buffer sizes")
	}
	x := make([]float64, p.s)
	scratch := make([]float64, p.n)
	var bp [NumBands]float64
	for c := lo; c < hi; c++ {
		for i := 0; i < p.s; i++ {
			x[i] = float64(in[c*p.s+i])
		}
		p.lp[c].Run(x)
		low := x[:p.op]
		for j := 0; j < p.op; j++ {
			low[j] = x[j*p.d+p.d-1]
		}
		p.hp[c].Run(low)
		p.nt[c].Run(low)
		hist := p.hist[c]
		pos := p.pos[c]
		f := 0
		for j := 0; j < p.op; j++ {
			v := float32(low[j])
			out[c*p.op+j] = v
			hist[pos] = float64(v)
			pos = (pos + 1) % p.n
			if f < len(due) && j == due[f] {
				p.bp.Compute(hist, pos, scratch, &bp)
				for b := 0; b < NumBands; b++ {
					feats[f][c*NumBands+b] = float32(bp[b])
				}
				f++
			}
		}
		p.pos[c] = pos
	}
}
