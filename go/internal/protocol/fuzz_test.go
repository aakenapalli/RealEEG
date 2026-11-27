package protocol

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func FuzzDecodeMessage(f *testing.F) {
	files, err := filepath.Glob(filepath.Join("..", "..", "..", "testdata", "protocol", "*.hex"))
	if err != nil {
		f.Fatal(err)
	}
	for _, p := range files {
		f.Add(loadVector(f, filepath.Base(p)))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := DecodeMessage(data)
		if err != nil {
			return
		}
		switch m.Type {
		case MsgFrame:
			samples := make([]float32, m.Frame.NChannels*m.Frame.SamplesPerFrame)
			m.Frame.DecodeSamples(samples)
			enc, err := EncodeFrame(m.Frame.Seq, m.Frame.TimestampNs, m.Frame.NChannels, m.Frame.SamplesPerFrame, samples)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(enc, data) {
				t.Fatalf("FRAME re-encode differs")
			}
		case MsgHello:
			enc, err := EncodeHello(m.Hello)
			if err != nil {
				if strings.HasPrefix(err.Error(), "protocol: HELLO body ") && strings.HasSuffix(err.Error(), " bytes exceeds maximum 262144") {
					return
				}
				t.Fatal(err)
			}
			m2, err := DecodeMessage(enc)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(m2.Hello, m.Hello) {
				t.Fatalf("HELLO round trip differs: %+v vs %+v", m2.Hello, m.Hello)
			}
		}
	})
}
