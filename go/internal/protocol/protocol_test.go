package protocol

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

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
	clean := strings.Join(strings.Fields(sb.String()), "")
	b, err := hex.DecodeString(clean)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func wrap(msgType uint8, body []byte) []byte {
	b := header(msgType, len(body))
	b = append(b, body...)
	return appendCRC(b)
}

func goldenHello() Hello {
	return Hello{0x9f3a5c0e12b47d68, "virtual-synthetic", 3, 2, 160.0, []string{"Fp1", "Cz", "O2"}}
}

const goldenHelloJSON = `{"stream_id":"9f3a5c0e12b47d68","device_name":"virtual-synthetic","n_channels":3,"samples_per_frame":2,"sample_rate_hz":160.0,"units":"uV","channel_names":["Fp1","Cz","O2"]}`

var helloKeyOrder = []string{"stream_id", "device_name", "n_channels", "samples_per_frame", "sample_rate_hz", "units", "channel_names"}

func helloJSON(overrides map[string]string) string {
	vals := map[string]string{
		"stream_id":         `"9f3a5c0e12b47d68"`,
		"device_name":       `"virtual-synthetic"`,
		"n_channels":        `3`,
		"samples_per_frame": `2`,
		"sample_rate_hz":    `160.0`,
		"units":             `"uV"`,
		"channel_names":     `["Fp1","Cz","O2"]`,
	}
	for k, v := range overrides {
		vals[k] = v
	}
	parts := make([]string, 0, len(helloKeyOrder))
	for _, k := range helloKeyOrder {
		parts = append(parts, `"`+k+`":`+vals[k])
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func requireKind(t *testing.T, err error, kind Kind) {
	t.Helper()
	var pe *Error
	if !errors.As(err, &pe) {
		t.Fatalf("error %v is not a *Error", err)
	}
	if pe.Kind != kind {
		t.Fatalf("kind = %v (%s), want %v", pe.Kind, pe.Msg, kind)
	}
}

func goldenSamples() []float32 { return []float32{12.5, 13.25, -3, 0, 1.5, -0.5} }

func goldenFrameMsg(t *testing.T) []byte {
	t.Helper()
	return loadVector(t, "frame_valid.hex")
}

func TestKindNames(t *testing.T) {
	want := []string{"BadMagicError", "UnsupportedVersionError", "UnknownMessageTypeError", "ReservedFieldError",
		"LimitError", "LengthMismatchError", "ChecksumError", "InvalidHelloError", "MessageOrderError", "DimensionMismatchError"}
	for i, w := range want {
		if got := Kind(i + 1).String(); got != w {
			t.Errorf("Kind(%d) = %q, want %q", i+1, got, w)
		}
	}
	if got := Kind(99).String(); got != "Kind(99)" {
		t.Errorf("Kind(99) = %q", got)
	}
}

func TestGoldenHelloDecodes(t *testing.T) {
	m, err := DecodeMessage(loadVector(t, "hello_valid.hex"))
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != MsgHello {
		t.Fatalf("type = %d", m.Type)
	}
	if !reflect.DeepEqual(m.Hello, goldenHello()) {
		t.Fatalf("hello = %+v", m.Hello)
	}
}

func TestGoldenFrameDecodes(t *testing.T) {
	m, err := DecodeMessage(goldenFrameMsg(t))
	if err != nil {
		t.Fatal(err)
	}
	f := m.Frame
	if m.Type != MsgFrame || f.Seq != 41 || f.TimestampNs != 1700000000123456789 || f.NChannels != 3 || f.SamplesPerFrame != 2 {
		t.Fatalf("frame = %+v", f)
	}
	dst := make([]float32, 6)
	f.DecodeSamples(dst)
	if !reflect.DeepEqual(dst, goldenSamples()) {
		t.Fatalf("samples = %v", dst)
	}
}

func TestGoldenHelloEncodes(t *testing.T) {
	b, err := EncodeHello(goldenHello())
	if err != nil {
		t.Fatal(err)
	}
	if want := loadVector(t, "hello_valid.hex"); !reflect.DeepEqual(b, want) {
		t.Fatalf("encoded HELLO differs\n got %x\nwant %x", b, want)
	}
}

func TestGoldenFrameEncodes(t *testing.T) {
	b, err := EncodeFrame(41, 1700000000123456789, 3, 2, goldenSamples())
	if err != nil {
		t.Fatal(err)
	}
	if want := loadVector(t, "frame_valid.hex"); !reflect.DeepEqual(b, want) {
		t.Fatalf("encoded FRAME differs\n got %x\nwant %x", b, want)
	}
}

func TestGoldenInvalidVectors(t *testing.T) {
	cases := map[string]Kind{
		"frame_bad_magic.hex":        BadMagic,
		"frame_bad_version.hex":      UnsupportedVersion,
		"frame_unknown_type.hex":     UnknownMessageType,
		"frame_reserved_nonzero.hex": ReservedField,
		"frame_bad_crc.hex":          Checksum,
		"frame_zero_channels.hex":    Limit,
		"frame_length_mismatch.hex":  LengthMismatch,
		"hello_missing_key.hex":      InvalidHello,
		"hello_extra_key.hex":        InvalidHello,
		"hello_duplicate_key.hex":    InvalidHello,
		"hello_wrong_case_key.hex":   InvalidHello,
		"hello_float_n_channels.hex": InvalidHello,
	}
	for name, kind := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := DecodeMessage(loadVector(t, name))
			requireKind(t, err, kind)
		})
	}
}

func TestDecodeHeaderShort(t *testing.T) {
	_, err := DecodeHeader(make([]byte, 11))
	requireKind(t, err, LengthMismatch)
}

func hdrBytes(msgType uint8, n uint32) []byte {
	b := make([]byte, 12)
	copy(b, "EEGP")
	b[4] = 1
	b[5] = msgType
	binary.LittleEndian.PutUint32(b[8:], n)
	return b
}

func TestBodyLengthLimits(t *testing.T) {
	for _, c := range []struct {
		typ uint8
		n   uint32
	}{{1, 262144}, {2, 16777236}} {
		if _, err := DecodeHeader(hdrBytes(c.typ, c.n)); err != nil {
			t.Errorf("type %d length %d: %v", c.typ, c.n, err)
		}
	}
	for _, c := range []struct {
		typ uint8
		n   uint32
	}{{1, 262145}, {2, 16777237}} {
		_, err := DecodeHeader(hdrBytes(c.typ, c.n))
		requireKind(t, err, Limit)
	}
}

func TestMessageLengthMismatch(t *testing.T) {
	m := goldenFrameMsg(t)
	_, err := DecodeMessage(append(append([]byte(nil), m...), 0))
	requireKind(t, err, LengthMismatch)
	_, err = DecodeMessage(m[:len(m)-1])
	requireKind(t, err, LengthMismatch)
}

func TestFrameBodyShorterThanFixedPart(t *testing.T) {
	_, err := DecodeMessage(wrap(2, make([]byte, 19)))
	requireKind(t, err, LengthMismatch)
}

func TestFrameDimensionLimits(t *testing.T) {
	for _, d := range [][2]int{{0, 1}, {4097, 1}, {1, 0}, {1, 1025}} {
		body := make([]byte, 20+d[0]*d[1]*4)
		binary.LittleEndian.PutUint16(body[16:], uint16(d[0]))
		binary.LittleEndian.PutUint16(body[18:], uint16(d[1]))
		_, err := DecodeMessage(wrap(2, body))
		requireKind(t, err, Limit)
	}
}

func TestSpecialFloatsRoundTrip(t *testing.T) {
	in := []float32{math.Float32frombits(0x7fc00001), float32(math.Inf(1)), float32(math.Inf(-1)), float32(math.Copysign(0, -1))}
	b, err := EncodeFrame(0, 0, 1, 4, in)
	if err != nil {
		t.Fatal(err)
	}
	m, err := DecodeMessage(b)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]float32, 4)
	m.Frame.DecodeSamples(out)
	for i := range in {
		if math.Float32bits(in[i]) != math.Float32bits(out[i]) {
			t.Errorf("sample %d: bits %08x, want %08x", i, math.Float32bits(out[i]), math.Float32bits(in[i]))
		}
	}
}

func requirePanic(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		t.Helper()
		r := recover()
		if r == nil {
			t.Fatal("did not panic")
		}
		if s, _ := r.(string); s != want {
			t.Fatalf("panic = %v, want %q", r, want)
		}
	}()
	fn()
}

func TestDecodeSamplesWrongLength(t *testing.T) {
	m, err := DecodeMessage(goldenFrameMsg(t))
	if err != nil {
		t.Fatal(err)
	}
	requirePanic(t, "protocol: DecodeSamples: dst has 5 elements, frame has 6 samples", func() {
		m.Frame.DecodeSamples(make([]float32, 5))
	})
}

func helloMsg() Message { return Message{Type: MsgHello, Hello: goldenHello()} }

func frameMsg(c, s int) Message {
	return Message{Type: MsgFrame, Frame: Frame{NChannels: c, SamplesPerFrame: s}}
}

func TestValidatorOrder(t *testing.T) {
	var v Validator
	requireKind(t, v.CheckHeader(Header{MsgType: MsgFrame, BodyLength: 44}), MessageOrder)
	requireKind(t, v.Check(frameMsg(3, 2)), MessageOrder)
	if err := v.Check(helloMsg()); err != nil {
		t.Fatal(err)
	}
	requireKind(t, v.CheckHeader(Header{MsgType: MsgHello, BodyLength: 10}), MessageOrder)
	requireKind(t, v.Check(helloMsg()), MessageOrder)
}

func TestValidatorDimensions(t *testing.T) {
	var v Validator
	if err := v.Check(helloMsg()); err != nil {
		t.Fatal(err)
	}
	if err := v.CheckHeader(Header{MsgType: MsgFrame, BodyLength: 44}); err != nil {
		t.Fatal(err)
	}
	requireKind(t, v.CheckHeader(Header{MsgType: MsgFrame, BodyLength: 45}), DimensionMismatch)
	if err := v.Check(frameMsg(3, 2)); err != nil {
		t.Fatal(err)
	}
	requireKind(t, v.Check(frameMsg(2, 3)), DimensionMismatch)
}

func TestValidatorHello(t *testing.T) {
	var v Validator
	if _, ok := v.Hello(); ok {
		t.Fatal("Hello() ok before HELLO")
	}
	if err := v.Check(helloMsg()); err != nil {
		t.Fatal(err)
	}
	h, ok := v.Hello()
	if !ok || !reflect.DeepEqual(h, goldenHello()) {
		t.Fatalf("Hello() = %+v, %v", h, ok)
	}
}

func TestValidatorPanics(t *testing.T) {
	var v Validator
	requirePanic(t, "protocol: CheckHeader: unknown msg_type", func() { _ = v.CheckHeader(Header{MsgType: 3}) })
	requirePanic(t, "protocol: Check: unknown message type", func() { _ = v.Check(Message{Type: 3}) })
}

func TestEncodeFrameRejects(t *testing.T) {
	cases := []struct {
		c, s, n int
	}{{0, 1, 0}, {4097, 1, 4097}, {1, 0, 0}, {1, 1025, 1025}, {2, 3, 5}}
	for _, c := range cases {
		_, err := EncodeFrame(0, 0, c.c, c.s, make([]float32, c.n))
		if err == nil {
			t.Errorf("%+v: no error", c)
			continue
		}
		var pe *Error
		if errors.As(err, &pe) {
			t.Errorf("%+v: error is a *Error", c)
		}
	}
}

func TestCRCHelper(t *testing.T) {
	m := wrap(2, make([]byte, 20+8))
	if binary.LittleEndian.Uint32(m[len(m)-4:]) != crc32.ChecksumIEEE(m[:len(m)-4]) {
		t.Fatal("wrap CRC wrong")
	}
}

func BenchmarkDecodeMessage(b *testing.B) {
	samples := make([]float32, 1024*600)
	msg, err := EncodeFrame(1, 2, 1024, 600, samples)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(msg)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := DecodeMessage(msg); err != nil {
			b.Fatal(err)
		}
	}
}
