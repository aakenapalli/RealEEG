// Package protocol decodes and encodes EEGP messages (docs/PROTOCOL.md).
package protocol

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
)

const (
	HeaderSize         = 12
	CRCSize            = 4
	FrameFixedSize     = 20
	Version            = 1
	MsgHello           = 1
	MsgFrame           = 2
	MaxChannels        = 4096
	MaxSamplesPerFrame = 1024
	MaxSampleRateHz    = 100000.0
	MaxHelloBody       = 262144
	MaxFrameBody       = FrameFixedSize + MaxChannels*MaxSamplesPerFrame*4 // 16777236
	MaxDeviceNameLen   = 64
	MaxChannelNameLen  = 32
	Units              = "uV"
)

var Magic = [4]byte{'E', 'E', 'G', 'P'}

// Kind classifies a protocol error.
type Kind int

const (
	BadMagic Kind = iota + 1
	UnsupportedVersion
	UnknownMessageType
	ReservedField
	Limit
	LengthMismatch
	Checksum
	InvalidHello
	MessageOrder
	DimensionMismatch
)

// Error is a protocol error.
type Error struct {
	Kind Kind
	Msg  string
}

func (e *Error) Error() string { return e.Msg }

func (k Kind) String() string {
	switch k {
	case BadMagic:
		return "BadMagicError"
	case UnsupportedVersion:
		return "UnsupportedVersionError"
	case UnknownMessageType:
		return "UnknownMessageTypeError"
	case ReservedField:
		return "ReservedFieldError"
	case Limit:
		return "LimitError"
	case LengthMismatch:
		return "LengthMismatchError"
	case Checksum:
		return "ChecksumError"
	case InvalidHello:
		return "InvalidHelloError"
	case MessageOrder:
		return "MessageOrderError"
	case DimensionMismatch:
		return "DimensionMismatchError"
	}
	return fmt.Sprintf("Kind(%d)", int(k))
}

func newError(k Kind, format string, args ...any) *Error {
	return &Error{Kind: k, Msg: fmt.Sprintf(format, args...)}
}

// Header is a decoded message header.
type Header struct {
	MsgType    uint8
	BodyLength uint32
}

// Hello is a decoded HELLO.
type Hello struct {
	StreamID        uint64
	DeviceName      string
	NChannels       int
	SamplesPerFrame int
	SampleRateHz    float64
	ChannelNames    []string
}

// Frame is a decoded FRAME. Samples aliases the message buffer passed to
// DecodeMessage: C*S*4 bytes of little-endian float32, channel-major.
type Frame struct {
	Seq             uint64
	TimestampNs     int64
	NChannels       int
	SamplesPerFrame int
	Samples         []byte
}

// Message is the result of DecodeMessage. Type is MsgHello or MsgFrame; only
// the matching field is set.
type Message struct {
	Type  uint8
	Hello Hello
	Frame Frame
}

// DecodeHeader decodes the 12-byte header at the start of b.
func DecodeHeader(b []byte) (Header, error) {
	if len(b) < HeaderSize {
		return Header{}, newError(LengthMismatch, "message is %d bytes, shorter than the 12-byte header", len(b))
	}
	if [4]byte(b[:4]) != Magic {
		return Header{}, newError(BadMagic, "bad magic %q, expected \"EEGP\"", string(b[:4]))
	}
	if b[4] != Version {
		return Header{}, newError(UnsupportedVersion, "unsupported version %d, expected 1", b[4])
	}
	t := b[5]
	if t != MsgHello && t != MsgFrame {
		return Header{}, newError(UnknownMessageType, "unknown message type %d", t)
	}
	if r := binary.LittleEndian.Uint16(b[6:8]); r != 0 {
		return Header{}, newError(ReservedField, "reserved field is %d, expected 0", r)
	}
	n := binary.LittleEndian.Uint32(b[8:12])
	max := uint32(MaxHelloBody)
	if t == MsgFrame {
		max = MaxFrameBody
	}
	if n > max {
		return Header{}, newError(Limit, "body length %d exceeds maximum %d for message type %d", n, max, t)
	}
	return Header{MsgType: t, BodyLength: n}, nil
}

// DecodeMessage decodes one whole message. It does not copy msg; a returned
// Frame's Samples aliases it.
func DecodeMessage(msg []byte) (Message, error) {
	h, err := DecodeHeader(msg)
	if err != nil {
		return Message{}, err
	}
	end := HeaderSize + int(h.BodyLength)
	if len(msg) != end+CRCSize {
		return Message{}, newError(LengthMismatch, "message is %d bytes, expected %d", len(msg), end+CRCSize)
	}
	computed := crc32.ChecksumIEEE(msg[:end])
	got := binary.LittleEndian.Uint32(msg[end:])
	if computed != got {
		return Message{}, newError(Checksum, "CRC mismatch: computed %08x, message has %08x", computed, got)
	}
	body := msg[HeaderSize:end]
	if h.MsgType == MsgHello {
		hello, err := decodeHelloBody(body)
		if err != nil {
			return Message{}, err
		}
		return Message{Type: MsgHello, Hello: hello}, nil
	}
	if h.BodyLength < FrameFixedSize {
		return Message{}, newError(LengthMismatch, "FRAME body length %d is shorter than the 20-byte fixed part", h.BodyLength)
	}
	seq := binary.LittleEndian.Uint64(body[0:])
	ts := int64(binary.LittleEndian.Uint64(body[8:]))
	c := int(binary.LittleEndian.Uint16(body[16:]))
	s := int(binary.LittleEndian.Uint16(body[18:]))
	if c < 1 || c > MaxChannels {
		return Message{}, newError(Limit, "n_channels %d outside 1..4096", c)
	}
	if s < 1 || s > MaxSamplesPerFrame {
		return Message{}, newError(Limit, "samples_per_frame %d outside 1..1024", s)
	}
	if int(h.BodyLength) != FrameFixedSize+c*s*4 {
		return Message{}, newError(LengthMismatch, "FRAME body length %d does not match 20 + %d x %d x 4 = %d",
			h.BodyLength, c, s, FrameFixedSize+c*s*4)
	}
	return Message{Type: MsgFrame, Frame: Frame{
		Seq: seq, TimestampNs: ts, NChannels: c, SamplesPerFrame: s, Samples: msg[32:end],
	}}, nil
}

// DecodeSamples converts the frame's samples to float32 (a bit cast).
func (f Frame) DecodeSamples(dst []float32) {
	if len(dst) != len(f.Samples)/4 {
		panic(fmt.Sprintf("protocol: DecodeSamples: dst has %d elements, frame has %d samples", len(dst), len(f.Samples)/4))
	}
	for i := range dst {
		dst[i] = math.Float32frombits(binary.LittleEndian.Uint32(f.Samples[4*i:]))
	}
}

func header(msgType uint8, bodyLen int) []byte {
	b := make([]byte, HeaderSize, HeaderSize+bodyLen+CRCSize)
	copy(b, Magic[:])
	b[4] = Version
	b[5] = msgType
	binary.LittleEndian.PutUint32(b[8:], uint32(bodyLen))
	return b
}

func appendCRC(b []byte) []byte {
	return binary.LittleEndian.AppendUint32(b, crc32.ChecksumIEEE(b))
}

// EncodeFrame encodes a FRAME message.
func EncodeFrame(seq uint64, timestampNs int64, nChannels, samplesPerFrame int, samples []float32) ([]byte, error) {
	if nChannels < 1 || nChannels > MaxChannels {
		return nil, fmt.Errorf("protocol: n_channels outside 1..4096")
	}
	if samplesPerFrame < 1 || samplesPerFrame > MaxSamplesPerFrame {
		return nil, fmt.Errorf("protocol: samples_per_frame outside 1..1024")
	}
	if len(samples) != nChannels*samplesPerFrame {
		return nil, fmt.Errorf("protocol: samples has %d elements, expected %d", len(samples), nChannels*samplesPerFrame)
	}
	b := header(MsgFrame, FrameFixedSize+len(samples)*4)
	b = binary.LittleEndian.AppendUint64(b, seq)
	b = binary.LittleEndian.AppendUint64(b, uint64(timestampNs))
	b = binary.LittleEndian.AppendUint16(b, uint16(nChannels))
	b = binary.LittleEndian.AppendUint16(b, uint16(samplesPerFrame))
	for _, v := range samples {
		b = binary.LittleEndian.AppendUint32(b, math.Float32bits(v))
	}
	return appendCRC(b), nil
}

// Validator checks the message order and dimensions of one connection.
// The zero value is ready to use.
type Validator struct {
	hello    Hello
	hasHello bool
}

// Hello returns the connection's HELLO once Check accepted one.
func (v *Validator) Hello() (Hello, bool) { return v.hello, v.hasHello }

// CheckHeader checks a header against the connection state.
func (v *Validator) CheckHeader(h Header) error {
	switch h.MsgType {
	case MsgHello:
		if v.hasHello {
			return newError(MessageOrder, "second HELLO received on this connection")
		}
	case MsgFrame:
		if !v.hasHello {
			return newError(MessageOrder, "FRAME received before HELLO")
		}
		c, s := v.hello.NChannels, v.hello.SamplesPerFrame
		want := FrameFixedSize + c*s*4
		if int(h.BodyLength) != want {
			return newError(DimensionMismatch, "FRAME body length %d does not match HELLO dimensions (%d, %d), expected %d",
				h.BodyLength, c, s, want)
		}
	default:
		panic("protocol: CheckHeader: unknown msg_type")
	}
	return nil
}

// Check checks a decoded message against the connection state.
func (v *Validator) Check(m Message) error {
	switch m.Type {
	case MsgHello:
		if v.hasHello {
			return newError(MessageOrder, "second HELLO received on this connection")
		}
		v.hello = m.Hello
		v.hasHello = true
	case MsgFrame:
		if !v.hasHello {
			return newError(MessageOrder, "FRAME received before HELLO")
		}
		if m.Frame.NChannels != v.hello.NChannels || m.Frame.SamplesPerFrame != v.hello.SamplesPerFrame {
			return newError(DimensionMismatch, "FRAME shape (%d, %d) does not match HELLO (%d, %d)",
				m.Frame.NChannels, m.Frame.SamplesPerFrame, v.hello.NChannels, v.hello.SamplesPerFrame)
		}
	default:
		panic("protocol: Check: unknown message type")
	}
	return nil
}
