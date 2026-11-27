package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

var helloKeys = []string{"stream_id", "device_name", "n_channels", "samples_per_frame", "sample_rate_hz", "units", "channel_names"}

func invalidHello(format string, args ...any) *Error {
	return &Error{Kind: InvalidHello, Msg: "invalid HELLO: " + fmt.Sprintf(format, args...)}
}

func printableASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

func stringValue(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 || raw[0] != '"' {
		return "", false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return s, true
}

func isIntegerLiteral(raw []byte) bool {
	i := 0
	if i < len(raw) && raw[i] == '-' {
		i++
	}
	if i >= len(raw) {
		return false
	}
	if raw[i] == '0' {
		return i+1 == len(raw)
	}
	if raw[i] < '1' || raw[i] > '9' {
		return false
	}
	for i++; i < len(raw); i++ {
		if raw[i] < '0' || raw[i] > '9' {
			return false
		}
	}
	return true
}

func integerValue(raw json.RawMessage) (int64, bool) {
	if !isIntegerLiteral(raw) {
		return 0, false
	}
	v, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func numberValue(raw json.RawMessage) (float64, bool) {
	if len(raw) == 0 || !(raw[0] == '-' || (raw[0] >= '0' && raw[0] <= '9')) {
		return 0, false
	}
	v, err := strconv.ParseFloat(string(raw), 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func isLowerHex16(s string) bool {
	if len(s) != 16 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func decodeHelloBody(body []byte) (Hello, error) {
	if !utf8.Valid(body) {
		return Hello{}, invalidHello("cannot parse body (not UTF-8)")
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	tok, err := dec.Token()
	if err != nil {
		return Hello{}, invalidHello("cannot parse body (%v)", err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return Hello{}, invalidHello("body is not a JSON object")
	}
	fields := make(map[string]json.RawMessage)
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return Hello{}, invalidHello("cannot parse body (%v)", err)
		}
		key, _ := kt.(string)
		if _, dup := fields[key]; dup {
			return Hello{}, invalidHello("duplicate key %q", key)
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return Hello{}, invalidHello("cannot parse body (%v)", err)
		}
		fields[key] = raw
	}
	tok, err = dec.Token()
	if err != nil {
		return Hello{}, invalidHello("cannot parse body (%v)", err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '}' {
		return Hello{}, invalidHello("cannot parse body (trailing data)")
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return Hello{}, invalidHello("cannot parse body (trailing data)")
	}

	if len(fields) != len(helloKeys) {
		return Hello{}, keysError(fields)
	}
	for _, k := range helloKeys {
		if _, ok := fields[k]; !ok {
			return Hello{}, keysError(fields)
		}
	}

	var h Hello
	sid, ok := stringValue(fields["stream_id"])
	if !ok || !isLowerHex16(sid) {
		return Hello{}, invalidHello("stream_id must be 16 lowercase hex digits")
	}
	h.StreamID, err = strconv.ParseUint(sid, 16, 64)
	if err != nil {
		return Hello{}, invalidHello("stream_id must be 16 lowercase hex digits")
	}
	name, ok := stringValue(fields["device_name"])
	if !ok || len(name) < 1 || len(name) > MaxDeviceNameLen || !printableASCII(name) {
		return Hello{}, invalidHello("device_name invalid")
	}
	h.DeviceName = name
	c, ok := integerValue(fields["n_channels"])
	if !ok || c < 1 || c > MaxChannels {
		return Hello{}, invalidHello("n_channels invalid")
	}
	h.NChannels = int(c)
	s, ok := integerValue(fields["samples_per_frame"])
	if !ok || s < 1 || s > MaxSamplesPerFrame {
		return Hello{}, invalidHello("samples_per_frame invalid")
	}
	h.SamplesPerFrame = int(s)
	rate, ok := numberValue(fields["sample_rate_hz"])
	if !ok || math.IsInf(rate, 0) || !(rate > 0) || rate > MaxSampleRateHz {
		return Hello{}, invalidHello("sample_rate_hz invalid")
	}
	h.SampleRateHz = rate
	if u, ok := stringValue(fields["units"]); !ok || u != Units {
		return Hello{}, invalidHello("units must be \"uV\"")
	}
	rawNames := fields["channel_names"]
	var items []json.RawMessage
	if len(rawNames) == 0 || rawNames[0] != '[' || json.Unmarshal(rawNames, &items) != nil {
		return Hello{}, invalidHello("channel_names must be an array")
	}
	if len(items) != h.NChannels {
		return Hello{}, invalidHello("channel_names length must equal n_channels")
	}
	h.ChannelNames = make([]string, len(items))
	for i, it := range items {
		n, ok := stringValue(it)
		if !ok || len(n) < 1 || len(n) > MaxChannelNameLen || !printableASCII(n) {
			return Hello{}, invalidHello("channel name invalid")
		}
		h.ChannelNames[i] = n
	}
	return h, nil
}

func keysError(fields map[string]json.RawMessage) error {
	got := make([]string, 0, len(fields))
	for k := range fields {
		got = append(got, k)
	}
	sort.Strings(got)
	want := append([]string(nil), helloKeys...)
	sort.Strings(want)
	return invalidHello("keys are %v, expected %v", got, want)
}

func quote(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return `"` + s + `"`
}

func formatRate(r float64) string {
	if r < 1e-4 {
		return strconv.FormatFloat(r, 'e', -1, 64)
	}
	s := strconv.FormatFloat(r, 'f', -1, 64)
	if !strings.Contains(s, ".") {
		s += ".0"
	}
	return s
}

// EncodeHello encodes a HELLO message, byte for byte like the Python encoder.
func EncodeHello(h Hello) ([]byte, error) {
	if len(h.DeviceName) < 1 || len(h.DeviceName) > MaxDeviceNameLen {
		return nil, errors.New("protocol: device_name length outside 1..64")
	}
	if !printableASCII(h.DeviceName) {
		return nil, errors.New("protocol: device_name must be printable ASCII")
	}
	if h.NChannels < 1 || h.NChannels > MaxChannels {
		return nil, errors.New("protocol: n_channels outside 1..4096")
	}
	if h.SamplesPerFrame < 1 || h.SamplesPerFrame > MaxSamplesPerFrame {
		return nil, errors.New("protocol: samples_per_frame outside 1..1024")
	}
	if !(h.SampleRateHz > 0 && h.SampleRateHz <= MaxSampleRateHz) {
		return nil, errors.New("protocol: sample_rate_hz must be > 0 and <= 100000")
	}
	if len(h.ChannelNames) != h.NChannels {
		return nil, errors.New("protocol: channel_names length must equal n_channels")
	}
	for _, n := range h.ChannelNames {
		if len(n) < 1 || len(n) > MaxChannelNameLen {
			return nil, errors.New("protocol: channel name length outside 1..32")
		}
		if !printableASCII(n) {
			return nil, errors.New("protocol: channel name must be printable ASCII")
		}
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, `{"stream_id":"%016x","device_name":%s,"n_channels":%d,"samples_per_frame":%d,"sample_rate_hz":%s,"units":"uV","channel_names":[`,
		h.StreamID, quote(h.DeviceName), h.NChannels, h.SamplesPerFrame, formatRate(h.SampleRateHz))
	for i, n := range h.ChannelNames {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(quote(n))
	}
	sb.WriteString("]}")
	body := sb.String()
	if len(body) > MaxHelloBody {
		return nil, fmt.Errorf("protocol: HELLO body %d bytes exceeds maximum 262144", len(body))
	}
	b := header(MsgHello, len(body))
	b = append(b, body...)
	return appendCRC(b), nil
}
