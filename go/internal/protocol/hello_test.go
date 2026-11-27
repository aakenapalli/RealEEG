package protocol

import (
	"reflect"
	"strings"
	"testing"
)

func decodeHelloString(body string) (Hello, error) {
	m, err := DecodeMessage(wrap(MsgHello, []byte(body)))
	return m.Hello, err
}

func TestHelloAcceptsEncodings(t *testing.T) {
	reversed := `{"channel_names":["Fp1","Cz","O2"],"units":"uV","sample_rate_hz":160.0,"samples_per_frame":2,"n_channels":3,"device_name":"virtual-synthetic","stream_id":"9f3a5c0e12b47d68"}`
	spaced := "{ \"stream_id\" : \"9f3a5c0e12b47d68\" ,\n\"device_name\":\"virtual-synthetic\",\n \"n_channels\": 3 , \"samples_per_frame\":2,\n\"sample_rate_hz\" : 160.0,\"units\":\"uV\",\n\"channel_names\" : [ \"Fp1\" , \"Cz\", \"O2\" ] }\n"
	bodies := []string{
		reversed,
		spaced,
		helloJSON(map[string]string{"device_name": `"virtual-synthetic"`}),
		helloJSON(map[string]string{"sample_rate_hz": `160`}),
		helloJSON(map[string]string{"sample_rate_hz": `1.6e2`}),
		helloJSON(map[string]string{"sample_rate_hz": `160.0`}),
	}
	for i, b := range bodies {
		h, err := decodeHelloString(b)
		if err != nil {
			t.Errorf("body %d: %v", i, err)
			continue
		}
		if !reflect.DeepEqual(h, goldenHello()) {
			t.Errorf("body %d: hello = %+v", i, h)
		}
	}
}

func TestHelloRejects(t *testing.T) {
	type tc struct{ name, body string }
	var cases []tc
	add := func(name, body string) { cases = append(cases, tc{name, body}) }
	field := func(key string, vals ...string) {
		for _, v := range vals {
			add(key+"="+shorten(v), helloJSON(map[string]string{key: v}))
		}
	}
	field("sample_rate_hz", `NaN`, `Infinity`, `-Infinity`, `1e999`, `0`, `-1`, `100000.5`, `true`, `"160"`, `null`)
	field("n_channels", `3.0`, `3e0`, `-1`, `0`, `4097`, `"3"`, `null`, strings.Repeat("1", 5000))
	field("samples_per_frame", `0`, `1025`)
	field("stream_id", `"9F3A5C0E12B47D68"`, `"9f3a5c0e12b47d6"`, `null`, `123`)
	field("device_name", `""`, `"`+strings.Repeat("a", 65)+`"`, `"a\u0007"`, `"é"`)
	field("units", `"mV"`)
	field("channel_names", `["Fp1","Cz"]`, `["Fp1","Cz",""]`, `["Fp1","Cz","`+strings.Repeat("a", 33)+`"]`, `["Fp1","Cz",3]`, `"Fp1"`)
	g := goldenHelloJSON
	add("extra key", strings.TrimSuffix(g, "}")+`,"extra":1}`)
	add("missing key", `{"stream_id":"9f3a5c0e12b47d68","device_name":"virtual-synthetic","n_channels":3,"samples_per_frame":2,"sample_rate_hz":160.0,"channel_names":["Fp1","Cz","O2"]}`)
	add("duplicate key", strings.TrimSuffix(g, "}")+`,"units":"uV"}`)
	add("wrong case key", strings.Replace(g, `"units"`, `"Units"`, 1))
	add("duplicate key escaped", strings.TrimSuffix(g, "}")+`,"units":"uV"}`)
	add("array body", `[]`)
	add("empty object", `{}`)
	add("empty body", ``)
	add("trailing x", g+" x")
	add("twice", g+g)
	add("0xff in name", strings.Replace(g, "virtual-synthetic", "virtual\xff", 1))
	add("deep array", helloJSON(map[string]string{"channel_names": strings.Repeat("[", 100000)}))
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := decodeHelloString(c.body)
			if err == nil {
				t.Fatal("accepted")
			}
			requireKind(t, err, InvalidHello)
		})
	}
}

func shorten(s string) string {
	if len(s) > 20 {
		return s[:20] + "..."
	}
	return s
}

func TestHelloMaxValues(t *testing.T) {
	h, err := decodeHelloString(helloJSON(map[string]string{"sample_rate_hz": `100000`}))
	if err != nil || h.SampleRateHz != 100000 {
		t.Fatalf("rate: %v %+v", err, h)
	}
	names := make([]string, 4096)
	quoted := make([]string, 4096)
	for i := range names {
		names[i] = "c" + itoa(i)
		quoted[i] = `"` + names[i] + `"`
	}
	body := helloJSON(map[string]string{"n_channels": `4096`, "channel_names": "[" + strings.Join(quoted, ",") + "]"})
	h, err = decodeHelloString(body)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(h.ChannelNames, names) {
		t.Fatal("names differ")
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for ; i > 0; i /= 10 {
		b = append([]byte{byte('0' + i%10)}, b...)
	}
	return string(b)
}

func TestEncodeHelloRateFormat(t *testing.T) {
	cases := []struct {
		rate float64
		want string
	}{{160, `"sample_rate_hz":160.0,`}, {0.5, `"sample_rate_hz":0.5,`}, {5e-05, `"sample_rate_hz":5e-05,`},
		{1e-4, `"sample_rate_hz":0.0001,`}, {100000, `"sample_rate_hz":100000.0,`}, {250.25, `"sample_rate_hz":250.25,`}}
	for _, c := range cases {
		h := goldenHello()
		h.SampleRateHz = c.rate
		b, err := EncodeHello(h)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(b), c.want) {
			t.Errorf("rate %v: %s does not contain %s", c.rate, b, c.want)
		}
	}
}

func TestEncodeHelloEscapes(t *testing.T) {
	h := goldenHello()
	h.DeviceName = `a"b\c`
	b, err := EncodeHello(h)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"device_name":"a\"b\\c"`) {
		t.Fatalf("body = %s", b)
	}
	m, err := DecodeMessage(b)
	if err != nil {
		t.Fatal(err)
	}
	if m.Hello.DeviceName != h.DeviceName {
		t.Fatalf("name = %q", m.Hello.DeviceName)
	}
}

func TestEncodeHelloRejects(t *testing.T) {
	mut := []func(h *Hello){
		func(h *Hello) { h.DeviceName = "" },
		func(h *Hello) { h.DeviceName = strings.Repeat("a", 65) },
		func(h *Hello) { h.DeviceName = "a\x07" },
		func(h *Hello) { h.NChannels = 0 },
		func(h *Hello) { h.NChannels = 4097 },
		func(h *Hello) { h.SamplesPerFrame = 0 },
		func(h *Hello) { h.SamplesPerFrame = 1025 },
		func(h *Hello) { h.SampleRateHz = 0 },
		func(h *Hello) { h.SampleRateHz = 100000.5 },
		func(h *Hello) { h.SampleRateHz = nan() },
		func(h *Hello) { h.ChannelNames = []string{"a", "b"} },
		func(h *Hello) { h.ChannelNames = []string{"a", "", "c"} },
		func(h *Hello) { h.ChannelNames = []string{"a", strings.Repeat("a", 33), "c"} },
		func(h *Hello) { h.ChannelNames = []string{"a", "b\x07", "c"} },
	}
	for i, m := range mut {
		h := goldenHello()
		m(&h)
		_, err := EncodeHello(h)
		if err == nil {
			t.Errorf("case %d: no error", i)
			continue
		}
		if _, ok := err.(*Error); ok {
			t.Errorf("case %d: error is a *Error", i)
		}
	}
}

func nan() float64 {
	var z float64
	return z / z
}
