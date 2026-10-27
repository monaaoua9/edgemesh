package telemetry

import (
	"errors"
	"math"
	"strings"
	"testing"
)

func TestBinaryRoundTrip(t *testing.T) {
	in := Reading{DeviceID: "dev-1", Metric: "engine_temp", Value: 91.25, TS: 1700000000123456, RecvTS: 1700000000123999, Flags: FlagAnomaly}
	b, err := in.AppendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	var out Reading
	if err := out.UnmarshalBinary(b); err != nil {
		t.Fatal(err)
	}
	if in != out {
		t.Fatalf("got %+v want %+v", out, in)
	}
}

func TestUnmarshalTruncated(t *testing.T) {
	in := Reading{DeviceID: "abc", Metric: "m", Value: 1}
	b, _ := in.AppendBinary(nil)
	for i := 0; i < len(b); i++ {
		var out Reading
		if err := out.UnmarshalBinary(b[:i]); err == nil {
			t.Fatalf("expected error for len %d", i)
		}
	}
}

func TestTooLongName(t *testing.T) {
	r := Reading{DeviceID: strings.Repeat("x", 256)}
	if _, err := r.AppendBinary(nil); !errors.Is(err, ErrTooLong) {
		t.Fatalf("got %v", err)
	}
}

func TestParsePayload(t *testing.T) {
	var r Reading
	if err := ParsePayload(EncodePayload("vibration", 3.5, 42), &r); err != nil {
		t.Fatal(err)
	}
	if r.Metric != "vibration" || r.Value != 3.5 || r.TS != 42 {
		t.Fatalf("bad parse: %+v", r)
	}
	if err := ParsePayload([]byte(`{"v":1}`), &r); err == nil {
		t.Fatal("missing metric should fail")
	}
	if err := ParsePayload([]byte(`nope`), &r); err == nil {
		t.Fatal("garbage should fail")
	}
}

func FuzzUnmarshalBinary(f *testing.F) {
	seed := Reading{DeviceID: "d", Metric: "m", Value: math.Pi}
	b, _ := seed.AppendBinary(nil)
	f.Add(b)
	f.Fuzz(func(t *testing.T, data []byte) {
		var r Reading
		_ = r.UnmarshalBinary(data) // must not panic
	})
}
