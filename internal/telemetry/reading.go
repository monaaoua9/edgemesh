package telemetry

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
)

const wireVersion = 1

const FlagAnomaly uint8 = 1

var (
	ErrShort   = errors.New("telemetry: short buffer")
	ErrVersion = errors.New("telemetry: unknown wire version")
	ErrTooLong = errors.New("telemetry: name longer than 255 bytes")
	ErrValue   = errors.New("telemetry: value is not finite")
)

// timestamps are unix microseconds everywhere. ms wasn't enough to see
// loopback latencies in the benchmarks.
type Reading struct {
	DeviceID string  `json:"device_id"`
	Metric   string  `json:"metric"`
	Value    float64 `json:"value"`
	TS       int64   `json:"ts_us"`
	RecvTS   int64   `json:"recv_us"`
	Flags    uint8   `json:"flags,omitempty"`
}

func (r *Reading) Anomalous() bool { return r.Flags&FlagAnomaly != 0 }

// layout: ver, flags, ts(8), recv(8), value(8), len+device, len+metric
func (r *Reading) AppendBinary(dst []byte) ([]byte, error) {
	if len(r.DeviceID) > 255 || len(r.Metric) > 255 {
		return dst, ErrTooLong
	}
	dst = append(dst, wireVersion, r.Flags)
	dst = binary.BigEndian.AppendUint64(dst, uint64(r.TS))
	dst = binary.BigEndian.AppendUint64(dst, uint64(r.RecvTS))
	dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(r.Value))
	dst = append(dst, byte(len(r.DeviceID)))
	dst = append(dst, r.DeviceID...)
	dst = append(dst, byte(len(r.Metric)))
	dst = append(dst, r.Metric...)
	return dst, nil
}

func (r *Reading) UnmarshalBinary(b []byte) error {
	if len(b) < 2+24+1 {
		return ErrShort
	}
	if b[0] != wireVersion {
		return ErrVersion
	}
	r.Flags = b[1]
	r.TS = int64(binary.BigEndian.Uint64(b[2:]))
	r.RecvTS = int64(binary.BigEndian.Uint64(b[10:]))
	r.Value = math.Float64frombits(binary.BigEndian.Uint64(b[18:]))

	b = b[26:]
	n := int(b[0])
	if len(b) < 1+n+1 {
		return ErrShort
	}
	r.DeviceID = string(b[1 : 1+n])
	b = b[1+n:]
	n = int(b[0])
	if len(b) < 1+n {
		return ErrShort
	}
	r.Metric = string(b[1 : 1+n])
	return nil
}

// what devices actually put on the wire
type devicePayload struct {
	M string  `json:"m"`
	V float64 `json:"v"`
	T int64   `json:"t"`
}

func ParsePayload(p []byte, r *Reading) error {
	var d devicePayload
	if err := json.Unmarshal(p, &d); err != nil {
		return err
	}
	if d.M == "" || len(d.M) > 255 {
		return errors.New("telemetry: bad metric name")
	}
	if math.IsNaN(d.V) || math.IsInf(d.V, 0) {
		return ErrValue
	}
	r.Metric, r.Value, r.TS = d.M, d.V, d.T
	return nil
}

func EncodePayload(metric string, value float64, tsUs int64) []byte {
	b, _ := json.Marshal(devicePayload{M: metric, V: value, T: tsUs})
	return b
}
