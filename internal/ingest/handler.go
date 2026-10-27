package ingest

import (
	"context"
	"errors"
	"strings"
	"time"

	"edgemesh/internal/fleet"
	"edgemesh/internal/metrics"
	"edgemesh/internal/telemetry"
)

var errForbidden = errors.New("ingest: client id does not match topic")

// Topic layout: devices/<id>/telemetry and devices/<id>/heartbeat
type HandlerMetrics struct {
	Malformed    *metrics.Counter
	Spoofed      *metrics.Counter
	Heartbeats   *metrics.Counter
	DeviceToEdge *metrics.Histogram // device timestamp -> gateway receipt (needs sane clocks)
}

func NewHandlerMetrics(reg *metrics.Registry) *HandlerMetrics {
	return &HandlerMetrics{
		Malformed:    reg.Counter("edge_messages_malformed_total", "payloads we could not parse"),
		Spoofed:      reg.Counter("edge_messages_spoofed_total", "publishes to another device's topic"),
		Heartbeats:   reg.Counter("edge_heartbeats_total", "heartbeat messages"),
		DeviceToEdge: reg.Histogram("edge_network_latency_seconds", "device timestamp to gateway receipt"),
	}
}

// Handler implements mqtt.Handler.
type Handler struct {
	pipe  *Pipeline
	fleet *fleet.Registry
	m     *HandlerMetrics
}

func NewHandler(p *Pipeline, f *fleet.Registry, m *HandlerMetrics) *Handler {
	return &Handler{pipe: p, fleet: f, m: m}
}

func (h *Handler) OnConnect(id string) { h.fleet.Touch(id) }

// not marking the device down here; a flapping link shouldn't reshuffle tasks.
// the heartbeat sweeper decides.
func (h *Handler) OnDisconnect(string) {}

func (h *Handler) OnPublish(ctx context.Context, clientID, topic string, payload []byte, qos byte) error {
	dev, kind, ok := parseTopic(topic)
	if !ok {
		h.m.Malformed.Inc()
		return nil // ack it, retrying a bad topic never helps
	}
	if dev != clientID {
		h.m.Spoofed.Inc()
		return errForbidden
	}
	h.fleet.Touch(dev)

	switch kind {
	case "heartbeat":
		h.m.Heartbeats.Inc()
		return nil
	case "telemetry":
		now := time.Now().UnixMicro()
		r := telemetry.Reading{DeviceID: dev, RecvTS: now}
		if err := telemetry.ParsePayload(payload, &r); err != nil {
			h.m.Malformed.Inc()
			return nil
		}
		if r.TS == 0 {
			r.TS = now
		} else if d := now - r.TS; d >= 0 && d < int64(time.Minute/time.Microsecond) {
			h.m.DeviceToEdge.Observe(time.Duration(d) * time.Microsecond)
		}
		return h.pipe.Submit(ctx, r, qos > 0)
	}
	return nil
}

func parseTopic(t string) (dev, kind string, ok bool) {
	rest, found := strings.CutPrefix(t, "devices/")
	if !found {
		return
	}
	dev, kind, found = strings.Cut(rest, "/")
	if !found || dev == "" || strings.Contains(kind, "/") {
		return "", "", false
	}
	return dev, kind, kind == "telemetry" || kind == "heartbeat"
}
