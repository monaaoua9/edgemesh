package sink

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"edgemesh/internal/telemetry"
)

// ErrPermanent means retrying the same batch will never work (bad payload etc).
var ErrPermanent = errors.New("sink: permanent failure")

type Batch struct {
	GatewayID string              `json:"gateway_id"`
	First     uint64              `json:"first"`
	Last      uint64              `json:"last"`
	Readings  []telemetry.Reading `json:"readings"`
}

type Sink interface {
	Send(ctx context.Context, b Batch) error
}

type HTTP struct {
	url    string
	token  string
	client *http.Client
}

// tlsCfg may be nil (system roots)
func NewHTTP(url, token string, timeout time.Duration, tlsCfg *tls.Config) *HTTP {
	return &HTTP{
		url:   url,
		token: token,
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        4,
				IdleConnTimeout:     60 * time.Second,
				TLSHandshakeTimeout: 5 * time.Second,
				TLSClientConfig:     tlsCfg,
				ForceAttemptHTTP2:   true,
			},
		},
	}
}

func (h *HTTP) Send(ctx context.Context, b Batch) error {
	var body bytes.Buffer
	zw := gzip.NewWriter(&body)
	if err := json.NewEncoder(zw).Encode(b); err != nil {
		return fmt.Errorf("%w: encode: %w", ErrPermanent, err)
	}
	if err := zw.Close(); err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, &body)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrPermanent, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("X-Gateway-ID", b.GatewayID)
	// cloud side dedupes on this; we are at-least-once
	req.Header.Set("X-Batch-ID", b.GatewayID+":"+strconv.FormatUint(b.First, 10)+"-"+strconv.FormatUint(b.Last, 10))
	if h.token != "" {
		req.Header.Set("Authorization", "Bearer "+h.token)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10)) // let the conn get reused

	switch {
	case resp.StatusCode/100 == 2:
		return nil
	case resp.StatusCode == http.StatusBadRequest,
		resp.StatusCode == http.StatusRequestEntityTooLarge,
		resp.StatusCode == http.StatusUnprocessableEntity:
		return fmt.Errorf("%w: http %d", ErrPermanent, resp.StatusCode)
	default:
		return fmt.Errorf("sink: http %d", resp.StatusCode)
	}
}

// Discard is for running the gateway with no upstream configured.
type Discard struct{}

func (Discard) Send(context.Context, Batch) error { return nil }
