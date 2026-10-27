package sink

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"edgemesh/internal/telemetry"
)

func batch() Batch {
	return Batch{GatewayID: "gw1", First: 10, Last: 11, Readings: []telemetry.Reading{{DeviceID: "d", Metric: "m", Value: 1}, {DeviceID: "d", Metric: "m", Value: 2}}}
}

func TestSendOK(t *testing.T) {
	var got Batch
	var hdr http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hdr = r.Header
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		json.NewDecoder(zr).Decode(&got)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	if err := NewHTTP(srv.URL, "tok", time.Second, nil).Send(context.Background(), batch()); err != nil {
		t.Fatal(err)
	}
	if len(got.Readings) != 2 || hdr.Get("X-Batch-ID") != "gw1:10-11" || hdr.Get("Authorization") != "Bearer tok" {
		t.Fatalf("%+v %v", got, hdr)
	}
}

func TestStatusClassification(t *testing.T) {
	cases := map[int]bool{ // status -> permanent?
		503: false, 500: false, 429: false, 400: true, 413: true, 422: true,
	}
	for code, perm := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }))
		err := NewHTTP(srv.URL, "", time.Second, nil).Send(context.Background(), batch())
		srv.Close()
		if err == nil || errors.Is(err, ErrPermanent) != perm {
			t.Errorf("status %d: err=%v permanent=%v", code, err, perm)
		}
	}
}

func TestUnreachableIsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	err := NewHTTP(url, "", 200*time.Millisecond, nil).Send(context.Background(), batch())
	if err == nil || errors.Is(err, ErrPermanent) {
		t.Fatalf("got %v", err)
	}
}
