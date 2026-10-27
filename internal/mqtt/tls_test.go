package mqtt

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"edgemesh/internal/tlsutil"
)

func TestMQTTOverMutualTLS(t *testing.T) {
	dev, err := tlsutil.GenerateDev(t.TempDir(), "localhost", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	scfg, err := tlsutil.Server(dev.Cert, dev.Key, dev.CA)
	if err != nil {
		t.Fatal(err)
	}
	h := &capture{}
	srv := NewServer(Config{}, h)
	raw, _ := net.Listen("tcp", "127.0.0.1:0")
	go srv.Serve(tls.NewListener(raw, scfg))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	})
	addr := raw.Addr().String()

	ccfg, _ := tlsutil.Client(dev.CA)
	ccfg.ServerName = "localhost"

	// without a client cert the TLS 1.3 handshake "succeeds" client-side but the
	// first read fails, so Dial (which waits for CONNACK) must error
	if _, err := Dial(addr, ClientOptions{ClientID: "nocert", TLS: ccfg, Timeout: time.Second}); err == nil {
		t.Fatal("client without certificate was accepted")
	}

	kp, err := tls.LoadX509KeyPair(dev.ClientCert, dev.ClientKey)
	if err != nil {
		t.Fatal(err)
	}
	ccfg.Certificates = []tls.Certificate{kp}
	c, err := Dial(addr, ClientOptions{ClientID: "withcert", TLS: ccfg})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Publish("devices/withcert/telemetry", []byte(`{"m":"x","v":1}`), 1); err != nil {
		t.Fatal(err)
	}
	eventually(t, "delivered", func() bool { return h.count() == 1 })
}
