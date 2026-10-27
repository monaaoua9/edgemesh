// Package tlsutil builds tls.Configs for the listeners and the upstream
// client. Server certs are re-read when the files change so rotation (cert-manager,
// certbot) needs no restart.
package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"time"
)

type reloader struct {
	certFile, keyFile string

	mu      sync.Mutex
	cert    *tls.Certificate
	modTime time.Time
	checked time.Time
}

func (r *reloader) get(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cert != nil && time.Since(r.checked) < 10*time.Second {
		return r.cert, nil
	}
	r.checked = time.Now()
	st, err := os.Stat(r.certFile)
	if err != nil {
		if r.cert != nil {
			return r.cert, nil // keep serving the old one rather than failing handshakes
		}
		return nil, err
	}
	if r.cert != nil && !st.ModTime().After(r.modTime) {
		return r.cert, nil
	}
	c, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		if r.cert != nil {
			return r.cert, nil
		}
		return nil, err
	}
	r.cert, r.modTime = &c, st.ModTime()
	return r.cert, nil
}

// Server returns a config for listeners. clientCA != "" turns on mTLS.
func Server(certFile, keyFile, clientCA string) (*tls.Config, error) {
	r := &reloader{certFile: certFile, keyFile: keyFile}
	if _, err := r.get(nil); err != nil { // fail at startup, not on the first device
		return nil, fmt.Errorf("tls: %w", err)
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: r.get, NextProtos: []string{"h2", "http/1.1"}}
	if clientCA != "" {
		pool, err := pool(clientCA)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

// Client returns a config trusting caFile in addition to nothing else (private PKI).
// Empty caFile = system roots.
func Client(caFile string) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		p, err := pool(caFile)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = p
	}
	return cfg, nil
}

func pool(file string) (*x509.CertPool, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	p := x509.NewCertPool()
	if !p.AppendCertsFromPEM(b) {
		return nil, fmt.Errorf("tls: no certificates in %s", file)
	}
	return p, nil
}
