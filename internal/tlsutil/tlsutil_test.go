package tlsutil

import (
	"crypto/tls"
	"os"
	"testing"
	"time"
)

func TestServerConfigAndReload(t *testing.T) {
	dir := t.TempDir()
	d1, err := GenerateDev(dir, "localhost")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := Server(d1.Cert, d1.Key, "")
	if err != nil {
		t.Fatal(err)
	}
	c1, err := cfg.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatal(err)
	}

	// rotate: regenerate in place, bump mtime, force the 10s throttle open
	time.Sleep(20 * time.Millisecond)
	if _, err := GenerateDev(dir, "localhost"); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(time.Minute)
	os.Chtimes(d1.Cert, future, future)
	time.Sleep(10*time.Second + 100*time.Millisecond)

	c2, err := cfg.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatal(err)
	}
	if string(c1.Certificate[0]) == string(c2.Certificate[0]) {
		t.Fatal("certificate was not reloaded")
	}
}

func TestBadFilesFailAtStartup(t *testing.T) {
	if _, err := Server("/nope.pem", "/nope.key", ""); err == nil {
		t.Fatal("missing cert accepted")
	}
	if _, err := Client("/nope-ca.pem"); err == nil {
		t.Fatal("missing CA accepted")
	}
}
