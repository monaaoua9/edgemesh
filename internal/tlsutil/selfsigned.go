package tlsutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// Dev holds paths of a freshly generated CA and a server cert signed by it.
type Dev struct{ CA, Cert, Key, ClientCert, ClientKey string }

// GenerateDev writes a throwaway CA, server and client cert into dir. For
// tests and local demos only.
func GenerateDev(dir string, hosts ...string) (Dev, error) {
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "edgemesh dev ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(30 * 24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	if err != nil {
		return Dev{}, err
	}
	caCert, _ := x509.ParseCertificate(caDER)

	d := Dev{
		CA: filepath.Join(dir, "ca.pem"), Cert: filepath.Join(dir, "server.pem"), Key: filepath.Join(dir, "server-key.pem"),
		ClientCert: filepath.Join(dir, "client.pem"), ClientKey: filepath.Join(dir, "client-key.pem"),
	}
	if err := writePEM(d.CA, "CERTIFICATE", caDER); err != nil {
		return d, err
	}

	issue := func(serial int64, cn string, usage x509.ExtKeyUsage, certPath, keyPath string) error {
		k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		tpl := &x509.Certificate{
			SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: cn},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(7 * 24 * time.Hour),
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
		}
		if usage == x509.ExtKeyUsageServerAuth {
			for _, h := range hosts {
				if ip := net.ParseIP(h); ip != nil {
					tpl.IPAddresses = append(tpl.IPAddresses, ip)
				} else {
					tpl.DNSNames = append(tpl.DNSNames, h)
				}
			}
		}
		der, err := x509.CreateCertificate(rand.Reader, tpl, caCert, &k.PublicKey, caKey)
		if err != nil {
			return err
		}
		kb, _ := x509.MarshalECPrivateKey(k)
		if err := writePEM(certPath, "CERTIFICATE", der); err != nil {
			return err
		}
		return writePEM(keyPath, "EC PRIVATE KEY", kb)
	}
	if err := issue(2, "gateway", x509.ExtKeyUsageServerAuth, d.Cert, d.Key); err != nil {
		return d, err
	}
	return d, issue(3, "device", x509.ExtKeyUsageClientAuth, d.ClientCert, d.ClientKey)
}

func writePEM(path, typ string, der []byte) error {
	return os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600)
}
