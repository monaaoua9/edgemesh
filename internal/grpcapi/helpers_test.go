package grpcapi

import "crypto/tls"

func loadPair(cert, key string) ([]tls.Certificate, error) {
	c, err := tls.LoadX509KeyPair(cert, key)
	return []tls.Certificate{c}, err
}
