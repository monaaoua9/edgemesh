// gencert writes a throwaway CA + server + client certificate for local TLS/mTLS demos.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"edgemesh/internal/tlsutil"
)

func main() {
	out := flag.String("out", "./certs", "output directory")
	hosts := flag.String("hosts", "localhost,127.0.0.1", "comma separated SANs")
	flag.Parse()
	if err := os.MkdirAll(*out, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	d, err := tlsutil.GenerateDev(*out, strings.Split(*hosts, ",")...)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("ca=%s\nserver=%s %s\nclient=%s %s\n", d.CA, d.Cert, d.Key, d.ClientCert, d.ClientKey)
}
