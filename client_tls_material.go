// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// loadURITLSMaterial applies the connection string's caCertificatePath and
// certificatePath to config. Errors name the option and never echo the path or file
// contents, which can hold key material.
func loadURITLSMaterial(config *tls.Config, uri ConnectionString) error {
	if uri.caCertificatePath != "" {
		pool, err := loadCertificateAuthorities(uri.caCertificatePath)
		if err != nil {
			return err
		}
		// Authorities replace the system trust store, and matter only while
		// validation is on; skipTlsValidation ignores them.
		config.RootCAs = pool
	}
	if uri.certificatePath != "" {
		certificate, err := loadClientCertificate(uri.certificatePath)
		if err != nil {
			return err
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return nil
}

func loadCertificateAuthorities(path string) (*x509.CertPool, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot read the caCertificatePath file", ErrInvalidConfiguration)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(contents) {
		return nil, fmt.Errorf("%w: the caCertificatePath file holds no PEM certificates", ErrInvalidConfiguration)
	}
	return pool, nil
}

// loadClientCertificate reads one PEM file holding the certificate chain and the
// unencrypted private key, in any order of blocks as long as the leaf comes first.
func loadClientCertificate(path string) (tls.Certificate, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("%w: cannot read the certificatePath file", ErrInvalidConfiguration)
	}
	certificate, err := tls.X509KeyPair(contents, contents)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("%w: the certificatePath file is not a PEM certificate chain with an unencrypted private key for its first certificate", ErrInvalidConfiguration)
	}
	return certificate, nil
}
