// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestParseConnectionString(t *testing.T) {
	for _, test := range []struct {
		name, input, address string
		count                int
		invalid              bool
	}{
		{"defaults", "chronicle://localhost", "localhost:35000", 1, false},
		{"IPv6", "chronicle://[::1]:35001", "[::1]:35001", 1, false},
		{"IPv6 default", "chronicle://[::1]", "[::1]:35000", 1, false},
		{"multiple", "chronicle://host:35000,[::1]:35002/", "host:35000", 2, false},
		{"encoded auth", "chronicle://my%40client:p%3Ass%40word@host", "host:35000", 1, false},
		{"SRV", "chronicle+srv://cluster", "cluster:35000", 1, false},
		{"anonymous", "chronicle://host/?auth=none", "host:35000", 1, false},
		{"case insensitive", "CHRONICLE://host/?AUTH=NONE&SkipTlsValidation=TRUE&SkipCompatibilityCheck=False", "host:35000", 1, false},
		{"case duplicate", "chronicle://host?auth=none&AUTH=NONE", "", 0, true},
		{"invalid query escape", "chronicle://host?auth=%ZZ", "", 0, true},
		{"API key", "chronicle://host?apiKey=sensitive", "host:35000", 1, false},
		{"scheme", "https://host", "", 0, true},
		{"empty host", "chronicle://", "", 0, true},
		{"partial credentials", "chronicle://client@host", "", 0, true},
		{"empty secret", "chronicle://client:@host", "", 0, true},
		{"auth conflict", "chronicle://client:secret@host?auth=none", "", 0, true},
		{"API conflict", "chronicle://client:secret@host?apiKey=key", "", 0, true},
		{"unknown", "chronicle://host?ignored=true", "", 0, true},
		{"duplicate", "chronicle://host?auth=none&auth=none", "", 0, true},
		{"invalid escape", "chronicle://client:%ZZ@host", "", 0, true},
		{"overflow port", "chronicle://host:65536", "", 0, true},
		{"zero port", "chronicle://host:0", "", 0, true},
		{"trailing colon", "chronicle://host:", "", 0, true},
		{"unbracketed IPv6", "chronicle://::1", "", 0, true},
		{"bracketed hostname", "chronicle://[host]:35000", "", 0, true},
		{"path", "chronicle://host/store", "", 0, true},
		{"fragment", "chronicle://host#secret", "", 0, true},
		{"bad boolean", "chronicle://host?skipTlsValidation=yes", "", 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			value, err := ParseConnectionString(test.input)
			if test.invalid {
				if !errors.Is(err, ErrInvalidConfiguration) {
					t.Fatalf("error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(value.Addresses()) != test.count || value.Addresses()[0].String() != test.address {
				t.Fatalf("addresses = %v", value.Addresses())
			}
			copy := value.Addresses()
			copy[0].Host = "mutated"
			if value.Addresses()[0].Host == "mutated" {
				t.Fatal("mutable addresses leaked")
			}
		})
	}
	value, err := ParseConnectionString("chronicle://my%40client:p%3Ass%40word@host")
	if err != nil || value.clientID != "my@client" || value.secret != "p:ss@word" {
		t.Fatal("credentials were not decoded")
	}
	defaults, err := ParseConnectionString("chronicle://localhost")
	if err != nil || defaults.clientID != developmentClient || defaults.secret != developmentSecret || defaults.skipTLS {
		t.Fatal("wrong defaults")
	}
}

func TestConnectionStringOptionValues(t *testing.T) {
	parsed, err := ParseConnectionString("chronicle://host?SkipTlsValidation=TRUE&SkipCompatibilityCheck=%20True%20&AUTH=NONE")
	if err != nil || !parsed.skipTLS || !parsed.tlsSpecified || !parsed.skipCompatibility || !parsed.noAuth {
		t.Fatalf("case insensitive values not applied: %v", err)
	}
	parsed, err = ParseConnectionString("chronicle://host?APIKEY=a+b%2Bc")
	if err != nil || parsed.apiKey != "a+b+c" {
		t.Fatal("plus decoding differs from C#", err)
	}
}

func TestConnectionStringRedaction(t *testing.T) {
	for _, input := range []string{"chronicle://user:sensitive@host", "chronicle://host?apiKey=sensitive", "chronicle://host?certificatePassword=sensitive"} {
		value, err := ParseConnectionString(input)
		if err != nil {
			t.Fatal(err)
		}
		for _, text := range []string{value.String(), fmt.Sprintf("%+v", value), fmt.Sprintf("%#v", value)} {
			if strings.Contains(text, "sensitive") {
				t.Fatal("credential leaked")
			}
		}
	}
}

func TestClientOptionValidation(t *testing.T) {
	for _, options := range [][]ClientOption{
		{nil}, {WithConnectTimeout(0)}, {WithTLS(nil)}, {WithTokenSource(nil)}, {WithGRPCConnection(nil)}, {WithTLS(&tls.Config{MinVersion: tls.VersionTLS11})},
		{WithConnectionString("chronicle://user:secret@host"), WithNoAuthentication()},
	} {
		client, err := NewClient(options...)
		if err == nil {
			_ = client.Close()
			t.Fatal("invalid options accepted")
		}
		if !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("error = %v", err)
		}
	}
	for _, input := range []string{"chronicle://host,other", "chronicle+srv://host", "chronicle://host?disableTls=true", "chronicle://host?certificatePath=a.pem", "chronicle://host?apiKey=key"} {
		_, err := NewClient(WithConnectionString(input))
		if !errors.Is(err, ErrUnsupported) {
			t.Fatalf("%s: %v", input, err)
		}
	}
}

func TestTLSValidationPrecedence(t *testing.T) {
	for _, options := range [][]ClientOption{
		{WithTLS(&tls.Config{MinVersion: tls.VersionTLS12}), WithDevelopmentDefaults()},
		{WithDevelopmentDefaults(), WithTLS(&tls.Config{MinVersion: tls.VersionTLS12})},
		{WithConnectionString("chronicle://localhost?skipTlsValidation=false"), WithDevelopmentDefaults()},
	} {
		config := clientConfig{uri: "chronicle://localhost", connectTimeout: 1}
		for _, option := range options {
			option(&config)
		}
		_, policy, err := validateConfig(config)
		if err != nil || policy.InsecureSkipVerify {
			t.Fatalf("explicit validation overridden: %v", err)
		}
	}
}

func FuzzParseConnectionString(f *testing.F) {
	for _, seed := range []string{"", "chronicle://localhost", "chronicle://u:p%40ss@[::1]:35000?auth=none", "chronicle+srv://host", "chronicle://one,two", "chronicle://[bad]", "chronicle://host?x=%zz"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		parsed, err := ParseConnectionString(input)
		if err != nil {
			return
		}
		if len(parsed.Addresses()) == 0 {
			t.Fatal("accepted empty endpoint")
		}
		if _, err := ParseConnectionString(parsed.String()); err != nil {
			t.Fatalf("redacted endpoint cannot parse: %v", err)
		}
	})
}
