// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package connection

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestOAuthSingleFlightAndRefreshFallback(t *testing.T) {
	var calls atomic.Int32
	var failure atomic.Bool
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/connect/token" || r.ParseForm() != nil || r.Form.Get("grant_type") != "client_credentials" || r.Form.Get("client_id") != "client" || r.Form.Get("client_secret") != "secret" {
			t.Error("incorrect token form")
		}
		if failure.Load() {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte("do not echo this sensitive body"))
			return
		}
		_, _ = w.Write([]byte(`{"access_token":"bearer-secret","token_type":"Bearer"}`))
	}))
	defer server.Close()
	source := NewOAuth(strings.TrimPrefix(server.URL, "https://"), "client", "secret", &tls.Config{InsecureSkipVerify: true})
	defer source.Close()
	var group sync.WaitGroup
	for range 16 {
		group.Go(func() {
			token, err := source.Token(t.Context())
			if err != nil || token.AccessToken != "bearer-secret" || time.Until(token.Expiry) < 59*time.Minute {
				t.Errorf("token = %v error = %v", token, err)
			}
		})
	}
	group.Wait()
	if calls.Load() != 1 {
		t.Fatalf("token requests = %d", calls.Load())
	}
	failure.Store(true)
	source.cached.Expiry = time.Now().Add(30 * time.Second)
	if token, err := source.Token(t.Context()); err != nil || token.AccessToken != "bearer-secret" {
		t.Fatal("still-valid fallback lost", err)
	}
	if _, err := source.Token(t.Context()); err != nil || calls.Load() != 2 {
		t.Fatal("failure throttle lost", err)
	}
	source.cached.Expiry = time.Now().Add(-time.Second)
	if _, err := source.Token(t.Context()); err == nil || strings.Contains(err.Error(), "sensitive body") {
		t.Fatal("expired token accepted or response body leaked")
	}
	if strings.Contains(fmt.Sprintf("%#v", Token{AccessToken: "bearer-secret"}), "bearer-secret") {
		t.Fatal("token leaked through formatting")
	}
}

func TestTokenWaitHonorsCancellation(t *testing.T) {
	source := NewOAuth("localhost:35000", "client", "secret", &tls.Config{MinVersion: tls.VersionTLS12})
	defer source.Close()
	source.gate <- struct{}{}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := source.Token(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	<-source.gate
}

func TestInvalidTokenResponsesFailClosed(t *testing.T) {
	for _, body := range []string{`{}`, `{"access_token":"token","expires_in":0}`, `{"access_token":"token","token_type":"Basic"}`, `{"access_token":"token","expires_in":31536001}`, `invalid`} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) }))
			defer server.Close()
			source := NewOAuth(strings.TrimPrefix(server.URL, "https://"), "client", "secret", &tls.Config{InsecureSkipVerify: true})
			defer source.Close()
			if _, err := source.Token(t.Context()); err == nil {
				t.Fatal("malformed token accepted")
			}
		})
	}
}
