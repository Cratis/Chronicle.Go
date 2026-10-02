// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package connection

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Token contains a bearer credential. It deliberately formats without its value.
type Token struct {
	// AccessToken is the sensitive bearer credential. Never log this field.
	AccessToken string
	// Expiry is the expiration time; zero means the external source owns expiry policy.
	Expiry time.Time
}

func (Token) String() string   { return "[redacted token]" }
func (Token) GoString() string { return "[redacted token]" }

// TokenSource supplies credentials and must honor cancellation; it may be called concurrently.
type TokenSource interface {
	Token(context.Context) (Token, error)
}

// OAuth owns one HTTP transport and serializes refreshes without background work.
type OAuth struct {
	client               *http.Client
	endpoint, id, secret string
	gate                 chan struct{}
	cached               Token
	failedAt             time.Time
	lastError            error
}

func NewOAuth(address, id, secret string, config *tls.Config) *OAuth {
	transport := &http.Transport{TLSClientConfig: config.Clone(), Proxy: http.ProxyFromEnvironment}
	return &OAuth{client: &http.Client{Transport: transport, Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("token endpoint redirect refused") }},
		endpoint: "https://" + address + "/connect/token", id: id, secret: secret, gate: make(chan struct{}, 1)}
}

func (s *OAuth) Close() { s.client.CloseIdleConnections() }

func (s *OAuth) Token(ctx context.Context) (Token, error) {
	select {
	case s.gate <- struct{}{}:
	case <-ctx.Done():
		return Token{}, ctx.Err()
	}
	defer func() { <-s.gate }()
	if err := ctx.Err(); err != nil {
		return Token{}, err
	}
	now := time.Now()
	if s.cached.AccessToken != "" && now.Before(s.cached.Expiry.Add(-time.Minute)) {
		return s.cached, nil
	}
	if now.Sub(s.failedAt) < 5*time.Second {
		if now.Before(s.cached.Expiry) {
			return s.cached, nil
		}
		return Token{}, s.lastError
	}
	token, err := s.fetch(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return Token{}, ctx.Err()
		}
		s.failedAt, s.lastError = now, err
		if now.Before(s.cached.Expiry) {
			return s.cached, nil
		}
		return Token{}, err
	}
	s.cached, s.failedAt, s.lastError = token, time.Time{}, nil
	return token, nil
}

func (s *OAuth) fetch(ctx context.Context) (Token, error) {
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {s.id}, "client_secret": {s.secret}}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return Token{}, errors.New("chronicle: invalid token endpoint")
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := s.client.Do(request)
	if err != nil {
		return Token{}, fmt.Errorf("chronicle: token exchange: %w", err)
	}
	// The body is fully read below; a read-only HTTP body's Close error cannot
	// change the token result or conceal an incomplete read.
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return Token{}, fmt.Errorf("chronicle: token endpoint returned HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 1024*1024+1))
	if err != nil {
		return Token{}, fmt.Errorf("chronicle: read token response: %w", err)
	}
	if len(body) > 1024*1024 {
		return Token{}, errors.New("chronicle: token response too large")
	}
	var data struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   *int64 `json:"expires_in"`
		TokenType   string `json:"token_type"`
	}
	if json.Unmarshal(body, &data) != nil || data.AccessToken == "" || (data.TokenType != "" && !strings.EqualFold(data.TokenType, "bearer")) {
		return Token{}, errors.New("chronicle: invalid token response")
	}
	seconds := int64(3600)
	if data.ExpiresIn != nil {
		seconds = *data.ExpiresIn
	}
	if seconds <= 0 || seconds > 31536000 {
		return Token{}, errors.New("chronicle: invalid token lifetime")
	}
	return Token{AccessToken: data.AccessToken, Expiry: time.Now().Add(time.Duration(seconds) * time.Second)}, nil
}
