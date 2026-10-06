// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package captures

import (
	"encoding/json"
	"strings"

	"github.com/cratis/chronicle.go/internal/jsonescape"
)

// AuthorizationKind identifies an inbound capture source's authorization scheme.
type AuthorizationKind string

const (
	// AuthorizationNone is the zero SourceAuthorization's kind, not an authorable option.
	AuthorizationNone AuthorizationKind = "none"
	// AuthorizationBasic identifies username/password authorization.
	AuthorizationBasic AuthorizationKind = "basic"
	// AuthorizationBearer identifies bearer-token authorization.
	AuthorizationBearer AuthorizationKind = "bearer"
	// AuthorizationOAuth identifies authority/client credentials authorization.
	AuthorizationOAuth AuthorizationKind = "oauth"
)

// SourceAuthorization is an immutable, redacted authorization value. Its zero
// value represents None. Construct present values through Webhook options.
// Copies are safe for concurrent use. JSON import/export fails with ErrUnsupported;
// no public credential export or decoder is provided.
type SourceAuthorization struct {
	kind   AuthorizationKind
	secret *authorizationSecret
}

// authorizationSecret is initialized once by an option constructor and never
// mutated. fmt cannot inspect a closure's captured values. A pointer to inline
// strings is insufficient: invalid-verb fallbacks (%s/%q) dereference it even
// when an unexported caller field prevents invoking Format.
type authorizationSecret struct {
	values func() (string, string, string)
}

func newAuthorizationSecret(first, second, third string) *authorizationSecret {
	return &authorizationSecret{values: func() (string, string, string) {
		return first, second, third
	}}
}

// Kind returns the scheme; the zero value returns AuthorizationNone.
func (a SourceAuthorization) Kind() AuthorizationKind {
	if a.kind == "" {
		return AuthorizationNone
	}
	return a.kind
}

// kernelJSON reproduces the C# converter solely for internal contract evidence.
// It is not a supported submission path: capture RPCs carry CDL only.
func (a SourceAuthorization) kernelJSON() ([]byte, error) {
	var first, second, third string
	if a.secret != nil {
		first, second, third = a.secret.values()
	}
	var value any
	switch a.Kind() {
	case AuthorizationBasic:
		value = struct {
			Type     AuthorizationKind `json:"type"`
			Username string            `json:"username"`
			Password string            `json:"password"`
		}{a.Kind(), first, second}
	case AuthorizationBearer:
		value = struct {
			Type  AuthorizationKind `json:"type"`
			Token string            `json:"token"`
		}{a.Kind(), first}
	case AuthorizationOAuth:
		value = struct {
			Type         AuthorizationKind `json:"type"`
			Authority    string            `json:"authority"`
			ClientID     string            `json:"clientId"`
			ClientSecret string            `json:"clientSecret"`
		}{a.Kind(), first, second, third}
	default:
		value = struct {
			Type AuthorizationKind `json:"type"`
		}{AuthorizationNone}
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return jsonescape.Strings(data)
}

// WebhookOption configures an inbound source. Nil, blank credentials or more
// than one authorization option fail at Build with ErrInvalidConfiguration.
// No option means absent authorization, not an explicit None value.
type WebhookOption func(*webhookConfiguration)

type webhookConfiguration struct {
	authorization SourceAuthorization
	count         int
	invalid       bool
}

func (c *webhookConfiguration) set(a SourceAuthorization) {
	c.count++
	if c.count > 1 {
		c.invalid = true
		return
	}
	first, second, third := a.secret.values()
	values := []string{first}
	if a.Kind() == AuthorizationBasic || a.Kind() == AuthorizationOAuth {
		values = append(values, second)
	}
	if a.Kind() == AuthorizationOAuth {
		values = append(values, third)
	}
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			c.invalid = true
		}
	}
	c.authorization = a
}

// WithBasicAuth authors inbound username/password authorization. Values are
// preserved verbatim; blank values fail at Build. It cannot yet be submitted.
func WithBasicAuth(username, password string) WebhookOption {
	authorization := SourceAuthorization{kind: AuthorizationBasic, secret: newAuthorizationSecret(username, password, "")}
	return func(c *webhookConfiguration) {
		c.set(authorization)
	}
}

// WithBearerToken authors inbound bearer authorization. A blank token fails at
// Build. It cannot yet be submitted to the CDL-only capture transport.
func WithBearerToken(token string) WebhookOption {
	authorization := SourceAuthorization{kind: AuthorizationBearer, secret: newAuthorizationSecret(token, "", "")}
	return func(c *webhookConfiguration) {
		c.set(authorization)
	}
}

// WithOAuth authors inbound authority/client credentials authorization, without
// contacting an authority. Blank values fail at Build. It cannot yet be submitted.
func WithOAuth(authority, clientID, clientSecret string) WebhookOption {
	authorization := SourceAuthorization{kind: AuthorizationOAuth, secret: newAuthorizationSecret(authority, clientID, clientSecret)}
	return func(c *webhookConfiguration) {
		c.set(authorization)
	}
}
