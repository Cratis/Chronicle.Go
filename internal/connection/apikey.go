// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package connection

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/grpc/credentials"
)

// APIKey sends a static API key as api-key metadata on every RPC, the way the other
// Chronicle clients do. It never appears in String, GoString or errors.
type APIKey struct {
	key    string
	secure bool
}

var _ credentials.PerRPCCredentials = APIKey{}

// NewAPIKey validates key and builds the per-RPC credentials. When secure is true
// the key travels only over TLS; secure is false for disableTls connections.
func NewAPIKey(key string, secure bool) (APIKey, error) {
	if key == "" || strings.ContainsAny(key, "\r\n") {
		return APIKey{}, errors.New("chronicle: invalid API key")
	}
	return APIKey{key: key, secure: secure}, nil
}

// GetRequestMetadata implements credentials.PerRPCCredentials.
func (a APIKey) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"api-key": a.key}, nil
}

// RequireTransportSecurity implements credentials.PerRPCCredentials.
func (a APIKey) RequireTransportSecurity() bool { return a.secure }

func (APIKey) String() string   { return "[redacted API key]" }
func (APIKey) GoString() string { return "[redacted API key]" }
