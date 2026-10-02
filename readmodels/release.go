// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/cratis/chronicle.go/contracts/compliance"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
)

// ErrRelease identifies a failed protected-value release. No original ciphertext
// or partially released document is returned on this error.
var ErrRelease = errors.New("chronicle: read-model release failed")

// ReleaseError preserves the underlying transport/protocol cause without including
// model data, subject values or potentially sensitive kernel error messages.
type ReleaseError struct {
	// Cause is the inspectable underlying error. Do not log transport diagnostics
	// without considering whether the server included sensitive data.
	Cause error
}

// Error returns a payload-free description.
func (e *ReleaseError) Error() string { return ErrRelease.Error() }

// Unwrap exposes ErrRelease and the underlying failure to errors.Is/As.
func (e *ReleaseError) Unwrap() []error { return []error{ErrRelease, e.Cause} }

// Release resolves protected values in a raw sink document using its registered
// schema. Persisted __subject/__subjects lineage remains in the payload. Missing
// subjects, kernel release errors and invalid replies fail closed. Unprotected
// documents are copied without a release RPC. Never release an already-released
// value a second time; Get's kernel path already performs release.
func (s *Service) Release(ctx context.Context, model Identifier, document json.RawMessage) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d, ok := s.catalog.LookupIdentifier(model)
	if !ok {
		return nil, notRegistered()
	}
	if !validDocument(bytes.TrimSpace(document)) {
		return nil, invalid("release requires a JSON object")
	}
	if len(d.definition.config.pii) == 0 {
		return append(json.RawMessage(nil), document...), nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(document, &fields); err != nil {
		return nil, &ReleaseError{Cause: faults.ErrProtocol}
	}
	subject := ""
	// Stored lineage is authoritative for raw documents; a typed instance normally
	// resolves the explicitly selected property, then the Go ID property.
	for _, name := range []string{"__subject", d.definition.config.subject, idProperty(d)} {
		if name == "" {
			continue
		}
		var value string
		if err := json.Unmarshal(fields[name], &value); err == nil && value != "" {
			subject = value
			break
		}
	}
	if subject == "" {
		return nil, &ReleaseError{Cause: invalid("protected model requires a subject")}
	}
	response, err := s.compliance.Release(ctx, &compliance.ReleaseRequest{EventStore: string(s.store), Namespace: string(s.namespace), Subject: subject, Schema: d.Schema(), Payload: string(document)})
	if err != nil {
		return nil, &ReleaseError{Cause: wire.RPCError(err)}
	}
	if response == nil || response.HasError || response.Error != "" || !validDocument(bytes.TrimSpace([]byte(response.Payload))) {
		return nil, &ReleaseError{Cause: faults.ErrProtocol}
	}
	return json.RawMessage(response.Payload), nil
}

func idProperty(d Descriptor) string {
	typ := d.GoType()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.IsExported() && strings.EqualFold(field.Name, "id") {
			name := strings.Split(field.Tag.Get("json"), ",")[0]
			if name == "-" {
				return ""
			}
			if name == "" {
				return "id"
			}
			return name
		}
	}
	return ""
}

// Release decrypts a value fetched directly from a sink. No value is returned on
// failure. T must carry its subject; use Service.Release for raw stored lineage.
func (r *Reader[T]) Release(ctx context.Context, value T) (T, error) {
	var zero T
	d, err := r.descriptor()
	if err != nil {
		return zero, err
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if len(d.definition.config.pii) == 0 {
		return value, nil
	}
	data, err := d.Marshal(value)
	if err != nil {
		return zero, fmt.Errorf("release serialization: %w", err)
	}
	released, err := r.service.Release(ctx, d.Identifier(), data)
	if err != nil {
		return zero, err
	}
	result, err := decode[T](Instance[json.RawMessage]{Value: released, Exists: true})
	if err != nil {
		return zero, &ReleaseError{Cause: err}
	}
	return result.Value, nil
}
