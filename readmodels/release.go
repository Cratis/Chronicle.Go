// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/contracts/compliance"
	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/serialization"
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
// schema. Top-level properties are released in groups selected by __subjects,
// falling back to the resolved default subject. Persisted lineage is preserved. Missing
// subjects, kernel release errors and invalid replies fail closed. Unprotected
// documents are copied without a release RPC. Every read path verifies release
// before delivery: the kernel handlers pass already released values unchanged.
// Namespace/global confidentiality does not require a subject. Subject-dependent
// values without an owner fail closed, even in mixed-scope models.
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
	if len(d.definition.protected) == 0 {
		return append(json.RawMessage(nil), document...), nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(document, &fields); err != nil {
		return nil, &ReleaseError{Cause: faults.ErrProtocol}
	}
	subject := ""
	// Stored lineage is authoritative for raw documents; a typed instance normally
	// resolves the explicitly selected property, then the Go ID property.
	subjectProperties := []string{"__subject", d.definition.config.subject}
	if id := releaseIDProperty(d); id != "" {
		subjectProperties = append(subjectProperties, id, "_id", "id", "Id", "ID")
	}
	for _, name := range subjectProperties {
		if name == "" {
			continue
		}
		if value := releaseSubject(fields[name]); value != "" {
			subject = value
			break
		}
	}
	var subjects map[string]string
	if lineage, ok := fields["__subjects"]; ok {
		if err := json.Unmarshal(lineage, &subjects); err != nil {
			return nil, &ReleaseError{Cause: faults.ErrProtocol}
		}
	}
	declared := make(map[string]bool)
	for _, field := range serialization.RootFields(d.definition.plan.Fields()) {
		declared[field.Name] = true
	}
	groups := make(map[string]map[string]json.RawMessage)
	for name, value := range fields {
		// The kernel rejects undeclared fields. Keep sink bookkeeping and
		// lineage locally, but never send them through the schema walk.
		if !declared[name] {
			continue
		}
		propertySubject, ok := subjects[name]
		if !ok {
			propertySubject = subject
		}
		if propertySubject == "" && (d.definition.protected[name] || ok) {
			return nil, &ReleaseError{Cause: invalid("protected value requires a subject")}
		}
		if groups[propertySubject] == nil {
			groups[propertySubject] = make(map[string]json.RawMessage)
		}
		groups[propertySubject][name] = value
	}
	ordered := make([]string, 0, len(groups))
	for group := range groups {
		ordered = append(ordered, group)
	}
	slices.Sort(ordered)
	for _, group := range ordered {
		payload, err := json.Marshal(groups[group])
		if err != nil {
			return nil, &ReleaseError{Cause: faults.ErrProtocol}
		}
		released, err := s.releaseSlice(ctx, d, group, payload)
		if err != nil {
			return nil, err
		}
		// Never merge a partial reply over retained ciphertext. Unprotected
		// bookkeeping may be omitted, but each protected root that was sent
		// must be replaced by the release response.
		for root := range d.definition.protected {
			if _, sent := groups[group][root]; sent {
				if _, returned := released[root]; !returned {
					return nil, &ReleaseError{Cause: faults.ErrProtocol}
				}
			}
		}
		for name, value := range released {
			// A group cannot overwrite another owner's values or stored lineage.
			if _, sent := groups[group][name]; sent && name != "__subject" && name != "__subjects" {
				fields[name] = value
			}
		}
	}
	// Validate only classified roots against the declared codec. Raw documents
	// may contain sink bookkeeping and differently represented unprotected IDs.
	protected := make(map[string]json.RawMessage, len(d.definition.protected))
	for name := range d.definition.protected {
		if value, ok := fields[name]; ok {
			protected[name] = value
		}
	}
	protectedJSON, err := json.Marshal(protected)
	if err != nil {
		return nil, &ReleaseError{Cause: faults.ErrProtocol}
	}
	if err := d.definition.plan.Unmarshal(protectedJSON, reflect.New(d.GoType()).Interface()); err != nil {
		return nil, &ReleaseError{Cause: err}
	}
	result, err := json.Marshal(fields)
	if err != nil {
		return nil, &ReleaseError{Cause: faults.ErrProtocol}
	}
	return result, nil
}

func releaseSubject(raw json.RawMessage) string {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return ""
	}
	switch value := value.(type) {
	case string:
		return value
	case json.Number:
		return value.String()
	case bool:
		if value {
			return "True"
		}
		return "False"
	default:
		return ""
	}
}

func (s *Service) releaseSlice(ctx context.Context, d Descriptor, subject string, payload []byte) (map[string]json.RawMessage, error) {
	response, err := s.compliance.Release(ctx, &compliance.ReleaseRequest{EventStore: string(s.store), Namespace: string(s.namespace), Subject: subject, Schema: d.Schema(), Payload: string(payload)})
	if err != nil {
		return nil, &ReleaseError{Cause: wire.RPCError(err)}
	}
	if response == nil || response.HasError || response.Error != "" || !validDocument(bytes.TrimSpace([]byte(response.Payload))) {
		return nil, &ReleaseError{Cause: faults.ErrProtocol}
	}
	if err := validateReleased(d.Schema(), payload, []byte(response.Payload)); err != nil {
		return nil, &ReleaseError{Cause: err}
	}
	var released map[string]json.RawMessage
	if err := json.Unmarshal([]byte(response.Payload), &released); err != nil {
		return nil, &ReleaseError{Cause: faults.ErrProtocol}
	}
	return released, nil
}

func idProperty(d Descriptor) string {
	id := ""
	for _, field := range serialization.RootFields(d.definition.plan.Fields()) {
		directives, _ := declarations.Parse(declarations.V1, field.Tag) // The plan validated tags.
		for _, directive := range directives {
			if directive.Name == "key" {
				return field.Path
			}
		}
		if strings.EqualFold(field.GoField, "id") {
			id = field.Path
		}
	}
	return id
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
	if len(d.definition.protected) == 0 {
		return value, nil
	}
	data, err := d.Marshal(value)
	if err != nil {
		return zero, &ReleaseError{Cause: err}
	}
	released, err := r.service.Release(ctx, d.Identifier(), data)
	if err != nil {
		return zero, err
	}
	released, err = normalizeID(released, d)
	if err != nil {
		return zero, &ReleaseError{Cause: err}
	}
	result, err := decode[T](Instance[json.RawMessage]{Value: released, Exists: true}, d)
	if err != nil {
		return zero, &ReleaseError{Cause: err}
	}
	return result.Value, nil
}
