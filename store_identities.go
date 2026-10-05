// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	contracts "github.com/cratis/chronicle.go/contracts/identities"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
)

// IdentityManager borrows its namespace-bound EventStore and Client. It starts no
// work and needs no Close. Concurrent calls use independent invocation state.
// Obtain it through EventStore.Identities; the zero value is unusable.
type IdentityManager struct{ store *EventStore }

// Identities returns a handle without I/O. Rename requires existing readiness;
// unlike ordinary store operations, it never starts or awaits registration.
func (s *EventStore) Identities() *IdentityManager { return &IdentityManager{store: s} }

// IdentityRenameDisposition distinguishes dispatch evidence from name observation.
type IdentityRenameDisposition uint8

const (
	// IdentityRenameNotDispatched means this invocation entered no rename RPC.
	IdentityRenameNotDispatched IdentityRenameDisposition = iota
	// IdentityRenameRefused means a valid command envelope explicitly refused execution.
	IdentityRenameRefused
	// IdentityRenameUnknown means mutation or its confirmation is uncertain.
	IdentityRenameUnknown
	// IdentityRenameObserved means exactly one subject had the requested post-read name.
	IdentityRenameObserved
)

// IdentityRenameResult contains only SDK-owned disposition and copied correlations.
// It never contains identity records, names, subjects or server diagnostics.
type IdentityRenameResult struct {
	// Disposition is observation/dispatch evidence, not an atomic matched-row receipt.
	Disposition IdentityRenameDisposition
	// Acknowledged means a valid successful command envelope, not that a row changed.
	// It survives subsequent cancellation or failed confirmation.
	Acknowledged bool
	// RequestCorrelationID is the once-selected operation correlation; zero before preparation.
	RequestCorrelationID metadata.CorrelationID
	// ResponseCorrelationID preserves command response presence; nil means absent.
	// It is independent of the request and owned by this result.
	ResponseCorrelationID *metadata.CorrelationID
}

// ErrIdentityRenameUnknown identifies an uncertain mutation or confirmation.
var ErrIdentityRenameUnknown = errors.New("chronicle: identity rename outcome unknown")

// ErrIdentityRenameRefused identifies an explicit command-envelope refusal.
var ErrIdentityRenameRefused = errors.New("chronicle: identity rename refused")

// IdentityRenameError reports fixed SDK categories without retaining borrowed
// errors or diagnostic payloads. Use Phase/Reason and errors.Is/As for inspection.
type IdentityRenameError struct {
	phase, reason string
	categories    []error
}

func (e *IdentityRenameError) Error() string {
	return "chronicle: identity rename failed (" + e.phase + ": " + e.reason + ")"
}

// Phase returns prepare, pre_read, command or post_read.
func (e *IdentityRenameError) Phase() string { return e.phase }

// Reason returns a fixed SDK reason, never a supplied diagnostic or identity.
func (e *IdentityRenameError) Reason() string { return e.reason }

// Unwrap exposes detached fixed categories, including context errors when known.
func (e *IdentityRenameError) Unwrap() []error { return append([]error(nil), e.categories...) }

func identityRenameError(phase string, failure identityFailure, disposition IdentityRenameDisposition) error {
	e := &IdentityRenameError{phase: phase, reason: failure.reason}
	if failure.category != nil {
		e.categories = append(e.categories, failure.category)
	}
	if failure.cancellation != nil && failure.cancellation != failure.category {
		e.categories = append(e.categories, failure.cancellation)
	}
	switch disposition {
	case IdentityRenameUnknown:
		e.categories = append(e.categories, ErrIdentityRenameUnknown)
	case IdentityRenameRefused:
		e.categories = append(e.categories, ErrIdentityRenameRefused)
	}
	return e
}

// Rename requests one namespace-scoped display-name update for an opaque subject.
// Accepted UTF-8 nonblank strings are preserved exactly. Correlation accepts zero
// or one value; explicit zero selects a fresh ID without consulting its provider.
// Audit providers run once outside SDK locks/leases. Actor/causation are frozen in
// context, but the command wire persists neither; correlation is sent as a header.
//
// The current generation/root must already be fully registered. Rename neither
// connects nor starts/joins readiness. Separate Ready calls are outside its privacy
// boundary. It uses at most two whole-namespace listings and one command, sharing
// the caller deadline without retry, polling or a default timeout. Listing requires
// permission and fits configured message bounds. Observation is not atomic and
// does not prove this command caused the name or historical cache convergence.
//
// A Rename holds the per-Client, per-store definition flight only during its
// own command RPC; destructive definition registrations (read-model and
// projection registration) use the same flight. Renames are not serialized
// across their reads. A Rename on the same Client and store (any namespace)
// that reaches a readiness check while anything else holds that flight (another
// Rename's command or a destructive definition registration) fails with reason
// registration_not_ready: before its own command it returns
// IdentityRenameNotDispatched; past its command (post_read) it returns
// IdentityRenameUnknown with Acknowledged true. Renames through other clients or
// processes are unaffected. Decide retries from the returned disposition, not
// from concurrency alone.
//
// SDK processing of this invocation discards borrowed error/panic payloads without
// invoking unknown error hooks. Independent background operations and borrowed
// implementations' own logging/explicit reentrant operations are not covered.
func (m *IdentityManager) Rename(ctx context.Context, subject string, name identities.Name, correlation ...metadata.CorrelationID) (result IdentityRenameResult, err error) {
	invalid := m == nil || m.store == nil || m.store.storeOwner == nil || m.store.client == nil || m.store.definitions == nil || nilValue(ctx) || len(correlation) > 1 ||
		!utf8.ValidString(subject) || !utf8.ValidString(string(name)) || strings.TrimSpace(subject) == "" || strings.TrimSpace(string(name)) == ""
	if invalid {
		return result, identityRenameError("prepare", identityFailure{reason: "configuration", category: ErrInvalidConfiguration}, result.Disposition)
	}
	if canceled := ctx.Err(); canceled != nil {
		return result, identityRenameError("prepare", identityContextFailure(canceled), result.Disposition)
	}
	var id metadata.CorrelationID
	if len(correlation) == 1 {
		id = correlation[0]
	}
	audit, err := m.store.client.config.outgoing.Resolve(ctx, id, len(correlation) == 1, metadata.CorrelationID{})
	if err != nil {
		return result, err
	} // Resolve's Phase A boundary owns its payload-free grammar.
	result.RequestCorrelationID = audit.Correlation
	ctx = audit.Context(ctx)
	c := m.store.client
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return result, identityRenameError("prepare", identityFailure{reason: "closed", category: ErrClosed}, result.Disposition)
	}
	g, root := c.current, m.store.definitions.root
	c.mu.Unlock()
	if g == nil || root == nil {
		return result, identityRenameError("prepare", identityFailure{reason: "registration_not_ready"}, result.Disposition)
	}
	p := m.store.identityReadiness(root)
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(g.ctx, cancel)
	defer func() { stop(); cancel() }()
	i := &identityInvocation{store: m.store, generation: g, root: root, readiness: p, ctx: ctx}
	if failure := i.check(); failure.reason != "" {
		return result, identityRenameError("prepare", failure, result.Disposition)
	}
	match, failure := i.list(subject)
	if failure.reason != "" {
		return result, identityRenameError("pre_read", failure, result.Disposition)
	}
	if match == nil {
		return result, identityRenameError("pre_read", identityFailure{reason: "not_found", category: &identities.NotFoundError{}}, result.Disposition)
	}
	request := &contracts.RenameIdentityRequest{EventStore: string(m.store.name), Namespace: string(m.store.namespace), Subject: subject, Name: string(name)}
	response := &contracts.CommandResult{}
	entered, failure := i.invoke(contracts.Identities_RenameIdentity_FullMethodName, request, response, true, func(codec *identityCodec) identityFailure {
		if !codec.decoded {
			return identityFailure{reason: "protocol", category: ErrProtocol}
		}
		// A valid decoded command preserves correlation presence even when its
		// envelope refuses execution or reports uncertainty.
		if response.CorrelationId != nil {
			selected := wire.Correlation(response.CorrelationId)
			result.ResponseCorrelationID = &selected
		}
		failure := identityEnvelope(response.IsAuthorized, len(response.ValidationResults), response.ExceptionMessages, response.ExceptionStackTrace)
		if failure.reason == "" && response.AuthorizationFailureReason != "" {
			return identityFailure{reason: "protocol", category: ErrProtocol}
		}
		failure.refused = failure.reason == "authorization" || failure.reason == "validation"
		if failure.reason == "" {
			result.Acknowledged = true
		}
		return failure
	})
	if failure.reason != "" {
		if entered {
			result.Disposition = IdentityRenameUnknown
			// Only decoder-backed explicit envelopes can establish refusal.
			if failure.refused {
				result.Disposition = IdentityRenameRefused
			}
		}
		return result, identityRenameError("command", failure, result.Disposition)
	}
	// Successful acknowledgment is recorded before any post-read admission check.
	result.Disposition = IdentityRenameUnknown
	match, failure = i.list(subject)
	if failure.reason == "" {
		switch {
		case match == nil:
			failure = identityFailure{reason: "missing_confirmation"}
		case match.Name != string(name):
			failure = identityFailure{reason: "name_mismatch"}
		}
	}
	if failure.reason != "" {
		return result, identityRenameError("post_read", failure, result.Disposition)
	}
	result.Disposition = IdentityRenameObserved
	return result, nil
}

func identityEnvelope(authorized bool, validation int, exceptions []string, stack string) identityFailure {
	if len(exceptions) > 0 || stack != "" {
		return identityFailure{reason: "execution"}
	}
	if !authorized {
		return identityFailure{reason: "authorization"}
	}
	if validation > 0 {
		return identityFailure{reason: "validation"}
	}
	return identityFailure{}
}
func (i *identityInvocation) list(subject string) (*contracts.IdentityDetailsResponse, identityFailure) {
	response := &contracts.QueryResult_IEnumerable_IdentityDetailsResponse{}
	_, failure := i.invoke(contracts.Identities_GetIdentities_FullMethodName, &contracts.GetIdentitiesRequest{EventStore: string(i.store.name), Namespace: string(i.store.namespace)}, response, false, func(codec *identityCodec) identityFailure {
		if !codec.decoded {
			return identityFailure{reason: "protocol", category: ErrProtocol}
		}
		return identityEnvelope(response.IsAuthorized, len(response.ValidationResults), response.ExceptionMessages, response.ExceptionStackTrace)
	})
	if failure.reason != "" {
		return nil, failure
	}
	if failure = i.check(); failure.reason != "" {
		return nil, failure
	}
	var match *contracts.IdentityDetailsResponse
	for _, row := range response.Data {
		if row == nil || row.Id != row.Subject {
			return nil, identityFailure{reason: "protocol", category: ErrProtocol}
		}
		if row.Subject != subject {
			continue
		}
		if match != nil {
			return nil, identityFailure{reason: "duplicate_subject", category: ErrProtocol}
		}
		match = row
	}
	return match, identityFailure{}
}
