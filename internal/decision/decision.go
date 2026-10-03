// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package decision is the SDK-only issuance and transport boundary for optimistic
// decision guards. Nothing here is a server-authenticated proof.
package decision

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/cratis/chronicle.go/contracts"
	"github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

var (
	ErrInvalid = errors.New("chronicle: invalid decision token")
	ErrTarget  = errors.New("chronicle: decision target mismatch")
	ErrOwner   = errors.New("chronicle: decision token belongs to another owner")
	ErrStale   = errors.New("chronicle: decision catalog or connection generation changed")
	ErrScope   = errors.New("chronicle: explicit scope conflicts with decision protection")
)

// Supported deliberately recognizes only the characterized release. Descriptor
// compatibility, a newer version and skipped preflight are not positive evidence.
func Supported(version, protocol string) bool {
	return (version == "19.29.4" || version == "19.29.4-development") && protocol == contracts.ProtocolVersion
}

// Target includes client identity, not merely wire coordinates.
type Target struct {
	Client                     any
	Store, Namespace, Sequence string
}

// Catalog is a frozen store-publication snapshot. A future runtime publication
// must invalidate the old epoch before replacing it.
type Catalog struct {
	Projections   []*projections.ProjectionDefinition
	Events        *events.Catalog
	Epoch         *atomic.Uint64
	ExpectedEpoch uint64
}

func NewCatalog(definitions []*projections.ProjectionDefinition, eventTypes *events.Catalog) *Catalog {
	catalog := &Catalog{Events: eventTypes, Epoch: &atomic.Uint64{}, ExpectedEpoch: 1}
	catalog.Epoch.Store(1)
	for _, definition := range definitions {
		catalog.Projections = append(catalog.Projections, proto.CloneOf(definition))
	}
	return catalog
}

// Provider cannot be implemented by applications without access to this internal
// package. Low-level transports and chronicletest do not issue tokens.
type Provider interface {
	DecisionCatalog() *Catalog
	DecisionTarget(sequence events.SequenceID) Target
	AcquireDecision(context.Context) (*Lease, error)
}

// Lease pins the same transport through agreement, tails, fold and cleanup, or
// through a protected append. Release is idempotent and must precede callbacks.
type Lease struct {
	Context    context.Context
	Conn       grpc.ClientConnInterface
	Generation uint64
	Check      func() error
	Release    func()
	Resolve    func(any)
}

// Evidence is immutable internal issuance data. Unavailable represents before
// the first event and is encoded as an explicit no-match expectation on commit.
type Evidence struct {
	Target            Target
	Model, Key        string
	Types             []events.TypeRef
	Boundary          events.SequenceNumber
	Catalog           *Catalog
	Epoch, Generation uint64
	Check             func() error
}

// Token is opaque. Copies share permanent first-enrollment redemption state.
// Its zero value cannot be enrolled. It is not serializable proof or authority
// over a direct RPC; only the SDK can issue it after a validated session read.
type Token struct{ state *redemption }
type redemption struct {
	mu       sync.Mutex
	evidence Evidence
	owner    any
}

// IsZero reports whether no token was issued (including every failed read).
func (t Token) IsZero() bool { return t.state == nil }

func Issue(e Evidence) Token {
	if validate(e) != nil {
		return Token{}
	}
	e.Types = slices.Clone(e.Types)
	return Token{state: &redemption{evidence: e}}
}

func validate(e Evidence) error {
	if e.Target.Client == nil || e.Target.Sequence != string(events.EventLog) || e.Model == "" || e.Key == "" || len(e.Types) == 0 || e.Catalog == nil || e.Check == nil {
		return ErrInvalid
	}
	if e.Epoch != e.Catalog.ExpectedEpoch || e.Epoch != e.Catalog.Epoch.Load() || e.Check() != nil {
		return ErrStale
	}
	return nil
}

// Guard is produced only by successful SDK enrollment. Its evidence cannot be
// changed by an application, and token copies never release their original owner.
type Guard struct{ evidence Evidence }

func Enroll(token Token, target Target, owner any, accept func(*Guard) error) error {
	if token.state == nil || owner == nil {
		return ErrInvalid
	}
	r := token.state
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.evidence.Target != target {
		return ErrTarget
	}
	if err := validate(r.evidence); err != nil {
		return err
	}
	if r.owner != nil && r.owner != owner {
		return ErrOwner
	}
	if err := accept(&Guard{evidence: r.evidence}); err != nil {
		return err
	}
	r.owner = owner
	return nil
}

func (g *Guard) Evidence() Evidence {
	e := g.evidence
	e.Types = slices.Clone(e.Types)
	return e
}

func (g *Guard) Validate(target Target, generation uint64) error {
	if g == nil {
		return ErrInvalid
	}
	if g.evidence.Target != target {
		return ErrTarget
	}
	if g.evidence.Generation != generation {
		return ErrStale
	}
	return validate(g.evidence)
}

type dispatchValidationKey struct{}

// WithDispatchValidation carries SDK-only epoch checks through authorization,
// which may invoke caller code before the actual transport dispatch.
func WithDispatchValidation(ctx context.Context, check func() error) context.Context {
	return context.WithValue(ctx, dispatchValidationKey{}, check)
}

func ValidateDispatch(ctx context.Context) error {
	if check, ok := ctx.Value(dispatchValidationKey{}).(func() error); ok {
		return check()
	}
	return nil
}

func Unsupported() error {
	return fmt.Errorf("%w: decision guards require the verified Chronicle 19.29.4 profile and compatibility preflight", faults.ErrUnsupported)
}
