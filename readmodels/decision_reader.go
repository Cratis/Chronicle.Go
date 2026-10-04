// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/decision"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/transactions"
	"github.com/google/uuid"
)

// ErrDecisionRequiresUnitOfWork means Get has no existing participant. Use
// GetDetached when the outer orchestrator will explicitly enroll later.
var ErrDecisionRequiresUnitOfWork = errors.New("chronicle: decision read requires an existing unit of work")

// DecisionRead contains explicitly present/absent model state and an opaque
// optimistic guard. Every failure returns its zero value, never usable evidence.
type DecisionRead[T any] struct {
	Instance Instance[T]                // Instance preserves removal progress even when absent.
	Token    transactions.DecisionToken // Token can be enrolled only by one unit/owner.
}

// Get reads and enrolls into the existing participant in ctx. It never creates
// an implicit unit or exposes its owner. Failed enrollment returns no read/token.
func (r *DecisionReader[T]) Get(ctx context.Context, key Key) (read DecisionRead[T], err error) {
	defer func() { err = decisionReadFailure(err) }()
	unit, ok := transactions.FromContext(ctx)
	if !ok {
		return DecisionRead[T]{}, ErrDecisionRequiresUnitOfWork
	}
	read, err = r.GetDetached(ctx, key)
	if err != nil {
		return DecisionRead[T]{}, err
	}
	if err = unit.Enroll(read.Token); err != nil {
		return DecisionRead[T]{}, err
	}
	return read, nil
}

// GetDetached uses at most three fresh projection sessions, without enrolling or
// creating a unit. Classified models fail local admission before lease/RPC work.
// It checks server agreement before and after each fold, captures
// an unfiltered pre-fold log boundary, and probes the source's dependency types
// separately. LastHandled is progress, never a proof or replacement boundary.
// Cleanup is awaited, with a five-second cancellation-detached metadata-preserving
// budget; cleanup/cancellation/generation/epoch failure returns no token. Application
// codecs run only after cleanup, agreement checks and release of counted RPC work;
// cancellation, connection generation and catalog epoch are rechecked afterward. The
// pinned protocol cannot atomically bind definitions or in-place history changes.
// Codec panics return DecisionCodecPanicError without retaining the panic value.
// Errors have payload-free messages; underlying returned causes remain deliberately
// inspectable through errors.Is/As and may contain sensitive transport diagnostics.
func (r *DecisionReader[T]) GetDetached(ctx context.Context, key Key) (read DecisionRead[T], err error) {
	defer func() { err = decisionReadFailure(err) }()
	admitted, err := r.assess()
	if err != nil {
		return DecisionRead[T]{}, err
	}
	if !validDecisionKey(key, admitted.key) {
		return DecisionRead[T]{}, &DecisionReadRefused{Model: admitted.descriptor.Identifier(), Reason: DecisionInvalidKey}
	}
	if err = ctx.Err(); err != nil {
		return DecisionRead[T]{}, err
	}
	service := r.reader.service
	// One lease spans every RPC attempt, never application codecs. A reconnect
	// must fail this read, never splice fold or cleanup onto another generation.
	lease, err := service.decisions.AcquireDecision(ctx)
	if err != nil {
		return DecisionRead[T]{}, err
	}
	defer lease.Release()
	callerContext := ctx
	ctx = lease.Context
	pinned := &Service{store: service.store, namespace: service.namespace, catalog: service.catalog,
		client: contracts.NewReadModelsClient(lease.Conn)}
	check := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if lease.Check() != nil || admitted.epoch != admitted.catalog.Epoch.Load() {
			return decision.ErrStale
		}
		return nil
	}
	ctx = decision.WithDispatchValidation(ctx, check)
	for attempt := range 3 {
		if err = check(); err != nil {
			return DecisionRead[T]{}, err
		}
		if err = checkDecisionAgreement(ctx, lease.Conn, string(service.store), admitted); err != nil {
			return DecisionRead[T]{}, err
		}
		boundary, err := decisionTail(ctx, lease, service, "", nil)
		if err != nil {
			return DecisionRead[T]{}, err
		}
		probe, err := decisionTail(ctx, lease, service, key, admitted.types)
		if err != nil {
			return DecisionRead[T]{}, err
		}
		raw, err := foldDecision(ctx, pinned, admitted.descriptor, key)
		if err != nil {
			return DecisionRead[T]{}, err
		}
		if err = check(); err != nil {
			return DecisionRead[T]{}, err
		}
		if err = checkDecisionAgreement(ctx, lease.Conn, string(service.store), admitted); err != nil {
			return DecisionRead[T]{}, err
		}
		if err = check(); err != nil {
			return DecisionRead[T]{}, err
		}
		reason := DecisionReadRefusalReason("")
		last := raw.LastHandled
		if probe != events.Unavailable && (last == nil || *last < probe) {
			reason = DecisionFoldIncomplete
		}
		if last != nil && (boundary == events.Unavailable || *last > boundary) {
			reason = DecisionFoldAhead
		}
		if reason != "" {
			if attempt < 2 {
				continue
			}
			return DecisionRead[T]{}, &DecisionReadRefused{Model: admitted.descriptor.Identifier(), Reason: reason}
		}
		// All RPCs, protocol checks and awaited cleanup are complete. A
		// codec may synchronously close the client, which joins counted work.
		// Release cancels lease.Context, so the final check uses the caller's
		// context and the separately retained generation/epoch identities.
		lease.Release()
		instance, err := decodeDecision[T](raw, admitted.descriptor)
		if err != nil {
			return DecisionRead[T]{}, err
		}
		if err = callerContext.Err(); err != nil {
			return DecisionRead[T]{}, err
		}
		if lease.Check() != nil || admitted.epoch != admitted.catalog.Epoch.Load() {
			return DecisionRead[T]{}, decision.ErrStale
		}
		token := decision.Issue(decision.Evidence{Target: service.decisions.DecisionTarget(events.EventLog), Model: string(admitted.descriptor.Identifier()), Key: string(key), Types: admitted.types, Boundary: boundary,
			Catalog: admitted.catalog, Epoch: admitted.epoch, Generation: lease.Generation, Check: lease.Check})
		if token.IsZero() {
			return DecisionRead[T]{}, decision.ErrStale
		}
		return DecisionRead[T]{Instance: instance, Token: token}, nil
	}
	return DecisionRead[T]{}, faults.ErrProtocol
}

func decisionTail(ctx context.Context, lease *decision.Lease, service *Service, key Key, types []events.TypeRef) (events.SequenceNumber, error) {
	ids := make([]string, len(types))
	for i, event := range types {
		ids[i] = string(event.ID)
	}
	response, err := sequences.NewEventSequencesClient(lease.Conn).TailSequenceNumber(ctx, &sequences.TailSequenceNumberRequest{
		EventStore: string(service.store), Namespace: string(service.namespace), EventSequenceId: string(events.EventLog), EventSourceId: string(key), EventTypeIds: strings.Join(ids, ",")})
	if err != nil {
		return 0, wire.RPCError(err)
	}
	if err = wire.CheckEnvelope(response); err != nil {
		return 0, err
	}
	if err = wire.RequireMessage(response, "Data"); err != nil {
		return 0, err
	}
	position := events.SequenceNumber(response.Data.SequenceNumber)
	if position != events.Unavailable && position >= events.Unavailable-2 {
		return 0, faults.ErrProtocol
	}
	return position, nil
}

func foldDecision(ctx context.Context, service *Service, descriptor Descriptor, key Key) (instance Instance[json.RawMessage], err error) {
	session, err := uuid.NewRandom()
	if err != nil {
		return Instance[json.RawMessage]{}, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(decision.CleanupContext(ctx), 5*time.Second)
		defer cancel()
		response, cleanupErr := service.client.DehydrateSession(cleanup, &contracts.DehydrateSessionRequest{
			EventStore: string(service.store), Namespace: string(service.namespace), EventSequenceId: string(events.EventLog), ReadModelIdentifier: string(descriptor.Identifier()), ReadModelKey: string(key), SessionId: session.String()})
		if cleanupErr == nil && response == nil {
			cleanupErr = faults.ErrProtocol
		}
		if cleanupErr != nil {
			err = errors.Join(decisionReadFailure(err), &decisionReadError{cause: wire.RPCError(cleanupErr), cleanup: true})
			instance = Instance[json.RawMessage]{}
		}
	}()
	raw, err := service.getInstance(ctx, descriptor, key, session.String())
	if err != nil {
		return Instance[json.RawMessage]{}, err
	}
	// Admission excludes every classified model. The kernel session response
	// already owns release; do not decrypt its final representation again.
	// Protocol/duplicate checks ran in getInstance; codec validation follows
	// cleanup outside the counted lease, before token issuance.
	// An empty fold can contain projection initial state. It is still absent;
	// null after a removal retains its meaningful LastHandled position.
	if raw.LastHandled == nil {
		raw.Exists = false
	}
	return raw, nil
}
