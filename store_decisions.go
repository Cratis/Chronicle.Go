// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/decision"
)

func (s *EventStore) initializeDecisions(snapshot registrySnapshot) {
	if s.definitions != nil {
		s.decisionCatalog = s.definitions.root.decisions
		return
	}
	var definitions []*contracts.ProjectionDefinition
	for _, projection := range snapshot.projections {
		definitions = append(definitions, projection.KernelDefinition())
	}
	if s.decisionCatalog != nil {
		s.decisionCatalog.Epoch.Add(1)
	}
	s.decisionCatalog = decision.NewCatalog(definitions, snapshot.events)
}

func (t *clientTransport) DecisionCatalog() *decision.Catalog {
	if t.decisionSnapshot != nil {
		return t.decisionSnapshot
	}
	if t.store == nil {
		return nil
	}
	return t.store.decisionCatalog
}

func (t *clientTransport) DecisionTarget(sequence events.SequenceID) decision.Target {
	if t.store == nil {
		return decision.Target{}
	}
	return decision.Target{Client: t.client, Store: string(t.store.name), Namespace: string(t.store.namespace), Sequence: string(sequence)}
}

func (t *clientTransport) AcquireDecision(ctx context.Context) (*decision.Lease, error) {
	g, ctx, done, err := t.acquire(ctx)
	if err != nil {
		return nil, err
	}
	if !g.decisions || t.store == nil {
		done()
		return nil, decision.Unsupported()
	}
	return &decision.Lease{Context: ctx, Conn: g.transport, Generation: g.number, Check: g.ctx.Err,
		Release: done, Resolve: t.store.resolveConstraintMessages}, nil
}
