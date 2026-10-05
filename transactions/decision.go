// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package transactions

import (
	"slices"

	"github.com/cratis/chronicle.go/internal/decision"
)

// DecisionToken is opaque SDK-issued optimistic read evidence, not a signed or
// server-authenticated proof. The zero value is invalid. Copies share permanent
// first-enrollment ownership; rollback does not permit reuse by another owner.
// Obtain tokens only from readmodels.DecisionReader. Tokens are not serializable.
type DecisionToken = decision.Token

var (
	// ErrInvalidDecision rejects zero or otherwise unissued tokens.
	ErrInvalidDecision = decision.ErrInvalid
	// ErrDecisionTarget rejects another client, store, namespace or sequence.
	ErrDecisionTarget = decision.ErrTarget
	// ErrDecisionOwner rejects redemption by a different unit, even after rollback.
	ErrDecisionOwner = decision.ErrOwner
	// ErrStaleDecision rejects changed catalog epochs and connection generations.
	ErrStaleDecision = decision.ErrStale
	// ErrDecisionScope rejects explicit colliding or unchecked scopes, in either enrollment order.
	ErrDecisionScope = decision.ErrScope
)

// DecisionConflict identifies a decision affected by a reported concurrency
// violation. Key may be sensitive; applications decide whether to display it.
type DecisionConflict struct {
	Model string // Model is the registered model identifier.
	Key   string // Key is the source key read by the decision.
}

// GetDecisionConflicts maps the retained result's violation labels to enrolled
// model/key pairs. It returns an owned snapshot, empty before a rejected commit.
func (u *UnitOfWork) GetDecisionConflicts() []DecisionConflict {
	if u == nil {
		return nil
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	var result []DecisionConflict
	for _, violation := range u.result.ConcurrencyViolations {
		for _, conflict := range u.decisionConflicts[string(violation.SourceID)] {
			if !slices.Contains(result, conflict) {
				result = append(result, conflict)
			}
		}
	}
	return result
}

// Enroll atomically validates and enrolls a token into this open participant.
// The first successful enrollment permanently binds all token copies to this
// unit's existing Owner. Failed enrollment changes neither token nor unit. Reads
// of one source merge at the earliest boundary and union their event-type IDs.
// Only the separately retained Owner can complete; no owner is put in context.
func (u *UnitOfWork) Enroll(token DecisionToken) error {
	if u == nil {
		return ErrInvalidDecision
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.openError(); err != nil {
		return err
	}
	return decision.Enroll(token, u.sequence.DecisionTarget(), u, func(guard *decision.Guard) error {
		pending, err := u.pending.WithDecisionGuard(guard)
		if err != nil {
			return err
		}
		evidence := guard.Evidence()
		conflict := DecisionConflict{Model: evidence.Model, Key: evidence.Key}
		if u.decisionConflicts == nil {
			u.decisionConflicts = make(map[string][]DecisionConflict)
		}
		if !slices.Contains(u.decisionConflicts[evidence.Key], conflict) {
			u.decisionConflicts[evidence.Key] = append(u.decisionConflicts[evidence.Key], conflict)
		}
		u.pending, u.hasWork = pending, true
		return nil
	})
}
