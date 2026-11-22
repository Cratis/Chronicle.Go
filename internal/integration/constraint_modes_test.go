//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"testing"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
)

func TestKernelUniqueModesReplacingValues(t *testing.T) {
	for _, tc := range []struct {
		name     string
		modes    []constraints.UniqueMode
		retained bool
	}{
		{name: "default"},
		{name: "per source", modes: []constraints.UniqueMode{constraints.PerEventSource}},
		{name: "per value", modes: []constraints.UniqueMode{constraints.PerValue}, retained: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, _, store := constraintsFixture(t, func(e constraintEvents) *constraints.Builder {
				builder := constraints.UniqueValues("ModeEmail").On(e.claimed, "email")
				for _, mode := range tc.modes {
					builder.WithMode(mode)
				}
				return builder
			})
			sequence := store.EventLog()
			for _, email := range []string{"a@example.test", "a@example.test", "b@example.test"} {
				requireConstraintCommit(t, appendConstraintEvent(t, ctx, sequence, "owner", AddressClaimed{Email: email}))
			}
			result := appendConstraintEvent(t, ctx, sequence, "claim-a", AddressClaimed{Email: "a@example.test"})
			if tc.retained {
				requireConstraintRejection(t, result, "ModeEmail")
				requireSourceCount(t, ctx, sequence, "claim-a", 0)
			} else {
				requireConstraintCommit(t, result)
				requireSourceCount(t, ctx, sequence, "claim-a", 1)
			}
			requireModeValueConflict(t, ctx, sequence, "claim-b", "b@example.test")
			requireSourceCount(t, ctx, sequence, "owner", 3)
		})
	}
}

func TestKernelPerValueRemovalReleasesAllOwnerValues(t *testing.T) {
	ctx, _, store := constraintsFixture(t, func(e constraintEvents) *constraints.Builder {
		return constraints.UniqueValues("ModeEmail").On(e.claimed, "email").
			WithMode(constraints.PerValue).RemovedWith(e.released)
	})
	sequence := store.EventLog()
	for _, email := range []string{"a@example.test", "b@example.test", "a@example.test"} {
		requireConstraintCommit(t, appendConstraintEvent(t, ctx, sequence, "owner", AddressClaimed{Email: email}))
	}
	// The remover carries no unique properties. A different source cannot release
	// either retained value, while the owner's remover must release both.
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, sequence, "stranger", AddressReleased{}))
	requireModeValueConflict(t, ctx, sequence, "claim-a", "a@example.test")
	requireModeValueConflict(t, ctx, sequence, "claim-b", "b@example.test")
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, sequence, "owner", AddressReleased{}))
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, sequence, "claim-a", AddressClaimed{Email: "a@example.test"}))
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, sequence, "claim-b", AddressClaimed{Email: "b@example.test"}))
	requireSourceCount(t, ctx, sequence, "claim-a", 1)
	requireSourceCount(t, ctx, sequence, "claim-b", 1)
	requireSourceCount(t, ctx, sequence, "owner", 4)
}

func requireModeValueConflict(t *testing.T, ctx context.Context, sequence *eventsequences.Sequence, source events.SourceID, email string) {
	t.Helper()
	requireConstraintRejection(t, appendConstraintEvent(t, ctx, sequence, source, AddressClaimed{Email: email}), "ModeEmail")
	requireSourceCount(t, ctx, sequence, source, 0)
}
