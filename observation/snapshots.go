// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observation

import (
	"context"
	"slices"
	"strings"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/observation"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

// ID is a stable observer identifier shared by projections, reactors and reducers.
type ID string

// Partition is the opaque observer partition key, not necessarily an event source.
type Partition string

// Type identifies the observer's kind, preserving unknown numeric values.
type Type int32

const (
	// UnknownType means no kind is reported.
	UnknownType Type = iota
	// Reactor is a client-side event handler.
	Reactor
	// Projection is a kernel projection.
	Projection
	// Reducer is a client-side fold.
	Reducer
	// External is an external observer.
	External
)

// Owner identifies where an observer runs, preserving unknown numeric values.
type Owner int32

const (
	// NoOwner means no owner is reported.
	NoOwner Owner = iota
	// ClientOwner means a connected client owns execution.
	ClientOwner
	// KernelOwner means the kernel owns execution.
	KernelOwner
)

// RunningState describes runtime state, preserving unknown numeric values.
type RunningState int32

const (
	// UnknownState means no runtime state is reported.
	UnknownState RunningState = iota
	// Active means the observer is active, not necessarily caught up.
	Active
	// Suspended means observation is suspended.
	Suspended
	// Replaying means replay is underway.
	Replaying
	// Disconnected means no active connection.
	Disconnected
	// Quarantined means automatic execution is fenced.
	Quarantined
)

// FailureKind distinguishes failures, preserving unknown numeric values.
type FailureKind int32

const (
	// UnknownFailure means the kernel supplied no classification.
	UnknownFailure FailureKind = iota
	// HandlingFailure means the event handler failed.
	HandlingFailure
	// TimeoutFailure means handling timed out.
	TimeoutFailure
	// DisconnectedFailure means the client disconnected.
	DisconnectedFailure
)

// Information is an immutable server snapshot. Zero is invalid. A running
// state or a tail is not a guarantee of durable checkpoint persistence.
type Information struct {
	value             *contracts.ObserverInformation
	service           *Service
	subscriptionKnown bool
}

// ID returns the observer identity.
func (i Information) ID() ID { return ID(i.value.GetId()) }

// Sequence returns the source event sequence.
func (i Information) Sequence() events.SequenceID {
	return events.SequenceID(i.value.GetEventSequenceId())
}

// Type returns the observer kind.
func (i Information) Type() Type { return Type(i.value.GetType()) }

// Owner returns the execution owner.
func (i Information) Owner() Owner { return Owner(i.value.GetOwner()) }

// EventTypes returns owned exact subscribed event references in server order.
func (i Information) EventTypes() []events.TypeRef {
	result := make([]events.TypeRef, 0, len(i.value.GetEventTypes()))
	for _, v := range i.value.GetEventTypes() {
		result = append(result, events.TypeRef{ID: events.TypeID(v.Id), Generation: events.Generation(v.Generation)})
	}
	return result
}

// Next returns the reported next position, preserving reserved sentinels.
func (i Information) Next() events.SequenceNumber {
	return events.SequenceNumber(i.value.GetNextEventSequenceNumber())
}

// LastHandled returns the last handled position, preserving Unavailable.
func (i Information) LastHandled() events.SequenceNumber {
	return events.SequenceNumber(i.value.GetLastHandledEventSequenceNumber())
}

// Tail returns the observer's reported source tail, preserving Unavailable.
func (i Information) Tail() events.SequenceNumber {
	return events.SequenceNumber(i.value.GetTailEventSequenceNumber())
}

// RunningState returns runtime state independently of progress.
func (i Information) RunningState() RunningState { return RunningState(i.value.GetRunningState()) }

// SubscriptionKnown reports whether subscription state was queried. Only Get
// supplies it; List does not ask the kernel's live observer activation.
func (i Information) SubscriptionKnown() bool { return i.subscriptionKnown }

// IsSubscribed reports a server-side subscription, not local goroutine lifetime.
// False when SubscriptionKnown is false means unavailable, NOT disconnected.
func (i Information) IsSubscribed() bool { return i.subscriptionKnown && i.value.GetIsSubscribed() }

// IsReplayable reports the server's replay policy.
func (i Information) IsReplayable() bool { return i.value.GetIsReplayable() }

// HandledEventCount returns the reported count.
func (i Information) HandledEventCount() uint64 { return i.value.GetHandledEventCount() }

// Remove requests store-wide removal using this snapshot's identity and sequence.
// The current definition is checked again; a stale sequence fails before mutation.
func (i Information) Remove(ctx context.Context) (RemovalResult, error) {
	if i.service == nil {
		return RemovalResult{}, invalid("observer snapshot required")
	}
	return i.service.RemoveFrom(ctx, i.ID(), i.Sequence())
}

// FailedPartition is an immutable diagnostic snapshot. Resolved and quarantined
// remain independent; a cleared quarantine does not establish successful recovery.
type FailedPartition struct {
	id                    uuid.UUID
	observer              ID
	partition             Partition
	attempts              []FailedAttempt
	resolved, quarantined bool
}

// ID returns the failure record's UUID.
func (p FailedPartition) ID() uuid.UUID { return p.id }

// Observer returns the failed observer identity.
func (p FailedPartition) Observer() ID { return p.observer }

// Partition returns the failed partition key.
func (p FailedPartition) Partition() Partition { return p.partition }

// Attempts returns an owned slice of immutable attempts in server order.
func (p FailedPartition) Attempts() []FailedAttempt { return slices.Clone(p.attempts) }

// IsResolved reports the kernel resolution flag.
func (p FailedPartition) IsResolved() bool { return p.resolved }

// IsQuarantined reports the kernel quarantine flag.
func (p FailedPartition) IsQuarantined() bool { return p.quarantined }

// FailedAttempt is an immutable failure attempt diagnostic.
type FailedAttempt struct {
	occurred time.Time
	position events.SequenceNumber
	messages []string
	stack    string
	kind     FailureKind
}

// Occurred returns the failure instant with its original offset.
func (a FailedAttempt) Occurred() time.Time { return a.occurred }

// Position returns the failed event's sequence position.
func (a FailedAttempt) Position() events.SequenceNumber { return a.position }

// Messages returns owned diagnostic messages; they may contain sensitive data.
func (a FailedAttempt) Messages() []string { return slices.Clone(a.messages) }

// StackTrace returns the recorded stack, which may contain sensitive data.
func (a FailedAttempt) StackTrace() string { return a.stack }

// Kind returns the failure classification.
func (a FailedAttempt) Kind() FailureKind { return a.kind }

func decodeInformation(v *contracts.ObserverInformation) (Information, error) {
	if v == nil || strings.TrimSpace(v.Id) == "" || strings.TrimSpace(v.EventSequenceId) == "" {
		return Information{}, faults.ErrProtocol
	}
	for _, ref := range v.EventTypes {
		if ref == nil || strings.TrimSpace(ref.Id) == "" || ref.Generation == 0 {
			return Information{}, faults.ErrProtocol
		}
	}
	return Information{value: proto.Clone(v).(*contracts.ObserverInformation)}, nil
}
func decodeFailures(values []*contracts.FailedPartition) ([]FailedPartition, error) {
	result := make([]FailedPartition, 0, len(values))
	for _, v := range values {
		if v == nil || v.Id == nil || uuid.UUID(wire.Correlation(v.Id)) == uuid.Nil || strings.TrimSpace(v.ObserverId) == "" {
			return nil, faults.ErrProtocol
		}
		p := FailedPartition{id: uuid.UUID(wire.Correlation(v.Id)), observer: ID(v.ObserverId), partition: Partition(v.Partition), resolved: v.IsResolved, quarantined: v.IsQuarantined}
		for _, a := range v.Attempts {
			if a == nil || a.Occurred == nil {
				return nil, faults.ErrProtocol
			}
			occurred, err := time.Parse(time.RFC3339Nano, a.Occurred.Value)
			if err != nil {
				return nil, faults.ErrProtocol
			}
			p.attempts = append(p.attempts, FailedAttempt{occurred, events.SequenceNumber(a.SequenceNumber), slices.Clone(a.Messages), a.StackTrace, FailureKind(a.Kind)})
		}
		result = append(result, p)
	}
	return result, nil
}
