// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observation

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/observation"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
)

// InfiniteTimeout disables server and helper deadlines, but not caller cancellation.
const InfiniteTimeout time.Duration = -1

// DefaultTimeout is the five-second server budget used by the C# client.
const DefaultTimeout = 5 * time.Second

// ClientGrace gives the server 200ms to report outstanding observers after its budget.
const ClientGrace = 200 * time.Millisecond

// CannotWaitError reports committed work without an observer administration surface.
// An unavailable append tail is different and completes trivially.
type CannotWaitError struct {
	// Store is the original append store.
	Store metadata.StoreName
	// Sequence is the original append sequence.
	Sequence events.SequenceID
}

func (e *CannotWaitError) Error() string {
	return "chronicle: committed append has no observer completion surface"
}

// Completion owns immutable original append coordinates and per-event-type tails.
// Zero means no committed tail and completes trivially, not proof of a commit.
// Construct from an append operation's Completion method or NewCompletion.
type Completion struct {
	store       metadata.StoreName
	namespace   metadata.Namespace
	sequence    events.SequenceID
	first, tail events.SequenceNumber
	tails       []typeTail
	available   bool
}
type typeTail struct {
	ref      events.TypeRef
	position events.SequenceNumber
}

// NewCompletion snapshots exact input types zipped with confirmed positions.
// Repeated type IDs use their greatest actual position and that entry's generation,
// never the whole batch tail. Position zero is valid. Empty arrays or one Unavailable
// position mean no committed tail. Unknown append outcomes must not use this constructor.
func NewCompletion(store metadata.StoreName, namespace metadata.Namespace, sequence events.SequenceID, types []events.TypeRef, positions []events.SequenceNumber) (Completion, error) {
	if len(types) != len(positions) {
		return Completion{}, invalid("completion type and position counts differ")
	}
	if len(positions) == 0 || (len(positions) == 1 && positions[0] == events.Unavailable) {
		return Completion{}, nil
	}
	if strings.TrimSpace(string(store)) == "" || strings.TrimSpace(string(namespace)) == "" || strings.TrimSpace(string(sequence)) == "" {
		return Completion{}, invalid("original completion coordinates required")
	}
	c := Completion{store: store, namespace: namespace, sequence: sequence, first: positions[0], tail: positions[0], available: true}
	indexes := map[events.TypeID]int{}
	for i, ref := range types {
		p := positions[i]
		if strings.TrimSpace(string(ref.ID)) == "" || ref.Generation == 0 || p >= events.Unavailable-2 {
			return Completion{}, invalid("invalid completion type or position")
		}
		if p < c.first {
			c.first = p
		}
		if p > c.tail {
			c.tail = p
		}
		if index, ok := indexes[ref.ID]; ok {
			if p > c.tails[index].position {
				c.tails[index] = typeTail{ref, p}
			}
		} else {
			indexes[ref.ID] = len(c.tails)
			c.tails = append(c.tails, typeTail{ref, p})
		}
	}
	return c, nil
}

// Target returns a detached legacy coordinate snapshot. Exact generations remain
// private in Completion; use this Completion, not a reconstructed catalog guess, to wait.
func (c Completion) Target() CompletionTarget {
	if !c.available {
		return CompletionTarget{}
	}
	first := c.first
	tails := make(map[events.TypeID]events.SequenceNumber, len(c.tails))
	for _, t := range c.tails {
		tails[t.ref.ID] = t.position
	}
	return CompletionTarget{Store: c.store, Namespace: c.namespace, Sequence: c.sequence, First: &first, EventTypeTails: tails}
}

// CompletionResult is immutable server processing evidence, not a durable sink
// checkpoint or transaction guarantee. Zero is not success.
type CompletionResult struct {
	success, timedOut, trivial bool
	failures                   []FailedPartition
	outstanding                []ID
}

// IsSuccess reports the server's processing success (or a trivial unavailable tail).
func (r CompletionResult) IsSuccess() bool { return r.success }

// TimedOut reports the server budget or client grace deadline expiring.
func (r CompletionResult) TimedOut() bool { return r.timedOut }

// Trivial distinguishes no committed tail from actual observer-processing evidence.
func (r CompletionResult) Trivial() bool { return r.trivial }

// FailedPartitions returns owned immutable failure snapshots.
func (r CompletionResult) FailedPartitions() []FailedPartition { return slices.Clone(r.failures) }

// OutstandingObservers returns an owned list of observers outstanding at timeout.
func (r CompletionResult) OutstandingObservers() []ID { return slices.Clone(r.outstanding) }

// WaitForCompletion waits for observers of the original appended event types.
// The kernel also reports success when there are no matching observers. This is
// not producer readiness or durable sink persistence evidence.
// The service's store/namespace must match; sequence comes ONLY from completion.
// Nil service with a committed tail returns CannotWaitError. Zero timeout selects
// 5s server + 200ms client grace. InfiniteTimeout sends 0 and adds no deadline.
// Caller cancellation/deadline remains an error; server/client-budget expiration
// is a TimedOut result. No RPC/mutation is retried and no goroutine is started.
func (s *Service) WaitForCompletion(ctx context.Context, c Completion, timeout time.Duration) (CompletionResult, error) {
	if err := ctx.Err(); err != nil {
		return CompletionResult{}, err
	}
	if !c.available {
		return CompletionResult{success: true, trivial: true}, nil
	}
	if s == nil {
		return CompletionResult{}, &CannotWaitError{c.store, c.sequence}
	}
	if c.store != s.store || c.namespace != s.namespace {
		return CompletionResult{}, invalid("completion belongs to another store or namespace")
	}
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	if timeout < 0 && timeout != InfiniteTimeout {
		return CompletionResult{}, invalid("invalid completion timeout")
	}
	request := &contracts.WaitForObserverCompletionRequest{EventStore: string(c.store), Namespace: string(c.namespace), EventSequenceId: string(c.sequence), TailEventSequenceNumber: uint64(c.tail), FirstEventSequenceNumber: uint64(c.first), HasFirstEventSequenceNumber: true}
	for _, tail := range c.tails {
		request.EventTypeTails = append(request.EventTypeTails, &contracts.AppendedEventTypeTail{EventType: &contracts.EventType{Id: string(tail.ref.ID), Generation: uint32(tail.ref.Generation)}, SequenceNumber: uint64(tail.position)})
	}
	callCtx := ctx
	var cancel context.CancelFunc
	if timeout != InfiniteTimeout {
		if timeout > time.Duration(1<<63-1)-ClientGrace {
			return CompletionResult{}, invalid("completion timeout overflows client grace")
		}
		request.TimeoutMilliseconds = max(1, timeout.Milliseconds())
		callCtx, cancel = context.WithTimeout(ctx, timeout+ClientGrace)
		defer cancel()
	}
	response, err := s.client.WaitForCompletion(callCtx, request)
	if err != nil {
		err = wire.RPCError(err)
		if ctx.Err() != nil {
			return CompletionResult{}, errors.Join(ctx.Err(), err)
		}
		if callCtx.Err() == context.DeadlineExceeded && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) {
			return CompletionResult{timedOut: true}, nil
		}
		return CompletionResult{}, err
	}
	if response == nil {
		return CompletionResult{}, faults.ErrProtocol
	}
	if response.IsSuccess && (response.TimedOut || len(response.FailedPartitions) > 0 || len(response.OutstandingObservers) > 0) {
		return CompletionResult{}, fmt.Errorf("%w: contradictory completion response", faults.ErrProtocol)
	}
	if !response.IsSuccess && !response.TimedOut && len(response.FailedPartitions) == 0 && len(response.OutstandingObservers) == 0 {
		return CompletionResult{}, faults.ErrProtocol
	}
	failed, err := decodeFailures(response.FailedPartitions)
	if err != nil {
		return CompletionResult{}, err
	}
	result := CompletionResult{success: response.IsSuccess, timedOut: response.TimedOut, failures: failed}
	for _, id := range response.OutstandingObservers {
		if strings.TrimSpace(id) == "" {
			return CompletionResult{}, faults.ErrProtocol
		}
		result.outstanding = append(result.outstanding, ID(id))
	}
	return result, nil
}
