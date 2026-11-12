// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observation

import (
	"errors"
	"fmt"
	"time"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
)

// Outcome errors reported through an EventBatch.
var (
	// ErrPartitionNotInBatch is returned by Fail and Defer for a partition or
	// event the batch does not hold.
	ErrPartitionNotInBatch = errors.New("chronicle: the batch holds no events for the partition")
	// ErrPartitionFailedInBatch explains a partition failed through Fail with a nil error.
	ErrPartitionFailedInBatch = errors.New("chronicle: partition was failed by a batch method")
	// ErrDeferralNotSupportedByKernel explains a deferral the connected kernel cannot
	// honor because it does not deliver pages. The partition is failed instead.
	ErrDeferralNotSupportedByKernel = errors.New("chronicle: the kernel does not accept deferred partitions")
	// ErrPartitionDeferred identifies a DeferredError.
	ErrPartitionDeferred = errors.New("chronicle: partition deferred")
)

// Registration errors. Each also matches chronicle.ErrInvalidConfiguration, and
// arrives wrapped in the reactor or reducer DeclarationError naming the method.
var (
	// ErrBatchMethodObservesNothing: a method takes an *EventBatch but no
	// registration declares which event types it observes.
	ErrBatchMethodObservesNothing = configuration("a batch method must declare the event types it observes with observation.Observes")
	// ErrObservesOnlyOnBatchMethods: a batch registration names a method that does
	// not take an *EventBatch, or a batch callback has no observed event types.
	ErrObservesOnlyOnBatchMethods = configuration("observed event types apply only to a method taking *observation.EventBatch")
	// ErrBatchMethodObservesUnknownEventType: an observed type matches no registered event type.
	ErrBatchMethodObservesUnknownEventType = configuration("a batch method observes a type that is not a registered event type")
	// ErrEventTypeObservedByMoreThanOneMethod: an event type reaches a batch method
	// and another method. Each event type must reach exactly one method.
	ErrEventTypeObservedByMoreThanOneMethod = configuration("an event type reaches more than one method; each event type must reach exactly one method, a batch method or a per-event method, never both")
	// ErrBatchContextOnlyForBatchMethods: a per-event method takes a BatchContext.
	ErrBatchContextOnlyForBatchMethods = configuration("observation.BatchContext is only available to a method taking *observation.EventBatch")
	// ErrInvalidBatchMethodSignature: a batch method's parameters or results are
	// not supported. A reactor batch method returns nothing or error; returning
	// events from a batch method is not supported.
	ErrInvalidBatchMethodSignature = configuration("unsupported batch method signature")
	// ErrReducerHasMoreThanOneBatchMethod: a reducer is a single fold, so it has one batch method at most.
	ErrReducerHasMoreThanOneBatchMethod = configuration("a reducer is a single fold, so it has one batch method at most")
	// ErrReducerMixesBatchAndPerEventMethods: a reducer folds a partition's events
	// in order, so it uses a batch method or per-event methods, never both.
	ErrReducerMixesBatchAndPerEventMethods = configuration("a reducer uses one batch method or per-event methods, never both")
)

func configuration(message string) error {
	return fmt.Errorf("%w: %s", faults.ErrInvalidConfiguration, message)
}

// DeferredError reports that a reducer batch method deferred its partition.
// It matches ErrPartitionDeferred; the observer runtime turns it into a deferred
// partition result. The read model is not written.
type DeferredError struct {
	Partition  events.SourceID
	RetryAfter time.Duration
}

func (e *DeferredError) Error() string {
	return fmt.Sprintf("chronicle: partition %q deferred for %s", e.Partition, e.RetryAfter)
}

// Is matches ErrPartitionDeferred.
func (e *DeferredError) Is(target error) bool { return target == ErrPartitionDeferred }
