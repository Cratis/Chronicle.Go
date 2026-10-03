// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observation

import (
	"context"
	"fmt"
	"strings"

	contracts "github.com/cratis/chronicle.go/contracts/observation"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/administration"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/jobs"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// OutcomeUnknownError means an administration mutation may have taken effect
// despite an RPC/protocol failure. Do not automatically retry it.
type OutcomeUnknownError = administration.OutcomeUnknownError

// Service administers observers in an immutable store/namespace. It borrows the
// transport and starts no background work. Calls are concurrent-safe, context-first
// and never retry mutations. Remove is deliberately store-wide; see its contract.
type Service struct {
	store     metadata.StoreName
	namespace metadata.Namespace
	client    contracts.ObserversClient
	failures  contracts.FailedPartitionsClient
	jobs      *jobs.Service
}

// New constructs a low-level service without I/O. Prefer EventStore.Observers for
// connection/registration barriers. The caller owns the connection.
func New(store metadata.StoreName, namespace metadata.Namespace, conn grpc.ClientConnInterface) (*Service, error) {
	if strings.TrimSpace(string(store)) == "" || strings.TrimSpace(string(namespace)) == "" || conn == nil {
		return nil, invalid("store, namespace and connection required")
	}
	jobService, err := jobs.New(store, namespace, conn)
	if err != nil {
		return nil, err
	}
	return &Service{store, namespace, contracts.NewObserversClient(conn), contracts.NewFailedPartitionsClient(conn), jobService}, nil
}

// List returns immutable observer snapshots in the current namespace.
func (s *Service) List(ctx context.Context) ([]Information, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	response, err := s.client.GetObservers(ctx, &contracts.AllObserversRequest{EventStore: string(s.store), Namespace: string(s.namespace)})
	if err != nil {
		return nil, wire.RPCError(err)
	}
	if response == nil {
		return nil, faults.ErrProtocol
	}
	result := make([]Information, 0, len(response.Items))
	for _, v := range response.Items {
		i, err := decodeInformation(v)
		if err != nil {
			return nil, err
		}
		result = append(result, i)
	}
	return result, nil
}

// Get returns one observer's state. gRPC NotFound is (nil,nil); a malformed or
// mismatched successful reply is an error, not fabricated absence.
func (s *Service) Get(ctx context.Context, id ID, sequence events.SequenceID) (*Information, error) {
	if err := validate(ctx, id, sequence); err != nil {
		return nil, err
	}
	response, err := s.client.GetObserverInformation(ctx, &contracts.GetObserverInformationRequest{EventStore: string(s.store), Namespace: string(s.namespace), ObserverId: string(id), EventSequenceId: string(sequence)})
	if status.Code(err) == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, wire.RPCError(err)
	}
	result, err := decodeInformation(response)
	if err != nil {
		return nil, err
	}
	if result.ID() != id || result.Sequence() != sequence {
		return nil, faults.ErrProtocol
	}
	return &result, nil
}

// RemovalOutcome preserves all server refusal distinctions.
type RemovalOutcome int32

const (
	// Removed means the server removed the persistent observer records.
	Removed RemovalOutcome = iota
	// ObserverNotFound means no observer was found to remove.
	ObserverNotFound
	// ObserverActive means execution in some namespace prevents removal.
	ObserverActive
	// ObserverSubscribed means a subscribed client prevents removal.
	ObserverSubscribed
)

// RemovalResult is an owned value describing persistent removal, not local unsubscribe.
type RemovalResult struct {
	// Outcome is the exact server result.
	Outcome RemovalOutcome
	// BlockingNamespace identifies the namespace preventing removal, when supplied.
	BlockingNamespace metadata.Namespace
}

// Remove deletes persistent observer records across ALL namespaces in this store.
// It refuses active/subscribed observers and leaves read-model data and sink
// containers untouched. Like C# IObservers.Remove it sends EventLog. It does not
// unregister local declarations: use EventStore.UnregisterReactor/Reducer first
// when appropriate; running another client may still block removal.
func (s *Service) Remove(ctx context.Context, id ID) (RemovalResult, error) {
	if err := validate(ctx, id, events.EventLog); err != nil {
		return RemovalResult{}, err
	}
	r, err := s.client.RemoveObserver(ctx, &contracts.RemoveObserver{EventStore: string(s.store), Namespace: string(s.namespace), ObserverId: string(id), EventSequenceId: string(events.EventLog)})
	if err == nil && (r == nil || r.Outcome < 0 || r.Outcome > 3) {
		err = faults.ErrProtocol
	}
	if err != nil {
		return RemovalResult{}, administration.MutationError("remove observer", err)
	}
	return RemovalResult{RemovalOutcome(r.Outcome), metadata.Namespace(r.BlockingNamespace)}, nil
}

// FailedPartitions returns all failure records when observer is empty, or those
// for that observer. Attempts, resolution and quarantine flags remain distinct.
func (s *Service) FailedPartitions(ctx context.Context, observer ID) ([]FailedPartition, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if observer != "" && strings.TrimSpace(string(observer)) == "" {
		return nil, invalid("nonblank observer required")
	}
	r, err := s.failures.GetFailedPartitions(ctx, &contracts.GetFailedPartitionsRequest{EventStore: string(s.store), Namespace: string(s.namespace), ObserverId: string(observer)})
	if err != nil {
		return nil, wire.RPCError(err)
	}
	if r == nil {
		return nil, faults.ErrProtocol
	}
	result, err := decodeFailures(r.Items)
	if err != nil {
		return nil, err
	}
	for _, p := range result {
		if observer != "" && p.Observer() != observer {
			return nil, faults.ErrProtocol
		}
	}
	return result, nil
}

// Replay explicitly requests replay and returns the actual server-issued job
// handle. Acceptance is neither job completion nor observer catch-up. Unlike C#,
// an invalid/empty JobId fails with OutcomeUnknownError rather than JobId.NotSet.
// Sequence must be explicit; use the observer's declared source sequence.
func (s *Service) Replay(ctx context.Context, id ID, sequence events.SequenceID) (jobs.Handle, error) {
	if err := validate(ctx, id, sequence); err != nil {
		return jobs.Handle{}, err
	}
	r, err := s.client.Replay(ctx, &contracts.Replay{EventStore: string(s.store), Namespace: string(s.namespace), ObserverId: string(id), EventSequenceId: string(sequence)})
	var jobID uuid.UUID
	if err == nil {
		if r == nil {
			err = faults.ErrProtocol
		} else {
			jobID, err = uuid.Parse(r.JobId)
			if err != nil || jobID == uuid.Nil {
				err = faults.ErrProtocol
			}
		}
	}
	if err != nil {
		return jobs.Handle{}, administration.MutationError("replay observer", err)
	}
	return s.jobs.Handle(jobID)
}

// RecoveryOutcome preserves retry acceptance versus recovery refusals.
type RecoveryOutcome int32

const (
	// RecoveryStarted means recovery was accepted, not completed.
	RecoveryStarted RecoveryOutcome = iota
	// PartitionNotFound means no failed partition was found.
	PartitionNotFound
	// ObserverQuarantined means the observer fence prevented recovery.
	ObserverQuarantined
	// PartitionQuarantined means the partition fence prevented recovery.
	PartitionQuarantined
)

// RetryPartition explicitly requests recovery of one failed partition without
// clearing either quarantine. No automatic retry follows refusal or lost replies.
func (s *Service) RetryPartition(ctx context.Context, id ID, sequence events.SequenceID, partition Partition) (RecoveryOutcome, error) {
	if err := validate(ctx, id, sequence); err != nil {
		return 0, err
	}
	r, err := s.client.RetryPartition(ctx, &contracts.RetryPartition{EventStore: string(s.store), Namespace: string(s.namespace), ObserverId: string(id), EventSequenceId: string(sequence), Partition: string(partition)})
	if err == nil && (r == nil || r.Outcome < 0 || r.Outcome > 3) {
		err = faults.ErrProtocol
	}
	if err != nil {
		return 0, administration.MutationError("retry partition", err)
	}
	return RecoveryOutcome(r.Outcome), nil
}

// QuarantineOutcome describes whether a partition fence was cleared.
type QuarantineOutcome int32

const (
	// QuarantineCleared means the partition quarantine was cleared.
	QuarantineCleared QuarantineOutcome = iota
	// QuarantineNotFound means the partition record was not found.
	QuarantineNotFound
	// NotQuarantined means the partition was not quarantined.
	NotQuarantined
)

// QuarantineResult is an owned value. RetryRequested qualifies RetryOutcome;
// when false, the proto3 default Started value is NOT recovery evidence.
type QuarantineResult struct {
	// Outcome is the quarantine-clearing result.
	Outcome QuarantineOutcome
	// RetryRequested records the caller's explicit retry instruction.
	RetryRequested bool
	// RetryOutcome is meaningful only when RetryRequested and Outcome is QuarantineCleared.
	RetryOutcome RecoveryOutcome
}

// ClearPartitionQuarantine explicitly clears a fence, optionally requesting
// immediate recovery in the same call. Clearing does not prove recovery completed.
func (s *Service) ClearPartitionQuarantine(ctx context.Context, id ID, sequence events.SequenceID, partition Partition, retryImmediately bool) (QuarantineResult, error) {
	if err := validate(ctx, id, sequence); err != nil {
		return QuarantineResult{}, err
	}
	r, err := s.client.ClearPartitionQuarantine(ctx, &contracts.ClearPartitionQuarantine{EventStore: string(s.store), Namespace: string(s.namespace), ObserverId: string(id), EventSequenceId: string(sequence), Partition: string(partition), RetryImmediately: retryImmediately})
	if err == nil && (r == nil || r.Outcome < 0 || r.Outcome > 2 || r.RetryOutcome < 0 || r.RetryOutcome > 3) {
		err = faults.ErrProtocol
	}
	if err != nil {
		return QuarantineResult{}, administration.MutationError("clear partition quarantine", err)
	}
	return QuarantineResult{QuarantineOutcome(r.Outcome), retryImmediately, RecoveryOutcome(r.RetryOutcome)}, nil
}

// ReplayPartition explicitly requests one partition replay. The wire returns no
// job identity; nil means acknowledgement only, not replay completion.
func (s *Service) ReplayPartition(ctx context.Context, id ID, sequence events.SequenceID, partition Partition) error {
	if err := validate(ctx, id, sequence); err != nil {
		return err
	}
	r, err := s.client.ReplayPartition(ctx, &contracts.ReplayPartition{EventStore: string(s.store), Namespace: string(s.namespace), ObserverId: string(id), EventSequenceId: string(sequence), Partition: string(partition)})
	return emptyMutation("replay partition", r, err)
}

// ClearFailedPartitions explicitly discards failure records, not the underlying
// cause. It does not claim recovery or successful observer processing.
func (s *Service) ClearFailedPartitions(ctx context.Context, id ID, sequence events.SequenceID) error {
	if err := validate(ctx, id, sequence); err != nil {
		return err
	}
	r, err := s.client.ClearFailedPartitions(ctx, &contracts.ClearFailedPartitions{EventStore: string(s.store), Namespace: string(s.namespace), ObserverId: string(id), EventSequenceId: string(sequence)})
	return emptyMutation("clear failed partitions", r, err)
}

// ClearObserverQuarantine explicitly clears the observer-level fence. The empty
// response acknowledges the command only; it cannot prove processing completion.
func (s *Service) ClearObserverQuarantine(ctx context.Context, id ID, sequence events.SequenceID) error {
	if err := validate(ctx, id, sequence); err != nil {
		return err
	}
	r, err := s.client.ClearObserverQuarantine(ctx, &contracts.ClearObserverQuarantine{EventStore: string(s.store), Namespace: string(s.namespace), ObserverId: string(id), EventSequenceId: string(sequence)})
	return emptyMutation("clear observer quarantine", r, err)
}
func emptyMutation(operation string, r *emptypb.Empty, err error) error {
	if err == nil && r == nil {
		err = faults.ErrProtocol
	}
	return administration.MutationError(operation, err)
}
func validate(ctx context.Context, id ID, sequence events.SequenceID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(string(id)) == "" || strings.TrimSpace(string(sequence)) == "" {
		return invalid("observer and sequence required")
	}
	return nil
}
func invalid(message string) error {
	return fmt.Errorf("%w: %s", faults.ErrInvalidConfiguration, message)
}
