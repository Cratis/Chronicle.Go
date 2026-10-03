// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package jobs administers namespace-scoped kernel jobs. Acceptance, progress,
// terminal status and disappearance are different evidence, never interchangeable.
package jobs

import (
	"context"
	"fmt"
	"strings"

	contracts "github.com/cratis/chronicle.go/contracts/jobs"
	"github.com/cratis/chronicle.go/internal/administration"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

// ID identifies a kernel job using an RFC UUID with .NET Guid wire conversion.
type ID = uuid.UUID

// StepID identifies a kernel job step.
type StepID = uuid.UUID

// OutcomeUnknownError reports ambiguous dispatched administration mutations.
type OutcomeUnknownError = administration.OutcomeUnknownError

// Service is an immutable, concurrent-safe store/namespace handle. It borrows
// its connection, starts no goroutines and never retries mutations.
type Service struct {
	store     metadata.StoreName
	namespace metadata.Namespace
	client    contracts.JobsClient
}

// New constructs a low-level service without I/O. Prefer EventStore.Jobs for
// registration/lifecycle barriers. The caller owns the connection.
func New(store metadata.StoreName, namespace metadata.Namespace, conn grpc.ClientConnInterface) (*Service, error) {
	if strings.TrimSpace(string(store)) == "" || strings.TrimSpace(string(namespace)) == "" || conn == nil {
		return nil, invalid("store, namespace and connection required")
	}
	return &Service{store, namespace, contracts.NewJobsClient(conn)}, nil
}

// Store returns this service's immutable store coordinate.
func (s *Service) Store() metadata.StoreName { return s.store }

// Namespace returns this service's immutable namespace coordinate.
func (s *Service) Namespace() metadata.Namespace { return s.namespace }

// Handle is an immutable job identity bound to its originating service. It does
// not assert that the job still exists, or that replay has completed.
type Handle struct {
	service *Service
	id      ID
}

// Handle returns a job reference without I/O. A zero UUID is invalid.
func (s *Service) Handle(id ID) (Handle, error) {
	if id == uuid.Nil {
		return Handle{}, invalid("job ID required")
	}
	return Handle{s, id}, nil
}

// ID returns the kernel job ID; zero identifies an invalid handle.
func (h Handle) ID() ID { return h.id }

// Get reads this job; absence returns (nil, nil), never a fabricated completed job.
func (h Handle) Get(ctx context.Context) (*Job, error) {
	if h.service == nil {
		return nil, invalid("job handle required")
	}
	return h.service.Get(ctx, h.id)
}

// Steps returns snapshots of this job's steps. An empty list does not prove completion.
func (h Handle) Steps(ctx context.Context) ([]Step, error) {
	if h.service == nil {
		return nil, invalid("job handle required")
	}
	return h.service.Steps(ctx, h.id)
}

// List returns immutable snapshots in server order. Malformed replies fail atomically.
func (s *Service) List(ctx context.Context) ([]Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	response, err := s.client.AllJobs(ctx, &contracts.AllJobsRequest{EventStore: string(s.store), Namespace: string(s.namespace)})
	if err != nil {
		return nil, wire.RPCError(err)
	}
	if err = wire.CheckEnvelope(response); err != nil {
		return nil, err
	}
	result := make([]Job, 0, len(response.Data))
	seen := map[ID]bool{}
	for _, value := range response.Data {
		job, err := decodeJob(value, s)
		if err != nil {
			return nil, err
		}
		if seen[job.ID()] {
			return nil, faults.ErrProtocol
		}
		seen[job.ID()] = true
		result = append(result, job)
	}
	return result, nil
}

// Get follows C# GetJob by searching List. Nil means absent at this observation,
// including a job removed after completion or deletion; the reason is unknowable.
func (s *Service) Get(ctx context.Context, id ID) (*Job, error) {
	if id == uuid.Nil {
		return nil, invalid("job ID required")
	}
	all, err := s.List(ctx)
	if err != nil {
		return nil, err
	}
	for _, job := range all {
		if job.ID() == id {
			return &job, nil
		}
	}
	return nil, nil
}

// Steps reads the selected job's immutable step snapshots in server order.
func (s *Service) Steps(ctx context.Context, id ID) ([]Step, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if id == uuid.Nil {
		return nil, invalid("job ID required")
	}
	response, err := s.client.GetJobSteps(ctx, &contracts.GetJobStepsRequest{EventStore: string(s.store), Namespace: string(s.namespace), JobId: wire.Guid(metadata.CorrelationID(id))})
	if err != nil {
		return nil, wire.RPCError(err)
	}
	if err = wire.CheckEnvelope(response); err != nil {
		return nil, err
	}
	result := make([]Step, 0, len(response.Data))
	seen := map[StepID]bool{}
	for _, value := range response.Data {
		step, err := decodeStep(value)
		if err != nil {
			return nil, err
		}
		if seen[step.ID()] {
			return nil, faults.ErrProtocol
		}
		seen[step.ID()] = true
		result = append(result, step)
	}
	return result, nil
}

// Stop requests stopping, not proof of stopped progress. No automatic retry.
func (s *Service) Stop(ctx context.Context, id ID) error { return s.mutate(ctx, id, "stop job") }

// Resume requests resumption, not proof of running or successful completion.
func (s *Service) Resume(ctx context.Context, id ID) error { return s.mutate(ctx, id, "resume job") }

// Delete requests deletion. Use WaitForDeletion to observe absence; absence
// cannot establish whether the job completed before it disappeared.
func (s *Service) Delete(ctx context.Context, id ID) error { return s.mutate(ctx, id, "delete job") }
func (s *Service) mutate(ctx context.Context, id ID, operation string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if id == uuid.Nil {
		return invalid("job ID required")
	}
	var response *contracts.CommandResult
	var err error
	guid := wire.Guid(metadata.CorrelationID(id))
	switch operation {
	case "stop job":
		response, err = s.client.StopJob(ctx, &contracts.StopJobRequest{EventStore: string(s.store), Namespace: string(s.namespace), JobId: guid})
	case "resume job":
		response, err = s.client.ResumeJob(ctx, &contracts.ResumeJobRequest{EventStore: string(s.store), Namespace: string(s.namespace), JobId: guid})
	case "delete job":
		response, err = s.client.DeleteJob(ctx, &contracts.DeleteJobRequest{EventStore: string(s.store), Namespace: string(s.namespace), JobId: guid})
	}
	if err == nil {
		err = wire.CheckEnvelope(response)
	}
	return administration.MutationError(operation, err)
}
func invalid(message string) error {
	return fmt.Errorf("%w: %s", faults.ErrInvalidConfiguration, message)
}
