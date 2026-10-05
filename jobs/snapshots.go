// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package jobs

import (
	"context"
	"slices"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/jobs"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

// Status is the kernel job lifecycle status, independent of progress counters.
// Unknown numeric values are preserved and are not terminal or success evidence.
type Status int32

const (
	// None means no lifecycle status has been recorded.
	None Status = iota
	// PreparingJob means job preparation is underway.
	PreparingJob
	// PreparingSteps means steps are being prepared.
	PreparingSteps
	// StartingSteps means prepared steps are starting.
	StartingSteps
	// Running means work is in progress.
	Running
	// CompletedSuccessfully is explicit successful terminal evidence.
	CompletedSuccessfully
	// CompletedWithFailures is terminal evidence with failures.
	CompletedWithFailures
	// Stopped means the job has stopped, not completed successfully.
	Stopped
	// Failed is explicit failure evidence.
	Failed
	// Removing means deletion is underway.
	Removing
)

// Terminal reports completion/failure, not stopped or removing states, matching C#.
func (s Status) Terminal() bool {
	return s == CompletedSuccessfully || s == CompletedWithFailures || s == Failed
}

// StepStatus is the kernel lifecycle status of a job step. Unknown numeric
// values are preserved without treating them as successful completion.
type StepStatus int32

const (
	// StepUnknown means the status is unknown.
	StepUnknown StepStatus = iota
	// StepScheduled means queued work.
	StepScheduled
	// StepRunning means work is in progress.
	StepRunning
	// StepCompletedSuccessfully means successful completion.
	StepCompletedSuccessfully
	// StepCompletedWithFailure means completion with failure.
	StepCompletedWithFailure
	// StepStopped means stopped work.
	StepStopped
	// StepFailed means failed work.
	StepFailed
	// StepRemoving means deletion is underway.
	StepRemoving
)

// Progress is an owned value snapshot; flags are server evidence, not inferred
// from counts. IsCompleted can include failed steps and is not successful status.
type Progress struct {
	// TotalSteps is the planned step count.
	TotalSteps int32
	// SuccessfulSteps is the successfully finished step count.
	SuccessfulSteps int32
	// FailedSteps is the failed step count.
	FailedSteps int32
	// StoppedSteps is the stopped step count.
	StoppedSteps int32
	// IsCompleted is the kernel's progress-completion flag.
	IsCompleted bool
	// IsStopped is the kernel's progress-stopped flag.
	IsStopped bool
	// Message is the current progress diagnostic.
	Message string
}

// StatusChange is an immutable job status-transition snapshot.
type StatusChange struct {
	status   Status
	occurred time.Time
	messages []string
	stack    string
}

// Status returns the reported lifecycle status.
func (c StatusChange) Status() Status { return c.status }

// Occurred returns when the status was recorded, preserving its offset.
func (c StatusChange) Occurred() time.Time { return c.occurred }

// ExceptionMessages returns an owned copy of failure messages.
func (c StatusChange) ExceptionMessages() []string { return slices.Clone(c.messages) }

// ExceptionStackTrace returns the diagnostic stack; it may contain sensitive data.
func (c StatusChange) ExceptionStackTrace() string { return c.stack }

// Job is an immutable snapshot. Its zero value is invalid; Get/List construct it.
type Job struct {
	handle  Handle
	value   *contracts.JobSummaryResponse
	created time.Time
	changes []StatusChange
}

// ID returns the job's UUID.
func (j Job) ID() ID { return j.handle.ID() }

// Handle returns the original store/namespace-bound identity for later reads.
func (j Job) Handle() Handle { return j.handle }

// Type returns the kernel job type name.
func (j Job) Type() string { return j.value.GetType() }

// Details returns the kernel's job details.
func (j Job) Details() string { return j.value.GetDetails() }

// Status returns the reported lifecycle status.
func (j Job) Status() Status { return Status(j.value.GetStatus()) }

// Created returns the creation instant and offset.
func (j Job) Created() time.Time { return j.created }

// StatusChanges returns an owned slice of immutable transitions in server order.
func (j Job) StatusChanges() []StatusChange { return slices.Clone(j.changes) }

// Progress returns an owned value snapshot.
func (j Job) Progress() Progress {
	p := j.value.GetProgress()
	return Progress{p.GetTotalSteps(), p.GetSuccessfulSteps(), p.GetFailedSteps(), p.GetStoppedSteps(), p.GetIsCompleted(), p.GetIsStopped(), p.GetMessage()}
}

// Steps reads steps using the snapshot's original identity and coordinates.
func (j Job) Steps(ctx context.Context) ([]Step, error) { return j.handle.Steps(ctx) }

// StepProgress is an owned step-progress value snapshot.
type StepProgress struct {
	// Percentage is the kernel's integral completion percentage.
	Percentage int32
	// Message is the step's progress message.
	Message string
}

// StepStatusChange is an immutable step transition.
type StepStatusChange struct {
	status   StepStatus
	occurred time.Time
	messages []string
	stack    string
}

// Status returns the reported step status.
func (c StepStatusChange) Status() StepStatus { return c.status }

// Occurred returns when the transition was recorded.
func (c StepStatusChange) Occurred() time.Time { return c.occurred }

// ExceptionMessages returns a detached slice of diagnostic messages.
func (c StepStatusChange) ExceptionMessages() []string { return slices.Clone(c.messages) }

// ExceptionStackTrace returns the recorded failure stack.
func (c StepStatusChange) ExceptionStackTrace() string { return c.stack }

// Step is an immutable step snapshot, constructed by Steps. Zero is invalid.
type Step struct {
	value   *contracts.JobStepSummaryResponse
	changes []StepStatusChange
}

// ID returns the step UUID, not its parent job ID.
func (s Step) ID() StepID { return uuid.UUID(wire.Correlation(s.value.GetId())) }

// Type returns the kernel step type.
func (s Step) Type() string { return s.value.GetType() }

// Name returns the step name.
func (s Step) Name() string { return s.value.GetName() }

// Status returns the reported step status.
func (s Step) Status() StepStatus { return StepStatus(s.value.GetStatus()) }

// Progress returns a detached progress value.
func (s Step) Progress() StepProgress {
	p := s.value.GetProgress()
	return StepProgress{p.GetPercentage(), p.GetMessage()}
}

// StatusChanges returns an owned slice of immutable transitions.
func (s Step) StatusChanges() []StepStatusChange { return slices.Clone(s.changes) }

func decodeJob(value *contracts.JobSummaryResponse, s *Service) (Job, error) {
	if value == nil || value.Id == nil || uuid.UUID(wire.Correlation(value.Id)) == uuid.Nil || value.Type == "" || value.Created == nil || value.Progress == nil {
		return Job{}, faults.ErrProtocol
	}
	created, err := time.Parse(time.RFC3339Nano, value.Created.Value)
	if err != nil {
		return Job{}, faults.ErrProtocol
	}
	p := value.Progress
	if p.TotalSteps < 0 || p.SuccessfulSteps < 0 || p.FailedSteps < 0 || p.StoppedSteps < 0 {
		return Job{}, faults.ErrProtocol
	}
	job := Job{handle: Handle{s, uuid.UUID(wire.Correlation(value.Id))}, value: proto.Clone(value).(*contracts.JobSummaryResponse), created: created}
	for _, change := range value.StatusChanges {
		if change == nil || change.Occurred == nil {
			return Job{}, faults.ErrProtocol
		}
		occurred, err := time.Parse(time.RFC3339Nano, change.Occurred.Value)
		if err != nil {
			return Job{}, faults.ErrProtocol
		}
		job.changes = append(job.changes, StatusChange{Status(change.Status), occurred, slices.Clone(change.ExceptionMessages), change.ExceptionStackTrace})
	}
	return job, nil
}
func decodeStep(value *contracts.JobStepSummaryResponse) (Step, error) {
	if value == nil || value.Id == nil || uuid.UUID(wire.Correlation(value.Id)) == uuid.Nil || value.Type == "" || value.Progress == nil || value.Progress.Percentage < 0 || value.Progress.Percentage > 100 {
		return Step{}, faults.ErrProtocol
	}
	step := Step{value: proto.Clone(value).(*contracts.JobStepSummaryResponse)}
	for _, change := range value.StatusChanges {
		if change == nil || change.Occurred == nil {
			return Step{}, faults.ErrProtocol
		}
		occurred, err := time.Parse(time.RFC3339Nano, change.Occurred.Value)
		if err != nil {
			return Step{}, faults.ErrProtocol
		}
		step.changes = append(step.changes, StepStatusChange{StepStatus(change.Status), occurred, slices.Clone(change.ExceptionMessages), change.ExceptionStackTrace})
	}
	return step, nil
}
