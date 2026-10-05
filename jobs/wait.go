// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package jobs

import (
	"context"
	"errors"
	"strings"
	"time"
)

// InfiniteTimeout disables the helper's deadline, never the caller's context.
const InfiniteTimeout time.Duration = -1

// DefaultTimeout is the C# jobs helper's default five-second budget.
const DefaultTimeout = 5 * time.Second

// AbsentError means a required job is absent. Seen distinguishes disappearance
// after observation from initial absence. Neither proves successful completion.
type AbsentError struct {
	// ID identifies the missing job.
	ID ID
	// Seen says whether a prior poll observed this job.
	Seen bool
}

func (e *AbsentError) Error() string { return "chronicle: job absent; completion unknown" }

// TerminalError carries actual failed or stopped job evidence, not a timeout.
type TerminalError struct {
	// Job is the last immutable server snapshot.
	Job Job
}

func (e *TerminalError) Error() string {
	return "chronicle: job failed or stopped before successful completion"
}

// WaitForCompletion requires explicit CompletedSuccessfully status. A removed
// job returns AbsentError, even if the server normally deletes completed jobs.
// Failed, CompletedWithFailures and Stopped return TerminalError immediately.
func (s *Service) WaitForCompletion(ctx context.Context, id ID, timeout time.Duration) (*Job, error) {
	return s.wait(ctx, id, timeout, func(j Job) (bool, error) {
		switch j.Status() {
		case CompletedSuccessfully:
			return true, nil
		case Failed, CompletedWithFailures, Stopped:
			return false, &TerminalError{Job: j}
		default:
			return false, nil
		}
	})
}

// WaitForCompletion waits for this handle's job using explicit terminal evidence.
func (h Handle) WaitForCompletion(ctx context.Context, timeout time.Duration) (*Job, error) {
	if h.service == nil {
		return nil, invalid("job handle required")
	}
	return h.service.WaitForCompletion(ctx, h.id, timeout)
}

// WaitForTerminalOrAbsent follows C# WaitTillJobCompletesOrIsDeleted. Nil is
// absence, not success; retained failed jobs are returned with their actual status.
func (s *Service) WaitForTerminalOrAbsent(ctx context.Context, id ID, timeout time.Duration) (*Job, error) {
	job, err := s.wait(ctx, id, timeout, func(j Job) (bool, error) { return j.Status().Terminal(), nil })
	var absent *AbsentError
	if errors.As(err, &absent) {
		return nil, nil
	}
	return job, err
}

// WaitForProgressCompleted requires the kernel's progress flag. Inspect status
// and FailedSteps separately; this flag does not mean successful completion.
func (s *Service) WaitForProgressCompleted(ctx context.Context, id ID, timeout time.Duration) (*Job, error) {
	return s.WaitFor(ctx, id, timeout, func(j Job) bool { return j.Progress().IsCompleted })
}

// WaitForProgressStopped requires the actual kernel progress-stopped flag.
func (s *Service) WaitForProgressStopped(ctx context.Context, id ID, timeout time.Duration) (*Job, error) {
	return s.WaitFor(ctx, id, timeout, func(j Job) bool { return j.Progress().IsStopped })
}

// WaitFor polls a caller predicate every 50ms, without goroutines. Predicate
// calls are synchronous and must not block. Missing jobs return AbsentError.
// Zero timeout selects five seconds; InfiniteTimeout still honors cancellation.
func (s *Service) WaitFor(ctx context.Context, id ID, timeout time.Duration, predicate func(Job) bool) (*Job, error) {
	if predicate == nil {
		return nil, invalid("job predicate required")
	}
	return s.wait(ctx, id, timeout, func(j Job) (bool, error) { return predicate(j), nil })
}
func (s *Service) wait(ctx context.Context, id ID, timeout time.Duration, predicate func(Job) (bool, error)) (*Job, error) {
	ctx, cancel, err := budget(ctx, timeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	seen := false
	for {
		job, err := s.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		if job == nil {
			return nil, &AbsentError{ID: id, Seen: seen}
		}
		seen = true
		done, err := predicate(*job)
		if done || err != nil {
			return job, err
		}
		if err = pause(ctx); err != nil {
			return job, err
		}
	}
}

// WaitForDeletion observes absence only; it cannot distinguish deletion from
// automatic cleanup. It never reports that processing completed successfully.
func (s *Service) WaitForDeletion(ctx context.Context, id ID, timeout time.Duration) error {
	_, err := s.wait(ctx, id, timeout, func(Job) (bool, error) { return false, nil })
	var absent *AbsentError
	if errors.As(err, &absent) {
		return nil
	}
	return err
}

// WaitForJobs waits for any jobs when typeSubstring is empty, otherwise for a
// case-sensitive type substring as in C#. Timeout is an error, not an empty success.
func (s *Service) WaitForJobs(ctx context.Context, typeSubstring string, timeout time.Duration) ([]Job, error) {
	return s.waitList(ctx, timeout, func(all []Job) ([]Job, bool) {
		matching := make([]Job, 0)
		for _, j := range all {
			if strings.Contains(j.Type(), typeSubstring) {
				matching = append(matching, j)
			}
		}
		return matching, len(matching) > 0
	})
}

// TryFindJobsOfType returns an empty slice on the helper's timeout only. Caller
// cancellation/deadline and RPC failures remain errors; absence proves no completion.
func (s *Service) TryFindJobsOfType(ctx context.Context, typeSubstring string, timeout time.Duration) ([]Job, error) {
	waitCtx, cancel, err := budget(ctx, timeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	found, err := s.WaitForJobs(waitCtx, typeSubstring, InfiniteTimeout)
	if errors.Is(err, context.DeadlineExceeded) && waitCtx.Err() == context.DeadlineExceeded && ctx.Err() == nil {
		return []Job{}, nil
	}
	return found, err
}

// WaitForNoJobs waits until no job in the selected statuses remains (empty means
// all statuses). Unknown numeric statuses may also be selected. This corrects
// C#'s inverted status predicate; it is not completion evidence.
func (s *Service) WaitForNoJobs(ctx context.Context, timeout time.Duration, statuses ...Status) error {
	selected := make(map[Status]bool, len(statuses))
	for _, status := range statuses {
		selected[status] = true
	}
	_, err := s.waitList(ctx, timeout, func(all []Job) ([]Job, bool) {
		for _, job := range all {
			if len(selected) == 0 || selected[job.Status()] {
				return all, false
			}
		}
		return all, true
	})
	return err
}
func (s *Service) waitList(ctx context.Context, timeout time.Duration, predicate func([]Job) ([]Job, bool)) ([]Job, error) {
	ctx, cancel, err := budget(ctx, timeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	for {
		all, err := s.List(ctx)
		if err != nil {
			return nil, err
		}
		result, done := predicate(all)
		if done {
			return result, nil
		}
		if err = pause(ctx); err != nil {
			return nil, err
		}
	}
}
func budget(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	if timeout == InfiniteTimeout {
		child, cancel := context.WithCancel(ctx)
		return child, cancel, nil
	}
	if timeout < 0 {
		return nil, nil, invalid("timeout must be positive, zero or InfiniteTimeout")
	}
	child, cancel := context.WithTimeout(ctx, timeout)
	return child, cancel, nil
}
func pause(ctx context.Context) error {
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
