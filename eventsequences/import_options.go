// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"fmt"
	"time"

	"github.com/cratis/chronicle.go/internal/faults"
)

// Import defaults match the .NET client's ImportOptions.
const (
	// DefaultMaxChunksInFlight is how many chunks are sent at the same time by default.
	DefaultMaxChunksInFlight = 4
	// DefaultMaxChunkAttempts is how many times a chunk is sent by default before
	// a transient transport failure is returned.
	DefaultMaxChunkAttempts = 5
)

// ImportOption configures Import, BeginImport or ReattachImport. Options apply in
// order and later scalar options win; a nil option or an invalid value fails the
// call before any RPC.
type ImportOption func(*importConfig)

type importConfig struct {
	onInvalid    OnInvalidEvent
	id           ImportID
	inFlight     int
	maxEvents    int
	attempts     int
	noWait       bool
	progress     func(ImportProgress)
	pollInterval time.Duration
	retryDelay   func(attempt int) time.Duration
	invalid      string
}

// WithOnInvalid selects what the import does with invalid events. The default is
// RejectInvalidEvents: nothing is committed. ReattachImport ignores it and keeps
// the policy the import was begun with.
func WithOnInvalid(policy OnInvalidEvent) ImportOption {
	return func(c *importConfig) {
		if policy != RejectInvalidEvents && policy != SkipInvalidEvents {
			c.invalid = fmt.Sprintf("unknown invalid-event policy %d", policy)
		}
		c.onInvalid = policy
	}
}

// WithImportID gives a new import this identity instead of a kernel-assigned one.
// Beginning an import whose ID already exists returns that import; to continue
// sending events to it, use ReattachImport, which knows how many it holds.
func WithImportID(id ImportID) ImportOption {
	return func(c *importConfig) {
		if id.IsZero() {
			c.invalid = "the import ID must not be zero"
		}
		c.id = id
	}
}

// WithMaxChunksInFlight bounds how many chunks are being sent at the same time.
// It must be at least one; the default is DefaultMaxChunksInFlight.
func WithMaxChunksInFlight(n int) ImportOption {
	return func(c *importConfig) {
		if n < 1 {
			c.invalid = "at least one chunk must be allowed in flight"
		}
		c.inFlight = n
	}
}

// WithMaxEventsPerChunk caps the events the client puts in one chunk. The
// kernel's own limit applies when it is lower. It must be at least one.
func WithMaxEventsPerChunk(n int) ImportOption {
	return func(c *importConfig) {
		if n < 1 {
			c.invalid = "a chunk must hold at least one event"
		}
		c.maxEvents = n
	}
}

// WithMaxChunkAttempts sets how many times a chunk is sent when the transport
// fails transiently. Sending again is safe: the kernel stages a chunk once per
// index. It must be at least one; the default is DefaultMaxChunkAttempts.
func WithMaxChunkAttempts(n int) ImportOption {
	return func(c *importConfig) {
		if n < 1 {
			c.invalid = "a chunk must be attempted at least once"
		}
		c.attempts = n
	}
}

// WithoutWaitingForCommit makes Complete return once the kernel has accepted the
// import for validation and commit, rather than once the commit has finished.
func WithoutWaitingForCommit() ImportOption {
	return func(c *importConfig) { c.noWait = true }
}

// WithImportProgress reports the kernel's progress to report while Import runs.
// It is called sequentially from a goroutine Import owns and joins before it
// returns; it must not block for long. Progress is best effort: a failure to
// observe it does not fail the import. Only Import accepts this option; a
// session reports progress through ImportSession.ObserveProgress.
func WithImportProgress(report func(ImportProgress)) ImportOption {
	return func(c *importConfig) {
		if report == nil {
			c.invalid = "the progress callback must not be nil"
		}
		c.progress = report
	}
}

func importOptions(options []ImportOption) (importConfig, error) {
	config := importConfig{inFlight: DefaultMaxChunksInFlight, attempts: DefaultMaxChunkAttempts, pollInterval: 500 * time.Millisecond,
		retryDelay: func(attempt int) time.Duration {
			return min(5*time.Second, time.Duration(attempt*attempt)*200*time.Millisecond)
		}}
	for _, option := range options {
		if option == nil {
			return importConfig{}, fmt.Errorf("%w: nil import option", faults.ErrInvalidConfiguration)
		}
		option(&config)
		if config.invalid != "" {
			return importConfig{}, fmt.Errorf("%w: %s", faults.ErrInvalidConfiguration, config.invalid)
		}
	}
	return config, nil
}
