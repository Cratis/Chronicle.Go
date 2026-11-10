// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"context"
	"fmt"
	"iter"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
)

// Import brings existing history into the sequence: it reads entries lazily,
// stages them on the kernel in chunks, and completes the import, which the
// kernel validates as a whole before committing any event. Entries keep their
// Occurred, Subject, tags, route, correlation, causation and caused-by identity;
// they get new sequence numbers.
//
// The returned result reports the outcome, including a failed or rejected
// import (State ImportFailed with Rejections); errors are local, transport or
// protocol failures. When an error occurs after the import began, the result
// carries its ImportID, and the import stays open on the kernel so it can be
// continued with ReattachImport. Cancelling ctx stops reading and sending.
// Progress is reported through WithImportProgress.
func (s *Sequence) Import(ctx context.Context, entries iter.Seq2[Entry, error], options ...ImportOption) (ImportResult, error) {
	if entries == nil {
		return ImportResult{}, fmt.Errorf("%w: nil import events", faults.ErrInvalidConfiguration)
	}
	config, err := importOptions(options)
	if err != nil {
		return ImportResult{}, err
	}
	session, err := s.beginImport(ctx, config)
	if err != nil {
		return ImportResult{}, err
	}
	if config.progress != nil {
		observing, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			// Progress is best effort, as in the .NET client: a failed observation does not fail the import.
			_ = session.ObserveProgress(observing, config.progress)
		}()
		defer func() {
			cancel()
			<-done
		}()
	}
	if err := session.Add(ctx, entries); err != nil {
		return ImportResult{ImportID: session.ID()}, err
	}
	return session.Complete(ctx)
}

// BeginImport opens an import session on the kernel. WithImportProgress is not
// accepted here; use ImportSession.ObserveProgress. A kernel that has no Imports
// service fails with an error wrapping ErrUnsupported (see the chronicle
// package), and a refusal returns *ImportCouldNotBeginError.
func (s *Sequence) BeginImport(ctx context.Context, options ...ImportOption) (*ImportSession, error) {
	config, err := importOptions(options)
	if err != nil {
		return nil, err
	}
	if config.progress != nil {
		return nil, fmt.Errorf("%w: WithImportProgress applies to Import; use ImportSession.ObserveProgress", faults.ErrInvalidConfiguration)
	}
	return s.beginImport(ctx, config)
}

func (s *Sequence) beginImport(ctx context.Context, config importConfig) (*ImportSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	service := s.importService()
	opened, err := service.begin(ctx, config.id, config.onInvalid)
	if err != nil {
		return nil, wire.RPCError(err)
	}
	if opened.err != "" {
		return nil, &ImportCouldNotBeginError{Reason: opened.err}
	}
	if opened.id.IsZero() {
		return nil, fmt.Errorf("%w: the kernel began an import without an ID", faults.ErrProtocol)
	}
	return newImportSession(s, service, opened.id, config, opened.maxChunkEvents, opened.maxChunkBytes, 0, 0), nil
}

// ReattachImport attaches to an import that is still open, for example after the
// producing process crashed mid-staging. The session continues after the
// kernel's acknowledged events (ImportSession.EventsAcknowledged); the caller
// skips that many source events. The import keeps the invalid-event policy it
// was begun with. An unknown ID returns *ImportCouldNotBeginError wrapping
// ErrImportNotFound; an import into another sequence is refused.
func (s *Sequence) ReattachImport(ctx context.Context, id ImportID, options ...ImportOption) (*ImportSession, error) {
	if id.IsZero() {
		return nil, fmt.Errorf("%w: the import ID must not be zero", faults.ErrInvalidConfiguration)
	}
	config, err := importOptions(options)
	if err != nil {
		return nil, err
	}
	if config.progress != nil {
		return nil, fmt.Errorf("%w: WithImportProgress applies to Import; use ImportSession.ObserveProgress", faults.ErrInvalidConfiguration)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	service := s.importService()
	snapshot, found, err := service.get(ctx, id)
	if err != nil {
		return nil, wire.RPCError(err)
	}
	if !found {
		return nil, &ImportCouldNotBeginError{Reason: fmt.Sprintf("import %s does not exist", id), Err: ErrImportNotFound}
	}
	if snapshot.sequence != string(s.id) {
		return nil, &ImportCouldNotBeginError{Reason: fmt.Sprintf("import %s imports into event sequence %q, not %q", id, snapshot.sequence, s.id)}
	}
	config.id, config.onInvalid = id, snapshot.onInvalid
	return newImportSession(s, service, id, config, snapshot.maxChunkEvents, snapshot.maxChunkBytes, snapshot.acknowledgedChunks, snapshot.acknowledgedEvents), nil
}

// ImportEntries adapts a slice for Import and ImportSession.Add. The slice is
// borrowed until the iteration ends.
func ImportEntries(entries []Entry) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		for _, entry := range entries {
			if !yield(entry, nil) {
				return
			}
		}
	}
}
