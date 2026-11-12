// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"context"
	"fmt"
	"iter"
	"math"
	"sync"
	"time"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ImportSession is an open import that events are added to in chunks, for
// producers that are not a single stream. Obtain it from BeginImport or
// ReattachImport. Nothing is committed before Complete.
//
// Add and Complete are serialized: a call waits for the one before it, so chunk
// order follows call order. Abandon may run concurrently with them. A session
// owns no goroutine between calls and needs no Close: dropping it without
// Complete or Abandon leaves the import open on the kernel, which can be
// reattached by ID until the kernel's staging retention abandons it. Events
// buffered for an unfinished chunk are then not sent.
type ImportSession struct {
	sequence     *Sequence
	service      importTransport
	id           ImportID
	config       importConfig
	maxEvents    int
	maxBytes     int
	acknowledged int64

	gate       chan struct{}
	slots      chan struct{}
	abandoning chan struct{}
	stop       sync.Once
	sends      sync.WaitGroup

	mu          sync.Mutex
	failure     error
	abandoned   bool
	nextChunk   int32
	nextEvent   int64
	buffer      []importEvent
	bufferBytes int
}

func newImportSession(s *Sequence, service importTransport, id ImportID, config importConfig, maxChunkEvents, maxChunkBytes, acknowledgedChunks int32, acknowledgedEvents int64) *ImportSession {
	maxEvents := math.MaxInt
	if maxChunkEvents > 0 {
		maxEvents = int(maxChunkEvents)
	}
	if config.maxEvents > 0 {
		maxEvents = min(maxEvents, config.maxEvents)
	}
	maxBytes := math.MaxInt
	if maxChunkBytes > 0 {
		maxBytes = int(maxChunkBytes)
	}
	return &ImportSession{sequence: s, service: service, id: id, config: config, maxEvents: maxEvents, maxBytes: maxBytes, acknowledged: acknowledgedEvents,
		gate: make(chan struct{}, 1), slots: make(chan struct{}, config.inFlight), abandoning: make(chan struct{}),
		nextChunk: acknowledgedChunks, nextEvent: acknowledgedEvents}
}

// ID returns the import's identity, which ReattachImport accepts.
func (i *ImportSession) ID() ImportID { return i.id }

// EventsAcknowledged returns how many events the kernel already held, in order
// and without a gap, when the session was attached. A reattaching producer
// continues after this many events; a new session starts at zero.
func (i *ImportSession) EventsAcknowledged() int64 { return i.acknowledged }

// Add reads events lazily and sends them in chunks within the kernel's limits,
// keeping at most the configured number of chunks in flight. It returns once
// every full chunk it produced is acknowledged; a trailing partial chunk stays
// buffered until the next Add or Complete. The first iterator error stops
// reading and is returned; events read before it stay in the import.
//
// A chunk refused by the kernel returns *ImportChunkRefusedError, and every later
// Add or Complete returns the same failure; reattach to continue. Cancelling ctx
// stops reading and cancels in-flight sends. After Abandon, Add returns
// ErrImportAbandoned once the iterator yields again or ends.
func (i *ImportSession) Add(ctx context.Context, entries iter.Seq2[Entry, error]) error {
	if entries == nil {
		return fmt.Errorf("%w: nil import events", faults.ErrInvalidConfiguration)
	}
	if err := i.enter(ctx); err != nil {
		return err
	}
	defer i.leave()
	err := i.read(ctx, entries)
	i.sends.Wait()
	if err == nil {
		err = i.state()
	}
	return err
}

func (i *ImportSession) read(ctx context.Context, entries iter.Seq2[Entry, error]) error {
	encoder, err := i.sequence.importEncoder(ctx)
	if err != nil {
		return err
	}
	for entry, err := range entries {
		if err != nil {
			return err
		}
		if err := i.state(); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		i.mu.Lock()
		index := i.nextEvent + int64(len(i.buffer))
		i.mu.Unlock()
		event, err := encoder.encode(ctx, entry, index)
		if err != nil {
			return err
		}
		if err := i.append(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

// Complete flushes buffered events, waits for every chunk to be acknowledged,
// closes staging with the expected chunk and event counts and, unless
// WithoutWaitingForCommit was given, polls until the import reaches a terminal
// state. A failed or rejected import is a result with State ImportFailed, not an
// error; errors are local, transport or protocol failures.
func (i *ImportSession) Complete(ctx context.Context) (ImportResult, error) {
	if err := i.enter(ctx); err != nil {
		return ImportResult{ImportID: i.id}, err
	}
	defer i.leave()
	err := i.flush(ctx)
	i.sends.Wait()
	if err == nil {
		err = i.state()
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return ImportResult{ImportID: i.id}, err
	}
	i.mu.Lock()
	chunks, count := i.nextChunk, i.nextEvent
	i.mu.Unlock()
	completed, err := i.service.complete(ctx, i.id, chunks, count)
	if err != nil {
		return ImportResult{ImportID: i.id}, wire.RPCError(err)
	}
	if completed.err == "" && !i.config.noWait {
		return i.waitForCommit(ctx)
	}
	return i.resultAfter(ctx, completed)
}

// Abandon discards everything staged; nothing is committed. No chunk is sent
// once it begins, an Add in progress stops at the iterator's next event, and
// chunks already handed to the kernel settle first, so none is staged after the
// import is abandoned. Later Add and Complete calls return ErrImportAbandoned.
func (i *ImportSession) Abandon(ctx context.Context) (ImportResult, error) {
	i.mu.Lock()
	i.abandoned = true
	i.mu.Unlock()
	i.stop.Do(func() { close(i.abandoning) })
	i.sends.Wait()
	if err := ctx.Err(); err != nil {
		return ImportResult{ImportID: i.id}, err
	}
	abandoned, err := i.service.abandon(ctx, i.id)
	if err != nil {
		return ImportResult{ImportID: i.id}, wire.RPCError(err)
	}
	return i.resultAfter(ctx, abandoned)
}

// Progress reads the import's current progress from the kernel.
func (i *ImportSession) Progress(ctx context.Context) (ImportProgress, error) {
	snapshot, found, err := i.service.get(ctx, i.id)
	if err != nil {
		return ImportProgress{}, wire.RPCError(err)
	}
	if !found {
		return ImportProgress{}, fmt.Errorf("%w: %s", ErrImportNotFound, i.id)
	}
	return snapshot.progress(), nil
}

// ObserveProgress calls report, sequentially and on the calling goroutine, for
// each progress update the kernel pushes. It returns nil once the import reaches
// a terminal state or the kernel ends the stream, and the context's error when
// ctx is done first.
func (i *ImportSession) ObserveProgress(ctx context.Context, report func(ImportProgress)) error {
	if report == nil {
		return fmt.Errorf("%w: nil progress callback", faults.ErrInvalidConfiguration)
	}
	err := i.service.observe(ctx, i.id, func(snapshot importSnapshot) bool {
		report(snapshot.progress())
		return !snapshot.result.State.IsTerminal()
	})
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return wire.RPCError(err)
}

// enter acquires the session for one Add or Complete.
func (i *ImportSession) enter(ctx context.Context) error {
	if err := i.state(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case i.gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-i.abandoning:
		return ErrImportAbandoned
	}
}

func (i *ImportSession) leave() { <-i.gate }

// state returns the abandonment or the first send failure.
func (i *ImportSession) state() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.abandoned {
		return ErrImportAbandoned
	}
	return i.failure
}

func (i *ImportSession) append(ctx context.Context, event importEvent) error {
	size := event.size()
	i.mu.Lock()
	full := len(i.buffer) > 0 && (len(i.buffer) >= i.maxEvents || i.bufferBytes > i.maxBytes-size)
	i.mu.Unlock()
	if full {
		if err := i.flush(ctx); err != nil {
			return err
		}
	}
	i.mu.Lock()
	i.buffer = append(i.buffer, event)
	i.bufferBytes += size
	i.mu.Unlock()
	return nil
}

// flush hands the buffered events to a send goroutine once an in-flight slot is
// free. The goroutine is joined by the Add or Complete that started it.
func (i *ImportSession) flush(ctx context.Context) error {
	i.mu.Lock()
	empty := len(i.buffer) == 0
	i.mu.Unlock()
	if empty {
		return nil
	}
	select {
	case i.slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-i.abandoning:
		return ErrImportAbandoned
	}
	i.mu.Lock()
	// Checked and registered together, so Abandon waits for every chunk sent
	// before it and none is sent after it.
	if i.abandoned {
		i.mu.Unlock()
		<-i.slots
		return ErrImportAbandoned
	}
	chunk := importChunk{id: i.id, index: i.nextChunk, firstEventIndex: i.nextEvent, events: i.buffer}
	i.nextChunk++
	i.nextEvent += int64(len(i.buffer))
	i.buffer, i.bufferBytes = nil, 0
	i.sends.Add(1)
	i.mu.Unlock()
	go i.send(ctx, chunk)
	return nil
}

var transientImportCodes = []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted, codes.Internal}

func (i *ImportSession) send(ctx context.Context, chunk importChunk) {
	defer i.sends.Done()
	defer func() { <-i.slots }()
	for attempt := 1; ; attempt++ {
		ack, err := i.service.addChunk(ctx, chunk)
		if err == nil {
			if ack.err != "" {
				i.fail(&ImportChunkRefusedError{ChunkIndex: chunk.index, Reason: ack.err})
			} else if ack.index != chunk.index {
				i.fail(fmt.Errorf("%w: chunk %d acknowledged as %d", faults.ErrProtocol, chunk.index, ack.index))
			}
			return
		}
		if ctx.Err() != nil || attempt >= i.config.attempts || !isTransient(err) {
			i.fail(wire.RPCError(err))
			return
		}
		// Sending the chunk again is safe: the kernel stages a chunk under its index once.
		timer := time.NewTimer(i.config.retryDelay(attempt))
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			i.fail(ctx.Err())
			return
		case <-i.abandoning:
			timer.Stop()
			i.fail(ErrImportAbandoned)
			return
		}
	}
}

func isTransient(err error) bool {
	code := status.Code(err)
	for _, transient := range transientImportCodes {
		if code == transient {
			return true
		}
	}
	return false
}

func (i *ImportSession) fail(err error) {
	i.mu.Lock()
	if i.failure == nil {
		i.failure = err
	}
	i.mu.Unlock()
}

func (i *ImportSession) waitForCommit(ctx context.Context) (ImportResult, error) {
	for {
		snapshot, found, err := i.service.get(ctx, i.id)
		if err != nil {
			return ImportResult{ImportID: i.id}, wire.RPCError(err)
		}
		if found && snapshot.result.State.IsTerminal() {
			return snapshot.result.clone(), nil
		}
		timer := time.NewTimer(i.config.pollInterval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ImportResult{ImportID: i.id}, ctx.Err()
		}
	}
}

// read2 reads the result after a status reply, falling back to the reply.
func (i *ImportSession) resultAfter(ctx context.Context, reply importStatus) (ImportResult, error) {
	snapshot, found, err := i.service.get(ctx, i.id)
	if err != nil {
		return ImportResult{ImportID: i.id, State: reply.state, FailureReason: reply.err}, wire.RPCError(err)
	}
	if !found {
		return ImportResult{ImportID: i.id, State: reply.state, FailureReason: reply.err,
			FirstSequenceNumber: events.Unavailable, LastSequenceNumber: events.Unavailable}, nil
	}
	return snapshot.result.clone(), nil
}
