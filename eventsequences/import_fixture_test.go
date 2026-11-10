// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/events"
	"google.golang.org/grpc"
)

type imported struct {
	Name string `json:"name"`
}

type unusedConnection struct{ grpc.ClientConnInterface }

// fakeImports stages chunks in memory like the kernel: once per index.
type fakeImports struct {
	mu             sync.Mutex
	id             ImportID
	maxEvents      int32
	maxBytes       int32
	beginErr       error
	beganWith      []OnInvalidEvent
	staged         map[int32]importChunk
	order          []int32
	add            func(ctx context.Context, chunk importChunk) (importChunkAck, error)
	completed      [][2]int64
	completeReply  importStatus
	abandoned      int
	snapshot       *importSnapshot
	pending        int // get calls that report a non-terminal state first
	updates        []importSnapshot
	inFlight       int
	maxConcurrency int
}

func newFakeImports(t *testing.T) *fakeImports {
	t.Helper()
	id, err := NewImportID()
	if err != nil {
		t.Fatal(err)
	}
	return &fakeImports{id: id, staged: map[int32]importChunk{}}
}

func (f *fakeImports) begin(_ context.Context, id ImportID, onInvalid OnInvalidEvent) (importOpened, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.beganWith = append(f.beganWith, onInvalid)
	if f.beginErr != nil {
		return importOpened{}, f.beginErr
	}
	if !id.IsZero() {
		f.id = id
	}
	return importOpened{id: f.id, maxChunkEvents: f.maxEvents, maxChunkBytes: f.maxBytes}, nil
}

func (f *fakeImports) addChunk(ctx context.Context, chunk importChunk) (importChunkAck, error) {
	f.mu.Lock()
	f.inFlight++
	f.maxConcurrency = max(f.maxConcurrency, f.inFlight)
	add := f.add
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.inFlight--
		f.mu.Unlock()
	}()
	if add != nil {
		ack, err := add(ctx, chunk)
		if err != nil || ack.err != "" {
			return ack, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.staged[chunk.index]; !ok {
		f.staged[chunk.index] = chunk
		f.order = append(f.order, chunk.index)
	}
	return importChunkAck{index: chunk.index}, nil
}

func (f *fakeImports) complete(_ context.Context, _ ImportID, chunks int32, count int64) (importStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completed = append(f.completed, [2]int64{int64(chunks), count})
	return f.completeReply, nil
}

func (f *fakeImports) abandon(context.Context, ImportID) (importStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.abandoned++
	return importStatus{state: ImportAbandoned}, nil
}

func (f *fakeImports) get(context.Context, ImportID) (importSnapshot, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.snapshot == nil {
		return importSnapshot{}, false, nil
	}
	snapshot := *f.snapshot
	if f.pending > 0 {
		f.pending--
		snapshot.result.State = ImportCommitting
	}
	return snapshot, true, nil
}

func (f *fakeImports) observe(ctx context.Context, _ ImportID, yield func(importSnapshot) bool) error {
	f.mu.Lock()
	updates := append([]importSnapshot(nil), f.updates...)
	f.mu.Unlock()
	for _, update := range updates {
		if !yield(update) {
			return nil
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

// stagedEvents returns every staged event in chunk-index order.
func (f *fakeImports) stagedEvents() []importEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	var result []importEvent
	for index := int32(0); ; index++ {
		chunk, ok := f.staged[index]
		if !ok {
			return result
		}
		result = append(result, chunk.events...)
	}
}

func importFixture(t *testing.T) (*Sequence, *fakeImports) {
	t.Helper()
	definition, err := events.Define[imported](events.WithID("imported"))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(definition.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	sequence, err := New("store", "Default", "event-log", catalog, unusedConnection{})
	if err != nil {
		t.Fatal(err)
	}
	fake := newFakeImports(t)
	fake.snapshot = &importSnapshot{sequence: "event-log", result: ImportResult{ImportID: fake.id, State: ImportCompleted}}
	sequence.imports = fake
	return sequence, fake
}

// fastImports keeps polling and retry delays short.
func fastImports(c *importConfig) {
	c.pollInterval = time.Millisecond
	c.retryDelay = func(int) time.Duration { return time.Millisecond }
}

func entries(n int) []Entry {
	result := make([]Entry, n)
	for i := range result {
		result[i] = Entry{Source: "customer", Event: imported{Name: "n"}}
	}
	return result
}
