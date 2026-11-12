// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestImportCutsChunksWithinTheKernelEventLimit(t *testing.T) {
	sequence, fake := importFixture(t)
	fake.maxEvents = 3
	result, err := sequence.Import(t.Context(), ImportEntries(entries(10)), fastImports)
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsSuccess() || result.ImportID != fake.id {
		t.Fatalf("result = %+v", result)
	}
	var sizes, firsts []int64
	for index := int32(0); index < 4; index++ {
		chunk := fake.staged[index]
		sizes, firsts = append(sizes, int64(len(chunk.events))), append(firsts, chunk.firstEventIndex)
	}
	if !slices.Equal(sizes, []int64{3, 3, 3, 1}) || !slices.Equal(firsts, []int64{0, 3, 6, 9}) || len(fake.staged) != 4 {
		t.Fatalf("chunk sizes %v, first indexes %v, staged %d", sizes, firsts, len(fake.staged))
	}
	if !slices.Equal(fake.completed, [][2]int64{{4, 10}}) {
		t.Fatalf("CompleteImport counts = %v, want [[4 10]]", fake.completed)
	}
}

func TestImportClientEventLimitAppliesBelowTheKernelLimit(t *testing.T) {
	sequence, fake := importFixture(t)
	fake.maxEvents = 100
	if _, err := sequence.Import(t.Context(), ImportEntries(entries(5)), fastImports, WithMaxEventsPerChunk(2)); err != nil {
		t.Fatal(err)
	}
	if len(fake.staged) != 3 || !slices.Equal(fake.completed, [][2]int64{{3, 5}}) {
		t.Fatalf("staged %d chunks, completed %v", len(fake.staged), fake.completed)
	}
}

func TestImportCutsChunksWithinTheKernelByteLimit(t *testing.T) {
	sequence, fake := importFixture(t)
	probe, err := sequence.importEncoder(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	one, err := probe.encode(t.Context(), Entry{Source: "customer", Event: imported{Name: "n"}, IdempotencyKey: "k-0"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	size := one.size()
	if size != len(one.content)+len("k-0") {
		t.Fatalf("size = %d, want content plus key", size)
	}
	fake.maxBytes = int32(2*size + 1)
	values := make([]Entry, 7)
	for i := range values {
		values[i] = Entry{Source: "customer", Event: imported{Name: "n"}, IdempotencyKey: IdempotencyKey(fmt.Sprintf("k-%d", i))}
	}
	if _, err := sequence.Import(t.Context(), ImportEntries(values), fastImports); err != nil {
		t.Fatal(err)
	}
	for index, chunk := range fake.staged {
		total := 0
		for _, event := range chunk.events {
			total += event.size()
		}
		if total > int(fake.maxBytes) || (index < 3 && len(chunk.events) != 2) {
			t.Fatalf("chunk %d has %d events and %d bytes, limit %d", index, len(chunk.events), total, fake.maxBytes)
		}
	}
	if len(fake.staged) != 4 {
		t.Fatalf("staged %d chunks, want 4", len(fake.staged))
	}
}

func TestImportKeepsAtMostFourChunksInFlight(t *testing.T) {
	sequence, fake := importFixture(t)
	fake.maxEvents = 1
	// 21 events: Add sends 20 chunks, the last event is flushed by Complete once
	// Add has joined its own chunks.
	const added, total = 20, 21
	entered, release := make(chan struct{}, total), make(chan struct{})
	fake.add = func(ctx context.Context, chunk importChunk) (importChunkAck, error) {
		entered <- struct{}{}
		select {
		case <-release:
			return importChunkAck{}, nil
		case <-ctx.Done():
			return importChunkAck{}, ctx.Err()
		}
	}
	requested := make(chan int, total)
	source := func(yield func(Entry, error) bool) {
		for k := 1; k <= total; k++ {
			requested <- k
			if !yield(Entry{Source: "customer", Event: imported{Name: "n"}}, nil) {
				return
			}
		}
	}
	done := make(chan error, 1)
	go func() {
		_, err := sequence.Import(t.Context(), source, fastImports)
		done <- err
	}()
	for range DefaultMaxChunksInFlight {
		<-entered
	}
	// With four one-event chunks in flight the client has read the sixth event,
	// which completes a fifth chunk that waits for a slot; it must read no further.
	for k := 0; k != DefaultMaxChunksInFlight+2; {
		k = <-requested
	}
	select {
	case k := <-requested:
		t.Fatalf("read event %d with %d chunks in flight", k, DefaultMaxChunksInFlight)
	case <-time.After(50 * time.Millisecond):
	}
	// Release a chunk only once the client has filled every in-flight slot it can.
	in, out := DefaultMaxChunksInFlight, 0
	for out < total {
		limit := added
		if out == added {
			limit = total
		}
		if in < limit && in-out < DefaultMaxChunksInFlight {
			<-entered
			in++
			continue
		}
		release <- struct{}{}
		out++
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if fake.maxConcurrency != DefaultMaxChunksInFlight || len(fake.staged) != total {
		t.Fatalf("max concurrent AddImportChunk = %d, want %d; staged %d", fake.maxConcurrency, DefaultMaxChunksInFlight, len(fake.staged))
	}
}

func TestImportReadsLazily(t *testing.T) {
	sequence, fake := importFixture(t)
	fake.maxEvents = 2
	var read int
	source := func(yield func(Entry, error) bool) {
		for range 6 {
			read++
			fake.mu.Lock()
			staged := len(fake.staged)
			fake.mu.Unlock()
			// Never more than one chunk plus the event being read ahead of what is sent.
			if read-2*staged > 2+2*DefaultMaxChunksInFlight {
				t.Errorf("read %d events with %d chunks staged", read, staged)
			}
			if !yield(Entry{Source: "customer", Event: imported{Name: "n"}}, nil) {
				return
			}
		}
	}
	if _, err := sequence.Import(t.Context(), source, fastImports); err != nil {
		t.Fatal(err)
	}
	if read != 6 {
		t.Fatalf("read %d events", read)
	}
}

func TestImportReattachContinuesAfterAcknowledgedChunks(t *testing.T) {
	sequence, fake := importFixture(t)
	fake.snapshot = &importSnapshot{sequence: "event-log", onInvalid: SkipInvalidEvents, maxChunkEvents: 2, acknowledgedChunks: 3, acknowledgedEvents: 5,
		result: ImportResult{ImportID: fake.id, State: ImportCompleted}}
	session, err := sequence.ReattachImport(t.Context(), fake.id, fastImports, WithOnInvalid(RejectInvalidEvents))
	if err != nil {
		t.Fatal(err)
	}
	if session.ID() != fake.id || session.EventsAcknowledged() != 5 || session.config.onInvalid != SkipInvalidEvents {
		t.Fatalf("session %s acknowledged %d policy %d", session.ID(), session.EventsAcknowledged(), session.config.onInvalid)
	}
	if err := session.Add(t.Context(), ImportEntries(entries(3))); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Complete(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(fake.order, []int32{3, 4}) || fake.staged[3].firstEventIndex != 5 || fake.staged[4].firstEventIndex != 7 {
		t.Fatalf("staged order %v firsts %d %d", fake.order, fake.staged[3].firstEventIndex, fake.staged[4].firstEventIndex)
	}
	if !slices.Equal(fake.completed, [][2]int64{{5, 8}}) {
		t.Fatalf("CompleteImport counts = %v, want [[5 8]]", fake.completed)
	}
}

func TestReattachRefusesUnknownImportsAndOtherSequences(t *testing.T) {
	sequence, fake := importFixture(t)
	other, _ := NewImportID()
	fake.snapshot = nil
	_, err := sequence.ReattachImport(t.Context(), other)
	var refused *ImportCouldNotBeginError
	if !errors.As(err, &refused) || !errors.Is(err, ErrImportNotFound) {
		t.Fatalf("unknown import err = %v", err)
	}
	fake.snapshot = &importSnapshot{sequence: "outbox"}
	if _, err = sequence.ReattachImport(t.Context(), fake.id); !errors.As(err, &refused) || errors.Is(err, ErrImportNotFound) || !strings.Contains(err.Error(), "outbox") {
		t.Fatalf("other sequence err = %v", err)
	}
	if _, err = sequence.ReattachImport(t.Context(), ImportID{}); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatalf("zero ID err = %v", err)
	}
}

func TestImportReportsRejectionsAsAResult(t *testing.T) {
	sequence, fake := importFixture(t)
	fake.snapshot.result = ImportResult{ImportID: fake.id, State: ImportFailed, EventsStaged: 2, RejectedCount: 1, FailureReason: "invalid events",
		FirstSequenceNumber: events.Unavailable, LastSequenceNumber: events.Unavailable,
		Rejections: []ImportRejection{{Index: 1, EventType: "imported+1", Reasons: []string{"schema"}}}}
	result, err := sequence.Import(t.Context(), ImportEntries(entries(2)), fastImports)
	if err != nil {
		t.Fatal(err)
	}
	if result.IsSuccess() || result.State != ImportFailed || result.RejectedCount != 1 || len(result.Rejections) != 1 ||
		result.Rejections[0].Index != 1 || !slices.Equal(result.Rejections[0].Reasons, []string{"schema"}) || result.FailureReason != "invalid events" {
		t.Fatalf("result = %+v", result)
	}
	result.Rejections[0].Reasons[0] = "changed"
	if fake.snapshot.result.Rejections[0].Reasons[0] != "schema" {
		t.Fatal("result shares its rejections with the transport")
	}
}

func TestImportStopsOnARefusedChunk(t *testing.T) {
	sequence, fake := importFixture(t)
	fake.maxEvents = 1
	fake.add = func(_ context.Context, chunk importChunk) (importChunkAck, error) {
		if chunk.index == 1 {
			return importChunkAck{index: 1, err: "too large"}, nil
		}
		return importChunkAck{}, nil
	}
	result, err := sequence.Import(t.Context(), ImportEntries(entries(3)), fastImports, WithMaxChunksInFlight(1))
	var refused *ImportChunkRefusedError
	if !errors.As(err, &refused) || refused.ChunkIndex != 1 || refused.Reason != "too large" {
		t.Fatalf("err = %v", err)
	}
	if result.ImportID != fake.id || len(fake.completed) != 0 {
		t.Fatalf("result %+v, completed %v", result, fake.completed)
	}
}

func TestImportRetriesTransientFailuresOnly(t *testing.T) {
	sequence, fake := importFixture(t)
	attempts := 0
	fake.add = func(context.Context, importChunk) (importChunkAck, error) {
		attempts++
		if attempts < 3 {
			return importChunkAck{}, status.Error(codes.Unavailable, "down")
		}
		return importChunkAck{}, nil
	}
	if _, err := sequence.Import(t.Context(), ImportEntries(entries(1)), fastImports); err != nil || attempts != 3 {
		t.Fatalf("err = %v after %d attempts", err, attempts)
	}

	sequence, fake = importFixture(t)
	attempts = 0
	fake.add = func(context.Context, importChunk) (importChunkAck, error) {
		attempts++
		return importChunkAck{}, status.Error(codes.InvalidArgument, "bad")
	}
	if _, err := sequence.Import(t.Context(), ImportEntries(entries(1)), fastImports); status.Code(err) != codes.InvalidArgument || attempts != 1 {
		t.Fatalf("err = %v after %d attempts", err, attempts)
	}

	sequence, fake = importFixture(t)
	attempts = 0
	fake.add = func(context.Context, importChunk) (importChunkAck, error) {
		attempts++
		return importChunkAck{}, status.Error(codes.Unavailable, "down")
	}
	if _, err := sequence.Import(t.Context(), ImportEntries(entries(1)), fastImports, WithMaxChunkAttempts(2)); status.Code(err) != codes.Unavailable || attempts != 2 {
		t.Fatalf("err = %v after %d attempts", err, attempts)
	}
}

func TestImportReportsProgress(t *testing.T) {
	sequence, fake := importFixture(t)
	fake.updates = []importSnapshot{
		{eventsReceived: 2, result: ImportResult{ImportID: fake.id, State: ImportStaging, EventsStaged: 2}},
		{eventsReceived: 3, result: ImportResult{ImportID: fake.id, State: ImportCommitting, EventsStaged: 3, EventsCommitted: 1}},
		{eventsReceived: 3, result: ImportResult{ImportID: fake.id, State: ImportCompleted, EventsStaged: 3, EventsCommitted: 3}},
		{result: ImportResult{State: ImportFailed}},
	}
	fake.pending = 2
	var reported []ImportProgress
	progressed := make(chan struct{})
	result, err := sequence.Import(t.Context(), ImportEntries(entries(3)), fastImports, WithImportProgress(func(progress ImportProgress) {
		reported = append(reported, progress)
		if progress.State == ImportCompleted {
			close(progressed)
		}
	}))
	if err != nil || !result.IsSuccess() {
		t.Fatalf("result %+v err %v", result, err)
	}
	<-progressed
	states := make([]ImportState, len(reported))
	for i, progress := range reported {
		states[i] = progress.State
	}
	if !slices.Equal(states, []ImportState{ImportStaging, ImportCommitting, ImportCompleted}) || reported[1].EventsCommitted != 1 || reported[0].EventsReceived != 2 {
		t.Fatalf("progress = %+v", reported)
	}

	session, err := sequence.BeginImport(t.Context(), fastImports)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.ObserveProgress(t.Context(), func(ImportProgress) {}); err != nil {
		t.Fatalf("ObserveProgress ending at a terminal state = %v", err)
	}
	if progress, err := session.Progress(t.Context()); err != nil || progress.ImportID != fake.id {
		t.Fatalf("Progress = %+v, %v", progress, err)
	}
	if _, err := sequence.BeginImport(t.Context(), WithImportProgress(func(ImportProgress) {})); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatalf("BeginImport with progress err = %v", err)
	}
}

func TestImportStopsWhenTheContextIsCancelled(t *testing.T) {
	sequence, fake := importFixture(t)
	fake.maxEvents = 2
	ctx, cancel := context.WithCancel(t.Context())
	var read int
	endless := func(yield func(Entry, error) bool) {
		for {
			read++
			if read == 5 {
				cancel()
			}
			if !yield(Entry{Source: "customer", Event: imported{Name: "n"}}, nil) {
				return
			}
		}
	}
	result, err := sequence.Import(ctx, endless, fastImports)
	if !errors.Is(err, context.Canceled) || result.ImportID != fake.id || read != 5 || len(fake.completed) != 0 {
		t.Fatalf("err %v, read %d, completed %v", err, read, fake.completed)
	}

	sequence, fake = importFixture(t)
	ctx, cancel = context.WithCancel(t.Context())
	fake.add = func(ctx context.Context, _ importChunk) (importChunkAck, error) {
		cancel()
		<-ctx.Done()
		return importChunkAck{}, status.FromContextError(ctx.Err()).Err()
	}
	if _, err = sequence.Import(ctx, ImportEntries(entries(3)), fastImports); !errors.Is(err, context.Canceled) {
		t.Fatalf("in-flight cancellation err = %v", err)
	}
	if _, err = sequence.Import(ctx, ImportEntries(entries(1))); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled before begin err = %v", err)
	}
}

func TestImportWaitsForTheCommitUnlessToldNotTo(t *testing.T) {
	sequence, fake := importFixture(t)
	fake.pending = 3
	result, err := sequence.Import(t.Context(), ImportEntries(entries(1)), fastImports)
	if err != nil || result.State != ImportCompleted || fake.pending != 0 {
		t.Fatalf("result %+v err %v pending %d", result, err, fake.pending)
	}
	fake.pending = 3
	result, err = sequence.Import(t.Context(), ImportEntries(entries(1)), fastImports, WithoutWaitingForCommit())
	if err != nil || result.State != ImportCommitting || fake.pending != 2 {
		t.Fatalf("not waiting: result %+v err %v pending %d", result, err, fake.pending)
	}
}

func TestAbandonStopsAnAddInProgress(t *testing.T) {
	sequence, fake := importFixture(t)
	fake.maxEvents = 1
	session, err := sequence.BeginImport(t.Context(), fastImports)
	if err != nil {
		t.Fatal(err)
	}
	yielded, resume := make(chan struct{}), make(chan struct{})
	source := func(yield func(Entry, error) bool) {
		for {
			if !yield(Entry{Source: "customer", Event: imported{Name: "n"}}, nil) {
				return
			}
			yielded <- struct{}{}
			<-resume
		}
	}
	added := make(chan error, 1)
	go func() { added <- session.Add(t.Context(), source) }()
	<-yielded
	abandoned := make(chan ImportResult, 1)
	go func() {
		result, err := session.Abandon(t.Context())
		if err != nil {
			t.Error(err)
		}
		abandoned <- result
	}()
	for {
		session.mu.Lock()
		stopped := session.abandoned
		session.mu.Unlock()
		if stopped {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(resume)
	if err := <-added; !errors.Is(err, ErrImportAbandoned) {
		t.Fatalf("Add err = %v", err)
	}
	<-abandoned
	if fake.abandoned != 1 {
		t.Fatalf("AbandonImport calls = %d", fake.abandoned)
	}
	if err := session.Add(t.Context(), ImportEntries(entries(1))); !errors.Is(err, ErrImportAbandoned) {
		t.Fatalf("Add after Abandon err = %v", err)
	}
	if _, err := session.Complete(t.Context()); !errors.Is(err, ErrImportAbandoned) {
		t.Fatalf("Complete after Abandon err = %v", err)
	}
}

func TestImportCarriesWhatTheSourceSays(t *testing.T) {
	sequence, fake := importFixture(t)
	correlation, _ := metadata.NewCorrelationID()
	shared, _ := metadata.NewCorrelationID()
	occurred := time.Date(2019, 3, 4, 5, 6, 7, 0, time.FixedZone("", 2*60*60))
	subject := events.Subject("person")
	position := events.SequenceNumber(42)
	first := Entry{Source: "customer", Event: imported{Name: "a"}, Route: Route{SourceType: "crm", StreamType: "accounts", StreamID: "s"},
		Tags: []events.Tag{"legacy"}, NamedTags: []events.NamedTag{{Name: "region", Value: "eu"}}, Occurred: &occurred, Subject: &subject,
		Causation:     []metadata.Causation{{Occurred: occurred, Type: "Migration", Properties: map[string]string{"batch": "1"}}},
		CorrelationID: correlation, CausedBy: &identities.Identity{Subject: "u1", Name: "Ada"}, Generation: 2, SourceSequenceNumber: &position, IdempotencyKey: "evt-1"}
	second := Entry{Source: "customer", Event: imported{Name: "b"}}
	ctx := metadata.WithCorrelation(t.Context(), shared)
	if _, err := sequence.Import(ctx, ImportEntries([]Entry{first, second}), fastImports); err != nil {
		t.Fatal(err)
	}
	staged := fake.stagedEvents()
	if len(staged) != 2 {
		t.Fatalf("staged %d events", len(staged))
	}
	a, b := staged[0], staged[1]
	if a.sourceID != "customer" || a.sourceType != "crm" || a.streamType != "accounts" || a.streamID != "s" || a.subject != "person" ||
		a.correlation != correlation || a.causedBy.GetName() != "Ada" || a.eventType.GetId() != "imported" || a.eventType.GetGeneration() != 2 ||
		a.sourceSequenceNumber == nil || *a.sourceSequenceNumber != 42 || a.idempotencyKey != "evt-1" || a.occurred != "2019-03-04T05:06:07.0000000+02:00" ||
		!slices.Contains(a.tags, "legacy") || len(a.namedTags) != 1 || a.namedTags[0].GetValue() != "eu" ||
		len(a.causation) != 1 || a.causation[0].GetProperties()["batch"] != "1" || !strings.Contains(a.content, `"a"`) {
		t.Fatalf("first event = %+v", a)
	}
	if b.correlation != shared || b.sourceSequenceNumber != nil || b.idempotencyKey != "" || b.eventType.GetGeneration() != 1 || b.subject != "customer" || b.occurred != "" {
		t.Fatalf("second event = %+v", b)
	}
}

func TestImportRejectsInvalidEntriesBeforeSendingThem(t *testing.T) {
	reserved := events.Unavailable
	cases := map[string]Entry{
		"blank source":    {Event: imported{}},
		"unregistered":    {Source: "c", Event: struct{}{}},
		"blank key":       {Source: "c", Event: imported{}, IdempotencyKey: " "},
		"reserved number": {Source: "c", Event: imported{}, SourceSequenceNumber: &reserved},
		"blank named tag": {Source: "c", Event: imported{}, NamedTags: []events.NamedTag{{Value: "v"}}},
	}
	for name, entry := range cases {
		t.Run(name, func(t *testing.T) {
			sequence, fake := importFixture(t)
			_, err := sequence.Import(t.Context(), ImportEntries([]Entry{entry}), fastImports)
			if !errors.Is(err, faults.ErrInvalidConfiguration) && !errors.Is(err, faults.ErrNotRegistered) {
				t.Fatalf("err = %v", err)
			}
			if len(fake.staged) != 0 || len(fake.completed) != 0 {
				t.Fatal("an invalid entry reached the kernel")
			}
		})
	}
	sequence, _ := importFixture(t)
	failure := errors.New("source broke")
	broken := iter.Seq2[Entry, error](func(yield func(Entry, error) bool) { yield(Entry{}, failure) })
	if _, err := sequence.Import(t.Context(), broken, fastImports); !errors.Is(err, failure) {
		t.Fatalf("iterator error = %v", err)
	}
	if _, err := sequence.Import(t.Context(), nil); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatalf("nil iterator err = %v", err)
	}
}

func TestAppendsRejectImportOnlyFields(t *testing.T) {
	sequence, _ := importFixture(t)
	key := IdempotencyKey("k")
	for _, entry := range []Entry{{Source: "c", Event: imported{}, IdempotencyKey: key}, {Source: "c", Event: imported{}, Generation: 2}} {
		if _, err := sequence.AppendBatch(t.Context(), []Entry{entry}); !errors.Is(err, faults.ErrInvalidConfiguration) {
			t.Fatalf("AppendBatch err = %v", err)
		}
	}
}

func TestImportOptionsAreValidated(t *testing.T) {
	sequence, fake := importFixture(t)
	for name, option := range map[string]ImportOption{
		"nil": nil, "zero in flight": WithMaxChunksInFlight(0), "zero events": WithMaxEventsPerChunk(0), "zero attempts": WithMaxChunkAttempts(0),
		"zero id": WithImportID(ImportID{}), "unknown policy": WithOnInvalid(7), "nil progress": WithImportProgress(nil),
	} {
		if _, err := sequence.BeginImport(t.Context(), option); !errors.Is(err, faults.ErrInvalidConfiguration) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	if len(fake.beganWith) != 0 {
		t.Fatal("an invalid option reached the kernel")
	}
	id, _ := NewImportID()
	session, err := sequence.BeginImport(t.Context(), WithImportID(id), WithOnInvalid(SkipInvalidEvents))
	if err != nil || session.ID() != id || !slices.Equal(fake.beganWith, []OnInvalidEvent{SkipInvalidEvents}) {
		t.Fatalf("session %v err %v policies %v", session, err, fake.beganWith)
	}
	fake.beginErr = status.Error(codes.Unimplemented, "no imports")
	if _, err := sequence.BeginImport(t.Context()); !errors.Is(err, faults.ErrUnsupported) {
		t.Fatalf("Unimplemented err = %v", err)
	}
}

func TestImportIDUsesTheCanonicalUUIDText(t *testing.T) {
	const text = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	id, err := ParseImportID(text)
	if err != nil || id.String() != text {
		t.Fatalf("ParseImportID = %s, %v", id, err)
	}
	encoded, err := id.MarshalText()
	var decoded ImportID
	if err != nil || string(encoded) != text || decoded.UnmarshalText(encoded) != nil || decoded != id {
		t.Fatalf("text round trip %q -> %s", encoded, decoded)
	}
	if decoded.UnmarshalText([]byte("nope")) == nil || !(ImportID{}).IsZero() {
		t.Fatal("invalid text or zero handling")
	}
	if ImportCommitting.String() != "Committing" || ImportState(9).String() != "ImportState(9)" || !ImportAbandoned.IsTerminal() || ImportStaging.IsTerminal() {
		t.Fatal("ImportState names or terminality")
	}
}
