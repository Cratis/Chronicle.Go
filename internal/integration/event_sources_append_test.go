//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
)

type hostedSourceAppendOutcome struct {
	disposition eventsequences.Disposition
	checked     bool
	diagnostics []eventsequences.AppendError
	err         error
}

func hostedSingleSourceOutcome(result eventsequences.AppendResult, err error) hostedSourceAppendOutcome {
	return hostedSourceAppendOutcome{result.Disposition, result.ConcurrencyCheckPerformed, result.Errors, errors.Join(err, result.Err())}
}

func hostedBatchSourceOutcome(result eventsequences.BatchResult, err error) hostedSourceAppendOutcome {
	return hostedSourceAppendOutcome{result.Disposition, result.ConcurrencyCheckPerformed, result.Errors, errors.Join(err, result.Err())}
}

func hostedSourceEntry(source events.SourceID, value string) eventsequences.Entry {
	return eventsequences.Entry{Source: source, Event: HostedSourceEvent{value}, EventSource: "Billing", Route: eventsequences.Route{StreamType: "changes", StreamID: "west"}}
}

func appendHostedSourceVariant(ctx context.Context, sequence *eventsequences.Sequence, entry eventsequences.Entry, variant string) hostedSourceAppendOutcome {
	options := []eventsequences.AppendOption{eventsequences.WithEventSource(entry.EventSource), eventsequences.WithRoute(entry.Route)}
	if variant == "single named" || variant == "many named" {
		options = append(options, eventsequences.WithNamedTags(events.NamedTag{Name: "origin", Value: "hosted"}))
	}
	switch variant {
	case "single", "single named":
		return hostedSingleSourceOutcome(sequence.Append(ctx, entry.Source, entry.Event, options...))
	case "many", "many named":
		return hostedBatchSourceOutcome(sequence.AppendMany(ctx, entry.Source, []any{entry.Event, entry.Event}, options...))
	default:
		return appendHostedSourceEntries(ctx, sequence, entry, variant)
	}
}

func appendHostedSourceEntries(ctx context.Context, sequence *eventsequences.Sequence, entry eventsequences.Entry, variant string) hostedSourceAppendOutcome {
	entries := []eventsequences.Entry{entry}
	switch variant {
	case "batch named":
		return hostedBatchSourceOutcome(sequence.AppendBatch(ctx, entries, eventsequences.WithBatchNamedTags(events.NamedTag{Name: "origin", Value: "hosted"})))
	case "prepared":
		prepared, err := sequence.PrepareBatch(ctx, entries)
		if err != nil {
			return hostedSourceAppendOutcome{err: err}
		}
		return hostedBatchSourceOutcome(sequence.AppendPreparedBatch(ctx, prepared))
	default:
		return hostedBatchSourceOutcome(sequence.AppendBatch(ctx, entries))
	}
}

func assertHostedSourceCommitted(t *testing.T, outcome hostedSourceAppendOutcome) {
	t.Helper()
	if outcome.err != nil {
		t.Fatal(outcome.err)
	}
	if outcome.disposition != eventsequences.Committed {
		t.Fatalf("append = %+v", outcome)
	}
}

func readHostedSource(t *testing.T, f *kernelFixture, sequence *eventsequences.Sequence, source events.SourceID) []events.Appended {
	t.Helper()
	result, err := sequence.ReadSource(f.ctx, source, eventsequences.SourceFilter{SourceType: "Billing", StreamType: "changes", StreamID: "west"})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func assertHostedSourceContext(t *testing.T, context events.Context) {
	t.Helper()
	for field, values := range map[string][2]string{
		"source name": {string(context.EventSource), "Billing"}, "source type": {string(context.SourceType), "Billing"},
		"stream type": {string(context.StreamType), "changes"}, "stream ID": {string(context.StreamID), "west"},
	} {
		if values[0] != values[1] {
			t.Errorf("%s = %q, want %q", field, values[0], values[1])
		}
	}
}

func TestKernelNamedEventSourceAppendShapesAndAutomaticConcurrency(t *testing.T) {
	f, store := newHostedSourceStore(t)
	registerHostedSource(t, f, store)
	sequence := store.EventLog()
	for _, variant := range []string{"single", "single named", "many", "many named", "batch", "batch named", "prepared"} {
		t.Run(variant, func(t *testing.T) {
			entry := hostedSourceEntry(events.SourceID(strings.ReplaceAll(variant, " ", "-")), variant)
			assertHostedSourceCommitted(t, appendHostedSourceVariant(f.ctx, sequence, entry, "single"))
			outcome := appendHostedSourceVariant(f.ctx, sequence, entry, variant)
			assertHostedSourceCommitted(t, outcome)
			// The persisted type is Billing, not the outgoing route's Default.
			// A wrong automatic tail filter would resolve empty and leave this unchecked.
			if !outcome.checked {
				t.Fatal("second named append did not check the existing scope")
			}
			appended := readHostedSource(t, f, sequence, entry.Source)
			want := map[string]int{"many": 3, "many named": 3}[variant]
			if want == 0 {
				want = 2
			}
			if len(appended) != want {
				t.Fatalf("appended = %d, want %d", len(appended), want)
			}
			for _, event := range appended {
				assertHostedSourceContext(t, event.Context)
			}
		})
	}
}

func TestKernelNamedEventSourceExplicitStaleScopeIsNotRewritten(t *testing.T) {
	f, store := newHostedSourceStore(t)
	registerHostedSource(t, f, store)
	sequence := store.EventLog()
	entry := hostedSourceEntry("stale-source", "first")
	assertHostedSourceCommitted(t, appendHostedSourceVariant(f.ctx, sequence, entry, "single"))
	first := readHostedSource(t, f, sequence, entry.Source)
	if len(first) != 1 {
		t.Fatal(first)
	}
	sourceType, streamType, streamID := events.SourceType("Billing"), events.StreamType("changes"), events.StreamID("west")
	scope := eventsequences.Scope{Filter: eventsequences.ScopeFilter{SourceID: &entry.Source, SourceType: &sourceType, StreamType: &streamType, StreamID: &streamID}, Expectation: eventsequences.Exact(first[0].Context.SequenceNumber)}
	assertHostedSourceCommitted(t, appendHostedSourceVariant(f.ctx, sequence, entry, "single"))
	result, err := sequence.Append(f.ctx, entry.Source, entry.Event, eventsequences.WithEventSource(entry.EventSource), eventsequences.WithRoute(entry.Route), eventsequences.WithScope(scope))
	if err != nil {
		t.Fatal(err)
	}
	var conflict *eventsequences.ConcurrencyError
	if !errors.As(result.Err(), &conflict) {
		t.Fatalf("stale scope error = %v", result.Err())
	}
	if result.Disposition != eventsequences.Rejected || len(result.ConcurrencyViolations) != 1 {
		t.Fatal(result)
	}
	if after := readHostedSource(t, f, sequence, entry.Source); len(after) != 2 {
		t.Fatal("stale append changed history", after)
	}
}

func invalidHostedSourceEntries() map[string]eventsequences.Entry {
	unknown := hostedSourceEntry("unknown-source", "refused")
	unknown.EventSource = "Missing"
	mismatch := hostedSourceEntry("mismatched-type", "refused")
	mismatch.Route.SourceType = "Other"
	stream := hostedSourceEntry("unknown-stream", "refused")
	stream.Route.StreamType = "Missing"
	return map[string]eventsequences.Entry{"unregistered name": unknown, "mismatched source type": mismatch, "unregistered stream": stream}
}

func assertHostedSourceRefused(t *testing.T, outcome hostedSourceAppendOutcome) {
	t.Helper()
	if outcome.disposition == eventsequences.Committed {
		t.Fatal("invalid named source committed")
	}
	if len(outcome.diagnostics) == 0 {
		t.Fatalf("missing source diagnostics: %+v", outcome)
	}
	if outcome.err == nil {
		t.Fatalf("rejection was not inspectable: %+v", outcome)
	}
	// Errors-only replies intentionally retain the SDK's existing Unknown disposition.
	t.Logf("source rejection: disposition=%v diagnostics=%v error=%v", outcome.disposition, outcome.diagnostics, outcome.err)
}

func TestKernelNamedEventSourceRejectsInvalidAppendRoutes(t *testing.T) {
	f, store := newHostedSourceStore(t)
	registerHostedSource(t, f, store)
	for name, entry := range invalidHostedSourceEntries() {
		for _, variant := range []string{"single", "many named", "batch", "prepared"} {
			t.Run(name+"/"+variant, func(t *testing.T) {
				assertHostedSourceRefused(t, appendHostedSourceVariant(f.ctx, store.EventLog(), entry, variant))
				appended, err := store.EventLog().ReadSource(f.ctx, entry.Source, eventsequences.SourceFilter{})
				if err != nil {
					t.Fatal(err)
				}
				if len(appended) != 0 {
					t.Fatal("refused append left events", appended)
				}
			})
		}
	}
}
