// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observerruntime

import (
	"errors"
	"reflect"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
)

func TestEventContextRejectsInvalidEnvelope(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*contracts.EventContext) *contracts.EventContext
	}{
		{"nil context", func(*contracts.EventContext) *contracts.EventContext { return nil }},
		{"nil type", func(c *contracts.EventContext) *contracts.EventContext { c.EventType = nil; return c }},
		{"empty type", func(c *contracts.EventContext) *contracts.EventContext { c.EventType.Id = ""; return c }},
		{"zero generation", func(c *contracts.EventContext) *contracts.EventContext { c.EventType.Generation = 0; return c }},
		{"reserved sequence", func(c *contracts.EventContext) *contracts.EventContext {
			c.SequenceNumber = uint64(events.Unavailable - 2)
			return c
		}},
		{"empty source", func(c *contracts.EventContext) *contracts.EventContext { c.EventSourceId = ""; return c }},
		{"wrong store", func(c *contracts.EventContext) *contracts.EventContext { c.EventStore = "other"; return c }},
		{"wrong namespace", func(c *contracts.EventContext) *contracts.EventContext { c.Namespace = "other"; return c }},
		{"missing time", func(c *contracts.EventContext) *contracts.EventContext { c.Occurred = nil; return c }},
		{"invalid time", func(c *contracts.EventContext) *contracts.EventContext { c.Occurred.Value = "invalid"; return c }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.mutate(appended("PageInput", "source", 1).Context)
			ec, err := eventContext(c, "store", "Default", "sequence")
			if !errors.Is(err, faults.ErrProtocol) || !reflect.DeepEqual(ec, events.Context{}) {
				t.Fatalf("context = %+v, error = %v", ec, err)
			}
		})
	}
}

func contextWithMetadata() *contracts.EventContext {
	c := appended("PageInput", "source", 1).Context
	c.EventStore, c.Namespace = "", ""
	c.Causation = []*contracts.Causation{{Occurred: c.Occurred, Type: "first", Properties: map[string]string{"key": "value"}}}
	c.Tags = []string{"one", "two"}
	c.NamedTags = []*contracts.NamedTag{{Name: "name", Value: "value"}}
	return c
}

func TestEventContextCopiesMetadataAndDefaultsSubject(t *testing.T) {
	c := contextWithMetadata()
	ec, err := eventContext(c, "store", "Default", "sequence")
	if err != nil {
		t.Fatal(err)
	}
	assertContextLocation(t, ec)
	assertContextCausation(t, ec)
	c.Causation[0].Properties["key"] = "changed"
	c.Tags[0], c.NamedTags[0].Value = "changed", "changed"
	assertContextCausation(t, ec)
	assertContextTags(t, ec, []events.Tag{"one", "two"}, []events.NamedTag{{Name: "name", Value: "value"}})
}

func assertContextLocation(t *testing.T, ec events.Context) {
	t.Helper()
	if ec.Store != "store" || ec.Namespace != "Default" {
		t.Fatalf("location = %+v", ec)
	}
	if ec.Sequence != "sequence" || ec.Subject != "source" {
		t.Fatalf("sequence and subject = %+v", ec)
	}
}

func assertContextCausation(t *testing.T, ec events.Context) {
	t.Helper()
	if len(ec.Causation) != 1 {
		t.Fatalf("causation = %+v", ec.Causation)
	}
	if ec.Causation[0].Type != "first" || ec.Causation[0].Properties["key"] != "value" {
		t.Fatalf("causation = %+v", ec.Causation)
	}
}

func assertContextTags(t *testing.T, ec events.Context, tags []events.Tag, named []events.NamedTag) {
	t.Helper()
	if !reflect.DeepEqual(ec.Tags, tags) || !reflect.DeepEqual(ec.NamedTags, named) {
		t.Fatalf("tags = %v, named tags = %v", ec.Tags, ec.NamedTags)
	}
}

func invalidContextMetadata(name string, c *contracts.EventContext) ([]events.Tag, []events.NamedTag) {
	switch name {
	case "nil cause":
		c.Causation = append(c.Causation, nil)
	case "invalid cause time":
		c.Causation = append(c.Causation, &contracts.Causation{})
	case "nil named tag":
		c.NamedTags = append(c.NamedTags, nil)
		return []events.Tag{"one", "two"}, []events.NamedTag{{Name: "name", Value: "value"}}
	}
	return nil, nil
}

func TestEventContextKeepsMetadataDecodedBeforeFailure(t *testing.T) {
	for _, name := range []string{"nil cause", "invalid cause time", "nil named tag"} {
		t.Run(name, func(t *testing.T) {
			c := contextWithMetadata()
			wantTags, wantNamed := invalidContextMetadata(name, c)
			ec, err := eventContext(c, "store", "Default", "sequence")
			if !errors.Is(err, faults.ErrProtocol) {
				t.Fatalf("error = %v", err)
			}
			assertContextLocation(t, ec)
			assertContextCausation(t, ec)
			assertContextTags(t, ec, wantTags, wantNamed)
		})
	}
}

func TestDecodePartitionKeepsEventsBeforeProtocolFailure(t *testing.T) {
	for _, invalid := range []*contracts.AppendedEvent{nil, {}, appended("PageInput", "source", 2)} {
		r := pageReactor(t, func() *PerEventReactor { return &PerEventReactor{&pageRecorder{}} })
		decoded := r.decodePartition("source", []*contracts.AppendedEvent{appended("PageInput", "source", 2), invalid})
		if !errors.Is(decoded.Failed, faults.ErrProtocol) {
			t.Fatalf("error = %v", decoded.Failed)
		}
		if decoded.FailedAt != 3 || len(decoded.Events) != 1 {
			t.Fatalf("partition = %+v", decoded)
		}
	}
}
