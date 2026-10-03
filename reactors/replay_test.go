// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reactors_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/reactors"
)

type ReplayArtifact struct{ calls *[]string }

func (r *ReplayArtifact) Live(Ordered)     { *r.calls = append(*r.calls, "live") }
func (r *ReplayArtifact) Rebuild(Ordered)  { *r.calls = append(*r.calls, "rebuild") }
func (r *ReplayArtifact) ReplayOnly(Other) { *r.calls = append(*r.calls, "only") }

func TestReplayMapsAndOnceOnlyPolicies(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options []reactors.Option
		want    []string
	}{
		{"replacement", []reactors.Option{reactors.Replay("Rebuild", "ReplayOnly")}, []string{"live", "rebuild", "only", "live"}},
		{"once live does not suppress replacement", []reactors.Option{reactors.Replay("Rebuild", "ReplayOnly"), reactors.OnceOnly("Live")}, []string{"live", "rebuild", "only", "live"}},
		{"once replacement does not fall back to live", []reactors.Option{reactors.Replay("Rebuild", "ReplayOnly"), reactors.OnceOnly("Rebuild")}, []string{"live", "only", "live"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls []string
			d, err := reactors.Define[*ReplayArtifact](func() *ReplayArtifact { return &ReplayArtifact{&calls} }, tc.options...)
			if err != nil {
				t.Fatal(err)
			}
			p := compile(t, d)
			if len(p.EventTypes()) != 2 {
				t.Fatal(p.EventTypes())
			}
			lease, err := p.Activate(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			for _, delivery := range []struct {
				event any
				id    events.TypeID
				state events.ObservationState
			}{
				{&Ordered{}, "Ordered", events.ObservationInitial},
				{&Other{}, "Other", events.ObservationInitial},
				{&Ordered{}, "Ordered", events.ObservationReplay | events.ObservationInitial},
				{&Other{}, "Other", events.ObservationReplay},
				{&Ordered{}, "Ordered", events.ObservationInitial}, // recovery still invokes
			} {
				if err := lease.Invoke(t.Context(), delivery.event, events.Context{EventType: events.TypeRef{ID: delivery.id}, ObservationState: delivery.state}, &testRuntime{}); err != nil {
					t.Fatal(err)
				}
			}
			if err := lease.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(calls, tc.want) {
				t.Fatalf("calls %v want %v", calls, tc.want)
			}
		})
	}
}

func TestReplayTypedCallbacksFallbackSkipAndDuplicates(t *testing.T) {
	for _, once := range []bool{false, true} {
		calls := 0
		h := reactors.On(func(context.Context, Ordered) error { calls++; return nil })
		if once {
			h = h.OnceOnly()
		}
		d, err := reactors.DefineHandlers("typed", []reactors.Handler{h}, reactors.OnceOnly())
		if err != nil {
			t.Fatal(err)
		}
		p := compile(t, d)
		if p.IsReplayable() {
			t.Fatal("reactor OnceOnly not reflected in plan")
		}
		lease, err := p.Activate(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		for _, state := range []events.ObservationState{events.ObservationInitial, events.ObservationReplay, events.ObservationInitial} {
			if err := lease.Invoke(t.Context(), &Ordered{}, events.Context{EventType: events.TypeRef{ID: "Ordered"}, ObservationState: state}, &testRuntime{}); err != nil {
				t.Fatal(err)
			}
		}
		if err := lease.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
		want := 3
		if once {
			want = 2
		}
		if calls != want {
			t.Fatalf("calls %d want %d", calls, want)
		}
	}
	h := reactors.On(func(context.Context, Ordered) error { return nil })
	c, m := catalogs(t)
	for _, handlers := range [][]reactors.Handler{{h, h}, {h.DuringReplay(), h.DuringReplay()}} {
		d, err := reactors.DefineHandlers("duplicate", handlers)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = reactors.Compile(d, c, m, nil); err == nil {
			t.Fatal("duplicate policy binding admitted")
		}
	}
	d, err := reactors.DefineHandlers("separate", []reactors.Handler{h, h.DuringReplay()})
	if err != nil {
		t.Fatal(err)
	}
	compile(t, d)
}

func TestReplaySelectorsMustIdentifyDiscoveredMethods(t *testing.T) {
	c, m := catalogs(t)
	for _, option := range []reactors.Option{reactors.Replay("unknown"), reactors.OnceOnly(""), reactors.Replay("hidden")} {
		d, err := reactors.Define[*Handlers](func() *Handlers { return nil }, option)
		if err != nil {
			t.Fatal(err)
		}
		_, err = reactors.Compile(d, c, m, nil)
		var detail *reactors.DeclarationError
		if !errors.As(err, &detail) {
			t.Fatalf("expected declaration error, got %v", err)
		}
	}
	if _, err := reactors.Define[*Handlers](func() *Handlers { return nil }, reactors.Replay()); err == nil {
		t.Fatal("empty replay selector accepted")
	}
}

func TestReplayNamePrefixHasNoSemantics(t *testing.T) {
	var calls []string
	d, err := reactors.Define[*ReplayArtifact](func() *ReplayArtifact { return &ReplayArtifact{&calls} })
	if err != nil {
		t.Fatal(err)
	}
	if err = invoke(t, compile(t, d), &Other{}, "Other", &testRuntime{}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(calls, []string{"only"}) {
		t.Fatal(calls)
	}
}
