//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

type HistoryDefaultSecret struct {
	ID   string
	Name string `json:"name" chronicle:"pii;set(HistorySecretChanged)"`
}
type HistoryPassiveDefaultSecret HistoryDefaultSecret
type HistoryPassiveLowerSecret HistorySecret
type HistorySecretAudit struct {
	ID    string
	Count int32 `chronicle:"count(HistorySecretChanged)"`
}

// Chronicle#4561 (fixed in 19.32.2): projection replay, history, sessions and
// immediate projections release each value with the subject it was written
// under. Every public SDK replay route is exercised for default Id and lowercase
// id, with a key that differs from the subject, before and after erasure.
func TestKernelModelHistoryProtectedReleaseProfile(t *testing.T) {
	f := newKernelFixture(t)
	r := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[HistorySecretChanged](r); err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[HistoryDefaultSecret](r)
	if err != nil {
		t.Fatal(err)
	}
	lower, err := chronicle.RegisterReadModel[HistorySecret](r)
	if err != nil {
		t.Fatal(err)
	}
	passiveDefault, err := chronicle.RegisterReadModel[HistoryPassiveDefaultSecret](r)
	if err != nil {
		t.Fatal(err)
	}
	passiveLower, err := chronicle.RegisterReadModel[HistoryPassiveLowerSecret](r)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.AddProjection(projections.ModelBound(passiveDefault, projections.Passive())); err != nil {
		t.Fatal(err)
	}
	if err = r.AddProjection(projections.ModelBound(passiveLower, projections.Passive())); err != nil {
		t.Fatal(err)
	}
	audit, err := chronicle.RegisterReadModel[HistorySecretAudit](r)
	if err != nil {
		t.Fatal(err)
	}
	store, err := f.client(r).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	reader := readmodels.For(store.ReadModels(), model)
	lowerReader := readmodels.For(store.ReadModels(), lower)
	// Exactly 256 decoded bytes resemble legacy RSA ciphertext. It is legitimate
	// application plaintext; a second decryption would corrupt it.
	plaintext := base64.StdEncoding.EncodeToString(make([]byte, 256))
	for _, source := range []events.SourceID{"owner", "different-source"} {
		appendSuccessfully(t, f.ctx, store, source, HistorySecretChanged{Name: plaintext}, eventsequences.WithSubject("owner"))
	}
	awaitHistoryCollection(t, f.ctx, reader, func(c readmodels.Collection[HistoryDefaultSecret]) bool {
		return len(c.Instances) == 2 && c.Instances[0].Value.Name == plaintext && c.Instances[1].Value.Name == plaintext
	})
	awaitHistoryCollection(t, f.ctx, lowerReader, func(c readmodels.Collection[HistorySecret]) bool {
		return len(c.Instances) == 2 && c.Instances[0].Value.Name == plaintext && c.Instances[1].Value.Name == plaintext
	})
	for _, erased := range []bool{false, true} {
		want := plaintext
		if erased {
			if err := store.Compliance().ErasePII(f.ctx, "owner"); err != nil {
				t.Fatal(err)
			}
			want = ""
		}
		state := map[bool]string{false: "before erasure", true: "after erasure"}[erased]
		for _, id := range []readmodels.Identifier{model.Identifier(), lower.Identifier()} {
			replayed, err := store.ReadModels().GetAll(f.ctx, id, new(events.Count(2)))
			if err != nil || len(replayed.Instances) != 2 {
				t.Fatalf("%s %s collection replay: %+v %v", state, id, replayed, err)
			}
			legacy, err := store.ReadModels().ReplayProjection(f.ctx, id, 2)
			if err != nil || len(legacy) != 2 {
				t.Fatalf("%s %s legacy replay: %v", state, id, err)
			}
			documents := append([]json.RawMessage{replayed.Instances[0].Value, replayed.Instances[1].Value}, legacy...)
			for _, document := range documents {
				var fields map[string]json.RawMessage
				var value struct {
					Name string `json:"name"`
				}
				if json.Unmarshal(document, &fields) != nil || json.Unmarshal(document, &value) != nil {
					t.Fatalf("%s %s replay document is not JSON", state, id)
				}
				if fields["__subject"] != nil || fields["__subjects"] != nil || value.Name != want {
					t.Fatalf("%s %s replay released %d bytes, want %d", state, id, len(value.Name), len(want))
				}
			}
		}
		for _, source := range []readmodels.Key{"owner", "different-source"} {
			keyed, err := reader.Get(f.ctx, source)
			if err != nil || !keyed.Exists || keyed.Value.Name != want {
				t.Fatal("materialized keyed lineage or second decryption", err)
			}
			typed, err := reader.GetAll(f.ctx, new(events.Count(2)))
			if err != nil || len(typed.Instances) != 2 || typed.Instances[0].Value.Name != want || typed.Instances[1].Value.Name != want {
				t.Fatalf("%s typed default replay: %+v %v", state, typed, err)
			}
			for _, history := range [][]readmodels.Snapshot[json.RawMessage]{
				mustSnapshots(t, f, store, model.Identifier(), source),
				mustSnapshots(t, f, store, lower.Identifier(), source),
			} {
				var value struct {
					Name string `json:"name"`
				}
				if len(history) != 1 || json.Unmarshal(history[0].Instance, &value) != nil || value.Name != want {
					t.Fatalf("%s history for %s released %d bytes, want %d", state, source, len(value.Name), len(want))
				}
				e, err := events.Decode[HistorySecretChanged](store.EventTypes(), history[0].Events[0])
				if err != nil || e.Name != want || history[0].Events[0].Context.Subject != "owner" {
					t.Fatal("history contribution release", err)
				}
			}
			session, err := reader.NewSession(source)
			if err != nil {
				t.Fatal(err)
			}
			hydrated, err := session.Get(f.ctx)
			if closeErr := session.Close(f.ctx); closeErr != nil {
				t.Fatal(closeErr)
			}
			if err != nil || !hydrated.Exists || hydrated.Value.Name != want {
				t.Fatalf("%s session for %s: %v", state, source, err)
			}
			lowerSession, err := lowerReader.NewSession(source)
			if err != nil {
				t.Fatal(err)
			}
			lowerHydrated, err := lowerSession.Get(f.ctx)
			if closeErr := lowerSession.Close(f.ctx); closeErr != nil {
				t.Fatal(closeErr)
			}
			if err != nil || !lowerHydrated.Exists || lowerHydrated.Value.Name != want {
				t.Fatalf("%s lowercase session for %s: %v", state, source, err)
			}
			immediate, err := readmodels.For(store.ReadModels(), passiveDefault).Get(f.ctx, source)
			if err != nil || !immediate.Exists || immediate.Value.Name != want {
				t.Fatalf("%s immediate default for %s: %+v %v", state, source, immediate, err)
			}
			lowerImmediate, err := readmodels.For(store.ReadModels(), passiveLower).Get(f.ctx, source)
			if err != nil || !lowerImmediate.Exists || lowerImmediate.Value.Name != want {
				t.Fatalf("%s immediate lowercase for %s: %+v %v", state, source, lowerImmediate, err)
			}
			// The model has no protected fields; contributions are released with
			// their own event subject and schema.
			contributions, err := readmodels.For(store.ReadModels(), audit).GetSnapshots(f.ctx, source)
			if err != nil || len(contributions) != 1 || len(contributions[0].Events) != 1 {
				t.Fatal("contribution history", err)
			}
		}
		passiveAll, err := readmodels.For(store.ReadModels(), passiveDefault).GetAll(f.ctx, nil)
		if err != nil || len(passiveAll.Instances) != 2 || passiveAll.Instances[0].Value.Name != want || passiveAll.Instances[1].Value.Name != want {
			t.Fatalf("%s passive default collection: %+v %v", state, passiveAll, err)
		}
		passiveLowerAll, err := readmodels.For(store.ReadModels(), passiveLower).GetAll(f.ctx, nil)
		if err != nil || len(passiveLowerAll.Instances) != 2 || passiveLowerAll.Instances[0].Value.Name != want || passiveLowerAll.Instances[1].Value.Name != want {
			t.Fatalf("%s passive lowercase collection: %+v %v", state, passiveLowerAll, err)
		}
	}
}

func mustSnapshots(t *testing.T, f *kernelFixture, store *chronicle.EventStore, model readmodels.Identifier, key readmodels.Key) []readmodels.Snapshot[json.RawMessage] {
	t.Helper()
	history, err := store.ReadModels().GetSnapshots(f.ctx, model, key)
	if err != nil {
		t.Fatalf("history %s/%s: %v", model, key, err)
	}
	return history
}
