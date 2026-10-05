//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/google/uuid"
)

type HistoryDefaultSecret struct {
	ID   string
	Name string `json:"name" chronicle:"pii;set(HistorySecretChanged)"`
}
type HistorySecretAudit struct {
	ID    string
	Count int32 `chronicle:"count(HistorySecretChanged)"`
}

// This is a characterization of 19.29.4, not a promise that all server reads are
// safe. Raw RPCs below deliberately bypass SDK refusal to witness its reason.
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
	audit, err := chronicle.RegisterReadModel[HistorySecretAudit](r)
	if err != nil {
		t.Fatal(err)
	}
	store, err := f.client(r).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	reader := readmodels.For(store.ReadModels(), model)
	// Exactly 256 decoded bytes resemble legacy RSA ciphertext. It is legitimate
	// application plaintext; applying protection must encrypt it once more.
	plaintext := base64.StdEncoding.EncodeToString(make([]byte, 256))
	for _, source := range []events.SourceID{"owner", "different-source"} {
		appendSuccessfully(t, f.ctx, store, source, HistorySecretChanged{Name: plaintext}, eventsequences.WithSubject("owner"))
	}
	awaitHistoryCollection(t, f.ctx, reader, func(c readmodels.Collection[HistoryDefaultSecret]) bool {
		return len(c.Instances) == 2 && c.Instances[0].Value.Name == plaintext && c.Instances[1].Value.Name == plaintext
	})
	awaitHistoryCollection(t, f.ctx, readmodels.For(store.ReadModels(), lower), func(c readmodels.Collection[HistorySecret]) bool {
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
		for _, source := range []readmodels.Key{"owner", "different-source"} {
			keyed, err := reader.Get(f.ctx, source)
			if err != nil || !keyed.Exists || keyed.Value.Name != want {
				t.Fatal("materialized keyed lineage or second decryption", err)
			}
			for _, id := range []readmodels.Identifier{model.Identifier(), lower.Identifier()} {
				if v, err := store.ReadModels().GetAll(f.ctx, id, new(events.Count(2))); v.Instances != nil || !errors.Is(err, chronicle.ErrUnsupported) {
					t.Fatal("protected collection replay admitted", err)
				}
				if v, err := store.ReadModels().GetSnapshots(f.ctx, id, source); v != nil || !errors.Is(err, chronicle.ErrUnsupported) {
					t.Fatal("protected history admitted", err)
				}
				if v, err := store.ReadModels().ReplayProjection(f.ctx, id, 2); v != nil || !errors.Is(err, chronicle.ErrUnsupported) {
					t.Fatal("legacy protected replay admitted", err)
				}
			}
			if session, err := reader.NewSession(source); session != nil || !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatal("protected session admitted", err)
			}
			// Independent public contribution route: the model has no protected
			// fields, but history events are released with their own subject/schema.
			history, err := readmodels.For(store.ReadModels(), audit).GetSnapshots(f.ctx, source)
			if err != nil || len(history) != 1 || len(history[0].Events) != 1 {
				t.Fatal("contribution history", err)
			}
			e, err := events.Decode[HistorySecretChanged](store.EventTypes(), history[0].Events[0])
			if err != nil || e.Name != want || history[0].Events[0].Context.Subject != "owner" {
				t.Fatal("event release used model/source instead of event subject", err)
			}
		}
		// Default Id is not a release-subject alias. Raw collection replay still
		// returns encrypted strings after erasure, and includes no reliable lineage.
		raw, err := contracts.NewReadModelsClient(f.conn).GetAllInstances(f.ctx, &contracts.GetAllInstancesRequest{EventStore: string(f.storeName), Namespace: string(store.Namespace()), ReadModelIdentifier: string(model.Identifier()), EventSequenceId: "event-log", EventCount: 2})
		if err != nil || len(raw.Instances) != 2 {
			t.Fatal("raw default replay witness", err)
		}
		for _, document := range raw.Instances {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal([]byte(document), &fields); err != nil {
				t.Fatal(err)
			}
			if fields["Id"] == nil || fields["__subjects"] != nil || string(fields["name"]) == `""` || string(fields["name"]) == `"`+plaintext+`"` {
				t.Fatal("default Id/lineage defect witness changed")
			}
			// Supplying the actual event owner to the explicit unreleased API
			// proves these were encrypted values, without a ciphertext heuristic.
			fields["__subject"] = json.RawMessage(`"owner"`)
			unreleased, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			released, err := store.ReadModels().Release(f.ctx, model.Identifier(), unreleased)
			if err != nil {
				t.Fatal(err)
			}
			var value HistoryDefaultSecret
			if err := json.Unmarshal(released, &value); err != nil || value.Name != want {
				t.Fatal("default replay release witness", err)
			}
		}
		lowerRaw, err := contracts.NewReadModelsClient(f.conn).GetAllInstances(f.ctx, &contracts.GetAllInstancesRequest{EventStore: string(f.storeName), Namespace: string(store.Namespace()), ReadModelIdentifier: string(lower.Identifier()), EventSequenceId: "event-log", EventCount: 2})
		if err != nil || len(lowerRaw.Instances) != 2 {
			t.Fatal("raw lowercase replay witness", err)
		}
		for _, document := range lowerRaw.Instances {
			var value HistorySecret
			if err := json.Unmarshal([]byte(document), &value); err != nil {
				t.Fatal(err)
			}
			expected := want
			if value.ID == "different-source" {
				expected = ""
			}
			if value.Name != expected {
				t.Fatal("lowercase replay source-subject witness changed")
			}
		}
		// Keyed immediate/session replay substitutes the source for the subject.
		// Same-subject sessions release once, preserving cipher-shaped plaintext;
		// source != subject loses the value even before erasure. SDK refuses both.
		for _, source := range []string{"owner", "different-source"} {
			id := uuid.NewString()
			request := &contracts.GetInstanceByKeyRequest{EventStore: string(f.storeName), Namespace: string(store.Namespace()), ReadModelIdentifier: string(model.Identifier()), EventSequenceId: "event-log", ReadModelKey: source, SessionId: id}
			raw, err := contracts.NewReadModelsClient(f.conn).GetInstanceByKey(f.ctx, request)
			if err != nil {
				t.Fatal("raw session witness", err)
			}
			_, cleanup := contracts.NewReadModelsClient(f.conn).DehydrateSession(f.ctx, &contracts.DehydrateSessionRequest{EventStore: request.EventStore, Namespace: request.Namespace, ReadModelIdentifier: request.ReadModelIdentifier, EventSequenceId: request.EventSequenceId, ReadModelKey: source, SessionId: id})
			if cleanup != nil {
				t.Fatal(cleanup)
			}
			var value HistoryDefaultSecret
			if err := json.Unmarshal([]byte(raw.ReadModel), &value); err != nil {
				t.Fatal(err)
			}
			expected := want
			if source != "owner" {
				expected = ""
			}
			if value.Name != expected {
				t.Fatal("session source-subject witness changed")
			}
		}
	}
}
