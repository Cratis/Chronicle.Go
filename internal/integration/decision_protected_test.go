//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"encoding/base64"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/transactions"
)

type DecisionPersonRenamed struct {
	Name   string `json:"name" chronicle:"pii"`
	Secret string `json:"secret" chronicle:"encrypted(scope=namespace)"`
}
type DecisionPerson struct {
	ID     string `json:"id" chronicle:"key"`
	Name   string `json:"name" chronicle:"pii;set(DecisionPersonRenamed)"`
	Secret string `json:"secret" chronicle:"encrypted(scope=namespace);set(DecisionPersonRenamed)"`
}
type DecisionPersonDefault struct {
	ID     string
	Name   string `json:"name" chronicle:"pii;set(DecisionPersonRenamed)"`
	Secret string `json:"secret" chronicle:"encrypted(scope=namespace);set(DecisionPersonRenamed)"`
}

// Chronicle#4561 (fixed in 19.32.2): decision session folds release each value
// with the subject it was written under, so classified models are admitted like
// C#. The key differs from the subject; reads happen before and after erasure.
func TestKernelDecisionReadsReleaseClassifiedModelsWithEventSubjects(t *testing.T) {
	t.Run("lowercase_id", func(t *testing.T) { classifiedDecisionProfile[DecisionPerson](t) })
	t.Run("default_Id", func(t *testing.T) { classifiedDecisionProfile[DecisionPersonDefault](t) })
}

func classifiedDecisionProfile[T any](t *testing.T) {
	t.Helper()
	fixture := newKernelFixture(t)
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[DecisionPersonRenamed](registry); err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[T](registry)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.AddProjection(projections.ModelBound(model, projections.Passive())); err != nil {
		t.Fatal(err)
	}
	store, err := fixture.client(registry).EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	reader := readmodels.DecisionsFor(store.ReadModels(), model)
	if admission := reader.Admit(); !admission.IsAdmitted {
		t.Fatalf("classified decision admission: %+v", admission)
	}
	// Cipher-shaped plaintext would be corrupted by a second decryption.
	plaintext := base64.StdEncoding.EncodeToString(make([]byte, 256))
	appendSuccessfully(t, fixture.ctx, store, "source-not-subject", DecisionPersonRenamed{Name: plaintext, Secret: plaintext}, eventsequences.WithSubject("owner"))
	for _, erased := range []bool{false, true} {
		want := plaintext
		if erased {
			if err := store.Compliance().ErasePII(fixture.ctx, "owner"); err != nil {
				t.Fatal(err)
			}
			want = ""
		}
		unit, owner := decisionUnit(t, fixture.ctx, store)
		read, err := reader.Get(transactions.WithUnitOfWork(fixture.ctx, unit), "source-not-subject")
		if err != nil || !read.Instance.Exists || read.Token.IsZero() {
			t.Fatalf("erased=%t classified decision read: %v", erased, err)
		}
		data, err := model.Descriptor().Marshal(read.Instance.Value)
		if err != nil {
			t.Fatal(err)
		}
		checkReleaseRouteValue(t, "decision read", data, nil, want, plaintext)
		result, err := owner.Commit(fixture.ctx)
		if err != nil || !unit.IsSuccess() || !result.ConcurrencyCheckPerformed {
			t.Fatalf("erased=%t guarded commit: %+v %v", erased, result, err)
		}
	}
}
