// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/derivedchildrenfixtures"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type protectedDerivedChild struct {
	Secret string `chronicle:"pii"`
}
type confidentialDerivedChild struct {
	Secret string `chronicle:"encrypted"`
}
type sourceDerivedChild struct {
	Source events.SourceID `chronicle:"subject"`
}
type indexedDerivedChild struct {
	Name string `chronicle:"index"`
}
type protectedDerivedSibling struct {
	Items  []derivedchildrenfixtures.Child `chronicle:"children(ItemAdded,key=ItemId,parent-key=OrderId)"`
	Secret string                          `chronicle:"pii"`
}
type unusedDerivedFamily interface{}

func TestDerivedChildPreparationAuditsUnseenVariantsAndRejectsBeforeTransport(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	for _, registration := range []serialization.Codec{
		serialization.Derived[unusedDerivedFamily, protectedDerivedChild]("unseen-pii"),
		serialization.Derived[unusedDerivedFamily, confidentialDerivedChild]("unseen-encrypted"),
		serialization.Derived[unusedDerivedFamily, sourceDerivedChild]("unseen-subject"),
		serialization.Derived[unusedDerivedFamily, indexedDerivedChild]("unseen-index"),
	} {
		codecs, err := serialization.NewCodecs(serialization.Derived[derivedchildrenfixtures.Child, *derivedchildrenfixtures.Line]("line"), registration)
		if err != nil {
			t.Fatal(err)
		}
		registry := chronicle.NewRegistry()
		if _, err := chronicle.RegisterReadModel[derivedchildrenfixtures.Catalog](registry, readmodels.WithCodecs(codecs)); !errors.Is(err, faults.ErrInvalidConfiguration) {
			t.Fatalf("unseen variant accepted: %v", err)
		}
	}
	codecs, _ := derivedchildrenfixtures.Codecs()
	if _, err := readmodels.Define[protectedDerivedSibling](readmodels.WithCodecs(codecs)); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatalf("active protection walk accepted: %v", err)
	}
	for _, classification := range []compliance.Declaration{
		compliance.For[derivedchildrenfixtures.Child](compliance.Classification{PII: true}),
		compliance.For[derivedchildrenfixtures.Line](compliance.Classification{Encrypted: true}),
		compliance.Using(func(target compliance.Target) (compliance.Classification, error) {
			return compliance.Classification{PII: target.Type == reflect.TypeFor[derivedchildrenfixtures.Line]()}, nil
		}),
	} {
		if _, err := readmodels.Define[derivedchildrenfixtures.Catalog](readmodels.WithCodecs(codecs), readmodels.WithProtection(classification)); !errors.Is(err, faults.ErrInvalidConfiguration) {
			t.Fatalf("protected family accepted: %v", err)
		}
	}
	for _, id := range []string{"ambiguous", "bad)literal"} {
		registrations := []serialization.Codec{serialization.Derived[derivedchildrenfixtures.Child, *derivedchildrenfixtures.Line](id)}
		if id == "ambiguous" {
			registrations = append(registrations, serialization.Derived[derivedchildrenfixtures.Child, *otherDerivedLine]("other"))
		}
		codecs, err := serialization.NewCodecs(registrations...)
		if err != nil {
			t.Fatal(err)
		}
		registry := chronicle.NewRegistry()
		if _, err := chronicle.RegisterEvent[derivedchildrenfixtures.ItemAdded](registry); err != nil {
			t.Fatal(err)
		}
		if _, err := chronicle.RegisterEvent[derivedchildrenfixtures.ItemRemoved](registry); err != nil {
			t.Fatal(err)
		}
		if _, err := chronicle.RegisterEvent[derivedchildrenfixtures.ItemRenamed](registry); err != nil {
			t.Fatal(err)
		}
		if _, err := chronicle.RegisterReadModel[derivedchildrenfixtures.Catalog](registry, readmodels.WithCodecs(codecs)); err != nil {
			t.Fatal(err)
		}
		client, err := chronicle.Dial(t.Context(), chronicle.WithRegistry(registry), chronicle.WithDevelopmentDefaults(), chronicle.WithConnectionString("chronicle://"+strings.TrimPrefix(server.URL, "https://")))
		if client != nil {
			if closeErr := client.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
		}
		var declaration *projections.DeclarationError
		if !errors.As(err, &declaration) {
			t.Fatalf("invalid family was not rejected during preparation: %v", err)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid declarations made %d transport calls", calls.Load())
	}
}
