// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type sinkClassifiedModel struct{ ID, Name string }

func TestExplicitSinkSurvivesNamingAndDefaultCopies(t *testing.T) {
	for _, kind := range []readmodels.SinkType{readmodels.MongoDB, readmodels.SQL, readmodels.InMemory} {
		for _, configuration := range []string{"00000000-0000-0000-0000-000000000000", "00112233-4455-6677-8899-aabbccddeeff"} {
			want := readmodels.Sink{Type: kind, ConfigurationID: configuration}
			model, err := readmodels.Define[sinkClassifiedModel](readmodels.WithSink(want))
			if err != nil {
				t.Fatal(err)
			}
			named, err := model.Descriptor().WithNamingPolicy(serialization.CamelCase)
			if err != nil {
				t.Fatal(err)
			}
			for _, fallback := range []readmodels.SinkType{readmodels.SQL, readmodels.InMemory, readmodels.MongoDB} {
				named, err = named.WithDefaultSinkType(fallback)
				if err != nil || named.Sink() != want {
					t.Fatalf("explicit=%v fallback=%s got=%v err=%v", want, fallback, named.Sink(), err)
				}
			}
		}
	}
}

func TestDefaultSinkCopyPreservesFrozenClassificationAndSchema(t *testing.T) {
	calls := 0
	model, err := readmodels.Define[sinkClassifiedModel](readmodels.WithProtection(compliance.Using(func(compliance.Target) (compliance.Classification, error) {
		calls++
		return compliance.Classification{}, nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	frozenCalls := calls
	if frozenCalls == 0 {
		t.Fatal("provider not exercised")
	}
	for _, kind := range []readmodels.SinkType{readmodels.SQL, readmodels.InMemory} {
		named, err := model.Descriptor().WithNamingPolicy(serialization.CamelCase)
		if err != nil {
			t.Fatal(err)
		}
		resolved, err := named.WithDefaultSinkType(kind)
		if err != nil {
			t.Fatal(err)
		}
		if calls != frozenCalls || resolved.Schema() != named.Schema() || resolved.Generation() != named.Generation() || resolved.Sink().Type != kind {
			t.Fatal("sink resolution rebuilt schema or reran classification")
		}
		data, err := resolved.RebindJSON([]byte(`{"Id":"source","Name":"frozen"}`), model.Descriptor())
		if err != nil || string(data) != `{"id":"source","name":"frozen"}` {
			t.Fatalf("snapshot rebind: %s %v", data, err)
		}
	}
}
