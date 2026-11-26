// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"maps"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/serialization"
	"github.com/google/uuid"
)

type guidLabelsV1 struct {
	Labels map[string]string
}
type guidLabels struct {
	Labels map[uuid.UUID]string
}

func TestGuidDictionaryEventRegistrationAndHistory(t *testing.T) {
	registry := chronicle.NewRegistry()
	current, err := chronicle.RegisterEvent[guidLabels](registry, events.WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	old, err := chronicle.RegisterEventGeneration[guidLabelsV1](registry, current, 1)
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff")
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase} {
		catalog := catalogFor(t, registry, chronicle.WithNamingPolicy(policy))
		descriptor, _ := catalog.LookupRef(current.Ref())
		data, err := descriptor.Marshal(guidLabels{Labels: map[uuid.UUID]string{id: "x"}})
		if err != nil {
			t.Fatal(err)
		}
		var content map[string]map[string]string
		if err := json.Unmarshal(data, &content); err != nil {
			t.Fatal(err)
		}
		for _, labels := range content {
			if labels["00112233-4455-6677-8899-aabbccddeeff"] != "x" {
				t.Fatalf("%s", data)
			}
		}
		decoded, err := events.Decode[guidLabels](catalog, events.Appended{Context: events.Context{EventType: current.Ref()}, Content: data})
		if err != nil || decoded.Labels[id] != "x" {
			t.Fatalf("%+v %v", decoded, err)
		}
	}
	checkOldGenerationKeepsStringKeys(t, registry, old)
}

// checkOldGenerationKeepsStringKeys writes and reads captured historical
// string-keyed names, which must not be parsed as UUIDs, under both policies.
func checkOldGenerationKeepsStringKeys(t *testing.T, registry *chronicle.Registry, old events.Type[guidLabelsV1]) {
	t.Helper()
	names := map[string]string{"not-a-guid": "a", "Key": "b", "key": "c"}
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase} {
		catalog := catalogFor(t, registry, chronicle.WithNamingPolicy(policy))
		descriptor, _ := catalog.LookupRef(old.Ref())
		data, err := descriptor.Marshal(guidLabelsV1{Labels: names})
		if err != nil {
			t.Fatal(err)
		}
		value, err := events.Decode[guidLabelsV1](catalog, events.Appended{Context: events.Context{EventType: old.Ref()}, Content: data})
		if err != nil || !maps.Equal(value.Labels, names) {
			t.Fatalf("historical string keys changed: %+v %v", value, err)
		}
	}
}

func TestGuidDictionaryReadModelStaysRefused(t *testing.T) {
	if _, err := chronicle.RegisterReadModel[guidLabels](chronicle.NewRegistry()); err == nil || !errors.Is(err, chronicle.ErrUnsupported) && !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatalf("read model registration: %v", err)
	}
}

type piiStringLabels struct {
	Labels map[string]string `chronicle:"pii"`
}
type piiGuidLabels struct {
	Labels map[uuid.UUID]string `chronicle:"pii"`
}
type encryptedStringLabels struct {
	Labels map[string]string `chronicle:"encrypted"`
}
type encryptedGuidLabels struct {
	Labels map[uuid.UUID]string `chronicle:"encrypted"`
}

func guidProtectionProperties[T any](t *testing.T) json.RawMessage {
	t.Helper()
	event, err := chronicle.RegisterEvent[T](chronicle.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal([]byte(event.Descriptor().Schema()), &schema); err != nil {
		t.Fatal(err)
	}
	return schema.Properties["Labels"]
}

// Protection applies to the property exactly as for a string-keyed map; the
// Guid keys remain unprotected property names.
func TestGuidDictionaryProtectionMatchesStringDictionary(t *testing.T) {
	for name, pair := range map[string][2]json.RawMessage{
		"pii":       {guidProtectionProperties[piiStringLabels](t), guidProtectionProperties[piiGuidLabels](t)},
		"encrypted": {guidProtectionProperties[encryptedStringLabels](t), guidProtectionProperties[encryptedGuidLabels](t)},
	} {
		if !bytes.Contains(pair[0], []byte(`"compliance"`)) && !bytes.Contains(pair[0], []byte(`"security"`)) {
			t.Fatalf("%s: string map lost protection metadata: %s", name, pair[0])
		}
		if string(pair[0]) != string(pair[1]) {
			t.Fatalf("%s: string map %s, guid map %s", name, pair[0], pair[1])
		}
	}
	event, err := chronicle.RegisterEvent[piiGuidLabels](chronicle.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	data, err := event.Descriptor().Marshal(piiGuidLabels{Labels: map[uuid.UUID]string{uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff"): "x"}})
	if err != nil || string(data) != `{"Labels":{"00112233-4455-6677-8899-aabbccddeeff":"x"}}` {
		t.Fatalf("%s %v", data, err)
	}
}
