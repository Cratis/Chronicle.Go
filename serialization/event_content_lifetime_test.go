// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/internal/contentencoding"
	"github.com/cratis/chronicle.go/internal/derivedfixtures"
	"github.com/cratis/chronicle.go/serialization"
)

type delayedConcept struct{ entered, release chan struct{} }

func (delayedConcept) ConceptValue() string { return "value" }
func (v delayedConcept) MarshalJSON() ([]byte, error) {
	if v.entered != nil {
		close(v.entered)
		<-v.release
	}
	return []byte(`"value"`), nil
}
func (*delayedConcept) UnmarshalJSON([]byte) error  { return nil }
func (delayedConcept) MarshalText() ([]byte, error) { return []byte("value"), nil }
func (*delayedConcept) UnmarshalText([]byte) error  { return nil }

type delayedEvent struct{ Value delayedConcept }

func TestEventContentLateCodecCannotPublishIntoNextProvider(t *testing.T) {
	plan, err := serialization.Compile(reflect.TypeFor[delayedEvent]())
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	completed := make(chan error, 1)
	data, err := plan.MarshalContent(contentencoding.Request[serialization.EventContent]{Value: delayedEvent{}, Providers: []func(*serialization.EventContent) error{
		func(c *serialization.EventContent) error {
			go func() { completed <- c.Set("Value", delayedConcept{entered, release}) }()
			<-entered
			return nil
		},
		func(c *serialization.EventContent) error {
			close(release)
			if err := <-completed; err == nil {
				t.Error("expired setter published")
			}
			return c.Set("Value", delayedConcept{})
		},
	}})
	if err != nil || string(data) != `{"Value":"value"}` {
		t.Fatal(string(data), err)
	}
}

type editedNode struct {
	Value string
	Next  *editedNode
}
type structuredEdit struct {
	Node   *editedNode
	Values map[string]int64
	Last   string
}

func TestEventContentReplacementUsesNormalGraphChecks(t *testing.T) {
	plan, err := serialization.Compile(reflect.TypeFor[structuredEdit]())
	if err != nil {
		t.Fatal(err)
	}
	cyclic := &editedNode{}
	cyclic.Next = cyclic
	for _, test := range []struct {
		name, field string
		value       any
	}{
		{"cycle", "Node", cyclic},
		{"dictionary precision", "Values", map[string]int64{"number": 1<<53 + 1}},
		{"incompatible object", "Node", editedNode{}},
		{"reserved discriminator", "$type", "injected"},
	} {
		t.Run(test.name, func(t *testing.T) {
			data, err := plan.MarshalContent(contentencoding.Request[serialization.EventContent]{Value: structuredEdit{}, Providers: []func(*serialization.EventContent) error{func(c *serialization.EventContent) error { _ = c.Set(test.field, test.value); return nil }}})
			if err == nil || data != nil {
				t.Fatal(string(data), err)
			}
		})
	}
	node := &editedNode{Value: "owned"}
	data, err := plan.MarshalContent(contentencoding.Request[serialization.EventContent]{Value: structuredEdit{Node: &editedNode{Value: "before"}}, Providers: []func(*serialization.EventContent) error{func(c *serialization.EventContent) error {
		if err := c.Set("Node", (*editedNode)(nil)); err != nil {
			return err
		}
		if err := c.Set("Node", node); err != nil {
			return err
		}
		node.Value = "mutated"
		return nil
	}}})
	if err != nil || string(data) != `{"Last":"","Node":{"Value":"owned"}}` {
		t.Fatal(string(data), err)
	}
}

func TestEventContentUsesRegisteredDerivedPlan(t *testing.T) {
	plan := derivedPlan[derivedfixtures.MembersChanged](t, derivedCodecs(t), serialization.PreservePropertyNames)
	sample := derivedfixtures.Sample()
	data, err := plan.MarshalContent(contentencoding.Request[serialization.EventContent]{Value: sample, Providers: []func(*serialization.EventContent) error{func(c *serialization.EventContent) error { return c.Set("Primary", sample.Primary) }}})
	if err != nil {
		t.Fatal(err)
	}
	want, err := plan.Marshal(sample)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(want) {
		t.Fatal("derived representation changed", string(data))
	}
}
