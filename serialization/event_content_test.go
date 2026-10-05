// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/internal/contentencoding"
	"github.com/cratis/chronicle.go/serialization"
)

type editedEvent struct {
	Name     string  `json:"name"`
	Optional *string `json:"optional"`
	Count    int64   `json:"count"`
}

func TestEventContentOwnsOrderedValuesAndExpiresEachHandle(t *testing.T) {
	plan, err := serialization.Compile(reflect.TypeFor[editedEvent]())
	if err != nil {
		t.Fatal(err)
	}
	var previous *serialization.EventContent
	var copied serialization.EventContent
	text := "owned"
	data, err := plan.MarshalContent(contentencoding.Request[serialization.EventContent]{Value: editedEvent{Name: "before", Count: 9007199254740993}, Providers: []func(*serialization.EventContent) error{
		func(content *serialization.EventContent) error {
			previous = content
			copied = *content
			if err := content.Set("optional", &text); err != nil {
				return err
			}
			text = "changed"
			return content.Set("name", "after")
		},
		func(content *serialization.EventContent) error {
			if err := previous.Set("name", "late"); err == nil {
				t.Fatal("expired editor accepted mutation")
			}
			if err := copied.Set("name", "copied late"); err == nil {
				t.Fatal("copied handle escaped expiration")
			}
			raw, ok, err := content.Get("optional")
			if err != nil || !ok || string(raw) != `"owned"` {
				t.Fatalf("Get = %s, %v, %v", raw, ok, err)
			}
			raw[1] = 'X'
			return nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"name":"after","count":9007199254740993,"optional":"owned"}` {
		t.Fatalf("content = %s", data)
	}
}

func TestEventContentLatchesInvalidMutation(t *testing.T) {
	plan, err := serialization.Compile(reflect.TypeFor[editedEvent]())
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*serialization.EventContent)
	}{
		{"unknown", func(c *serialization.EventContent) { _ = c.Set("extra", "value") }},
		{"unknown Get", func(c *serialization.EventContent) { _, _, _ = c.Get("extra") }},
		{"Go spelling", func(c *serialization.EventContent) { _ = c.Set("Name", "value") }},
		{"wrong type", func(c *serialization.EventContent) { _ = c.Set("count", int(2)) }},
		{"raw JSON", func(c *serialization.EventContent) { _ = c.Set("name", json.RawMessage(`"value"`)) }},
		{"untyped nil", func(c *serialization.EventContent) { _ = c.Set("optional", nil) }},
		{"required remove", func(c *serialization.EventContent) { _ = c.Remove("name") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			data, err := plan.MarshalContent(contentencoding.Request[serialization.EventContent]{Value: editedEvent{}, Providers: []func(*serialization.EventContent) error{func(c *serialization.EventContent) error { test.mutate(c); return nil }}})
			if err == nil || data != nil {
				t.Fatalf("invalid edit returned %s, %v", data, err)
			}
		})
	}
}
