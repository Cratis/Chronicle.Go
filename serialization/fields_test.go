// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/serialization"
)

type projectionMetadata struct {
	ID      string     `json:"ID" chronicle:"key"`
	Label   string     `json:"DisplayName" chronicle:"set(Opened,from=name)"`
	Updated *time.Time `json:"updated,omitempty" chronicle:"context(Opened,from=occurred)"`
	Address struct {
		City string `json:"Town"`
	} `json:"address"`
}

func TestFieldMetadataSharesPlanAndOwnsSnapshots(t *testing.T) {
	plan, err := serialization.Compile(reflect.TypeFor[projectionMetadata]())
	if err != nil {
		t.Fatal(err)
	}
	fields := plan.Fields()
	if len(fields) != 5 || fields[0].Path != "ID" || fields[1].Path != "DisplayName" || fields[2].Scalar != serialization.String || fields[2].Format != "date-time" || !fields[2].Nullable || fields[4].Path != "address.Town" || fields[4].GoField != "Address.City" {
		t.Fatalf("%+v", fields)
	}
	fields[0].Index[0] = 99
	fields[0].Name = "mutated"
	if plan.Fields()[0].Index[0] != 0 || plan.Fields()[0].Name != "ID" {
		t.Fatal("metadata mutation escaped snapshot")
	}
	if err = plan.ValidateRole(declarations.Event); err == nil {
		t.Fatal("projection tags accepted as event metadata")
	}
	if _, err = events.Define[projectionMetadata](); err == nil {
		t.Fatal("event declaration bypassed role validation")
	}
}
func TestDeclarationsOnIgnoredFieldsFailClosed(t *testing.T) {
	for _, typ := range []reflect.Type{
		reflect.TypeFor[struct {
			Secret string `json:"-" chronicle:"pii"`
		}](),
		reflect.TypeFor[struct {
			secret string `chronicle:"set(E)"`
		}](),
		reflect.TypeFor[struct {
			ID string `chronicle:"unknown"`
		}](),
	} {
		_, err := serialization.Compile(typ)
		var declaration *declarations.DeclarationError
		if !errors.As(err, &declaration) {
			t.Fatalf("%v: %v", typ, err)
		}
	}
}
