// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package constraints_test

import (
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/constraints"
)

func TestUniqueModeSelection(t *testing.T) {
	if int32(constraints.PerEventSource) != 0 || int32(constraints.PerValue) != 1 {
		t.Fatal("unique mode numeric identities changed")
	}
	for _, tc := range []struct {
		name  string
		modes []constraints.UniqueMode
		want  constraints.UniqueMode
	}{
		{name: "default", want: constraints.PerEventSource},
		{name: "explicit source", modes: []constraints.UniqueMode{constraints.PerEventSource}, want: constraints.PerEventSource},
		{name: "explicit value", modes: []constraints.UniqueMode{constraints.PerValue}, want: constraints.PerValue},
		{name: "last source", modes: []constraints.UniqueMode{constraints.PerValue, constraints.PerEventSource}, want: constraints.PerEventSource},
		{name: "last value", modes: []constraints.UniqueMode{constraints.PerEventSource, constraints.PerValue}, want: constraints.PerValue},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := constraints.UniqueValues("email").On(descriptor[Contact](t), "emailAddress")
			for _, mode := range tc.modes {
				builder.WithMode(mode)
			}
			if got := build(t, builder).Mode(); got != tc.want {
				t.Fatalf("mode = %d, want %d", got, tc.want)
			}
		})
	}
	if got := build(t, constraints.UniqueEventTypes(descriptor[Contact](t))).Mode(); got != constraints.PerEventSource {
		t.Fatalf("event-type mode = %d, want zero", got)
	}
}

func TestInvalidUniqueModesFailBuild(t *testing.T) {
	contact := descriptor[Contact](t)
	for name, builder := range map[string]*constraints.Builder{
		"negative":           constraints.UniqueValues("email").On(contact, "emailAddress").WithMode(-1),
		"unknown":            constraints.UniqueValues("email").On(contact, "emailAddress").WithMode(2),
		"maximum":            constraints.UniqueValues("email").On(contact, "emailAddress").WithMode(1<<31 - 1),
		"invalid then valid": constraints.UniqueValues("email").On(contact, "emailAddress").WithMode(-1).WithMode(constraints.PerValue),
		"valid then invalid": constraints.UniqueValues("email").On(contact, "emailAddress").WithMode(constraints.PerValue).WithMode(2),
		"event-type source":  constraints.UniqueEventTypes(contact).WithMode(constraints.PerEventSource),
		"event-type value":   constraints.UniqueEventTypes(contact).WithMode(constraints.PerValue),
		"event-type unknown": constraints.UniqueEventTypes(contact).WithMode(-1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := builder.Build(); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
				t.Fatalf("error = %v, want ErrInvalidConfiguration", err)
			}
		})
	}
}

func TestUniqueModePreservesFirstBuilderError(t *testing.T) {
	contact := descriptor[Contact](t)
	for name, builder := range map[string]*constraints.Builder{
		"earlier option error": constraints.UniqueValues("email").On(contact, "emailAddress").WithMessageProvider(nil),
		"mode error":           constraints.UniqueValues("email").On(contact, "emailAddress").WithMode(-1),
		"wrong kind":           constraints.UniqueEventTypes(contact).WithMode(constraints.PerValue),
	} {
		t.Run(name, func(t *testing.T) {
			_, first := builder.Build()
			if !errors.Is(first, chronicle.ErrInvalidConfiguration) {
				t.Fatalf("first error = %v, want ErrInvalidConfiguration", first)
			}
			builder.WithMode(2).WithMode(constraints.PerEventSource).WithMessageProvider(nil)
			if _, err := builder.Build(); err != first {
				t.Fatalf("error = %v, want original error %v", err, first)
			}
		})
	}
}

func TestUniqueModeSnapshotsAndToBuilder(t *testing.T) {
	for _, tc := range []struct {
		name        string
		mode, other constraints.UniqueMode
	}{
		{"source", constraints.PerEventSource, constraints.PerValue},
		{"value", constraints.PerValue, constraints.PerEventSource},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mode, other := tc.mode, tc.other
			builder := constraints.UniqueValues("email").On(descriptor[Contact](t), "emailAddress").WithMode(mode)
			definition := build(t, builder)
			if got := build(t, builder.WithMode(other)).Mode(); got != other {
				t.Fatalf("edited builder mode = %d, want %d", got, other)
			}
			copyBuilder := definition.ToBuilder()
			if got := build(t, copyBuilder).Mode(); got != mode {
				t.Fatalf("ToBuilder mode = %d, want %d", got, mode)
			}
			if got := build(t, copyBuilder.WithMode(other)).Mode(); got != other {
				t.Fatalf("edited copy mode = %d, want %d", got, other)
			}
			if got := definition.Mode(); got != mode {
				t.Fatalf("snapshot mode = %d, want %d", got, mode)
			}
		})
	}
}
