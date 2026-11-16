// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reducers_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/reducers"
)

type wrongCurrentAndReturn struct{}

func (wrongCurrentAndReturn) Fold(Changed, *Other) error { return nil }

func TestCompileRejectsWrongCurrentBeforeWrongReturn(t *testing.T) {
	catalog, model, models := catalog(t)
	declaration, err := reducers.Define[wrongCurrentAndReturn](model, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = reducers.Compile(declaration, catalog, models, nil)
	var detail *reducers.DeclarationError
	if !errors.As(err, &detail) {
		t.Fatalf("expected reducer declaration error, got %v", err)
	}
	if detail.Method != "Fold" || detail.Parameter != reflect.TypeFor[Changed]() {
		t.Fatalf("unexpected failure location: %+v", detail)
	}
	want := faults.ErrInvalidConfiguration.Error() + ": current must be the associated model or pointer"
	if detail.Cause.Error() != want {
		t.Fatalf("cause = %q, want %q", detail.Cause, want)
	}
}
