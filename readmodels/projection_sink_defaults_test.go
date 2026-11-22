// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/readmodels"
)

func TestProjectionBindingAcceptsInheritedNoneButNotExplicitActiveNone(t *testing.T) {
	model, err := readmodels.Define[sinkClassifiedModel]()
	if err != nil {
		t.Fatal(err)
	}
	inherited, err := model.Descriptor().WithDefaultSinkType(readmodels.NoSink)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := readmodels.BindProjection(inherited, "projection", events.EventLog, false)
	if err != nil || bound.Sink().Type != readmodels.NoSink || model.Descriptor().Sink().Type != readmodels.MongoDB {
		t.Fatal("inherited None binding lost", err)
	}
}

func TestExplicitNoneKeepsProjectionBindingRestrictions(t *testing.T) {
	explicit, err := readmodels.Define[sinkClassifiedModel](readmodels.WithSink(readmodels.Sink{Type: readmodels.NoSink}), readmodels.WithObserver(readmodels.Projection, "projection"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := readmodels.BindProjection(explicit.Descriptor(), "projection", events.EventLog, false); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatal("explicit active None restriction changed", err)
	}
	if _, err := readmodels.BindProjection(explicit.Descriptor(), "projection", events.EventLog, true); err != nil {
		t.Fatal("explicit passive None rejected", err)
	}
}
