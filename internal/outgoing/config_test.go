// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package outgoing_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/outgoing"
	"github.com/cratis/chronicle.go/metadata"
)

func TestHandledAuditValuesMaskParentAndPreserveRoot(t *testing.T) {
	parentID, err := metadata.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	ctx := metadata.WithCorrelation(t.Context(), parentID)
	ctx = metadata.WithIdentity(ctx, identities.Identity{Subject: "parent"})
	root := metadata.Causation{Occurred: time.Now(), Type: "Root", Properties: map[string]string{"owner": "local"}}
	ctx = metadata.WithCausation(ctx, metadata.Causation{Occurred: time.Now(), Type: "parent"})
	for _, actor := range []identities.Identity{identities.NotSet(), {}, {Subject: "system"}} {
		config := outgoing.Config{
			Identity: func(context.Context) (identities.Identity, bool, error) { return actor, true, nil },
			Correlation: func(context.Context) (metadata.CorrelationID, bool, error) {
				return metadata.CorrelationID{}, true, nil
			},
			Causation: func(context.Context) ([]metadata.Causation, bool, error) { return nil, true, nil }, Root: &root,
		}
		selected, err := config.Resolve(ctx, metadata.CorrelationID{}, false, parentID)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(selected.Identity, actor) || !selected.CorrelationPresent || selected.Correlation == parentID || len(selected.Causes) != 1 || selected.Causes[0].Type != "Root" {
			t.Fatal(selected)
		}
	}
	upstream := metadata.Causation{Type: "Root", Occurred: time.Now(), Properties: map[string]string{"owner": "upstream"}}
	selected, err := (outgoing.Config{Root: &root}).Resolve(metadata.WithCausationChain(ctx, []metadata.Causation{upstream}), metadata.CorrelationID{}, false, metadata.CorrelationID{})
	if err != nil || len(selected.Causes) != 1 || selected.Causes[0].Properties["owner"] != "upstream" {
		t.Fatal(selected, err)
	}
}

func TestProviderCancellationErrorIsNotCallerCancellation(t *testing.T) {
	config := outgoing.Config{Identity: func(context.Context) (identities.Identity, bool, error) {
		return identities.Identity{}, false, context.Canceled
	}}
	_, err := config.Resolve(t.Context(), metadata.CorrelationID{}, false, metadata.CorrelationID{})
	var failure *metadata.ProviderError
	if !errors.As(err, &failure) || errors.Is(err, context.Canceled) || failure.Phase != "identity" || failure.EventIndex != -1 {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = config.Resolve(ctx, metadata.CorrelationID{}, false, metadata.CorrelationID{})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestExplicitNotSetHasContextPresence(t *testing.T) {
	if _, ok := metadata.IdentityFrom(t.Context()); ok {
		t.Fatal("absent actor present")
	}
	actor, ok := metadata.IdentityFrom(metadata.WithIdentity(t.Context(), identities.NotSet()))
	if !ok || actor.Subject != identities.NotSet().Subject {
		t.Fatal(actor, ok)
	}
}
