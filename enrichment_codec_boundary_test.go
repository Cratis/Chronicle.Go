// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/identities"
)

type reviewCodecError struct{ hooks *int }

func (e reviewCodecError) Error() string { *e.hooks++; return "secret" }
func (e reviewCodecError) Is(error) bool { *e.hooks++; return true }
func (e reviewCodecError) As(any) bool   { *e.hooks++; return true }
func (e reviewCodecError) Unwrap() error { *e.hooks++; return nil }

type reviewConcept struct{ failure error }

func (reviewConcept) ConceptValue() string           { return "" }
func (v reviewConcept) MarshalJSON() ([]byte, error) { return []byte("secret"), v.failure }
func (*reviewConcept) UnmarshalJSON([]byte) error    { panic("decoder must not run") }
func (reviewConcept) MarshalText() ([]byte, error)   { panic("text must not run") }
func (*reviewConcept) UnmarshalText([]byte) error    { panic("decoder must not run") }

type reviewCodecEvent struct{ Value reviewConcept }

func TestPublicOutgoingBaseCodecBoundaryWithoutEnrichers(t *testing.T) {
	for _, identityOnly := range []bool{false, true} {
		t.Run(fmt.Sprint(identityOnly), func(t *testing.T) {
			registry := chronicle.NewRegistry()
			definition, err := chronicle.RegisterEvent[reviewCodecEvent](registry)
			if err != nil {
				t.Fatal(err)
			}
			kernel := &fakeKernel{}
			var options []chronicle.ClientOption
			options = append(options, chronicle.WithRegistry(registry))
			if identityOnly {
				options = append(options, chronicle.WithIdentityProvider(func(context.Context) (identities.Identity, bool, error) {
					return identities.Identity{Subject: "actor"}, true, nil
				}))
			}
			client, _ := testClient(t, kernel, options...)
			ctx := testContext(t)
			store, err := client.EventStore(ctx, "customers")
			if err != nil {
				t.Fatal(err)
			}
			hooks := 0
			application := reviewCodecError{&hooks}
			_, err = store.EventLog().Append(ctx, "A", reviewCodecEvent{reviewConcept{application}})
			var failure *events.PreparationError
			if !errors.As(err, &failure) || failure.ProviderIndex != -1 || failure.EventIndex != 0 || failure.Phase != "content" {
				t.Fatalf("error=%#v", err)
			}
			_ = fmt.Sprint(err)
			if errors.Is(err, errors.New("innocent")) || errors.Is(err, application) || hooks != 0 || kernel.appendCalls.Load() != 0 {
				t.Fatal("outgoing error retained application or dispatched", hooks, kernel.appendCalls.Load())
			}
			// Standalone encoding preserves its ordinary deliberate-inspection API.
			ordinary := errors.New("ordinary")
			_, err = definition.Descriptor().Marshal(reviewCodecEvent{reviewConcept{ordinary}})
			if !errors.Is(err, ordinary) {
				t.Fatal("standalone descriptor lost ordinary codec identity")
			}
		})
	}
}
