// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReadModelPresenceAndCollectionNormalization(t *testing.T) {
	model := person(t, readmodels.WithIdentifier("Example.Person"))
	for _, tc := range []struct {
		name, payload string
		position      uint64
		exists        bool
	}{
		{"missing", "null", ^uint64(0), false}, {"removed", " null ", 7, false},
		{"zero model", "{}", 0, true}, {"nested collections", `{"children":[{}],"count":9007199254740993}`, 9007199254740993, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, ctx := serviceFixture(t, &modelKernel{get: func(_ context.Context, r *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
				if r.EventStore != "store" || r.Namespace != "tenant-a" || r.ReadModelIdentifier != "Example.Person" || r.ReadModelKey != " padded key " || r.EventSequenceId != "event-log" || r.SessionId != "" {
					t.Errorf("request: %+v", r)
				}
				return &contracts.GetInstanceByKeyResponse{ReadModel: tc.payload, LastHandledEventSequenceNumber: tc.position}, nil
			}}, model.Descriptor())
			result, err := readmodels.For(service, model).Get(ctx, " padded key ")
			if err != nil {
				t.Fatal(err)
			}
			if result.Exists != tc.exists || (result.LastHandled == nil) != (tc.position == ^uint64(0)) {
				t.Fatalf("result: %+v", result)
			}
			if result.LastHandled != nil && uint64(*result.LastHandled) != tc.position {
				t.Fatal("sequence precision lost")
			}
			if tc.exists && (result.Value.Children == nil || result.Value.Optional != nil || result.Value.Properties != nil) {
				t.Fatalf("nullability: %+v", result.Value)
			}
			if tc.name == "nested collections" && (result.Value.Children[0].Names == nil || result.Value.Count != 9007199254740993) {
				t.Fatal(result.Value)
			}
			raw, err := service.Get(ctx, model.Identifier(), " padded key ")
			if err != nil || raw.Exists != tc.exists {
				t.Fatalf("raw: %+v %v", raw, err)
			}
			if !tc.exists && raw.Value != nil {
				t.Fatal("absence manufactured a document")
			}
			if tc.exists && string(raw.Value) != strings.TrimSpace(tc.payload) {
				t.Fatal("raw document changed")
			}
		})
	}
}

func TestReadModelErrorsAndUnsupportedKernel(t *testing.T) {
	model := person(t)
	for _, tc := range []struct {
		name, payload string
		code          codes.Code
		identity      error
	}{
		{"empty", "", codes.OK, chronicle.ErrProtocol}, {"array", "[]", codes.OK, chronicle.ErrProtocol}, {"scalar", "1", codes.OK, chronicle.ErrProtocol}, {"bad json", "{", codes.OK, chronicle.ErrProtocol},
		{"wrong type", `{"count":"secret"}`, codes.OK, chronicle.ErrProtocol},
		{"unsupported", "", codes.Unimplemented, chronicle.ErrUnsupported}, {"canceled", "", codes.Canceled, context.Canceled}, {"deadline", "", codes.DeadlineExceeded, context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, ctx := serviceFixture(t, &modelKernel{get: func(context.Context, *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
				if tc.code != codes.OK {
					return nil, status.Error(tc.code, "failure")
				}
				return &contracts.GetInstanceByKeyResponse{ReadModel: tc.payload, LastHandledEventSequenceNumber: uint64(events.Unavailable)}, nil
			}}, model.Descriptor())
			result, err := readmodels.For(service, model).Get(ctx, "key")
			if !errors.Is(err, tc.identity) || result.Exists || result.LastHandled != nil {
				t.Fatalf("result %+v error %v", result, err)
			}
			if tc.code != codes.OK && status.Code(err) != tc.code {
				t.Fatalf("lost grpc status: %v", err)
			}
			if err != nil && strings.Contains(err.Error(), "secret") {
				t.Fatal("error leaked content")
			}
		})
	}
}

func TestReadModelValidationNeverDispatches(t *testing.T) {
	model := person(t)
	var calls atomic.Int32
	service, ctx := serviceFixture(t, &modelKernel{get: func(context.Context, *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
		calls.Add(1)
		return nil, nil
	}}, model.Descriptor())
	reader := readmodels.For(service, model)
	if _, err := reader.Get(ctx, ""); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	if _, err := reader.Get(ctx, "*"); !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := reader.Get(canceled, "key"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := service.Get(ctx, "unknown", "key"); !errors.Is(err, chronicle.ErrNotRegistered) {
		t.Fatal(err)
	}
	for _, r := range []*readmodels.Reader[Person]{readmodels.For(service, person(t)), readmodels.For[Person](nil, model), readmodels.For(service, readmodels.Model[Person]{})} {
		if _, err := r.Get(ctx, "key"); !errors.Is(err, chronicle.ErrNotRegistered) {
			t.Fatal(err)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid call dispatched")
	}
}
