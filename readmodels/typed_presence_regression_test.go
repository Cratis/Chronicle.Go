// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/readmodels"
)

func TestTypedNamingPlanPreservesMissingKeyAndDistinctID(t *testing.T) {
	model, err := readmodels.Define[plannedWatchModel]()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, data string
		want       plannedWatchModel
	}{
		{name: "missing key", data: `{"ID":"external"}`, want: plannedWatchModel{ExternalID: "external"}},
		{name: "null key", data: `{"Id":null,"ID":"external"}`, want: plannedWatchModel{ExternalID: "external"}},
		{name: "sink alias and distinct ID", data: `{"_id":"person","ID":"external"}`, want: plannedWatchModel{ID: "person", ExternalID: "external"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, ctx := serviceFixture(t, &modelKernel{get: func(context.Context, *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
				return &contracts.GetInstanceByKeyResponse{ReadModel: tc.data}, nil
			}}, model.Descriptor())
			got, err := readmodels.For(service, model).Get(ctx, "person")
			if err != nil || !got.Exists || got.Value != tc.want {
				t.Fatalf("Get = %+v, %v; want %+v", got, err, tc.want)
			}
		})
	}
}

func TestTypedInjectionAbsenceNeverManufacturesState(t *testing.T) {
	model, err := readmodels.Define[plannedWatchModel]()
	if err != nil {
		t.Fatal(err)
	}
	service, ctx := serviceFixture(t, &modelKernel{get: func(context.Context, *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
		return &contracts.GetInstanceByKeyResponse{ReadModel: "null", LastHandledEventSequenceNumber: 8}, nil
	}}, model.Descriptor())
	got, err := readmodels.For(service, model).Get(ctx, "absent")
	if err != nil || got.Exists || got.LastHandled == nil || *got.LastHandled != 8 {
		t.Fatalf("Get = %+v, %v", got, err)
	}
	pointer, err := service.GetValue(ctx, reflect.TypeFor[*plannedWatchModel](), "absent")
	if err != nil || pointer != (*plannedWatchModel)(nil) {
		t.Fatalf("pointer = %+v, %v", pointer, err)
	}
	value, err := service.GetValue(ctx, reflect.TypeFor[plannedWatchModel](), "absent")
	if value != nil || !errors.Is(err, chronicle.ErrNotRegistered) {
		t.Fatalf("value = %+v, %v", value, err)
	}
}
