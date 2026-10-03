// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/cratis/chronicle.go/contracts/compliance"
	"github.com/cratis/chronicle.go/readmodels"
)

type protectedAliasModel struct {
	ID         string
	ExternalID string  `json:"ID"`
	Owner      *string `json:"owner"`
	Value      string  `json:"value" chronicle:"pii"`
}

func TestReleaseNeverUsesIndependentDeclaredIDAsSubject(t *testing.T) {
	model, err := readmodels.Define[protectedAliasModel](readmodels.WithSubjectProperty("owner"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, document string }{
		{"missing root", `{"ID":"external","value":"ciphertext"}`},
		{"null root", `{"Id":null,"ID":"external","value":"ciphertext"}`},
		{"empty root", `{"Id":"","ID":"external","value":"ciphertext"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			service, ctx := serviceFixture(t, &modelKernel{release: func(_ context.Context, request *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
				calls.Add(1)
				return &compliance.ReleaseResponse{Payload: request.Payload}, nil
			}}, model.Descriptor())
			got, err := service.Release(ctx, model.Identifier(), json.RawMessage(tc.document))
			if got != nil || !errors.Is(err, readmodels.ErrRelease) {
				t.Errorf("Release = %s, %v; want fail closed", got, err)
			}
			if calls.Load() != 0 {
				t.Errorf("release dispatched %d RPCs without an owner", calls.Load())
			}
		})
	}
	t.Run("typed empty root", func(t *testing.T) {
		var calls atomic.Int32
		service, ctx := serviceFixture(t, &modelKernel{release: func(_ context.Context, request *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
			calls.Add(1)
			return &compliance.ReleaseResponse{Payload: request.Payload}, nil
		}}, model.Descriptor())
		got, err := readmodels.For(service, model).Release(ctx, protectedAliasModel{ExternalID: "external", Value: "ciphertext"})
		if !reflect.DeepEqual(got, protectedAliasModel{}) || !errors.Is(err, readmodels.ErrRelease) || calls.Load() != 0 {
			t.Fatalf("typed Release = %+v, %v; calls %d", got, err, calls.Load())
		}
	})
}

func TestReleaseExplicitSubjectCanSelectIndependentID(t *testing.T) {
	model, err := readmodels.Define[protectedAliasModel](readmodels.WithSubjectProperty("ID"))
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	service, ctx := serviceFixture(t, &modelKernel{release: func(_ context.Context, request *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
		calls.Add(1)
		if request.Subject != "external" {
			t.Error("explicit subject selection was lost")
		}
		return &compliance.ReleaseResponse{Payload: request.Payload}, nil
	}}, model.Descriptor())
	got, err := service.Release(ctx, model.Identifier(), json.RawMessage(`{"ID":"external","value":"ciphertext"}`))
	if got == nil || err != nil || calls.Load() != 1 {
		t.Fatalf("explicit ID subject = %s, %v; calls %d", got, err, calls.Load())
	}
}

func TestReleaseAliasFilteringPreservesSubjectAndLineagePrecedence(t *testing.T) {
	model, err := readmodels.Define[protectedAliasModel](readmodels.WithSubjectProperty("owner"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, document string
		subjects       []string
	}{
		{"explicit subject", `{"ID":"external","owner":"explicit","value":"ciphertext"}`, []string{"explicit"}},
		{"stored subject", `{"Id":null,"ID":"external","owner":"explicit","__subject":"stored","value":"ciphertext"}`, []string{"stored"}},
		{"per-property lineage", `{"Id":"","ID":"external","owner":"explicit","__subject":"stored","__subjects":{"value":"nested"},"value":"ciphertext"}`, []string{"nested", "stored"}},
		{"per-property owner without root", `{"ID":"external","__subjects":{"value":"nested"},"value":"ciphertext"}`, []string{"", "nested"}},
		{"genuine sink alias", `{"_id":"sink-owner","ID":"external","value":"ciphertext"}`, []string{"sink-owner"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			subjects := make(chan string, 4)
			service, ctx := serviceFixture(t, &modelKernel{release: func(_ context.Context, request *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
				subjects <- request.Subject
				return &compliance.ReleaseResponse{Payload: request.Payload}, nil
			}}, model.Descriptor())
			got, err := service.Release(ctx, model.Identifier(), json.RawMessage(tc.document))
			if got == nil || err != nil {
				t.Fatalf("Release = %s, %v", got, err)
			}
			var actual []string
			for len(subjects) != 0 {
				actual = append(actual, <-subjects)
			}
			slices.Sort(actual)
			if !slices.Equal(actual, tc.subjects) {
				t.Fatalf("release subjects = %v, want %v", actual, tc.subjects)
			}
			assertReleaseJSON(t, string(got), tc.document)
		})
	}
}
