//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"errors"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/patterns"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestKernelPatternsQueries asks a fresh namespace only through the public typed
// facade. No events, local inference, fake provider or kernel storage mutation
// manufacture established behavior. Empty answers are the honest expected result.
func TestKernelPatternsQueries(t *testing.T) {
	f := newKernelFixture(t)
	client := f.client(chronicle.NewRegistry())
	store, err := client.EventStore(f.ctx, f.storeName, chronicle.WithNamespace("patterns"))
	if err != nil {
		t.Fatal(err)
	}
	service := store.Patterns()
	moment := time.Date(2024, 1, 15, 9, 30, 0, 0, time.FixedZone("", 2*3600))
	queries := []struct {
		name string
		call func() (int, bool, error)
	}{
		{"MatchingPatterns", func() (int, bool, error) {
			result, err := service.GetPatterns(f.ctx, "test-scope", patterns.FacetSet{}, patterns.QueryOptions{})
			return len(result.Data), result.Data != nil, err
		}},
		{"UsualActions", func() (int, bool, error) {
			result, err := service.GetUsualActions(f.ctx, "test-scope", patterns.FacetSet{}, patterns.QueryOptions{})
			return len(result.Data), result.Data != nil, err
		}},
		{"At", func() (int, bool, error) {
			result, err := service.GetPatternsAt(f.ctx, "test-scope", &moment, patterns.FacetSet{}, patterns.QueryOptions{})
			return len(result.Data), result.Data != nil, err
		}},
		{"PatternsForScope", func() (int, bool, error) {
			result, err := service.GetPatternsForScope(f.ctx, "test-scope")
			return len(result.Data), result.Data != nil, err
		}},
		{"AllPatternScopes", func() (int, bool, error) {
			result, err := service.GetScopes(f.ctx)
			return len(result.Data), result.Data != nil, err
		}},
	}
	for _, query := range queries {
		t.Run(query.name, func(t *testing.T) {
			count, present, err := query.call()
			if err != nil {
				var unsupported *patterns.UnsupportedError
				if !errors.As(err, &unsupported) || !errors.Is(err, patterns.ErrUnsupported) || status.Code(unsupported.Cause) != codes.Unimplemented {
					t.Fatal(err)
				}
				t.Fatal("pinned kernel query availability regressed: typed UnsupportedError, raw status Unimplemented")
			}
			if count != 0 || !present {
				t.Fatalf("fresh namespace answer: count=%d, non-nil data=%v", count, present)
			}
			t.Log("actual typed kernel response: empty established behavior")
		})
	}
}
