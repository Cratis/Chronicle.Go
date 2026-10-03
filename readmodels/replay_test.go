// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/readmodels"
)

func TestProjectionReplayIsBoundedAndNormalizesKeysWithoutPartialResults(t *testing.T) {
	model := person(t)
	descriptor, err := readmodels.BindProjection(model.Descriptor(), "projection", "catalog", false)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	kernel := &modelKernel{replay: func(_ context.Context, r *contracts.GetAllInstancesRequest) (*contracts.GetAllInstancesResponse, error) {
		calls++
		if r.EventStore != "store" || r.Namespace != "tenant-a" || r.EventSequenceId != "catalog" || r.EventCount != 7 {
			t.Errorf("replay coordinates: %+v", r)
		}
		if calls == 1 {
			return &contracts.GetAllInstancesResponse{Instances: []string{`{"_id":"one","name":"Ada"}`}}, nil
		}
		return &contracts.GetAllInstancesResponse{Instances: []string{`{"_id":"one"}`, `null`}}, nil
	}}
	service, ctx := serviceFixture(t, kernel, descriptor)
	values, err := service.ReplayProjection(ctx, model.Identifier(), 0)
	if err != nil || values == nil || len(values) != 0 || calls != 0 {
		t.Fatalf("empty replay dispatched an RPC: %s %v calls=%d", values, err, calls)
	}
	for _, count := range []uint64{math.MaxInt32 + 1, 1 << 32, math.MaxUint64} {
		values, err = service.ReplayProjection(ctx, model.Identifier(), count)
		if !errors.Is(err, chronicle.ErrInvalidConfiguration) || values != nil || calls != 0 {
			t.Fatalf("out-of-range replay %d: %s %v calls=%d", count, values, err, calls)
		}
	}
	values, err = service.ReplayProjection(ctx, model.Identifier(), 7)
	if err != nil || len(values) != 1 || !strings.Contains(string(values[0]), `"id":"one"`) {
		t.Fatalf("replay: %s %v", values, err)
	}
	values, err = service.ReplayProjection(ctx, model.Identifier(), 7)
	if !errors.Is(err, chronicle.ErrProtocol) || values != nil {
		t.Fatalf("malformed partial result: %s %v", values, err)
	}
	if _, err = service.ReplayProjection(ctx, model.Identifier(), ^uint64(0)); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatalf("unlimited replay: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = service.ReplayProjection(canceled, model.Identifier(), 7); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if calls != 2 {
		t.Fatalf("invalid request dispatched %d calls", calls)
	}
}
