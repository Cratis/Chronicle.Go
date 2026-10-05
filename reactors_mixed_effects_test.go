// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"slices"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/reactors"
)

func TestMixedCustomAndEventEffectsArePreflightedInOrder(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		r := reactorRegistry(t)
		var trace []string
		if err := chronicle.RegisterReactorSideEffectHandler(r, &OrderedCommands{trace: &trace}); err != nil {
			t.Fatal(err)
		}
		values := []any{ReactorOutput{1}, ReturnedCommand{2}, ReactorOutput{3}}
		if unknown {
			values = append(values, "unclaimed")
		}
		registerEffect(t, r, values)
		k := &reactorKernel{}
		k.append = func(_ context.Context, request *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
			trace = append(trace, request.Content)
			return success(request, 0), nil
		}
		_, _, ctx := reactorClient(t, k, r)
		s := receive(t, ctx, k.sessions)
		s.batches <- batch(0)
		result := receive(t, ctx, s.results)
		want := []string{"classify-2", `{"number":1}`, "execute-2", `{"number":3}`}
		if unknown {
			want = []string{"classify-2"}
		}
		if !slices.Equal(trace, want) || (result.State == contracts.ObservationState_Failed) != unknown {
			t.Fatal(result, trace)
		}
	}
}

func TestCustomHandlerAndBuiltinBothRunWhenMatching(t *testing.T) {
	r := reactorRegistry(t)
	var trace []string
	registerEffect(t, r, ReactorOutput{1}, reactors.WithSideEffectHandlers(&CommandEffects{name: "also", trace: &trace, claimEvent: true}))
	k := &reactorKernel{}
	k.append = func(_ context.Context, request *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
		trace = append(trace, "append")
		return success(request, 0), nil
	}
	_, _, ctx := reactorClient(t, k, r)
	s := receive(t, ctx, k.sessions)
	s.batches <- batch(0)
	result := receive(t, ctx, s.results)
	if result.State != contracts.ObservationState_Success || !slices.Equal(trace, []string{"classify-also", "append", "handle-also"}) {
		t.Fatal(result, trace)
	}
}
