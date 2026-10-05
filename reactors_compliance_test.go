// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
)

type ProtectedReactorOutput struct {
	Owner string `chronicle:"subject"`
	Name  string `chronicle:"pii"`
}

func TestReactorEffectsRejectReservedSubjectsBeforeWrites(t *testing.T) {
	reserved := events.Subject("$chronicle-encrypted-value$namespace$")
	for _, path := range []string{"explicit", "tag", "resolver", "source", "many", "batch"} {
		t.Run(path, func(t *testing.T) {
			r := reactorRegistry(t)
			var options []events.TypeOption
			if path == "resolver" {
				options = append(options, events.WithSubjectResolver(func(ProtectedReactorOutput) (events.Subject, bool) { return reserved, true }))
			}
			if _, err := chronicle.RegisterEvent[ProtectedReactorOutput](r, options...); err != nil {
				t.Fatal(err)
			}
			value := ProtectedReactorOutput{Owner: string(reserved), Name: "fixture"}
			entry := eventsequences.Entry{Source: "target", Event: ProtectedReactorOutput{Name: "fixture"}}
			switch path {
			case "explicit":
				entry.Subject = &reserved
				registerEffect(t, r, entry)
			case "source":
				entry.Source = events.SourceID(reserved)
				registerEffect(t, r, entry)
			case "many":
				registerEffect(t, r, []ProtectedReactorOutput{{Owner: "ordinary"}, value})
			case "batch":
				registerEffect(t, r, []eventsequences.Entry{{Source: "ordinary", Event: ProtectedReactorOutput{Owner: "ordinary"}}, {Source: "target", Event: value}})
			default:
				registerEffect(t, r, value)
			}
			k := &reactorKernel{}
			var writes atomic.Int32
			k.append = func(context.Context, *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
				writes.Add(1)
				return &sequences.CommandResult_AppendResponse{}, nil
			}
			k.appendMany = func(*sequences.AppendManyRequest) *sequences.CommandResult_AppendManyResponse {
				writes.Add(1)
				return batchSuccess(2)
			}
			k.appendBatch = func(*sequences.AppendManyForEventSourcesRequest) *sequences.CommandResult_AppendManyResponse {
				writes.Add(1)
				return batchSuccess(2)
			}
			_, _, ctx := reactorClient(t, k, r)
			s := receive(t, ctx, k.sessions)
			s.batches <- batch(0)
			result := receive(t, ctx, s.results)
			if result.State == contracts.ObservationState_Success || writes.Load() != 0 {
				t.Fatalf("invalid effect was admitted: state=%v writes=%d", result.State, writes.Load())
			}
		})
	}
}
