// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/transactions"
)

func originResolverFixture(t *testing.T, resolver eventsequences.AppendOriginResolver, policies ...eventsequences.ConcurrencyPolicy) (*eventsequences.Sequence, *atomic.Int32) {
	t.Helper()
	definition, err := events.Define[opened](events.WithID("opened"))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(definition.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	handlers := make(map[string]rpcHandler)
	for _, method := range []string{"Append", "AppendWithNamedTags", "AppendMany", "AppendManyWithNamedTags", "AppendManyForEventSources", "AppendManyForEventSourcesWithNamedTags"} {
		single := method == "Append" || method == "AppendWithNamedTags"
		handlers[method] = func(context.Context, any) (any, error) {
			if single {
				return notificationResponse(true, "committed", metadata.CorrelationID{})
			}
			return batchSuccess(metadata.CorrelationID{}, true, 1), nil
		}
	}
	var policy eventsequences.ConcurrencyPolicy
	if len(policies) > 0 {
		policy = policies[0]
	}
	return parityFixture(t, handlers, catalog, policy, resolver)
}

type originAppendPath struct {
	kind                    string
	named, metadata, routed bool
}

func originAppendPaths() []originAppendPath {
	var result []originAppendPath
	for _, kind := range []string{"single", "many", "batch", "prepared"} {
		for _, named := range []bool{false, true} {
			for _, wrapped := range []bool{false, true} {
				if kind == "prepared" && wrapped {
					continue
				}
				result = append(result, originAppendPath{kind: kind, named: named, metadata: wrapped})
				if kind == "many" {
					result = append(result, originAppendPath{kind: kind, named: named, metadata: wrapped, routed: true})
				}
			}
		}
	}
	return result
}

func (p originAppendPath) append(ctx context.Context, s *eventsequences.Sequence) (eventsequences.Disposition, error) {
	scope := eventsequences.Scope{Expectation: eventsequences.NoCheck()}
	options := []eventsequences.AppendOption{eventsequences.WithScope(scope), eventsequences.WithTags("static")}
	batchOptions := []eventsequences.BatchOption{eventsequences.WithScopes(eventsequences.LabeledScope{Label: "A", Scope: scope}), eventsequences.WithBatchTags("static")}
	if p.named {
		options = append(options, eventsequences.WithNamedTags(events.NamedTag{Name: "tag", Value: "value"}))
		batchOptions = append(batchOptions, eventsequences.WithBatchNamedTags(events.NamedTag{Name: "tag", Value: "value"}))
	}
	if p.routed {
		options = append(options, eventsequences.WithRoute(eventsequences.Route{StreamID: "audit"}))
	}
	entries := []eventsequences.Entry{{Source: "A", Event: opened{}}}
	switch p.kind {
	case "single":
		if p.metadata {
			r, err := s.AppendWithMetadata(ctx, "A", opened{}, options...)
			return r.Result().Disposition, err
		}
		r, err := s.Append(ctx, "A", opened{}, options...)
		return r.Disposition, err
	case "many":
		if p.metadata {
			r, err := s.AppendManyWithMetadata(ctx, "A", []any{opened{}}, options...)
			return r.Result().Disposition, err
		}
		r, err := s.AppendMany(ctx, "A", []any{opened{}}, options...)
		return r.Disposition, err
	case "prepared":
		b, err := s.PrepareBatch(ctx, entries, batchOptions...)
		if err != nil {
			return eventsequences.Rejected, err
		}
		r, err := s.AppendPreparedBatch(ctx, b)
		return r.Disposition, err
	default:
		if p.metadata {
			r, err := s.AppendBatchWithMetadata(ctx, entries, batchOptions...)
			return r.Result().Disposition, err
		}
		r, err := s.AppendBatch(ctx, entries, batchOptions...)
		return r.Disposition, err
	}
}

func TestAppendOriginResolverEveryVariantOnceWithActualContext(t *testing.T) {
	for _, path := range originAppendPaths() {
		t.Run(fmt.Sprintf("%+v", path), func(t *testing.T) {
			ctx := eventsequences.WithOrigin(testContext(t), eventsequences.NewOrigin())
			origin := eventsequences.NewOrigin()
			var resolutions int
			s, calls := originResolverFixture(t, func(actual context.Context) (eventsequences.Origin, bool, error) {
				resolutions++
				if actual != ctx {
					t.Error("resolver did not receive the actual call context")
				}
				return origin, true, nil
			})
			var received []eventsequences.AppendNotification
			defer s.OnAppend(func(n eventsequences.AppendNotification) { received = append(received, n) })()
			disposition, err := path.append(ctx, s)
			if err != nil || disposition != eventsequences.Committed || resolutions != 1 || calls.Load() != 1 || len(received) != 1 || received[0].Origin != origin {
				t.Fatalf("disposition=%v err=%v resolutions=%d RPCs=%d notifications=%+v", disposition, err, resolutions, calls.Load(), received)
			}
		})
	}
}

func TestAppendOriginResolverHandledZeroAndUnhandledFallback(t *testing.T) {
	inherited, selected := eventsequences.NewOrigin(), eventsequences.NewOrigin()
	for _, tc := range []struct {
		name     string
		returned eventsequences.Origin
		handled  bool
		want     eventsequences.Origin
	}{
		{"override inherited", selected, true, selected},
		{"mask inherited", eventsequences.Origin{}, true, eventsequences.Origin{}},
		{"fallback inherited", selected, false, inherited},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := originResolverFixture(t, func(context.Context) (eventsequences.Origin, bool, error) { return tc.returned, tc.handled, nil })
			var got eventsequences.Origin
			defer s.OnAppend(func(n eventsequences.AppendNotification) { got = n.Origin })()
			ctx := eventsequences.WithOrigin(testContext(t), inherited)
			if _, err := s.Append(ctx, "A", opened{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()})); err != nil {
				t.Fatal(err)
			}
			if got != tc.want || eventsequences.OriginFrom(ctx) != inherited {
				t.Fatal("incorrect selection or caller context changed")
			}
		})
	}
}

func TestAppendOriginResolverFailuresRejectWithoutRPCNotificationOrPayload(t *testing.T) {
	for _, panicked := range []bool{false, true} {
		for _, path := range originAppendPaths() {
			t.Run(fmt.Sprintf("panic=%v/%+v", panicked, path), func(t *testing.T) {
				secret := "sensitive context payload"
				failure := errors.New(secret)
				var resolutions, notifications int
				s, calls := originResolverFixture(t, func(context.Context) (eventsequences.Origin, bool, error) {
					resolutions++
					if panicked {
						panic(secret)
					}
					return eventsequences.NewOrigin(), false, failure
				})
				defer s.OnAppend(func(eventsequences.AppendNotification) { notifications++ })()
				disposition, err := path.append(eventsequences.WithOrigin(testContext(t), eventsequences.NewOrigin()), s)
				var typed *eventsequences.AppendOriginResolutionError
				var unknown *eventsequences.OutcomeUnknownError
				if !errors.As(err, &typed) || typed.Panicked != panicked || errors.As(err, &unknown) || errors.Is(err, failure) || disposition != eventsequences.Rejected || calls.Load() != 0 || notifications != 0 || resolutions != 1 {
					t.Fatalf("disposition=%v err=%v resolutions=%d RPCs=%d notifications=%d", disposition, err, resolutions, calls.Load(), notifications)
				}
				if strings.Contains(fmt.Sprintf("%v %+v %#v", err, err, err), secret) || errors.Unwrap(err) != nil {
					t.Fatal("callback payload exposed")
				}
			})
		}
	}
}

func TestAppendOriginResolverFailureBeforeAutomaticScopeRPC(t *testing.T) {
	s, calls := originResolverFixture(t, func(context.Context) (eventsequences.Origin, bool, error) {
		return eventsequences.Origin{}, false, errors.New("private")
	})
	ctx := testContext(t)
	_, _ = s.Append(ctx, "A", opened{})
	_, _ = s.AppendMany(ctx, "A", []any{opened{}})
	_, _ = s.AppendBatch(ctx, []eventsequences.Entry{{Source: "A", Event: opened{}}})
	batch, err := s.PrepareBatch(ctx, []eventsequences.Entry{{Source: "A", Event: opened{}}})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = s.AppendPreparedBatch(ctx, batch)
	if calls.Load() != 0 {
		t.Fatalf("RPCs = %d", calls.Load())
	}
}

func TestPreparedAppendOriginResolverUsesDispatchContextOnly(t *testing.T) {
	var resolutions int
	var actual context.Context
	s, calls := originResolverFixture(t, func(ctx context.Context) (eventsequences.Origin, bool, error) {
		resolutions++
		actual = ctx
		return eventsequences.OriginFrom(ctx), true, nil
	})
	ctx := testContext(t)
	batch, err := s.PrepareBatch(eventsequences.WithOrigin(ctx, eventsequences.NewOrigin()), []eventsequences.Entry{{Source: "A", Event: opened{}}}, eventsequences.WithScopes(eventsequences.LabeledScope{Label: "A", Scope: eventsequences.Scope{Expectation: eventsequences.NoCheck()}}))
	if err != nil || resolutions != 0 || calls.Load() != 0 {
		t.Fatalf("preparation err=%v resolutions=%d RPCs=%d", err, resolutions, calls.Load())
	}
	var origins []eventsequences.Origin
	defer s.OnAppend(func(n eventsequences.AppendNotification) { origins = append(origins, n.Origin) })()
	for range 2 {
		origin := eventsequences.NewOrigin()
		dispatchCtx := eventsequences.WithOrigin(ctx, origin)
		if _, err := s.AppendPreparedBatch(dispatchCtx, batch); err != nil {
			t.Fatal(err)
		}
		if actual != dispatchCtx || origins[len(origins)-1] != origin {
			t.Fatal("preparation context leaked into resolver")
		}
	}
	if resolutions != 2 || calls.Load() != 2 {
		t.Fatal("prepared dispatch resolved more than once")
	}
}

func TestUnitCommitBypassesFailingExternalAppendOriginResolver(t *testing.T) {
	for _, panicked := range []bool{false, true} {
		t.Run(fmt.Sprintf("panic=%v", panicked), func(t *testing.T) {
			var resolutions int
			s, calls := originResolverFixture(t, func(context.Context) (eventsequences.Origin, bool, error) {
				resolutions++
				if panicked {
					panic("external resolver must not run")
				}
				return eventsequences.Origin{}, false, errors.New("external resolver must not run")
			})
			ctx := eventsequences.WithOrigin(testContext(t), eventsequences.NewOrigin())
			unit, owner, err := transactions.Begin(ctx, s)
			if err != nil {
				t.Fatal(err)
			}
			if err = unit.Stage(ctx, []eventsequences.Entry{{Source: "A", Event: opened{}}}, eventsequences.LabeledScope{Label: "A", Scope: eventsequences.Scope{Expectation: eventsequences.NoCheck()}}); err != nil {
				t.Fatal(err)
			}
			var received []eventsequences.AppendNotification
			defer s.OnAppend(func(n eventsequences.AppendNotification) { received = append(received, n) })()
			result, err := owner.Commit(eventsequences.WithOrigin(ctx, eventsequences.NewOrigin()))
			if err != nil || result.Disposition != eventsequences.Committed || resolutions != 0 || calls.Load() != 1 || len(received) != 1 || received[0].Origin != unit.Origin() {
				t.Fatalf("result=%+v err=%v resolutions=%d RPCs=%d notifications=%+v", result, err, resolutions, calls.Load(), received)
			}
			// Possessing the public unit origin is not a completion capability.
			_, err = s.Append(eventsequences.WithOrigin(ctx, unit.Origin()), "A", opened{})
			var failure *eventsequences.AppendOriginResolutionError
			if !errors.As(err, &failure) || resolutions != 1 || calls.Load() != 1 || len(received) != 1 {
				t.Fatal("public WithOrigin bypassed the external resolver")
			}
		})
	}
}

func TestUnitOriginCapabilityCannotAuthorizeOtherAppendsFromCallbackContext(t *testing.T) {
	var commitCtx context.Context
	var resolutions int
	policy := eventsequences.ConcurrencyPolicy{Strategy: strategyFunc(func(ctx context.Context, _ *eventsequences.Sequence, _ eventsequences.ScopeFilter) (eventsequences.Scope, error) {
		commitCtx = ctx
		return eventsequences.Scope{Expectation: eventsequences.NoCheck()}, nil
	})}
	s, calls := originResolverFixture(t, func(context.Context) (eventsequences.Origin, bool, error) {
		resolutions++
		return eventsequences.Origin{}, false, errors.New("private failure")
	}, policy)
	ctx := testContext(t)
	unit, owner, err := transactions.Begin(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	entries := []eventsequences.Entry{{Source: "A", Event: opened{}}}
	if err = unit.Stage(ctx, entries); err != nil {
		t.Fatal(err)
	}
	if _, err = owner.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if commitCtx == nil || calls.Load() != 1 || resolutions != 0 {
		t.Fatal("unit commit did not bypass resolution")
	}
	// A custom strategy can retain its context, but it cannot turn that into
	// owner authority for immediate writes or caller-owned prepared batches.
	_, immediateErr := s.Append(commitCtx, "A", opened{})
	other, err := s.PrepareBatch(ctx, entries)
	if err != nil {
		t.Fatal(err)
	}
	_, preparedErr := s.AppendPreparedBatch(commitCtx, other)
	for _, failure := range []error{immediateErr, preparedErr} {
		var typed *eventsequences.AppendOriginResolutionError
		if !errors.As(failure, &typed) {
			t.Fatalf("reused context bypassed resolution: %v", failure)
		}
	}
	if calls.Load() != 1 || resolutions != 2 {
		t.Fatalf("RPCs=%d resolutions=%d", calls.Load(), resolutions)
	}
}

type commandOriginKey struct{}
type nestedAppendKey struct{}

func TestConcurrentSameCorrelationCommandsResolveDistinctOriginsAndReenter(t *testing.T) {
	var resolutions atomic.Int32
	var s *eventsequences.Sequence
	var callback eventsequences.AppendOriginResolver = func(ctx context.Context) (eventsequences.Origin, bool, error) {
		resolutions.Add(1)
		// Subscription operations and a nested append must not deadlock resolver
		// invocation. Mark only this test's nested call to bound recursion.
		s.OnAppend(func(eventsequences.AppendNotification) {})()
		if ctx.Value(nestedAppendKey{}) == nil {
			if _, err := s.Append(context.WithValue(ctx, nestedAppendKey{}, true), "A", opened{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()})); err != nil {
				return eventsequences.Origin{}, false, err
			}
		}
		origin, ok := ctx.Value(commandOriginKey{}).(eventsequences.Origin)
		return origin, ok, nil
	}
	var calls *atomic.Int32
	s, calls = originResolverFixture(t, callback)
	correlation, err := metadata.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	ctx := metadata.WithCorrelation(eventsequences.WithOrigin(testContext(t), eventsequences.NewOrigin()), correlation)
	var mu sync.Mutex
	received := make(map[eventsequences.Origin]int)
	defer s.OnAppend(func(n eventsequences.AppendNotification) {
		mu.Lock()
		defer mu.Unlock()
		if n.CorrelationID != correlation {
			t.Error("correlation was changed")
		}
		received[n.Origin]++
	})()
	const commands = 16
	origins := make([]eventsequences.Origin, commands)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i := range commands {
		origins[i] = eventsequences.NewOrigin()
		workers.Go(func() {
			<-start
			commandCtx := context.WithValue(ctx, commandOriginKey{}, origins[i])
			if _, err := s.Append(commandCtx, "A", opened{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()})); err != nil {
				t.Error(err)
			}
		})
	}
	close(start)
	workers.Wait()
	if calls.Load() != commands*2 || resolutions.Load() != commands*2 || len(received) != commands {
		t.Fatalf("RPCs=%d resolutions=%d origins=%d", calls.Load(), resolutions.Load(), len(received))
	}
	for _, origin := range origins {
		if received[origin] != 2 {
			t.Fatalf("origin %v notifications=%d, want 2", origin, received[origin])
		}
	}
}
