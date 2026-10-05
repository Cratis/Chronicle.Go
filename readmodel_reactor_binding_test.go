// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
)

type bindingModel struct{ ID string }
type bindingEffect struct{}
type bindingEffects struct {
	calls chan reactors.SideEffectContext
}

func (*bindingEffects) CanHandleReturnType(typ reflect.Type) bool {
	return typ == reflect.TypeFor[bindingEffect]()
}
func (*bindingEffects) CanHandle(_ reactors.SideEffectContext, value any) bool {
	_, ok := value.(bindingEffect)
	return ok
}
func (h *bindingEffects) Handle(_ context.Context, ctx reactors.SideEffectContext, _ any) error {
	h.calls <- ctx
	return nil
}

func TestReadModelReactorsBindCallbackAndEffectMetadataToSelectedStore(t *testing.T) {
	for _, selectedRegistry := range []bool{false, true} {
		for _, materialized := range []bool{false, true} {
			for _, storeName := range []StoreName{"orders", "consumer"} {
				t.Run(string(storeName)+map[bool]string{false: "/changes", true: "/materialized"}[materialized]+map[bool]string{false: "/default", true: "/selected"}[selectedRegistry], func(t *testing.T) {
					registry := NewRegistry()
					if _, err := RegisterEvent[OriginFirst](registry, events.WithSourceStore("orders")); err != nil {
						t.Fatal(err)
					}
					model, err := RegisterReadModel[bindingModel](registry)
					if err != nil {
						t.Fatal(err)
					}
					if err = RegisterReducerHandlers(registry, model, "fold", []reducers.Handler{reducers.On(func(context.Context, OriginFirst, *bindingModel, events.Context) (*bindingModel, error) {
						return nil, nil
					})}); err != nil {
						t.Fatal(err)
					}
					callbacks := make(chan events.Context, 1)
					effects := &bindingEffects{calls: make(chan reactors.SideEffectContext, 1)}
					if err = RegisterReactorSideEffectHandler(registry, effects); err != nil {
						t.Fatal(err)
					}
					options := []reactors.ReadModelOption{reactors.WithReadModelErrorHandler(func(_ context.Context, err error) { t.Error(err) })}
					if materialized {
						options = append(options, reactors.Materialized(nil))
					}
					if err = RegisterReadModelReactorHandlers(registry, "callback", model, []reactors.ReadModelHandler{reactors.ReadModelOn(readmodels.Added, func(_ *bindingModel, ctx events.Context) bindingEffect {
						callbacks <- ctx
						return bindingEffect{}
					})}, options...); err != nil {
						t.Fatal(err)
					}
					clientOptions := []ClientOption{WithRegistry(registry)}
					if selectedRegistry {
						clientOptions = []ClientOption{WithRegistryForStore(storeName, registry)}
					}
					outer, kernel := newModelReactorKernel()
					client, ctx := supervisionClient(t, outer, clientOptions...)
					snapshot, err := client.selectedStoreSnapshot(storeName)
					if err != nil {
						t.Fatal(err)
					}
					store := &EventStore{storeOwner: &storeOwner{client: client, name: storeName, namespace: DefaultNamespace}}
					if err = store.initializeReadModelsFromSnapshot(snapshot); err != nil {
						t.Fatal(err)
					}
					plan := store.readModelReactorPlans()[0]
					want := events.EventLog
					if storeName != "orders" {
						want = "inbox-orders"
					}
					if plan.Model().EventSequence() != want {
						t.Fatalf("plan sequence = %s, want %s", plan.Model().EventSequence(), want)
					}
					if materialized {
						if err = client.connect(ctx, false); err != nil {
							t.Fatal(err)
						}
						service, err := readmodels.New(storeName, DefaultNamespace, snapshot.models, client.transport)
						if err != nil {
							t.Fatal(err)
						}
						runCtx, cancel := context.WithCancel(ctx)
						defer cancel()
						kernel.windows <- &contracts.ObserveInstancesResponse{Instances: []string{`{"id":"item"}`}}
						windows, err := service.Materialized().ObserveInstances(runCtx, model.Identifier(), nil)
						if err != nil {
							t.Fatal(err)
						}
						run := &readModelReactorRun{store: store, plan: plan, windows: windows}
						done := make(chan error, 1)
						go func() { done <- run.Run(runCtx) }()
						t.Cleanup(func() {
							cancel()
							timer := time.NewTimer(2 * time.Second)
							defer timer.Stop()
							select {
							case <-done:
							case <-timer.C:
								t.Error("materialized dispatch did not join")
							}
						})
					} else {
						change := readmodels.Change[json.RawMessage]{Type: readmodels.Added, HasValue: true, Value: json.RawMessage(`{"id":"item"}`), Context: events.Context{Store: storeName, Namespace: DefaultNamespace, Sequence: want, SourceID: "item"}}
						if err = plan.Dispatch(ctx, change, reactorStoreRuntime{store}); err != nil {
							t.Fatal(err)
						}
					}
					select {
					case got := <-callbacks:
						if got.Sequence != want || got.Store != storeName || got.Namespace != DefaultNamespace {
							t.Fatalf("callback metadata = %+v", got)
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					select {
					case got := <-effects.calls:
						if got.Delivery.Sequence != want || got.Delivery.Store != storeName || got.Delivery.Namespace != DefaultNamespace {
							t.Fatalf("effect delivery = %+v", got.Delivery)
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					frozen := client.readModelReactors.defaults
					if selectedRegistry {
						frozen = client.readModelReactors.stores[storeName]
					}
					if frozen[0].Model().EventSequence() != "inbox-orders" || frozen[0] == plan {
						t.Fatal("store binding mutated or reused the compiled plan")
					}
				})
			}
		}
	}
}
