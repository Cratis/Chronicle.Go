//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	compliancecontracts "github.com/cratis/chronicle.go/contracts/compliance"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
	"google.golang.org/grpc"
)

type ReleaseRouteChanged struct {
	Owner  string `chronicle:"subject"`
	Name   string `json:"name" chronicle:"pii"`
	Secret string `json:"secret" chronicle:"encrypted(scope=namespace)"`
}
type ReleaseRouteTicked struct{ Tick int32 }
type ReleaseRouteDefault struct {
	ID     string
	Owner  string `chronicle:"subject;set(ReleaseRouteChanged)"`
	Name   string `json:"name" chronicle:"pii;set(ReleaseRouteChanged)"`
	Secret string `json:"secret" chronicle:"encrypted(scope=namespace);set(ReleaseRouteChanged)"`
	Tick   int32  `chronicle:"set(ReleaseRouteTicked)"`
}
type ReleaseRouteLower struct {
	ID     string `json:"id" chronicle:"key"`
	Owner  string `chronicle:"subject;set(ReleaseRouteChanged)"`
	Name   string `json:"name" chronicle:"pii;set(ReleaseRouteChanged)"`
	Secret string `json:"secret" chronicle:"encrypted(scope=namespace);set(ReleaseRouteChanged)"`
	Tick   int32  `chronicle:"set(ReleaseRouteTicked)"`
}
type ReleaseRouteReduced struct {
	ID     string `json:"id"`
	Owner  string `chronicle:"subject"`
	Name   string `json:"name" chronicle:"pii"`
	Secret string `json:"secret" chronicle:"encrypted(scope=namespace)"`
	Tick   int32
}

// This wrapper observes the public RPC boundary before SDK decoding. It neither
// rewrites payloads nor bypasses registration, server release, or SDK admission.
// Only synthetic fixture documents are retained, never credentials or metadata.
type releaseRouteTransport struct {
	grpc.ClientConnInterface
	releases  atomic.Int32
	mu        sync.Mutex
	documents []releaseRouteDocument
}

type releaseRouteDocument struct{ route, data string }

func (c *releaseRouteTransport) record(message any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch v := message.(type) {
	case *contracts.ReadModelChangeset:
		if !v.Subscribed {
			c.documents = append(c.documents, releaseRouteDocument{"watch", v.ReadModel})
		}
	case *contracts.GetInstancesResponse:
		for _, data := range v.Instances {
			c.documents = append(c.documents, releaseRouteDocument{"get-window", data})
		}
	case *contracts.ObserveInstancesResponse:
		for _, data := range v.Instances {
			c.documents = append(c.documents, releaseRouteDocument{"observe-window", data})
		}
	}
}
func (c *releaseRouteTransport) Invoke(ctx context.Context, method string, args, reply any, options ...grpc.CallOption) error {
	if method == compliancecontracts.Compliance_Release_FullMethodName {
		c.releases.Add(1)
	}
	err := c.ClientConnInterface.Invoke(ctx, method, args, reply, options...)
	if err == nil {
		c.record(reply)
	}
	return err
}
func (c *releaseRouteTransport) NewStream(ctx context.Context, descriptor *grpc.StreamDesc, method string, options ...grpc.CallOption) (grpc.ClientStream, error) {
	stream, err := c.ClientConnInterface.NewStream(ctx, descriptor, method, options...)
	if err != nil {
		return nil, err
	}
	return &releaseRouteStream{ClientStream: stream, owner: c}, nil
}

type releaseRouteStream struct {
	grpc.ClientStream
	owner *releaseRouteTransport
}

func (s *releaseRouteStream) RecvMsg(message any) error {
	err := s.ClientStream.RecvMsg(message)
	if err == nil {
		s.owner.record(message)
	}
	return err
}

func TestKernelReleaseRouteProfiles(t *testing.T) {
	t.Run("default_Id", func(t *testing.T) { releaseRouteProfile[ReleaseRouteDefault](t) })
	t.Run("lowercase_id", func(t *testing.T) { releaseRouteProfile[ReleaseRouteLower](t) })
}

func releaseRouteProfile[T any](t *testing.T) {
	t.Helper()
	f := newKernelFixture(t)
	r := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[ReleaseRouteChanged](r); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[ReleaseRouteTicked](r); err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[T](r)
	if err != nil {
		t.Fatal(err)
	}
	reduced, err := chronicle.RegisterReadModel[ReleaseRouteReduced](r)
	if err != nil {
		t.Fatal(err)
	}
	folds := make(chan ReleaseRouteReduced, 8)
	if err := chronicle.RegisterReducerHandlers(r, reduced, "release-route-reducer", []reducers.Handler{
		reducers.On(func(_ context.Context, e ReleaseRouteChanged, _ *ReleaseRouteReduced, ec events.Context) (*ReleaseRouteReduced, error) {
			v := &ReleaseRouteReduced{ID: string(ec.SourceID), Owner: e.Owner, Name: e.Name, Secret: e.Secret}
			folds <- *v
			return v, nil
		}),
		reducers.On(func(_ context.Context, e ReleaseRouteTicked, current *ReleaseRouteReduced, _ events.Context) (*ReleaseRouteReduced, error) {
			if current == nil {
				return nil, nil
			}
			v := *current
			v.Tick = e.Tick
			folds <- v
			return &v, nil
		}),
	}); err != nil {
		t.Fatal(err)
	}
	store, err := f.client(r).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	// The reducer stream is open, but the kernel subscribes it asynchronously.
	// Appending first would start a reducer catch-up for "source-not-owner" that
	// drops the projection's later Ticked event (https://github.com/Cratis/Chronicle/issues/4558).
	awaitObserversObserving(t, f, store.Namespace(), append([]string{"release-route-reducer", string(model.Identifier())}, eventLogStatisticsObservers...)...)
	transport := &releaseRouteTransport{ClientConnInterface: f.conn}
	service, err := readmodels.New(f.storeName, store.Namespace(), store.ReadModels().Catalog(), transport)
	if err != nil {
		t.Fatal(err)
	}
	reader := readmodels.For(service, model)
	if sub, err := service.Watch(f.ctx, model.Identifier()); sub != nil || !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatal("raw protected watch admitted", err)
	}
	if sub, err := reader.Watch(f.ctx); sub != nil || !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatal("typed protected watch admitted", err)
	}
	if sub, err := store.ReadModels().Watch(f.ctx, reduced.Identifier()); sub != nil || !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatal("raw classified local watch admitted", err)
	}
	if sub, err := readmodels.For(store.ReadModels(), reduced).Watch(f.ctx); sub != nil || !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatal("typed classified local watch admitted", err)
	}
	// Public raw RPC characterizes why SDK admission refuses this route. This
	// is not a private release bypass and its output is never called safe.
	watchContext, cancelWatch := context.WithCancel(f.ctx)
	defer cancelWatch()
	rawWatch, err := contracts.NewReadModelsClient(transport).Watch(watchContext, &contracts.WatchRequest{EventStore: string(f.storeName), Namespace: string(store.Namespace()), ReadModelIdentifier: string(model.Identifier()), EventSequenceId: "event-log"})
	if err != nil {
		t.Fatal(err)
	}
	ack, err := rawWatch.Recv()
	if err != nil || !ack.Subscribed {
		t.Fatal("watch readiness", err)
	}
	plaintext := base64.StdEncoding.EncodeToString(make([]byte, 256))
	for _, erased := range []bool{false, true} {
		want := plaintext
		if !erased {
			appendSuccessfully(t, f.ctx, store, "source-not-owner", ReleaseRouteChanged{Owner: "owner", Name: plaintext, Secret: plaintext})
		} else {
			if err := store.Compliance().ErasePII(f.ctx, "owner"); err != nil {
				t.Fatal(err)
			}
			want = ""
			appendSuccessfully(t, f.ctx, store, "source-not-owner", ReleaseRouteTicked{Tick: 1})
		}
		awaitHistoryCollection(t, f.ctx, readmodels.For(store.ReadModels(), model), func(c readmodels.Collection[T]) bool {
			if len(c.Instances) != 1 {
				return false
			}
			data, marshalErr := model.Descriptor().Marshal(c.Instances[0].Value)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			var fields struct{ Tick int32 }
			if json.Unmarshal(data, &fields) != nil {
				t.Fatal("invalid model")
			}
			return !erased || fields.Tick == 1
		})
		if _, err := rawWatch.Recv(); err != nil {
			t.Fatal("raw watch witness", err)
		}
		select {
		case value := <-folds:
			checkReleaseRouteTyped(t, "actual successful fold", reduced.Descriptor(), value, nil, want, plaintext)
		case <-f.ctx.Done():
			t.Fatal("no actual reducer fold")
		}
		rawPage, pageErr := service.Materialized().GetInstances(f.ctx, model.Identifier(), nil)
		if pageErr != nil || len(rawPage) != 1 {
			t.Errorf("raw page: count=%d error=%v", len(rawPage), pageErr)
		} else {
			checkReleaseRouteValue(t, "raw page", rawPage[0], nil, want, plaintext)
		}
		typedPage, pageErr := reader.Materialized().GetInstances(f.ctx, nil)
		if pageErr != nil || len(typedPage) != 1 {
			t.Errorf("typed page: count=%d error=%v", len(typedPage), pageErr)
		} else {
			checkReleaseRouteTyped(t, "typed page", model.Descriptor(), typedPage[0], nil, want, plaintext)
		}
		rawWindow, windowErr := service.Materialized().ObserveInstances(f.ctx, model.Identifier(), nil)
		if windowErr != nil {
			t.Errorf("raw window: %v", windowErr)
		} else {
			values, recvErr := rawWindow.Recv()
			if recvErr != nil || len(values) != 1 {
				t.Errorf("raw window: count=%d error=%v", len(values), recvErr)
			} else {
				checkReleaseRouteValue(t, "raw window", values[0], nil, want, plaintext)
			}
			closeReleaseRoute(t, rawWindow)
		}
		typedWindow, windowErr := reader.Materialized().ObserveInstances(f.ctx, nil)
		if windowErr != nil {
			t.Errorf("typed window: %v", windowErr)
		} else {
			values, recvErr := typedWindow.Recv()
			if recvErr != nil || len(values) != 1 {
				t.Errorf("typed window: count=%d error=%v", len(values), recvErr)
			} else {
				checkReleaseRouteTyped(t, "typed window", model.Descriptor(), values[0], nil, want, plaintext)
			}
			closeReleaseRoute(t, typedWindow)
		}
		transport.mu.Lock()
		documents := append([]releaseRouteDocument(nil), transport.documents...)
		transport.documents = nil
		transport.mu.Unlock()
		if len(documents) != 5 {
			t.Errorf("captured public response documents=%d, want 5", len(documents))
		}
		for _, document := range documents {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal([]byte(document.data), &fields); err != nil {
				t.Fatal(err)
			}
			t.Logf("server %s erased=%t shape: %s", document.route, erased, releaseRouteShape(fields))
			if fields["__subject"] != nil || fields["__subjects"] != nil || fields["name"] == nil || fields["secret"] == nil {
				t.Fatal("release response shape witness changed")
			}
			if document.route == "watch" {
				// Chronicle#4566: lowercase identity and loaded-state updates
				// corrupt already-plaintext values before SDK decoding.
				watchName, watchSecret := plaintext, plaintext
				var zero T
				_, lowercase := any(zero).(ReleaseRouteLower)
				if erased || lowercase {
					watchName, watchSecret = "", ""
				}
				checkReleaseRouteValue(t, "unsupported server watch", json.RawMessage(document.data), nil, watchName, watchSecret)
			} else {
				checkReleaseRouteValue(t, "pre-decode server "+document.route, json.RawMessage(document.data), nil, want, plaintext)
			}
		}
		if calls := transport.releases.Swap(0); calls != 0 {
			t.Errorf("second Release RPCs=%d", calls)
		}
	}
}
func releaseRouteShape(fields map[string]json.RawMessage) string {
	shape := make(map[string]int, len(fields))
	for name, value := range fields {
		shape[name] = len(value)
	}
	data, _ := json.Marshal(shape)
	return string(data)
}

func closeReleaseRoute[T any](t *testing.T, subscription *readmodels.Subscription[T]) {
	t.Helper()
	if err := subscription.Close(); err != nil {
		t.Error(err)
	}
}
func checkReleaseRouteTyped(t *testing.T, route string, descriptor readmodels.Descriptor, value any, err error, name, secret string) {
	t.Helper()
	if err != nil {
		t.Errorf("%s: %v", route, err)
		return
	}
	data, err := descriptor.Marshal(value)
	checkReleaseRouteValue(t, route, data, err, name, secret)
}
func checkReleaseRouteValue(t *testing.T, route string, data json.RawMessage, err error, name, secret string) {
	t.Helper()
	if err != nil {
		t.Errorf("%s: %v", route, err)
		return
	}
	var value struct {
		Name   string `json:"name"`
		Secret string `json:"secret"`
	}
	if json.Unmarshal(data, &value) != nil {
		t.Errorf("%s: invalid document", route)
		return
	}
	if value.Name != name || value.Secret != secret {
		t.Errorf("%s: pii matches=%t confidentiality matches=%t (lengths %d/%d)", route, value.Name == name, value.Secret == secret, len(value.Name), len(value.Secret))
	}
}
