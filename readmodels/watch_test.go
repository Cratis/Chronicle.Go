// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/compliance"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

type watchKernel struct {
	contracts.UnimplementedReadModelsServer
	contracts.UnimplementedMaterializedReadModelsServer
	compliance.UnimplementedComplianceServer
	release   func(context.Context, *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error)
	watch     func(*contracts.WatchRequest, grpc.ServerStreamingServer[contracts.ReadModelChangeset]) error
	observe   func(*contracts.ObserveInstancesRequest, grpc.ServerStreamingServer[contracts.ObserveInstancesResponse]) error
	get       func(context.Context, *contracts.GetInstancesRequest) (*contracts.GetInstancesResponse, error)
	active    atomic.Int32
	getOne    func(context.Context, *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error)
	dehydrate func(context.Context, *contracts.DehydrateSessionRequest) (*emptypb.Empty, error)
}

func (k *watchKernel) GetInstanceByKey(ctx context.Context, r *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
	return k.getOne(ctx, r)
}
func (k *watchKernel) DehydrateSession(ctx context.Context, r *contracts.DehydrateSessionRequest) (*emptypb.Empty, error) {
	return k.dehydrate(ctx, r)
}

func (k *watchKernel) Watch(r *contracts.WatchRequest, stream grpc.ServerStreamingServer[contracts.ReadModelChangeset]) error {
	k.active.Add(1)
	defer k.active.Add(-1)
	return k.watch(r, stream)
}
func (k *watchKernel) ObserveInstances(r *contracts.ObserveInstancesRequest, stream grpc.ServerStreamingServer[contracts.ObserveInstancesResponse]) error {
	k.active.Add(1)
	defer k.active.Add(-1)
	return k.observe(r, stream)
}
func (k *watchKernel) GetInstances(ctx context.Context, r *contracts.GetInstancesRequest) (*contracts.GetInstancesResponse, error) {
	return k.get(ctx, r)
}

func (k *watchKernel) Release(ctx context.Context, r *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
	if err := validateReleaseRequest(r); err != nil {
		return nil, err
	}
	return k.release(ctx, r)
}

func watchFixture(t *testing.T, k *watchKernel, model readmodels.Descriptor, options ...readmodels.ServiceOption) (*readmodels.Service, context.Context) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.WaitForHandlers(true))
	contracts.RegisterReadModelsServer(server, k)
	contracts.RegisterMaterializedReadModelsServer(server, k)
	compliance.RegisterComplianceServer(server, k)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := server.Serve(listener); err != nil {
			t.Error(err)
		}
	}()
	conn, err := grpc.NewClient("passthrough:///watch", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
		server.Stop()
		_ = listener.Close()
		<-done
		if k.active.Load() != 0 {
			t.Error("leaked server handler")
		}
	})
	catalog, err := readmodels.NewCatalog(model)
	if err != nil {
		t.Fatal(err)
	}
	service, err := readmodels.New("store", "tenant-a", catalog, conn, options...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return service, ctx
}
func watchedPerson(t *testing.T) readmodels.Model[Person] {
	return person(t, readmodels.WithObserver(readmodels.Projection, "people"), readmodels.WithEventSequence("other-sequence"))
}
func sendChange(namespace string, kind contracts.ReadModelChangeType, value string) *contracts.ReadModelChangeset {
	return &contracts.ReadModelChangeset{Namespace: namespace, ModelKey: "person", ReadModel: value, ChangeType: kind, Removed: kind == contracts.ReadModelChangeType_Removed, EventSequenceNumber: 7, Occurred: &contracts.SerializableDateTimeOffset{Value: "2026-03-01T12:00:00.0000000+02:00"}}
}
func TestWatchReadinessOrderingAndInterruption(t *testing.T) {
	model := watchedPerson(t)
	entered, subscribed := make(chan struct{}), make(chan struct{})
	k := &watchKernel{watch: func(request *contracts.WatchRequest, stream grpc.ServerStreamingServer[contracts.ReadModelChangeset]) error {
		if request.EventStore != "store" || request.Namespace != "tenant-a" || request.EventSequenceId != "other-sequence" || request.ReadModelIdentifier != string(model.Identifier()) {
			t.Errorf("wrong watch route: %v", request)
		}
		close(entered)
		// A forwarder can race the ready signal; no change may be lost.
		if err := stream.Send(sendChange("tenant-a", contracts.ReadModelChangeType_Added, `{"_id":"person","name":"Ada"}`)); err != nil {
			return err
		}
		select {
		case <-subscribed:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
		for _, message := range []*contracts.ReadModelChangeset{{Subscribed: true}, sendChange("tenant-a", contracts.ReadModelChangeType_Modified, `{"id":"person","name":"Grace"}`), sendChange("tenant-a", contracts.ReadModelChangeType_Removed, `null`)} {
			if err := stream.Send(message); err != nil {
				return err
			}
		}
		return status.Error(codes.Unavailable, "interrupted")
	}}
	s, ctx := watchFixture(t, k, model.Descriptor())
	type result struct {
		subscription *readmodels.Subscription[readmodels.Change[Person]]
		err          error
	}
	opened := make(chan result, 1)
	go func() { subscription, err := readmodels.For(s, model).Watch(ctx); opened <- result{subscription, err} }()
	<-entered
	select {
	case <-opened:
		t.Fatal("returned before Subscribed")
	default:
	}
	close(subscribed)
	got := <-opened
	if got.err != nil {
		t.Fatal(got.err)
	}
	defer func() {
		if err := got.subscription.Close(); err != nil {
			t.Error(err)
		}
	}()
	for _, kind := range []readmodels.ChangeType{readmodels.Added, readmodels.Modified, readmodels.Removed} {
		change, err := got.subscription.Recv()
		if err != nil || change.Type != kind || change.Key != "person" || change.Context.Namespace != "tenant-a" || change.Context.Sequence != "other-sequence" || change.Context.EventType.ID != "" || change.Context.SequenceNumber != 7 {
			t.Fatalf("change: %+v %v", change, err)
		}
		if kind == readmodels.Removed {
			if change.HasValue {
				t.Fatal("removal manufactured a value")
			}
		} else if !change.HasValue || change.Value.ID != "person" || change.Value.Children == nil {
			t.Fatalf("model normalization: %+v", change)
		}
	}
	_, err := got.subscription.Recv()
	if !errors.Is(err, readmodels.ErrInterrupted) || status.Code(err) != codes.Unavailable {
		t.Fatalf("terminal: %v", err)
	}
}
func TestWatchCancellationBeforeReadyAndCloseJoin(t *testing.T) {
	for _, ready := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-ready", true: "after-ready"}[ready], func(t *testing.T) {
			entered, stopped := make(chan struct{}), make(chan struct{})
			k := &watchKernel{watch: func(_ *contracts.WatchRequest, stream grpc.ServerStreamingServer[contracts.ReadModelChangeset]) error {
				defer close(stopped)
				close(entered)
				if ready {
					if err := stream.Send(&contracts.ReadModelChangeset{Subscribed: true}); err != nil {
						return err
					}
				}
				<-stream.Context().Done()
				return stream.Context().Err()
			}}
			model := watchedPerson(t)
			s, parent := watchFixture(t, k, model.Descriptor())
			ctx, cancel := context.WithCancel(parent)
			defer cancel()
			if !ready {
				go func() { <-entered; cancel() }()
			}
			sub, err := s.Watch(ctx, model.Identifier())
			if !ready {
				if !errors.Is(err, context.Canceled) || sub != nil {
					t.Fatalf("startup: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if err := sub.Close(); err != nil {
					t.Fatal(err)
				}
				if err := sub.Close(); err != nil {
					t.Fatal(err)
				}
				select {
				case <-sub.Done():
				default:
					t.Fatal("close did not join")
				}
				if !errors.Is(sub.Err(), context.Canceled) {
					t.Fatalf("close outcome: %v", sub.Err())
				}
			}
			select {
			case <-stopped:
			case <-parent.Done():
				t.Fatal("server leaked")
			}
		})
	}
}
func TestWatchOverloadAndNamespaceFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name      string
		namespace string
		buffer    int
		bytes     int
		want      error
	}{
		{"count", "tenant-a", 1, 10000, readmodels.ErrOverloaded}, {"bytes", "tenant-a", 10, 1, readmodels.ErrOverloaded}, {"namespace", "tenant-b", 10, 10000, chronicle.ErrProtocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start := make(chan struct{})
			k := &watchKernel{watch: func(_ *contracts.WatchRequest, stream grpc.ServerStreamingServer[contracts.ReadModelChangeset]) error {
				if err := stream.Send(&contracts.ReadModelChangeset{Subscribed: true}); err != nil {
					return err
				}
				select {
				case <-start:
				case <-stream.Context().Done():
					return stream.Context().Err()
				}
				for range 3 {
					if err := stream.Send(sendChange(tc.namespace, contracts.ReadModelChangeType_Added, `{"id":"person"}`)); err != nil {
						return err
					}
				}
				<-stream.Context().Done()
				return stream.Context().Err()
			}}
			model := watchedPerson(t)
			s, ctx := watchFixture(t, k, model.Descriptor())
			sub, err := s.Watch(ctx, model.Identifier(), readmodels.WithWatchBuffer(tc.buffer, tc.bytes))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := sub.Close(); err != nil {
					t.Error(err)
				}
			}()
			close(start)
			select {
			case <-sub.Done():
			case <-ctx.Done():
				t.Fatal("watch did not terminate")
			}
			if !errors.Is(sub.Err(), tc.want) {
				t.Fatalf("terminal = %v want %v", sub.Err(), tc.want)
			}
			if tc.name != "count" {
				value, err := sub.Recv()
				if err == nil || value.HasValue {
					t.Fatal("emitted unsafe value")
				}
			}
		})
	}
}
func TestWatchMalformedChangesAndEarlyIteratorExit(t *testing.T) {
	for _, document := range []string{"", "[]", "null", `{"name":123}`} {
		t.Run(document, func(t *testing.T) {
			k := &watchKernel{watch: func(_ *contracts.WatchRequest, stream grpc.ServerStreamingServer[contracts.ReadModelChangeset]) error {
				if err := stream.Send(&contracts.ReadModelChangeset{Subscribed: true}); err != nil {
					return err
				}
				if err := stream.Send(sendChange("tenant-a", contracts.ReadModelChangeType_Added, document)); err != nil {
					return err
				}
				<-stream.Context().Done()
				return stream.Context().Err()
			}}
			model := watchedPerson(t)
			s, ctx := watchFixture(t, k, model.Descriptor())
			sub, err := readmodels.For(s, model).Watch(ctx)
			if err != nil {
				if !errors.Is(err, chronicle.ErrProtocol) {
					t.Fatal(err)
				}
				return
			}
			defer func() {
				if err := sub.Close(); err != nil {
					t.Error(err)
				}
			}()
			_, err = sub.Recv()
			if !errors.Is(err, chronicle.ErrProtocol) {
				t.Fatalf("decode failure = %v", err)
			}
		})
	}
	k := &watchKernel{watch: func(_ *contracts.WatchRequest, stream grpc.ServerStreamingServer[contracts.ReadModelChangeset]) error {
		for _, m := range []*contracts.ReadModelChangeset{{Subscribed: true}, sendChange("tenant-a", contracts.ReadModelChangeType_Added, `{"id":"person"}`)} {
			if err := stream.Send(m); err != nil {
				return err
			}
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	}}
	model := watchedPerson(t)
	s, ctx := watchFixture(t, k, model.Descriptor())
	sub, err := s.Watch(ctx, model.Identifier())
	if err != nil {
		t.Fatal(err)
	}
	for _, err := range sub.Values() {
		if err != nil {
			t.Fatal(err)
		}
		break
	}
	select {
	case <-sub.Done():
	default:
		t.Fatal("iterator did not close")
	}
}
func TestMaterializedWindowPaging(t *testing.T) {
	model := watchedPerson(t)
	for _, tc := range []struct {
		name       string
		window     *readmodels.Window
		page, size int32
		want       []string
	}{
		{"default", nil, 0, 50, []string{"0", "1", "2", "3", "4"}},
		{"negative-skip", &readmodels.Window{Skip: -2, Take: 2}, 0, 2, []string{"0", "1"}},
		{"aligned", &readmodels.Window{Skip: 4, Take: 2}, 2, 2, []string{"0", "1"}},
		{"covering", &readmodels.Window{Skip: 1, Take: 3}, 0, 4, []string{"1", "2", "3"}},
		{"zero", &readmodels.Window{}, 0, 0, []string{}},
		{"negative-take", &readmodels.Window{Take: -2}, 0, 0, []string{}},
		{"unlimited", &readmodels.Window{Skip: 2, Take: -1}, 0, 2147483647, []string{"2", "3", "4"}},
		{"cap", &readmodels.Window{Skip: 2147483645, Take: 3}, 0, 2147483647, []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := &watchKernel{get: func(_ context.Context, r *contracts.GetInstancesRequest) (*contracts.GetInstancesResponse, error) {
				if r.Page != tc.page || r.PageSize != tc.size || r.Namespace != "tenant-a" || r.ReadModel != string(model.Identifier()) {
					t.Errorf("paging: %v", r)
				}
				return &contracts.GetInstancesResponse{Instances: []string{`{"id":"0"}`, `{"id":"1"}`, `{"id":"2"}`, `{"id":"3"}`, `{"id":"4"}`}}, nil
			}}
			s, ctx := watchFixture(t, k, model.Descriptor())
			values, err := readmodels.For(s, model).Materialized().GetInstances(ctx, tc.window)
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]string, len(values))
			for i, v := range values {
				ids[i] = v.ID
			}
			if !reflect.DeepEqual(ids, tc.want) {
				t.Fatalf("ids = %v want %v", ids, tc.want)
			}
		})
	}
}
func TestMaterializedObserveInitialSnapshotOrderingAndTerminal(t *testing.T) {
	k := &watchKernel{observe: func(r *contracts.ObserveInstancesRequest, stream grpc.ServerStreamingServer[contracts.ObserveInstancesResponse]) error {
		if r.Page != 0 || r.PageSize != 4 || r.Namespace != "tenant-a" {
			t.Errorf("window route: %v", r)
		}
		for _, name := range []string{"first", "second"} {
			if err := stream.Send(&contracts.ObserveInstancesResponse{Instances: []string{`{"id":"skip"}`, `{"id":"person","name":"` + name + `"}`}}); err != nil {
				return err
			}
		}
		return nil
	}}
	model := watchedPerson(t)
	s, ctx := watchFixture(t, k, model.Descriptor())
	sub, err := readmodels.For(s, model).Materialized().ObserveInstances(ctx, &readmodels.Window{Skip: 1, Take: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sub.Close(); err != nil {
			t.Error(err)
		}
	}()
	for _, name := range []string{"first", "second"} {
		window, err := sub.Recv()
		if err != nil || len(window) != 1 || window[0].Name != name {
			t.Fatalf("window: %v %v", window, err)
		}
	}
	_, err = sub.Recv()
	if !errors.Is(err, io.EOF) || !errors.Is(err, readmodels.ErrInterrupted) {
		t.Fatalf("terminal: %v", err)
	}
}
func TestMaterializedDifferUsesSerializedModelsAndExplicitWindowRemoval(t *testing.T) {
	model := watchedPerson(t)
	differ := &readmodels.WindowDiffer{}
	window := func(values ...string) []json.RawMessage {
		result := make([]json.RawMessage, len(values))
		for i, v := range values {
			result[i] = json.RawMessage(v)
		}
		return result
	}
	changes, err := differ.Diff(model.Descriptor(), window(`{"id":"a","name":"Ada"}`, `{"_id":"b"}`))
	if err != nil || len(changes) != 2 || changes[0].Type != readmodels.Added {
		t.Fatalf("initial: %+v %v", changes, err)
	}
	changes, err = differ.Diff(model.Descriptor(), window(`{"name":"Grace","id":"a"}`))
	if err != nil || len(changes) != 2 || changes[0].Type != readmodels.Modified || changes[1].Type != readmodels.Removed || changes[1].HasValue || changes[1].Key != "b" {
		t.Fatalf("diff: %+v %v", changes, err)
	}
	if _, err = differ.Diff(model.Descriptor(), window(`{"id":"a"}`, `{"id":"a"}`)); !errors.Is(err, chronicle.ErrProtocol) {
		t.Fatalf("duplicate: %v", err)
	}
	changes, err = differ.Diff(model.Descriptor(), window(`{"id":"a","name":"Grace","__lastHandledEventSequenceNumber":999}`))
	if err != nil || len(changes) != 0 {
		t.Fatalf("metadata-only: %+v %v", changes, err)
	}
}
func TestWatchUnknownModelReducerAndOptionsFailBeforeIO(t *testing.T) {
	model := person(t)
	s, ctx := watchFixture(t, &watchKernel{}, model.Descriptor())
	if _, err := s.Watch(ctx, "missing"); !errors.Is(err, chronicle.ErrNotRegistered) {
		t.Fatal(err)
	}
	if _, err := s.Watch(ctx, model.Identifier()); !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatal(err)
	}
	if _, err := s.Watch(ctx, model.Identifier(), readmodels.WithWatchBuffer(0, 1)); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
}
