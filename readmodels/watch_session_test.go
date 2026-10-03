// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestWatchAndSessionHaveIndependentLifetimes(t *testing.T) {
	dehydrated := make(chan struct{})
	var sessionID string
	k := &watchKernel{
		getOne: func(_ context.Context, r *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
			sessionID = r.SessionId
			return &contracts.GetInstanceByKeyResponse{ReadModel: `{"id":"person","name":"session"}`}, nil
		},
		dehydrate: func(_ context.Context, r *contracts.DehydrateSessionRequest) (*emptypb.Empty, error) {
			if r.SessionId == "" || r.SessionId != sessionID || r.Namespace != "tenant-a" || r.EventSequenceId != "other-sequence" {
				t.Errorf("session coordinates: %v", r)
			}
			close(dehydrated)
			return &emptypb.Empty{}, nil
		},
		watch: func(_ *contracts.WatchRequest, stream grpc.ServerStreamingServer[contracts.ReadModelChangeset]) error {
			if err := stream.Send(&contracts.ReadModelChangeset{Subscribed: true}); err != nil {
				return err
			}
			select {
			case <-dehydrated:
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
			if err := stream.Send(sendChange("tenant-a", contracts.ReadModelChangeType_Modified, `{"id":"person","name":"after-dehydrate"}`)); err != nil {
				return err
			}
			<-stream.Context().Done()
			return stream.Context().Err()
		},
	}
	model := watchedPerson(t)
	s, ctx := watchFixture(t, k, model.Descriptor())
	reader := readmodels.For(s, model)
	session, err := reader.NewSession("person")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = session.Get(ctx); err != nil {
		t.Fatal(err)
	}
	sub, err := reader.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sub.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err = session.Close(ctx); err != nil {
		t.Fatal(err)
	}
	value, err := sub.Recv()
	if err != nil || value.Value.Name != "after-dehydrate" {
		t.Fatalf("watch shared session lifetime: %+v %v", value, err)
	}
}
