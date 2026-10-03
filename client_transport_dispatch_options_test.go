// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"testing"

	"github.com/cratis/chronicle.go/contracts/clients"
	"github.com/cratis/chronicle.go/contracts/eventstores"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	"github.com/cratis/chronicle.go/contracts/observation"
	reactorcontracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	readmodelcontracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"google.golang.org/grpc"
)

type recordingBoundTransport struct {
	t     *testing.T
	calls int
}

func (r *recordingBoundTransport) check(options []grpc.CallOption) {
	r.t.Helper()
	var send, receive int
	for _, option := range options {
		switch o := option.(type) {
		case grpc.MaxSendMsgSizeCallOption:
			send = o.MaxSendMsgSize
		case grpc.MaxRecvMsgSizeCallOption:
			receive = o.MaxRecvMsgSize
		}
	}
	if send != 104857600 || receive != 48 {
		r.t.Fatalf("client-wide explicit bounds not applied: send=%d receive=%d", send, receive)
	}
	r.calls++
}
func (r *recordingBoundTransport) Invoke(_ context.Context, _ string, _, _ any, options ...grpc.CallOption) error {
	r.check(options)
	return nil
}
func (r *recordingBoundTransport) NewStream(ctx context.Context, _ *grpc.StreamDesc, _ string, options ...grpc.CallOption) (grpc.ClientStream, error) {
	r.check(options)
	return &lifecycleStream{ctx: ctx}, nil
}

func TestExplicitMessageBoundsReachEveryServiceAndPreserveCallerOptions(t *testing.T) {
	raw := &recordingBoundTransport{t: t}
	client, err := NewClient(WithNoAuthentication(), WithMaxSendMessageSize(defaultMaxMessageSize), WithMaxReceiveMessageSize(48), func(c *clientConfig) { c.borrowed, c.borrowedSet = raw, true })
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	}()
	g, err := client.newGeneration(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer g.cancel()
	// Explicit default-sized settings must still override a borrowed owner.
	options := make([]grpc.CallOption, 2, 8)
	options[0], options[1] = grpc.MaxCallSendMsgSize(9), grpc.MaxCallRecvMsgSize(10)
	for _, method := range []string{
		clients.ConnectionService_CheckCompatibility_FullMethodName,
		clients.ConnectionService_GetConnectedClients_FullMethodName,
		eventstores.EventStores_EnsureEventStore_FullMethodName,
		eventtypes.EventTypes_RegisterEventTypes_FullMethodName,
		readmodelcontracts.ReadModels_RegisterMany_FullMethodName,
		readmodelcontracts.ReadModels_GetInstanceByKey_FullMethodName,
		observation.Observers_GetObservers_FullMethodName,
		sequences.EventSequences_Append_FullMethodName,
	} {
		if err := g.transport.Invoke(t.Context(), method, nil, nil, options...); err != nil {
			t.Fatal(err)
		}
	}
	for _, method := range []string{
		clients.ConnectionService_Connect_FullMethodName,
		clients.ConnectionService_ObserveConnectedClients_FullMethodName,
		reactorcontracts.Reactors_Observe_FullMethodName,
		readmodelcontracts.ReadModels_Watch_FullMethodName,
	} {
		stream, err := g.transport.NewStream(t.Context(), &grpc.StreamDesc{}, method, options...)
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatal(err)
		}
	}
	if raw.calls != 12 {
		t.Fatal("missing service dispatch")
	}
	if options[0].(grpc.MaxSendMsgSizeCallOption).MaxSendMsgSize != 9 || options[1].(grpc.MaxRecvMsgSizeCallOption).MaxRecvMsgSize != 10 {
		t.Fatal("caller options mutated")
	}
	for _, option := range options[2:cap(options)] {
		if option != nil {
			t.Fatal("caller backing array mutated")
		}
	}
}
