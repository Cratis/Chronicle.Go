// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observerruntime_test

import (
	"context"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/observerruntime"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

type wireEvent struct{}

type registrationStream struct {
	grpc.ClientStream
	data []byte
}

func (s *registrationStream) SendMsg(message any) error {
	var err error
	s.data, err = proto.Marshal(message.(proto.Message))
	return err
}

type registrationConn struct {
	grpc.ClientConnInterface
	stream *registrationStream
}

func (c *registrationConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return c.stream, nil
}

// fieldBytes inspects actual nested request bytes, without decoding/re-encoding
// proto3 messages (which would discard a known scalar's explicit zero presence).
func fieldBytes(t *testing.T, data []byte, field protowire.Number) []byte {
	t.Helper()
	for len(data) > 0 {
		number, kind, n := protowire.ConsumeTag(data)
		if n < 0 {
			t.Fatal("invalid protobuf tag")
		}
		data = data[n:]
		if number == field && kind == protowire.BytesType {
			value, n := protowire.ConsumeBytes(data)
			if n < 0 {
				t.Fatal("invalid protobuf bytes")
			}
			return value
		}
		n = protowire.ConsumeFieldValue(number, kind, data)
		if n < 0 {
			t.Fatal("invalid protobuf field")
		}
		data = data[n:]
	}
	t.Fatalf("missing bytes field %d", field)
	return nil
}

// replayability mirrors ReactorDefinition.cs at Chronicle 2e31b0dfb:
// initialization/DefaultValue(true) survives absence; field 4 overrides it.
func replayability(t *testing.T, data []byte) (bool, []uint64) {
	t.Helper()
	value := true
	var occurrences []uint64
	for len(data) > 0 {
		number, kind, n := protowire.ConsumeTag(data)
		if n < 0 {
			t.Fatal("invalid protobuf tag")
		}
		data = data[n:]
		if number == 4 {
			if kind != protowire.VarintType {
				t.Fatal("incorrect replayability wire type")
			}
			v, n := protowire.ConsumeVarint(data)
			if n < 0 {
				t.Fatal("invalid protobuf varint")
			}
			occurrences = append(occurrences, v)
			value = v != 0
		}
		n = protowire.ConsumeFieldValue(number, kind, data)
		if n < 0 {
			t.Fatal("invalid protobuf field")
		}
		data = data[n:]
	}
	return value, occurrences
}

func TestProto3FalseReplayabilityIsAbsentAndCSharpDefaultsTrue(t *testing.T) {
	data, err := proto.Marshal(&contracts.ReactorDefinition{IsReplayable: false})
	if err != nil {
		t.Fatal(err)
	}
	value, occurrences := replayability(t, data)
	if !value || len(occurrences) != 0 {
		t.Fatalf("bare proto3 false = %x; receiver %v, fields %v", data, value, occurrences)
	}
}

func TestReactorRegistrationWirePreservesReplayability(t *testing.T) {
	event, err := events.Define[wireEvent]()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	models, err := readmodels.NewCatalog()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		options  []reactors.Option
		handlers []reactors.Handler
		want     bool
	}{
		{name: "default", want: true},
		{name: "reactor-wide once only", options: []reactors.Option{reactors.OnceOnly()}},
		{name: "method-level once only", handlers: []reactors.Handler{reactors.On(func(context.Context, wireEvent) error { return nil }).OnceOnly()}, want: true},
		{name: "mixed live and replay", handlers: []reactors.Handler{reactors.On(func(context.Context, wireEvent) error { return nil }).OnceOnly(), reactors.On(func(context.Context, wireEvent) error { return nil }).DuringReplay()}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handlers := tc.handlers
			if handlers == nil {
				handlers = []reactors.Handler{reactors.On(func(context.Context, wireEvent) error { return nil })}
			}
			declaration, err := reactors.DefineHandlers("wire-policy", handlers, tc.options...)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := reactors.Compile(declaration, catalog, models, nil)
			if err != nil {
				t.Fatal(err)
			}
			stream := &registrationStream{}
			if _, err := observerruntime.Open(t.Context(), &registrationConn{stream: stream}, "connection", "store", "tenant", plan, nil); err != nil {
				t.Fatal(err)
			}
			definition := fieldBytes(t, fieldBytes(t, fieldBytes(t, stream.data, 1), 1), 4)
			value, occurrences := replayability(t, definition)
			wantWire := uint64(0)
			if tc.want {
				wantWire = 1
			}
			if value != tc.want || len(occurrences) != 1 || occurrences[0] != wantWire {
				t.Fatalf("request %x: receiver %v; occurrences %v, want %v", stream.data, value, occurrences, tc.want)
			}
			// Ordinary Go peers must also see the correct boolean and all coordinates.
			var request contracts.ReactorMessage
			if err := proto.Unmarshal(stream.data, &request); err != nil {
				t.Fatal(err)
			}
			registration := request.Content.Value0
			if registration.ConnectionId != "connection" || registration.EventStore != "store" || registration.Namespace != "tenant" || registration.Reactor.IsReplayable != tc.want {
				t.Fatal(&request)
			}
		})
	}
}
