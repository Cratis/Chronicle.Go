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

type RegisteredEvent struct{ Value string }

type registrationConnection struct {
	grpc.ClientConnInterface
	stream *registrationStream
}

func (c registrationConnection) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return c.stream, nil
}

type registrationStream struct {
	grpc.ClientStream
	definition []byte
}

func (s *registrationStream) SendMsg(value any) error {
	var err error
	s.definition, err = proto.Marshal(value.(*contracts.ReactorMessage).Content.Value0.Reactor)
	return err
}

func TestReactorOnceOnlyEmitsExplicitFalseForCSharpDefault(t *testing.T) {
	for _, once := range []bool{false, true} {
		t.Run(map[bool]string{false: "replayable", true: "once-only"}[once], func(t *testing.T) {
			event, err := events.Define[RegisteredEvent]()
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
			var options []reactors.Option
			if once {
				options = append(options, reactors.OnceOnly())
			}
			declaration, err := reactors.DefineHandlers("observer", []reactors.Handler{reactors.On(func(context.Context, RegisteredEvent) error { return nil })}, options...)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := reactors.Compile(declaration, catalog, models, nil)
			if err != nil {
				t.Fatal(err)
			}
			stream := &registrationStream{}
			if _, err := observerruntime.Open(t.Context(), registrationConnection{stream: stream}, "connection", "store", "tenant", plan, nil); err != nil {
				t.Fatal(err)
			}
			// Model the C# initializer, then consume field 4 from the real outgoing
			// protobuf bytes. A generated Go receiver alone would hide this bug.
			replayable, present := true, false
			for data := stream.definition; len(data) > 0; {
				number, kind, n := protowire.ConsumeTag(data)
				if n < 0 {
					t.Fatal("invalid tag")
				}
				data = data[n:]
				if number == 4 {
					value, size := protowire.ConsumeVarint(data)
					if kind != protowire.VarintType || size < 0 {
						t.Fatal("invalid replay policy wire value")
					}
					replayable, present = value != 0, true
				}
				n = protowire.ConsumeFieldValue(number, kind, data)
				if n < 0 {
					t.Fatal("invalid field")
				}
				data = data[n:]
			}
			if replayable == once || (once && !present) {
				t.Fatalf("once=%t: C# replayable=%t field present=%t wire=%x", once, replayable, present, stream.definition)
			}
		})
	}
}
