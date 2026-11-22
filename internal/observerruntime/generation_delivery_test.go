// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observerruntime

import (
	"context"
	"errors"
	"testing"

	reactorcontracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	reducercontracts "github.com/cratis/chronicle.go/contracts/observation/reducers"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func deliveryPlans(t *testing.T, modes ...events.GenerationDelivery) (*Reactor, *Reducer) {
	t.Helper()
	var reactorOptions []reactors.Option
	var reducerOptions []reducers.Option
	for _, mode := range modes {
		reactorOptions = append(reactorOptions, reactors.WithGenerationDelivery(mode))
		reducerOptions = append(reducerOptions, reducers.WithGenerationDelivery(mode))
	}
	rd, err := reactors.DefineHandlers("delivery", []reactors.Handler{reactors.On(func(context.Context, PageInput) error { return nil })}, reactorOptions...)
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[Balance]()
	if err != nil {
		t.Fatal(err)
	}
	models, err := readmodels.NewCatalog(model.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	sd, err := reducers.DefineHandlers(model, "delivery", []reducers.Handler{reducers.On(func(_ context.Context, _ PageInput, current *Balance, _ events.Context) (*Balance, error) {
		return current, nil
	})}, reducerOptions...)
	if err != nil {
		t.Fatal(err)
	}
	catalog := pageCatalog(t)
	rp, err := reactors.Compile(rd, catalog, models, nil)
	if err != nil {
		t.Fatal(err)
	}
	sp, err := reducers.Compile(sd, catalog, models, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &Reactor{plan: rp, EventTypes: catalog, store: "store", namespace: "Default"}, &Reducer{plan: sp, EventTypes: catalog, store: "store", namespace: "Default"}
}

// Capture the serialized registration sent on each real Open path, not a
// separately manufactured definition. Both protocols share these wire values.
type deliveryConnection struct {
	grpc.ClientConnInterface
	stream *deliveryStream
}

func (c deliveryConnection) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return c.stream, nil
}

type deliveryStream struct {
	grpc.ClientStream
	reactor reactorcontracts.ReactorDefinition
	reducer reducercontracts.ReducerDefinition
}

func (s *deliveryStream) SendMsg(value any) error {
	switch message := value.(type) {
	case *reactorcontracts.ReactorMessage:
		data, err := proto.Marshal(message.Content.Value0.Reactor)
		if err != nil {
			return err
		}
		return proto.Unmarshal(data, &s.reactor)
	case *reducercontracts.ReducerMessage:
		data, err := proto.Marshal(message.Content.Value0.Reducer)
		if err != nil {
			return err
		}
		return proto.Unmarshal(data, &s.reducer)
	default:
		return faults.ErrProtocol
	}
}

func TestGenerationDeliveryRegistrationDefaultsAndLastWins(t *testing.T) {
	for _, modes := range [][]events.GenerationDelivery{nil, {events.Pinned}, {events.Pinned, events.Compatibility}, {events.Compatibility, events.Pinned}} {
		r, s := deliveryPlans(t, modes...)
		want := events.Compatibility
		if len(modes) > 0 {
			want = modes[len(modes)-1]
			modes[len(modes)-1] = events.GenerationDelivery(99) // frozen declarations do not retain caller state
		}
		assertRegisteredDelivery(t, r, s, want)
	}
}

func assertRegisteredDelivery(t *testing.T, r *Reactor, s *Reducer, want events.GenerationDelivery) {
	t.Helper()
	stream := &deliveryStream{}
	conn := deliveryConnection{stream: stream}
	if _, err := Open(t.Context(), conn, "connection", "store", "Default", r.plan, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReducer(t.Context(), conn, "connection", "store", "Default", s.plan); err != nil {
		t.Fatal(err)
	}
	actual := [4]events.GenerationDelivery{r.plan.GenerationDelivery(), s.plan.GenerationDelivery(), events.GenerationDelivery(stream.reactor.GenerationDelivery), events.GenerationDelivery(stream.reducer.GenerationDelivery)}
	if actual != [4]events.GenerationDelivery{want, want, want, want} {
		t.Fatalf("delivery=%d: got %v", want, actual)
	}
}

func TestGenerationDeliveryReducerFingerprint(t *testing.T) {
	_, compatibility := deliveryPlans(t)
	_, explicit := deliveryPlans(t, events.Compatibility)
	_, pinned := deliveryPlans(t, events.Pinned)
	if compatibility.plan.Fingerprint() != explicit.plan.Fingerprint() || compatibility.plan.Fingerprint() == pinned.plan.Fingerprint() {
		t.Fatal("delivery mode missing from reducer identity")
	}
}

func TestGenerationDeliveryRejectsUnknownMode(t *testing.T) {
	_, err := reactors.DefineHandlers("delivery", nil, reactors.WithGenerationDelivery(events.GenerationDelivery(99)))
	if !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	model, err := readmodels.Define[Balance]()
	if err != nil {
		t.Fatal(err)
	}
	_, err = reducers.DefineHandlers(model, "delivery", nil, reducers.WithGenerationDelivery(events.GenerationDelivery(99)))
	if !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
}

func TestGenerationDeliveryDecodesAndPreservesAppendMetadata(t *testing.T) {
	for _, mode := range []events.GenerationDelivery{events.Compatibility, events.Pinned} {
		r, s := deliveryPlans(t, mode)
		for _, generation := range []uint32{1, 2} {
			for _, original := range []uint32{0, 3} {
				input := appended("PageInput", "source", 7)
				input.Context.EventType.Generation = generation
				input.Context.AppendedGeneration = original
				input.GenerationalContent = map[int32]string{1: `{"Number":9}`, 2: `{"Number":8}`}
				wantNumber, wantGeneration := 7, generation
				if mode == events.Compatibility && generation != 1 {
					wantNumber, wantGeneration = 9, 1
				}
				ec, value, err := r.decode(input)
				assertDelivery(t, reducers.Event{Context: ec, Content: value}, err, deliveryExpectation{wantNumber, events.Generation(wantGeneration), events.Generation(original)})
				data, err := proto.Marshal(input)
				if err != nil {
					t.Fatal(err)
				}
				reducerInput := &reducercontracts.AppendedEvent{}
				if err := proto.Unmarshal(data, reducerInput); err != nil {
					t.Fatal(err)
				}
				decoded, err := s.decode(reducerInput)
				assertDelivery(t, decoded, err, deliveryExpectation{wantNumber, events.Generation(wantGeneration), events.Generation(original)})
			}
		}
	}
}

type deliveryExpectation struct {
	number     int
	generation events.Generation
	original   events.Generation
}

func assertDelivery(t *testing.T, actual reducers.Event, err error, want deliveryExpectation) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	got := deliveryExpectation{actual.Content.(*PageInput).Number, actual.Context.EventType.Generation, actual.Context.AppendedGeneration}
	if got != want {
		t.Fatalf("delivery=%+v want=%+v", got, want)
	}
}

func TestPinnedGenerationDeliveryRejectsInvalidContent(t *testing.T) {
	r, s := deliveryPlans(t, events.Pinned)
	for _, content := range []string{"", "null", "[]", "{", `{"Number":"wrong"}`} {
		input := appended("PageInput", "source", 1)
		input.Content = content
		input.GenerationalContent = map[int32]string{1: `{"Number":9}`}
		_, _, err := r.decode(input)
		if !errors.Is(err, faults.ErrProtocol) {
			t.Fatalf("reactor content %q: %v", content, err)
		}
		reducerInput := reducerAppended("source", 1)
		reducerInput.Content = content
		if _, err := s.decode(reducerInput); !errors.Is(err, faults.ErrProtocol) {
			t.Fatalf("reducer content %q: %v", content, err)
		}
	}
}
