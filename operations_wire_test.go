// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"encoding/hex"
	"os"
	"strings"
	"testing"

	oc "github.com/cratis/chronicle.go/contracts/observation"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// rawObserverFixtureCodec emits actual protobuf-net bytes, without re-marshaling
// through Go (which would erase the explicit false). Requests decode normally.
type rawObserverFixtureCodec struct{ fixtures map[string][]byte }

func (rawObserverFixtureCodec) Name() string { return "proto" }
func (c rawObserverFixtureCodec) Marshal(v any) ([]byte, error) {
	switch r := v.(type) {
	case *oc.ObserverInformation:
		return c.fixtures[r.Id], nil
	case *oc.IEnumerable_ObserverInformation:
		return c.fixtures["list"], nil
	default:
		return proto.Marshal(v.(proto.Message))
	}
}
func (rawObserverFixtureCodec) Unmarshal(data []byte, v any) error {
	return proto.Unmarshal(data, v.(proto.Message))
}

func TestObserverSnapshotsDecodeCSharpWirePresence(t *testing.T) {
	fixtures := make(map[string][]byte)
	for _, name := range []string{"replayable", "once-only", "list"} {
		data, err := os.ReadFile("testdata/observation/" + name + ".hex")
		if err != nil {
			t.Fatal(err)
		}
		fixtures[name], err = hex.DecodeString(strings.TrimSpace(string(data)))
		if err != nil || len(fixtures[name]) == 0 {
			t.Fatal("invalid fixture", name, err)
		}
	}
	conn := operationsConnection(t, func(_ context.Context, method string, request proto.Message) (proto.Message, error) {
		if methodName(method) == "GetObservers" {
			return &oc.IEnumerable_ObserverInformation{}, nil
		}
		return &oc.ObserverInformation{Id: request.(*oc.GetObserverInformationRequest).ObserverId}, nil
	}, grpc.ForceServerCodec(rawObserverFixtureCodec{fixtures}))
	o, _ := operationServices(t, conn)
	all, err := o.List(testContext(t))
	if err != nil || len(all) != 2 {
		t.Fatal(all, err)
	}
	for index, id := range []string{"replayable", "once-only"} {
		listed := all[index]
		if string(listed.ID()) != id || listed.IsReplayable() != (index == 0) || listed.SubscriptionKnown() || listed.IsSubscribed() {
			t.Fatalf("list %s: replayable=%t known=%t subscribed=%t", id, listed.IsReplayable(), listed.SubscriptionKnown(), listed.IsSubscribed())
		}
		got, err := o.Get(testContext(t), listed.ID(), "custom")
		if err != nil || got == nil {
			t.Fatal(got, err)
		}
		if got.IsReplayable() != (index == 0) || !got.SubscriptionKnown() || !got.IsSubscribed() {
			t.Fatalf("get %s: replayable=%t known=%t subscribed=%t", id, got.IsReplayable(), got.SubscriptionKnown(), got.IsSubscribed())
		}
	}
	// A raw generated client still follows proto3. Our fix must not register a
	// global codec or affect calls made independently on the same connection.
	raw, err := oc.NewObserversClient(conn).GetObserverInformation(testContext(t), &oc.GetObserverInformationRequest{ObserverId: "replayable"})
	if err != nil || raw.IsReplayable {
		t.Fatal("codec escaped the SDK List/Get calls", raw, err)
	}
}
