// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observation

import (
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/observation"
	"google.golang.org/protobuf/proto"
)

func TestInformationCodecPreservesExplicitAndRepeatedValues(t *testing.T) {
	for _, tc := range []struct {
		name string
		wire []byte
		want bool
	}{
		{"omitted", nil, true},
		{"false", []byte{0x50, 0}, false},
		{"true", []byte{0x50, 1}, true},
		{"last false", []byte{0x50, 1, 0x50, 0}, false},
		{"last true", []byte{0x50, 0, 0x50, 1}, true},
		{"unknown field", []byte{0xa0, 0x06, 0}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := &contracts.ObserverInformation{IsReplayable: !tc.want}
			if err := (informationCodec{}).Unmarshal(tc.wire, v); err != nil || v.IsReplayable != tc.want {
				t.Fatal(v, err)
			}
			list := &contracts.IEnumerable_ObserverInformation{}
			wire := append([]byte{0x0a, byte(len(tc.wire))}, tc.wire...)
			if err := (informationCodec{}).Unmarshal(wire, list); err != nil || len(list.Items) != 1 || list.Items[0].IsReplayable != tc.want {
				t.Fatal(list, err)
			}
		})
	}
}

func TestInformationCodecRejectsMalformedWire(t *testing.T) {
	for _, data := range [][]byte{{0x50}, {0x52, 0}, {0x50, 0x80}} {
		if err := (informationCodec{}).Unmarshal(data, &contracts.ObserverInformation{}); err == nil {
			t.Fatalf("accepted %x", data)
		}
	}
}

func TestInformationCodecLeavesRequestsUnchanged(t *testing.T) {
	request := &contracts.GetObserverInformationRequest{EventStore: "store", Namespace: "tenant", ObserverId: "observer", EventSequenceId: "custom"}
	data, err := (informationCodec{}).Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	decoded := &contracts.GetObserverInformationRequest{}
	if err := proto.Unmarshal(data, decoded); err != nil || !proto.Equal(request, decoded) {
		t.Fatal(decoded, err)
	}
}
