// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"context"
	"reflect"
	"slices"
	"testing"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"google.golang.org/protobuf/proto"
)

func TestDecisionReadChecksAgreementAroundFoldAndCleansUpBeforeRechecking(t *testing.T) {
	f := newDecisionFixture(t)
	var calls []string
	f.handle = func(_ context.Context, request any) (proto.Message, error) {
		name := reflect.TypeOf(request).Elem().Name()
		if tail, ok := request.(*sequences.TailSequenceNumberRequest); ok {
			name += ":" + tail.EventSourceId
		}
		calls = append(calls, name)
		return nil, nil
	}
	read, err := f.reader.GetDetached(t.Context(), "source")
	if err != nil {
		t.Fatal(err)
	}
	if read.Token.IsZero() {
		t.Fatal("successful read returned no token")
	}
	want := []string{"GetDefinitionsRequest", "GetAllDefinitionsRequest", "TailSequenceNumberRequest:", "TailSequenceNumberRequest:source",
		"GetInstanceByKeyRequest", "DehydrateSessionRequest", "GetDefinitionsRequest", "GetAllDefinitionsRequest"}
	if !slices.Equal(calls, want) {
		t.Fatalf("RPC order = %v, want %v", calls, want)
	}
}
