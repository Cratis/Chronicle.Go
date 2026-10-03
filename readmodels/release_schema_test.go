// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	contracts "github.com/cratis/chronicle.go/contracts/compliance"
	"github.com/cratis/chronicle.go/readmodels"
)

func TestReleaseRecursiveOverrideWithPopulatedEdges(t *testing.T) {
	type Tree struct {
		Name string
		Next *Tree
	}
	model, err := readmodels.Define[Tree](readmodels.WithProtection(compliance.Property("Next.Name", compliance.Classification{PII: true})))
	if err != nil {
		t.Fatal(err)
	}
	service, ctx := serviceFixture(t, &modelKernel{release: func(_ context.Context, r *contracts.ReleaseRequest) (*contracts.ReleaseResponse, error) {
		return &contracts.ReleaseResponse{Payload: `{"Name":"root","Next":{"Name":"released","Next":{"Name":"plain","Next":{"Name":"leaf","Next":null}}}}`}, nil
	}}, model.Descriptor())
	input := json.RawMessage(`{"__subject":"owner","Name":"root","Next":{"Name":"ciphertext","Next":{"Name":"plain","Next":{"Name":"leaf","Next":null}}}}`)
	result, err := service.Release(ctx, model.Identifier(), input)
	if err != nil {
		t.Fatal(err)
	}
	assertReleaseJSON(t, string(result), `{"__subject":"owner","Name":"root","Next":{"Name":"released","Next":{"Name":"plain","Next":{"Name":"leaf","Next":null}}}}`)
}

func TestReleaseSendsDeclaredBookkeepingButKeepsUndeclaredFieldsLocal(t *testing.T) {
	type Model struct {
		Name      string `json:"name" chronicle:"pii"`
		Watermark uint64 `json:"__lastHandledEventSequenceNumber"`
	}
	model, err := readmodels.Define[Model]()
	if err != nil {
		t.Fatal(err)
	}
	service, ctx := serviceFixture(t, &modelKernel{release: func(_ context.Context, r *contracts.ReleaseRequest) (*contracts.ReleaseResponse, error) {
		assertReleaseJSON(t, r.Payload, `{"name":"ciphertext","__lastHandledEventSequenceNumber":42}`)
		return &contracts.ReleaseResponse{Payload: `{"name":"released","__lastHandledEventSequenceNumber":42}`}, nil
	}}, model.Descriptor())
	input := json.RawMessage(`{"name":"ciphertext","__subject":"owner","__subjects":{"name":"owner"},"_id":"sink-id","__initialized":true,"__lastHandledEventSequenceNumber":42,"customBookkeeping":7}`)
	result, err := service.Release(ctx, model.Identifier(), input)
	if err != nil {
		t.Fatal(err)
	}
	assertReleaseJSON(t, string(result), `{"name":"released","__subject":"owner","__subjects":{"name":"owner"},"_id":"sink-id","__initialized":true,"__lastHandledEventSequenceNumber":42,"customBookkeeping":7}`)
}
