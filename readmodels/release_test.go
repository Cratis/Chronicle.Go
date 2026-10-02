// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/compliance"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReadModelReleaseFailsClosed(t *testing.T) {
	model := person(t, readmodels.WithPII("name"))
	for _, tc := range []struct {
		name     string
		response *compliance.ReleaseResponse
		rpc      error
		identity error
	}{
		{name: "success", response: &compliance.ReleaseResponse{Payload: `{"id":"owner","name":"Ada"}`}},
		{name: "kernel error", response: &compliance.ReleaseResponse{HasError: true, Error: "sensitive", Payload: `{"name":"ciphertext"}`}, identity: chronicle.ErrProtocol},
		{name: "inconsistent error", response: &compliance.ReleaseResponse{Error: "sensitive", Payload: `{}`}, identity: chronicle.ErrProtocol},
		{name: "missing payload", response: &compliance.ReleaseResponse{}, identity: chronicle.ErrProtocol},
		{name: "null", response: &compliance.ReleaseResponse{Payload: "null"}, identity: chronicle.ErrProtocol},
		{name: "wrong shape", response: &compliance.ReleaseResponse{Payload: `{"count":"sensitive"}`}, identity: chronicle.ErrProtocol},
		{name: "unsupported", rpc: status.Error(codes.Unimplemented, "unavailable"), identity: chronicle.ErrUnsupported},
		{name: "canceled", rpc: status.Error(codes.Canceled, "canceled"), identity: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, ctx := serviceFixture(t, &modelKernel{release: func(_ context.Context, r *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
				if r.Subject != "owner" || r.Namespace != "tenant-a" || r.EventStore != "store" || r.Schema != model.Descriptor().Schema() || !strings.Contains(r.Payload, "ciphertext") {
					t.Errorf("release request mismatch")
				}
				return tc.response, tc.rpc
			}}, model.Descriptor())
			result, err := readmodels.For(service, model).Release(ctx, Person{ID: "owner", Name: "ciphertext"})
			if tc.identity == nil {
				if err != nil || result.Name != "Ada" {
					t.Fatalf("%+v %v", result, err)
				}
				return
			}
			var release *readmodels.ReleaseError
			if !errors.Is(err, tc.identity) || !errors.Is(err, readmodels.ErrRelease) || !errors.As(err, &release) || result.Name != "" {
				t.Fatalf("%+v %v", result, err)
			}
			if strings.Contains(err.Error(), "sensitive") || strings.Contains(err.Error(), "ciphertext") {
				t.Fatal("release error leaked data")
			}
		})
	}
}

func TestReadModelReleaseSubjectAndNoMetadata(t *testing.T) {
	type SubjectModel struct {
		ID      string  `json:"customerId"`
		Subject *string `json:"subject"`
		Name    string  `json:"name"`
	}
	model, err := readmodels.Define[SubjectModel](readmodels.WithPII("name"), readmodels.WithSubjectProperty("subject"))
	if err != nil {
		t.Fatal(err)
	}
	requests := make(chan *compliance.ReleaseRequest, 4)
	service, ctx := serviceFixture(t, &modelKernel{release: func(_ context.Context, r *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
		requests <- r
		return &compliance.ReleaseResponse{Payload: `{"name":"released"}`}, nil
	}}, model.Descriptor())
	for _, tc := range []struct{ document, subject string }{
		{`{"customerId":"id","subject":"explicit","name":"ciphertext"}`, "explicit"},
		{`{"customerId":"id","subject":null,"name":"ciphertext"}`, "id"},
		{`{"customerId":"id","subject":"","name":"ciphertext"}`, "id"},
		{`{"customerId":"id","__subject":"stored","__subjects":{"name":"nested"},"name":"ciphertext"}`, "stored"},
	} {
		if _, err = service.Release(ctx, model.Identifier(), json.RawMessage(tc.document)); err != nil {
			t.Fatal(err)
		}
		request := <-requests
		if request.Subject != tc.subject || request.Payload != tc.document {
			t.Fatal("subject/lineage lost")
		}
	}
	if data, err := service.Release(ctx, model.Identifier(), json.RawMessage(`{"name":"ciphertext"}`)); !errors.Is(err, readmodels.ErrRelease) || data != nil {
		t.Fatalf("missing subject: %s %v", data, err)
	}
	unprotected := person(t)
	plain, plainCtx := serviceFixture(t, &modelKernel{}, unprotected.Descriptor())
	input := json.RawMessage(`{"name":"plain"}`)
	output, err := plain.Release(plainCtx, unprotected.Identifier(), input)
	if err != nil {
		t.Fatal(err)
	}
	output[0] = '!'
	if input[0] != '{' {
		t.Fatal("release returned borrowed JSON")
	}
}
