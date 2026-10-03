// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
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
		{name: "omitted protected property", response: &compliance.ReleaseResponse{Payload: `{"id":"owner"}`}, identity: chronicle.ErrProtocol},
		{name: "empty release object", response: &compliance.ReleaseResponse{Payload: `{}`}, identity: chronicle.ErrProtocol},
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
	for _, tc := range []struct {
		document string
		groups   map[string]string
	}{
		{`{"customerId":"id","subject":"explicit","name":"ciphertext"}`, map[string]string{"explicit": `{"customerId":"id","subject":"explicit","name":"ciphertext"}`}},
		{`{"customerId":"id","subject":null,"name":"ciphertext"}`, map[string]string{"id": `{"customerId":"id","subject":null,"name":"ciphertext"}`}},
		{`{"customerId":"id","subject":"","name":"ciphertext"}`, map[string]string{"id": `{"customerId":"id","subject":"","name":"ciphertext"}`}},
		{`{"customerId":"id","__subject":"stored","__subjects":{"name":"nested"},"name":"ciphertext"}`, map[string]string{
			"nested": `{"name":"ciphertext"}`,
			"stored": `{"customerId":"id"}`,
		}},
	} {
		if _, err = service.Release(ctx, model.Identifier(), json.RawMessage(tc.document)); err != nil {
			t.Fatal(err)
		}
		if len(requests) != len(tc.groups) {
			t.Fatalf("release calls = %d, want %d", len(requests), len(tc.groups))
		}
		seen := make(map[string]bool)
		for range tc.groups {
			request := <-requests
			payload, ok := tc.groups[request.Subject]
			if !ok || seen[request.Subject] {
				t.Fatal("unexpected or duplicate subject group")
			}
			seen[request.Subject] = true
			assertReleaseJSON(t, request.Payload, payload)
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

func assertReleaseJSON(t *testing.T, got, want string) {
	t.Helper()
	var actual, expected map[string]json.RawMessage
	if err := json.Unmarshal([]byte(got), &actual); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(want), &expected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("JSON = %s, want %s", got, want)
	}
}

func TestReadModelReleaseMultipleSubjectGroups(t *testing.T) {
	type Contact struct {
		Email string `json:"email"`
	}
	type GroupedModel struct {
		ID      string  `json:"id"`
		Name    string  `json:"name"`
		Contact Contact `json:"contact"`
		Count   uint64  `json:"count"`
	}
	model, err := readmodels.Define[GroupedModel](readmodels.WithPII("name", "contact.email"))
	if err != nil {
		t.Fatal(err)
	}
	groups := map[string]string{
		"a-owner": `{"name":"ciphertext"}`,
		"b-owner": `{"contact":{"email":"ciphertext"}}`,
		"default": `{"id":"fallback","count":7}`,
	}
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "merge", true: "failing later group"}[fail], func(t *testing.T) {
			requests := make(chan *compliance.ReleaseRequest, 3)
			service, ctx := serviceFixture(t, &modelKernel{release: func(_ context.Context, r *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
				requests <- r
				if fail && r.Subject == "b-owner" {
					return nil, status.Error(codes.PermissionDenied, "sensitive")
				}
				switch r.Subject {
				case "a-owner":
					return &compliance.ReleaseResponse{Payload: `{"name":"Ada"}`}, nil
				case "b-owner":
					return &compliance.ReleaseResponse{Payload: `{"contact":{"email":"grace@example.com"}}`}, nil
				default:
					return &compliance.ReleaseResponse{Payload: r.Payload}, nil
				}
			}}, model.Descriptor())
			input := json.RawMessage(`{"id":"fallback","name":"ciphertext","contact":{"email":"ciphertext"},"count":7,"__subject":"default","__subjects":{"name":"a-owner","contact":"b-owner"}}`)
			result, err := service.Release(ctx, model.Identifier(), input)
			wantCalls := 3
			if fail {
				wantCalls = 2
				if result != nil || !errors.Is(err, readmodels.ErrRelease) || status.Code(err) != codes.PermissionDenied {
					t.Fatalf("partial release returned %s, %v", result, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				assertReleaseJSON(t, string(result), `{"id":"fallback","name":"Ada","contact":{"email":"grace@example.com"},"count":7,"__subject":"default","__subjects":{"name":"a-owner","contact":"b-owner"}}`)
			}
			if len(requests) != wantCalls {
				t.Fatalf("release calls = %d, want %d", len(requests), wantCalls)
			}
			seen := make(map[string]bool)
			for range wantCalls {
				request := <-requests
				if seen[request.Subject] || groups[request.Subject] == "" {
					t.Fatal("unexpected or duplicate group")
				}
				seen[request.Subject] = true
				assertReleaseJSON(t, request.Payload, groups[request.Subject])
			}
		})
	}
}

func TestReadModelReleaseNumericSubjects(t *testing.T) {
	type NumericModel struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	}
	model, err := readmodels.Define[NumericModel](readmodels.WithPII("name"))
	if err != nil {
		t.Fatal(err)
	}
	requests := make(chan *compliance.ReleaseRequest, 1)
	service, ctx := serviceFixture(t, &modelKernel{release: func(_ context.Context, r *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
		requests <- r
		return &compliance.ReleaseResponse{Payload: r.Payload}, nil
	}}, model.Descriptor())
	for _, subject := range []string{`42`, `9007199254740993`, `-5`, `1.25`, `1e20`, `"owner"`} {
		if _, err = service.Release(ctx, model.Identifier(), json.RawMessage(`{"id":`+subject+`,"name":"ciphertext"}`)); err != nil {
			t.Fatal(err)
		}
		if request := <-requests; request.Subject != strings.Trim(subject, `"`) {
			t.Fatalf("subject = %q, want %s", request.Subject, subject)
		}
	}
	value := NumericModel{ID: 9007199254740993, Name: "ciphertext"}
	if result, err := readmodels.For(service, model).Release(ctx, value); err != nil || result != value {
		t.Fatalf("typed numeric release = %+v, %v", result, err)
	}
	if request := <-requests; request.Subject != "9007199254740993" {
		t.Fatalf("typed subject = %q", request.Subject)
	}
	for _, subject := range []string{`null`, `""`, `{}`, `[]`} {
		if result, err := service.Release(ctx, model.Identifier(), json.RawMessage(`{"id":`+subject+`,"name":"ciphertext"}`)); result != nil || !errors.Is(err, readmodels.ErrRelease) {
			t.Fatalf("invalid subject %s returned %s, %v", subject, result, err)
		}
	}
	if len(requests) != 0 {
		t.Fatal("invalid subject dispatched")
	}
}

func TestReadModelReleaseRejectsMalformedLineage(t *testing.T) {
	model := person(t, readmodels.WithPII("name"))
	service, ctx := serviceFixture(t, &modelKernel{release: func(context.Context, *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
		t.Error("invalid lineage dispatched")
		return &compliance.ReleaseResponse{Payload: `{}`}, nil
	}}, model.Descriptor())
	for _, lineage := range []string{`[]`, `"subject"`, `{"name":{}}`, `{"name":""}`} {
		if result, err := service.Release(ctx, model.Identifier(), json.RawMessage(`{"id":"owner","name":"ciphertext","__subjects":`+lineage+`}`)); result != nil || !errors.Is(err, readmodels.ErrRelease) {
			t.Fatalf("invalid lineage %s returned %s, %v", lineage, result, err)
		}
	}
}
