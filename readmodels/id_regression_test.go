// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cratis/chronicle.go/contracts/compliance"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/readmodels"
)

type untaggedIDModel struct {
	ID     string
	Name   string
	Nested struct{ ID string }
}

func TestReadModelNormalizesRootIDAlias(t *testing.T) {
	model, err := readmodels.Define[untaggedIDModel]()
	if err != nil {
		t.Fatal(err)
	}
	for _, alias := range []string{"_id", "id", "Id", "ID", "iD"} {
		t.Run(alias, func(t *testing.T) {
			payload := `{"` + alias + `":"owner","Nested":{"_id":"nested"},"__subject":"lineage"}`
			service, ctx := serviceFixture(t, &modelKernel{get: func(context.Context, *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
				return &contracts.GetInstanceByKeyResponse{ReadModel: payload}, nil
			}}, model.Descriptor())
			typed, err := readmodels.For(service, model).Get(ctx, "owner")
			if err != nil || typed.Value.ID != "owner" || typed.Value.Nested.ID != "" {
				t.Fatalf("typed: %+v %v", typed, err)
			}
			raw, err := service.Get(ctx, model.Identifier(), "owner")
			if err != nil {
				t.Fatal(err)
			}
			assertReleaseJSON(t, string(raw.Value), `{"Id":"owner","Nested":{"_id":"nested"},"__subject":"lineage"}`)
		})
	}
}

func TestReadModelDeclaredIDIsNotOverwritten(t *testing.T) {
	model, err := readmodels.Define[untaggedIDModel]()
	if err != nil {
		t.Fatal(err)
	}
	payload := `{"Id":"declared","_id":"sink"}`
	service, ctx := serviceFixture(t, &modelKernel{get: func(context.Context, *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
		return &contracts.GetInstanceByKeyResponse{ReadModel: payload}, nil
	}}, model.Descriptor())
	raw, err := service.Get(ctx, model.Identifier(), "key")
	if err != nil || string(raw.Value) != payload {
		t.Fatalf("raw: %s %v", raw.Value, err)
	}
	typed, err := readmodels.For(service, model).Get(ctx, "key")
	if err != nil || typed.Value.ID != "declared" {
		t.Fatalf("typed: %+v %v", typed, err)
	}
}

func TestReadModelReleaseUntaggedIDSubject(t *testing.T) {
	model, err := readmodels.Define[untaggedIDModel](readmodels.WithPII("Name"))
	if err != nil {
		t.Fatal(err)
	}
	service, ctx := serviceFixture(t, &modelKernel{release: func(_ context.Context, request *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
		if request.Subject != "owner" {
			t.Errorf("subject = %q", request.Subject)
		}
		var schema struct{ Properties map[string]any }
		if err := json.Unmarshal([]byte(request.Schema), &schema); err != nil {
			t.Errorf("release schema: %v", err)
		}
		if schema.Properties["Id"] == nil || schema.Properties["ID"] != nil {
			t.Errorf("release schema ID naming: %s", request.Schema)
		}
		return &compliance.ReleaseResponse{Payload: `{"Name":"released"}`}, nil
	}}, model.Descriptor())
	result, err := readmodels.For(service, model).Release(ctx, untaggedIDModel{ID: "owner", Name: "ciphertext"})
	if err != nil || result.ID != "owner" || result.Name != "released" {
		t.Fatalf("release: %+v %v", result, err)
	}
	for _, alias := range []string{"_id", "id", "Id", "ID"} {
		if _, err := service.Release(ctx, model.Identifier(), json.RawMessage(`{"`+alias+`":"owner","Name":"ciphertext"}`)); err != nil {
			t.Fatalf("%s: %v", alias, err)
		}
	}
}

func TestReadModelNormalizesDeclaredKeyProperty(t *testing.T) {
	type keyedModel struct {
		Key string `json:"customerKey" chronicle:"key"`
	}
	model, err := readmodels.Define[keyedModel]()
	if err != nil {
		t.Fatal(err)
	}
	service, ctx := serviceFixture(t, &modelKernel{get: func(context.Context, *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
		return &contracts.GetInstanceByKeyResponse{ReadModel: `{"_id":"owner"}`}, nil
	}}, model.Descriptor())
	result, err := readmodels.For(service, model).Get(ctx, "owner")
	if err != nil || result.Value.Key != "owner" {
		t.Fatalf("key: %+v %v", result, err)
	}
}
