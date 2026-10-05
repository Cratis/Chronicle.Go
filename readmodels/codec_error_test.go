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

	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

var errCodecSentinel = errors.New("private-sentinel-document-content")

type typedCodecFailure struct{}

func (*typedCodecFailure) Error() string { return "private-typed-document-content" }

type refusingConcept string

func (v refusingConcept) ConceptValue() string          { return string(v) }
func (v refusingConcept) MarshalJSON() ([]byte, error)  { return json.Marshal(string(v)) }
func (v refusingConcept) MarshalText() ([]byte, error)  { return []byte(v), nil }
func (v *refusingConcept) UnmarshalText(b []byte) error { *v = refusingConcept(b); return nil }
func (*refusingConcept) UnmarshalJSON(data []byte) error {
	if string(data) == `"sentinel"` {
		return errCodecSentinel
	}
	return &typedCodecFailure{}
}

type codecModel struct {
	ID    string          `json:"id"`
	Value refusingConcept `json:"value"`
}

func TestCodecErrorsPreserveCausesWithoutDisclosingMessages(t *testing.T) {
	model, err := readmodels.Define[codecModel]()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := serialization.CompileReadModel(reflect.TypeFor[codecModel]())
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"sentinel", "typed"} {
		t.Run(kind, func(t *testing.T) {
			data := json.RawMessage(`{"id":"source","value":"` + kind + `"}`)
			kernel := &modelKernel{get: func(context.Context, *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
				return &contracts.GetInstanceByKeyResponse{ReadModel: string(data), LastHandledEventSequenceNumber: 5}, nil
			}}
			service, ctx := serviceFixture(t, kernel, model.Descriptor())
			for _, path := range []string{"plan", "descriptor", "typed-read", "window"} {
				t.Run(path, func(t *testing.T) {
					var err error
					switch path {
					case "plan":
						value := codecModel{ID: "unchanged"}
						err = plan.Unmarshal(data, &value)
						if value.ID != "unchanged" || value.Value != "" {
							t.Fatal("failed decoder mutated target")
						}
					case "descriptor":
						_, err = model.Descriptor().Unmarshal(data)
					case "typed-read":
						instance, failure := readmodels.For(service, model).Get(ctx, "source")
						err = failure
						if instance.Exists {
							t.Fatal("failed decoder returned model")
						}
					case "window":
						var differ readmodels.WindowDiffer
						_, err = differ.Diff(model.Descriptor(), []json.RawMessage{data})
					}
					var safe *serialization.UnmarshalError
					var typed *typedCodecFailure
					if err == nil || strings.Contains(err.Error(), "private-") || !errors.Is(err, faults.ErrProtocol) || !errors.As(err, &safe) {
						t.Fatalf("unsafe or untyped codec error: %v", err)
					}
					if errors.Is(err, errCodecSentinel) != (kind == "sentinel") || errors.As(err, &typed) != (kind == "typed") {
						t.Fatalf("lost codec cause: %v", err)
					}
				})
			}
		})
	}
}
