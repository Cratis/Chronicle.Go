// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type decisionAsPanic struct{}

func (*decisionAsPanic) Error() string { panic("secret-error-format") }
func (*decisionAsPanic) As(any) bool   { panic("secret-as-panic") }

type decisionUnwrapPanic struct{}

func (*decisionUnwrapPanic) Error() string { panic("secret-error-format") }
func (*decisionUnwrapPanic) Unwrap() error { panic("secret-unwrap-panic") }

type decisionErrorMethodPanic struct{}

func (*decisionErrorMethodPanic) Error() string { panic("secret-error-format") }

func TestDecisionErrorCallbacksRemainInsideRecovery(t *testing.T) {
	ordinary := &decisionCodecFailure{message: "secret-ordinary-cause"}
	for _, tc := range []struct {
		name      string
		cause     error
		wantPanic bool
	}{
		{"As", &decisionAsPanic{}, true},
		{"Unwrap", &decisionUnwrapPanic{}, true},
		{"joined As", errors.Join(errors.New("first"), errors.Join(&decisionAsPanic{})), true},
		{"joined Unwrap", errors.Join(errors.New("first"), errors.Join(&decisionUnwrapPanic{})), true},
		{"joined callback panic", errors.Join(errors.New("first"), &serialization.CallbackPanicError{}), true},
		{"joined reader panic", errors.Join(errors.New("first"), &readmodels.CodecPanicError{}), true},
		{"joined ordinary identity", errors.Join(errors.New("first"), ordinary), false},
		{"Error method never invoked", &decisionErrorMethodPanic{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, protected := range []bool{false, true} {
				t.Run(map[bool]string{false: "decode", true: "protected validation"}[protected], func(t *testing.T) {
					registry := NewRegistry()
					if _, err := RegisterEvent[DecisionCodecChanged](registry); err != nil {
						t.Fatal(err)
					}
					var options []readmodels.ModelOption
					if protected {
						options = append(options, readmodels.WithPII("name"))
					}
					model, err := RegisterReadModel[DecisionCodecModel](registry, options...)
					if err != nil {
						t.Fatal(err)
					}
					client, ctx := supervisionClient(t, &supervisedKernel{}, WithRegistry(registry))
					value, err := json.Marshal(t.Name())
					if err != nil {
						t.Fatal(err)
					}
					raw := &decisionCodecConn{ClientConnInterface: &decisionProfileConn{ClientConnInterface: client.config.borrowed, version: "19.32.3", protocol: "19.32.3"}, document: `{"id":"source","name":` + string(value) + `}`}
					client.config.borrowed = raw
					store, err := client.EventStore(ctx, "store")
					if err != nil {
						t.Fatal(err)
					}
					decisionCodecActions.Store(t.Name(), func() error {
						if raw.cleaned.Load() != 1 {
							t.Error("codec ran before cleanup")
						}
						return tc.cause
					})
					t.Cleanup(func() { decisionCodecActions.Delete(t.Name()) })
					reader := readmodels.DecisionsFor(store.ReadModels(), model)
					read, err := reader.GetDetached(ctx, "source")
					if protected {
						var refused *readmodels.DecisionReadRefused
						if !errors.As(err, &refused) || refused.Reason != readmodels.DecisionProtectedModel || reader.Admit().IsAdmitted || !read.Token.IsZero() || read.Instance.Exists || raw.cleaned.Load() != 0 || raw.agreements.Load() != 0 || raw.released.Load() != 0 {
							t.Fatal("classified decision did work or issued evidence", err)
						}
						return
					}
					if err == nil || !read.Token.IsZero() || read.Instance.Exists || read.Instance.Value != (DecisionCodecModel{}) || raw.cleaned.Load() != 1 {
						t.Fatal("failed codec returned model/token or skipped cleanup")
					}
					if strings.Contains(err.Error(), "secret") || !errors.Is(err, ErrProtocol) {
						t.Fatal("unsafe error diagnostic or category")
					}
					var panicked *readmodels.DecisionCodecPanicError
					if errors.As(err, &panicked) != tc.wantPanic {
						t.Fatal("panic classification lost")
					}
					if tc.wantPanic {
						if errors.Unwrap(panicked) != ErrProtocol || errors.Is(err, tc.cause) {
							t.Fatal("panic retained an application cause")
						}
					} else if !errors.Is(err, tc.cause) {
						t.Fatal("ordinary error identity lost")
					}
					if tc.name == "joined ordinary identity" {
						var typed *decisionCodecFailure
						if !errors.As(err, &typed) || typed != ordinary {
							t.Fatal("nested ordinary typed identity lost")
						}
					}
				})
			}
		})
	}
}
