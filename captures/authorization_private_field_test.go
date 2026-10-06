// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package captures_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/captures"
)

// Concrete unexported fields make reflect.Value.CanInterface false: fmt cannot
// invoke the contained value's Format method and falls back to field traversal.
func privateCaptureWrappers[T any](value T) []any {
	return []any{
		struct{ value T }{value},
		struct{ outer struct{ inner T } }{struct{ inner T }{value}},
		struct{ values []T }{[]T{value}},
		struct{ values map[string]T }{map[string]T{"key": value}},
		struct{ value *T }{&value},
	}
}

func TestCaptureAuthorizationHiddenInUnexportedFields(t *testing.T) {
	for _, option := range syntheticAuthorizations() {
		source := captures.Webhook("/synthetic-capture", option)
		builder := new(captures.Builder).From(source).Key("id")
		definition, err := builder.Build("SyntheticCapture")
		if err != nil {
			t.Fatal(err)
		}
		authorization, _ := source.Authorization()
		cases := []struct {
			name     string
			wrappers []any
		}{
			{"authorization", privateCaptureWrappers(authorization)},
			{"source", privateCaptureWrappers(source)},
			{"definition", privateCaptureWrappers(definition)},
			{"builder", privateCaptureWrappers(*builder)},
			{"builder pointer", privateCaptureWrappers(builder)},
		}
		for _, tc := range cases {
			t.Run(string(authorization.Kind())+"/"+tc.name, func(t *testing.T) {
				for _, wrapper := range tc.wrappers {
					for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%T", "%p"} {
						t.Run(verb, func(t *testing.T) {
							assertCaptureCredentialsHidden(t, fmt.Sprintf(verb, wrapper))
						})
					}
					for _, format := range []string{"text", "JSON"} {
						t.Run(format, func(t *testing.T) {
							var out bytes.Buffer
							var handler slog.Handler = slog.NewTextHandler(&out, nil)
							if format == "JSON" {
								handler = slog.NewJSONHandler(&out, nil)
							}
							slog.New(handler).Info("capture", "value", wrapper)
							assertCaptureCredentialsHidden(t, out.String())
						})
					}
				}
			})
		}
	}
}

func assertCaptureCredentialsHidden(t *testing.T, text string) {
	t.Helper()
	for _, secret := range []string{"synthetic-user", "synthetic-password", "synthetic-token", "https://synthetic.invalid/oauth", "synthetic-client", "synthetic-secret"} {
		if strings.Contains(text, secret) || strings.Contains(text, fmt.Sprintf("%x", secret)) || strings.Contains(text, fmt.Sprintf("%X", secret)) {
			t.Fatal("private-field diagnostic exposed capture credentials")
		}
	}
}
