// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package captures_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/captures"
)

func TestCaptureAuthorizationRedacted(t *testing.T) {
	for _, option := range syntheticAuthorizations() {
		source := captures.Webhook("/synthetic-capture", option)
		b := new(captures.Builder).From(source).Key("id")
		d, err := b.Build("SyntheticCapture")
		if err != nil {
			t.Fatal(err)
		}
		auth, _ := source.Authorization()
		for _, value := range []any{source, &source, d, &d, *b, b, auth, &auth} {
			for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x"} {
				for _, formatted := range []any{value, struct{ Value any }{value}} {
					text := fmt.Sprintf(verb, formatted)
					assertCaptureRedacted(t, text)
				}
			}
			for _, handler := range []func(*bytes.Buffer) slog.Handler{
				func(out *bytes.Buffer) slog.Handler { return slog.NewTextHandler(out, nil) },
				func(out *bytes.Buffer) slog.Handler { return slog.NewJSONHandler(out, nil) },
			} {
				var out bytes.Buffer
				slog.New(handler(&out)).Info("capture", "value", value)
				assertCaptureRedacted(t, out.String())
			}
			if data, err := json.Marshal(value); !errors.Is(err, chronicle.ErrUnsupported) || len(data) != 0 {
				t.Fatal("JSON export did not fail closed")
			}
		}
	}
	for _, value := range []any{new(captures.SourceAuthorization), new(captures.Source), new(captures.Definition), new(captures.Builder)} {
		for _, input := range []string{`{}`, `null`, `{"type":"none"}`} {
			if err := json.Unmarshal([]byte(input), value); !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatal("JSON import did not fail closed")
			}
		}
		if _, err := json.Marshal(value); !errors.Is(err, chronicle.ErrUnsupported) {
			t.Fatal("zero-value JSON export did not fail closed")
		}
	}
}

func assertCaptureRedacted(t *testing.T, text string) {
	t.Helper()
	if !strings.Contains(text, "redacted") {
		t.Fatal("formatting did not identify redaction")
	}
	for _, secret := range []string{"synthetic-user", "synthetic-password", "synthetic-token", "https://synthetic.invalid/oauth", "synthetic-client", "synthetic-secret", "SyntheticCapture", "/synthetic-capture"} {
		if strings.Contains(text, secret) || strings.Contains(text, fmt.Sprintf("%x", secret)) {
			t.Fatal("capture formatting exposed configuration")
		}
	}
}
