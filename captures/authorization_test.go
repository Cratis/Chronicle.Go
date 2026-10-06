// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package captures_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/captures"
)

func syntheticAuthorizations() []captures.WebhookOption {
	return []captures.WebhookOption{
		captures.WithBasicAuth("synthetic-user", "synthetic-password"),
		captures.WithBearerToken("synthetic-token"),
		captures.WithOAuth("https://synthetic.invalid/oauth", "synthetic-client", "synthetic-secret"),
	}
}

func TestCaptureAuthorizationRejectsInvalidOptions(t *testing.T) {
	cases := []struct {
		name    string
		options []captures.WebhookOption
	}{
		{"nil", []captures.WebhookOption{nil}},
		{"blank username", []captures.WebhookOption{captures.WithBasicAuth(" \t", "synthetic-password")}},
		{"blank password", []captures.WebhookOption{captures.WithBasicAuth("synthetic-user", "\n")}},
		{"blank token", []captures.WebhookOption{captures.WithBearerToken("")}},
		{"blank authority", []captures.WebhookOption{captures.WithOAuth("\u2003", "synthetic-client", "synthetic-secret")}},
		{"blank client ID", []captures.WebhookOption{captures.WithOAuth("https://synthetic.invalid/oauth", "", "synthetic-secret")}},
		{"blank client secret", []captures.WebhookOption{captures.WithOAuth("https://synthetic.invalid/oauth", "synthetic-client", " ")}},
		{"duplicate", []captures.WebhookOption{captures.WithBearerToken("synthetic-token"), captures.WithBearerToken("synthetic-token")}},
		{"multiple", syntheticAuthorizations()},
		{"invalid then valid", []captures.WebhookOption{captures.WithBearerToken(""), captures.WithBearerToken("synthetic-token")}},
		{"valid then nil", []captures.WebhookOption{captures.WithBearerToken("synthetic-token"), nil}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := new(captures.Builder).From(captures.Webhook("/synthetic-capture", tc.options...)).Key("id").Build("SyntheticCapture")
			if !errors.Is(err, chronicle.ErrInvalidConfiguration) {
				t.Fatal("invalid authorization was not refused")
			}
			if err.Error() != chronicle.ErrInvalidConfiguration.Error()+": invalid capture source authorization configuration" {
				t.Fatal("authorization error is not fixed and value-free")
			}
		})
	}
}

func TestCaptureDeclarationNeverCarriesCredentials(t *testing.T) {
	const want = "capture SyntheticCapture\n  source webhook\n    path /synthetic-capture\n  key id\n"
	for _, option := range syntheticAuthorizations() {
		d, err := new(captures.Builder).From(captures.Webhook("/synthetic-capture", option)).Key("id").Build("SyntheticCapture")
		if err != nil {
			t.Fatal(err)
		}
		if d.Declaration() != want {
			t.Fatal("CDL changed or contains authorization")
		}
	}
	for _, source := range []captures.Source{{}, captures.API("SyntheticAPI", "", "1m"), captures.MessageTopic("synthetic"), captures.Webhook("/synthetic-capture")} {
		authorization, present := source.Authorization()
		if present || authorization.Kind() != captures.AuthorizationNone {
			t.Fatal("unauthorized source unexpectedly has authorization")
		}
	}
}

type authorizedCapturer struct{ source captures.Source }

func (c authorizedCapturer) Define(b *captures.Builder) error {
	b.From(c.source).Key("id")
	return nil
}

func TestCaptureAuthorizationSurvivesCopies(t *testing.T) {
	kinds := []captures.AuthorizationKind{captures.AuthorizationBasic, captures.AuthorizationBearer, captures.AuthorizationOAuth}
	for i, option := range syntheticAuthorizations() {
		source := captures.Webhook("/synthetic-capture", option)
		copySource := source
		want, present := source.Authorization()
		if !present || want.Kind() != kinds[i] {
			t.Fatal("source authorization kind differs")
		}
		got, present := copySource.Authorization()
		if !present || got != want {
			t.Fatal("source copy lost authorization")
		}
		b := new(captures.Builder).From(copySource).Key("id")
		first, err := b.Build("SyntheticCapture")
		if err != nil {
			t.Fatal(err)
		}
		second, err := b.Build("SyntheticCapture")
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := captures.Prepare("SyntheticCapture", authorizedCapturer{source})
		if err != nil {
			t.Fatal(err)
		}
		copyDefinition := first
		b.From(captures.Webhook("/other"))
		for _, d := range []captures.Definition{first, second, prepared, copyDefinition} {
			got, present := d.Authorization()
			if !present || got != want {
				t.Fatal("definition lost authorization")
			}
		}
		if first.ID() == second.ID() || first.Declaration() != second.Declaration() {
			t.Fatal("repeated Build changed snapshot semantics")
		}
	}
}

func ExampleWithBearerToken() {
	definition, err := new(captures.Builder).
		From(captures.Webhook("/synthetic-capture", captures.WithBearerToken("synthetic-token"))).
		Key("id").Build("SyntheticCapture")
	if err != nil {
		fmt.Println("configuration failed")
		return
	}
	authorization, present := definition.Authorization()
	fmt.Println(authorization.Kind(), present)
	fmt.Println(strings.Contains(definition.Declaration(), "synthetic-token"))
	// Output:
	// bearer true
	// false
}
