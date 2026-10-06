// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package captures

import (
	"context"
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
)

func TestSourceAuthorizationPreservesValuesAndDefaultEscaping(t *testing.T) {
	cases := []struct {
		name   string
		option WebhookOption
		want   string
	}{
		{"basic", WithBasicAuth(" synthetic-user+é ", "synthetic-\"&<>`'😀"), `{"type":"basic","username":" synthetic-user\u002B\u00E9 ","password":"synthetic-\u0022\u0026\u003C\u003E\u0060\u0027\uD83D\uDE00"}`},
		{"bearer", WithBearerToken(" synthetic-token\t\n "), `{"type":"bearer","token":" synthetic-token\t\n "}`},
		{"oauth", WithOAuth("https://synthetic.invalid/oauth?q=+", "synthetic-client\\", "synthetic-secret\u2028"), `{"type":"oauth","authority":"https://synthetic.invalid/oauth?q=\u002B","clientId":"synthetic-client\\","clientSecret":"synthetic-secret\u2028"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := Webhook("/synthetic-capture", tc.option)
			if _, err := new(Builder).From(source).Key("id").Build("SyntheticCapture"); err != nil {
				t.Fatal(err)
			}
			authorization, present := source.Authorization()
			data, err := authorization.kernelJSON()
			if !present || err != nil || string(data) != tc.want {
				t.Fatal("authorization values changed or escaping differs")
			}
		})
	}
}

func TestCaptureEmptyDefinitionPrecedesAuthorizationRefusal(t *testing.T) {
	// Even an internally constructed empty value must retain the existing
	// empty-declaration error. No connection or context is needed for this path.
	authorization, _ := Webhook("/synthetic-capture", WithBearerToken("synthetic-token")).Authorization()
	definition := Definition{authorization: authorization}
	service := new(Service)
	for _, submit := range []func(context.Context, Definition) error{service.Validate, service.Save} {
		if err := submit(context.Background(), definition); !errors.Is(err, faults.ErrInvalidConfiguration) {
			t.Fatal("empty definition lost its original error category")
		}
	}
}
