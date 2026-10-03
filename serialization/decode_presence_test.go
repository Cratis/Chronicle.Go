// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/serialization"
)

type distinctDecodeIDs struct {
	ID         string
	ExternalID string `json:"ID"`
}

func TestReadModelPlanDoesNotReuseAnotherFieldsExactName(t *testing.T) {
	plan, err := serialization.CompileReadModel(reflect.TypeFor[distinctDecodeIDs]())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		data string
		want distinctDecodeIDs
	}{
		{`{"Id":"model"}`, distinctDecodeIDs{ID: "model"}},
		{`{"ID":"external"}`, distinctDecodeIDs{ExternalID: "external"}},
		{`{"Id":null,"ID":"external"}`, distinctDecodeIDs{ExternalID: "external"}},
		{`{"Id":"model","ID":null}`, distinctDecodeIDs{ID: "model"}},
	} {
		t.Run(tc.data, func(t *testing.T) {
			var got distinctDecodeIDs
			if err := plan.Unmarshal([]byte(tc.data), &got); err != nil || got != tc.want {
				t.Fatalf("decode = %+v, %v; want %+v", got, err, tc.want)
			}
		})
	}
}
