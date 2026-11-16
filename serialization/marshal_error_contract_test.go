// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/serialization"
)

func TestMarshalNullCollectionErrorsKeepPropertyContext(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		want  string
	}{
		{"slice", struct{ Values []*int }{[]*int{nil}}, "property Values: chronicle: null collection element at 0"},
		{"array", struct{ Values [1]*int }{}, "property Values: chronicle: null collection element at 0"},
		{"map", struct{ Values map[string]*int }{map[string]*int{"key": nil}}, "property Values: chronicle: null map value"},
		{"nested", struct{ Values map[string][]*int }{map[string][]*int{"key": {nil}}}, "property Values: chronicle: null collection element at 0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := serialization.Compile(reflect.TypeOf(tc.value))
			if err != nil {
				t.Fatal(err)
			}
			_, err = plan.Marshal(tc.value)
			if err == nil {
				t.Fatal("null collection entry admitted")
			}
			if err.Error() != tc.want {
				t.Fatalf("got %q, want %q", err, tc.want)
			}
		})
	}
}
