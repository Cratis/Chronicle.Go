// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/internal/derivedfixtures"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

func TestDerivedDuplicateMembersFailBeforeDecodeOrSnapshotRebinding(t *testing.T) {
	plan := derivedPlan[derivedfixtures.MembersChanged](t, derivedCodecs(t), serialization.PreservePropertyNames)
	next, err := plan.WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	for name, input := range map[string]string{
		"nested duplicate parent hiding discriminator": `{"Primary":{"children":[{"_derivedTypeId":"robot","_derivedTypeId":"human"}],"children":[],"_derivedTypeId":"human"}}`,
		"containing object":                            `{"Primary":{"_derivedTypeId":"secret"},"Primary":{"_derivedTypeId":"robot"}}`,
		"ordinary nested property":                     `{"Primary":{"details":{"Name":"secret","Name":"public"},"_derivedTypeId":"human"}}`,
		"map key hiding discriminator":                 `{"Lookup":{"secret":{"_derivedTypeId":"robot","_derivedTypeId":"human"},"secret":{"_derivedTypeId":"robot"}}}`,
		"array nested in unknown property":             `{"unknown":[[{},[{"secret":1,"secret":2}]]]}`,
		"array member":                                 `{"Members":[{"count":1,"count":2,"_derivedTypeId":"robot"}]}`,
		"escaped ordinary property":                    `{"Primary":{"name":"secret","\u006eame":"public","_derivedTypeId":"human"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			before := derivedfixtures.Sample()
			value := before
			err := plan.Unmarshal([]byte(input), &value)
			if !errors.Is(err, faults.ErrProtocol) || strings.Contains(err.Error(), "secret") || !reflect.DeepEqual(value, before) {
				t.Fatalf("decode published data or unsafe error: %v", err)
			}
			data, err := plan.RebindJSON([]byte(input), next)
			if !errors.Is(err, faults.ErrProtocol) || data != nil || strings.Contains(err.Error(), "secret") {
				t.Fatalf("rebind published data or unsafe error: %v", err)
			}
		})
	}
}

func TestDerivedDuplicateScanRunsBeforeApplicationCodecs(t *testing.T) {
	codecs, err := serialization.NewCodecs(serialization.Derived[any, callbackVariant]("callback"))
	if err != nil {
		t.Fatal(err)
	}
	plan := derivedPlan[derivedEnvelope](t, codecs, 0)
	var value derivedEnvelope
	err = plan.Unmarshal([]byte(`{"value":{"value":"panic","_derivedTypeId":"callback"},"unknown":{"secret":1,"secret":2}}`), &value)
	var callback *serialization.CallbackError
	if !errors.Is(err, faults.ErrProtocol) || errors.As(err, &callback) || value.Value != nil {
		t.Fatalf("application codec ran before full structural validation: %v", err)
	}
}

func TestDerivedDuplicateScanCoversUnusedFixedArrayElements(t *testing.T) {
	type fixed struct{ Members [1]derivedfixtures.Member }
	plan := derivedPlan[fixed](t, derivedCodecs(t), serialization.PreservePropertyNames)
	var value fixed
	err := plan.Unmarshal([]byte(`{"Members":[{"_derivedTypeId":"robot"},{"secret":1,"secret":2}]}`), &value)
	if !errors.Is(err, faults.ErrProtocol) || value.Members[0] != nil {
		t.Fatalf("ignored array element escaped validation: %v", err)
	}
}

func TestDerivedDuplicateScanKeepsValidNestedFamiliesAndCaseSensitiveMembers(t *testing.T) {
	plan := derivedPlan[derivedfixtures.MembersChanged](t, derivedCodecs(t), serialization.PreservePropertyNames)
	input := []byte(`{"Primary":{"children":[{"children":[{"count":7,"_derivedTypeId":"robot"}],"_derivedTypeId":"human"}],"_derivedTypeId":"human"},"Lookup":{"x":{"count":1,"_derivedTypeId":"robot"},"X":{"count":2,"_derivedTypeId":"robot"}}}`)
	var value derivedfixtures.MembersChanged
	if err := plan.Unmarshal(input, &value); err != nil {
		t.Fatal(err)
	}
	child := value.Primary.(*derivedfixtures.HumanValue).Children[0].(*derivedfixtures.HumanValue).Children[0]
	if child != (derivedfixtures.RobotValue{Count: 7}) || len(value.Lookup) != 2 {
		t.Fatal("nested family or exact map names lost")
	}
	next, err := plan.WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	data, err := plan.RebindJSON(input, next)
	if err != nil {
		t.Fatal(err)
	}
	var rebound derivedfixtures.MembersChanged
	if err := next.Unmarshal(data, &rebound); err != nil || !reflect.DeepEqual(value, rebound) {
		t.Fatalf("valid nested family rebind: %v", err)
	}
}
