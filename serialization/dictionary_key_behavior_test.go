// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

// This oracle pins the critical observations of the packaged capture. Its
// expectations are written from independently constructed inputs and the owning
// source (Fundamentals ComplexKeyDictionaryJsonConverter assigns by decoded key,
// so the last spelling wins; enum keys are not supported by the configured
// options). It is deliberately small and separate from the case inventory.

const dictionaryNotSupported = "System.NotSupportedException>System.NotSupportedException"

type dictionaryRefusal struct {
	typ, input, stage string
}

// dictionaryRefusals are observed refusals; later stages are not attempted.
var dictionaryRefusals = []dictionaryRefusal{
	{"EnumKeyed", "value:declared", "serialize"},
	{"EnumKeyed", "value:unknown-member", "serialize"},
	{"EnumKeyed", "raw:names-collide", "deserialize"},
	{"EnumKeyed", "raw:numeric-name", "deserialize"},
	{"EnumKeyed", "raw:unknown-name", "deserialize"},
}

type dictionaryRead struct {
	typ, input string
	keyCount   int
	// found maps a lookup label to the JSON text of the value; every other
	// looked-up label must be absent.
	found map[string]string
	// rewriteMembers is the expected member count of the rewritten dictionary;
	// rows with an exact rewrite text additionally pin key and winning value.
	rewriteMembers int
}

var dictionaryReads = []dictionaryRead{
	{"StringKeyed", "value:ordinal-case", 2, map[string]string{"a": "1", "A": "2"}, 2},
	{"StringKeyed", "value:escaped", 2, map[string]string{"escaped": "1", "line-break": "2"}, 2},
	// The last duplicate spelling wins and only one member is rewritten.
	{"StringKeyed", "raw:duplicate-spelling", 1, map[string]string{"a": "2"}, 1},
	{"GuidKeyed", "value:zero-and-nonzero", 2, map[string]string{"zero": `"zero"`, "known": `"nonzero"`}, 2},
	{"GuidKeyed", "raw:spellings-collide", 1, map[string]string{"known": `"y"`}, 1},
	{"EnumKeyed", "raw:missing", 0, map[string]string{}, 0},
	{"ConceptKeyed", "value:escaped", 2, map[string]string{"escaped": `"a"`, "unicode": `"b"`}, 2},
	{"RecordKeyed", "value:two-distinct", 2, map[string]string{"a1": `"x"`, "a2": `"y"`}, 2},
	{"RecordKeyed", "value:escaped", 2, map[string]string{"escaped": `"x"`, "unicode": `"y"`}, 2},
	{"RecordKeyed", "raw:spellings-collide", 1, map[string]string{"a1": `"y"`}, 1},
}

func validateDictionaryFixture(fixture dictionaryFixture) error {
	if err := validateDictionaryStructure(fixture); err != nil {
		return err
	}
	return validateDictionaryBehavior(fixture)
}

func validateDictionaryBehavior(fixture dictionaryFixture) error {
	for i, policy := range dictionaryPolicies {
		cases, err := indexDictionaryCases(fixture.Profiles[i].Cases)
		if err != nil {
			return err
		}
		at := func(typ, input, stage string) dictionaryCase {
			return cases[dictionaryRef{typ, input, stage, ""}.id(policy)]
		}
		for _, refusal := range dictionaryRefusals {
			if err := checkDictionaryRefusal(at(refusal.typ, refusal.input, refusal.stage)); err != nil {
				return fmt.Errorf("%s %s/%s: %w", policy.short, refusal.typ, refusal.input, err)
			}
		}
		for _, read := range dictionaryReads {
			if err := checkDictionaryRead(read, at, policy); err != nil {
				return fmt.Errorf("%s %s/%s: %w", policy.short, read.typ, read.input, err)
			}
		}
	}
	return nil
}

func checkDictionaryRefusal(c dictionaryCase) error {
	if c.Status != dictionaryStatusError || c.Category == nil || *c.Category != dictionaryNotSupported {
		return errors.New("refusal changed")
	}
	return nil
}

func checkDictionaryRead(read dictionaryRead, at func(typ, input, stage string) dictionaryCase, policy dictionaryPolicy) error {
	lookup, rewrite := at(read.typ, read.input, "lookup"), at(read.typ, read.input, "rewrite")
	if at(read.typ, read.input, "deserialize").Status != dictionaryStatusOK || lookup.Output == nil || rewrite.Output == nil {
		return errors.New("read did not succeed")
	}
	if err := checkDictionaryLookup(read, *lookup.Output); err != nil {
		return err
	}
	if exact, pinned := dictionaryRewriteText(policy, read.typ, read.input); pinned && *rewrite.Output != exact {
		return errors.New("rewrite is not the exact canonical text")
	}
	names, _, err := dictionaryMemberNames(*rewrite.Output, policy.root)
	if err != nil || len(names) != read.rewriteMembers || dictionaryHasDuplicate(names) {
		return errors.New("rewrite member count changed")
	}
	if written := at(read.typ, read.input, "serialize"); written.Output != nil && *written.Output != *rewrite.Output {
		return errors.New("rewrite differs from the original write")
	}
	return nil
}

func checkDictionaryLookup(read dictionaryRead, output string) error {
	var lookup struct {
		KeyCount *int `json:"keyCount"`
		Lookups  []struct {
			Key   string  `json:"key"`
			Found bool    `json:"found"`
			Value *string `json:"value"`
		} `json:"lookups"`
	}
	if err := json.Unmarshal([]byte(output), &lookup); err != nil || lookup.KeyCount == nil || *lookup.KeyCount != read.keyCount {
		return errors.New("key count changed")
	}
	got := map[string]string{}
	for _, item := range lookup.Lookups {
		if item.Found && item.Value != nil {
			got[item.Key] = *item.Value
		}
	}
	if !reflect.DeepEqual(got, read.found) {
		return errors.New("lookup winners changed")
	}
	return nil
}
