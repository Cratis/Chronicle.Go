// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// These tests validate captured .NET evidence for dictionary keys. They do not
// describe a Go codec: no Go dictionary admission is implied or tested here.

func loadDictionaryFixture(t *testing.T) (dictionaryFixture, []byte) {
	t.Helper()
	data, err := os.ReadFile("testdata/dictionary-keys/profile.json")
	if err != nil {
		t.Fatal(err)
	}
	return decodeDictionaryFixture(t, data), data
}

// decodeDictionaryFixture also deep-copies, so mutations stay local.
func decodeDictionaryFixture(t *testing.T, data []byte) dictionaryFixture {
	t.Helper()
	var fixture dictionaryFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func findDictionaryCase(t *testing.T, fixture *dictionaryFixture, match func(dictionaryCase) bool) (profile, index int) {
	t.Helper()
	for p := range fixture.Profiles {
		for i, c := range fixture.Profiles[p].Cases {
			if match(c) {
				return p, i
			}
		}
	}
	t.Fatal("no case matches the mutation target; the fixture would be vacuous")
	return 0, 0
}

func TestDictionaryKeyPackagedProfile(t *testing.T) {
	fixture, data := loadDictionaryFixture(t)
	if err := validateDictionaryFixture(fixture); err != nil {
		t.Fatal(err)
	}
	t.Run("provenance names the committed bytes", func(t *testing.T) { checkDictionaryProvenance(t, data) })
	t.Run("fixture is not vacuous", func(t *testing.T) { checkDictionaryNotVacuous(t, fixture) })
	t.Run("coherent refusal replaced by success is rejected by behavior", checkDictionaryCoherentSuccess(data))
	for name, mutate := range dictionaryMutations {
		t.Run("rejects "+name, func(t *testing.T) {
			mutated := decodeDictionaryFixture(t, data)
			mutate(t, &mutated)
			if err := validateDictionaryFixture(mutated); err == nil {
				t.Fatal("mutated fixture was accepted")
			}
		})
	}
}

func checkDictionaryProvenance(t *testing.T, data []byte) {
	t.Helper()
	provenance, err := os.ReadFile("testdata/dictionary-keys/provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var recorded struct {
		Fixture   string            `json:"fixture"`
		SHA256    string            `json:"sha256"`
		Packages  map[string]string `json:"packages"`
		Generator string            `json:"generator"`
	}
	if err := json.Unmarshal(provenance, &recorded); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if recorded.Fixture != "profile.json" || recorded.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatal("provenance does not describe profile.json")
	}
	if recorded.Packages["Cratis.Chronicle"] != "19.153.0" || recorded.Packages["Cratis.Fundamentals"] != "7.33.1" || recorded.Generator != "capture.cs" {
		t.Fatal("provenance package pins changed")
	}
}

func checkDictionaryNotVacuous(t *testing.T, fixture dictionaryFixture) {
	t.Helper()
	counts := map[string]int{}
	for _, profile := range fixture.Profiles {
		for _, c := range profile.Cases {
			counts[c.Stage+"/"+c.Status]++
		}
	}
	for _, stage := range []string{"serialize", "deserialize", "schema", "expando-read", "expando-write", "lookup", "rewrite"} {
		if counts[stage+"/"+dictionaryStatusOK] == 0 {
			t.Errorf("no successful %s observation", stage)
		}
	}
	failures := counts["deserialize/"+dictionaryStatusError] + counts["serialize/"+dictionaryStatusError] + counts["parse/"+dictionaryStatusError]
	if failures == 0 {
		t.Error("capture recorded no failure; refusals must stay observable")
	}
}

// dictionaryTarget finds a case of the first (default) naming profile.
func dictionaryTarget(f *dictionaryFixture, typ, input, stage string) *dictionaryCase {
	id := dictionaryRef{typ, input, stage, ""}.id(dictionaryPolicies[0])
	for i := range f.Profiles[0].Cases {
		if f.Profiles[0].Cases[i].ID == id {
			return &f.Profiles[0].Cases[i]
		}
	}
	panic("no such case " + id)
}

// swapDictionaryRaw replaces a raw stimulus consistently in the parse input and
// output and the deserialize input, as a coherent but wrong capture would.
func swapDictionaryRaw(f *dictionaryFixture, typ, input, text string) {
	parse := dictionaryTarget(f, typ, input, "parse")
	parse.Input, parse.Output = &text, &text
	dictionaryTarget(f, typ, input, "deserialize").Input = &text
}

func setDictionaryOutput(c *dictionaryCase, output string) { c.Output = &output }

// checkDictionaryCoherentSuccess turns a refused read into a structurally valid
// success; only the behavior oracle may reject it.
func checkDictionaryCoherentSuccess(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		mutated := decodeDictionaryFixture(t, data)
		for _, stage := range []string{"deserialize", "lookup", "rewrite"} {
			c := dictionaryTarget(&mutated, "EnumKeyed", "raw:names-collide", stage)
			c.Status, c.Category = dictionaryStatusOK, nil
			setDictionaryOutput(c, `{"keyCount":0,"lookups":[]}`)
		}
		if err := validateDictionaryStructure(mutated); err != nil {
			t.Fatalf("mutant must be structurally coherent: %v", err)
		}
		if err := validateDictionaryBehavior(mutated); err == nil {
			t.Fatal("behavior oracle accepted a refusal replaced by success")
		}
	}
}

var dictionaryMutations = map[string]func(*testing.T, *dictionaryFixture){
	"wrong duplicate winner": func(t *testing.T, f *dictionaryFixture) {
		c := dictionaryTarget(f, "StringKeyed", "raw:duplicate-spelling", "lookup")
		setDictionaryOutput(c, strings.Replace(*c.Output, `"value":"2"`, `"value":"1"`, 1))
	},
	"wrong collision key count": func(t *testing.T, f *dictionaryFixture) {
		c := dictionaryTarget(f, "GuidKeyed", "raw:spellings-collide", "lookup")
		setDictionaryOutput(c, strings.Replace(*c.Output, `"keyCount":1`, `"keyCount":2`, 1))
	},
	"bad rewrite": func(t *testing.T, f *dictionaryFixture) {
		setDictionaryOutput(dictionaryTarget(f, "StringKeyed", "value:ordinal-case", "rewrite"), `{"Values":{"a":1}}`)
	},
	"duplicate output name": func(t *testing.T, f *dictionaryFixture) {
		setDictionaryOutput(dictionaryTarget(f, "StringKeyed", "value:ordinal-case", "serialize"), `{"Values":{"a":1,"a":2}}`)
	},
	"missing raw input": func(t *testing.T, f *dictionaryFixture) {
		dictionaryTarget(f, "StringKeyed", "raw:duplicate-spelling", "parse").Input = nil
	},
	"truncated output": func(t *testing.T, f *dictionaryFixture) {
		c := dictionaryTarget(f, "StringKeyed", "value:ordinal-case", "serialize")
		setDictionaryOutput(c, (*c.Output)[:len(*c.Output)-1])
	},
	"trailing data": func(t *testing.T, f *dictionaryFixture) {
		c := dictionaryTarget(f, "StringKeyed", "value:ordinal-case", "serialize")
		setDictionaryOutput(c, *c.Output+"}")
	},
	"relation preserving bad guid pair": func(t *testing.T, f *dictionaryFixture) {
		input := `{"Values":{"11111111-1111-1111-1111-111111111111":"x","11111111-1111-1111-1111-111111111111":"y"}}`
		swapDictionaryRaw(f, "GuidKeyed", "raw:spellings-collide", input)
	},
	"consistent x/y swap": func(t *testing.T, f *dictionaryFixture) {
		swapped := `{"Values":{"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa":"y","AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA":"x"}}`
		swapDictionaryRaw(f, "GuidKeyed", "raw:spellings-collide", swapped)
	},
	"guid rewrite winner x": func(t *testing.T, f *dictionaryFixture) {
		setDictionaryOutput(dictionaryTarget(f, "GuidKeyed", "raw:spellings-collide", "rewrite"), `{"Values":{"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa":"x"}}`)
	},
	"record rewrite wrong key": func(t *testing.T, f *dictionaryFixture) {
		c := dictionaryTarget(f, "RecordKeyed", "raw:spellings-collide", "rewrite")
		setDictionaryOutput(c, strings.Replace(*c.Output, "\\u0022a\\u0022,", "\\u0022b\\u0022,", 1))
	},
	"dropped failure": func(t *testing.T, f *dictionaryFixture) {
		p, i := findDictionaryCase(t, f, func(c dictionaryCase) bool { return c.Status == dictionaryStatusError })
		cases := f.Profiles[p].Cases
		f.Profiles[p].Cases = append(cases[:i:i], cases[i+1:]...)
	},
	"failure reported as success": func(t *testing.T, f *dictionaryFixture) {
		p, i := findDictionaryCase(t, f, func(c dictionaryCase) bool {
			return c.Stage == "deserialize" && c.Status == dictionaryStatusError
		})
		f.Profiles[p].Cases[i].Status = dictionaryStatusOK
	},
	"wrong schema role": func(t *testing.T, f *dictionaryFixture) {
		p, i := findDictionaryCase(t, f, func(c dictionaryCase) bool { return c.SchemaAPI == "Generate" && c.Stage == "schema" })
		f.Profiles[p].Cases[i].SchemaAPI = "GenerateForReadModel"
	},
	"duplicate ID": func(t *testing.T, f *dictionaryFixture) {
		f.Profiles[0].Cases[1].ID = f.Profiles[0].Cases[0].ID
	},
	"swapped naming policy": func(t *testing.T, f *dictionaryFixture) {
		f.Profiles[0].NamingPolicy, f.Profiles[1].NamingPolicy = f.Profiles[1].NamingPolicy, f.Profiles[0].NamingPolicy
	},
	"lost distinct key count": func(t *testing.T, f *dictionaryFixture) {
		p, i := findDictionaryCase(t, f, func(c dictionaryCase) bool {
			return c.Stage == "serialize" && c.Status == dictionaryStatusOK && c.ConstructedKeys != nil && *c.ConstructedKeys > 1
		})
		lost := *f.Profiles[p].Cases[i].ConstructedKeys - 1
		f.Profiles[p].Cases[i].ConstructedKeys = &lost
	},
	"dependent stage attempted after failure": func(t *testing.T, f *dictionaryFixture) {
		p, i := findDictionaryCase(t, f, func(c dictionaryCase) bool { return c.Status == dictionaryStatusNotAttempted })
		output := "{}"
		f.Profiles[p].Cases[i].Status, f.Profiles[p].Cases[i].Output = dictionaryStatusOK, &output
	},
}
