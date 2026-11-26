// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import "strings"

// This inventory is written independently of capture.cs: it names every
// observation the packaged dictionary-key capture must contain. It is a list of
// stimuli and their structural invariants, never expected wire output.

type dictionaryInput struct {
	id string
	// keys is the number of distinct dictionary keys constructed for a value
	// input; dictionaryNullWire marks a null dictionary; raw inputs use -1.
	keys int
	// raw is "duplicate" when the stimulus repeats one property spelling,
	// "collide" when two distinct spellings may resolve to one typed key.
	raw string
}

const dictionaryNullWire = -2

type dictionaryType struct {
	name   string
	inputs []dictionaryInput
}

func dictionaryValue(id string, keys int) dictionaryInput {
	return dictionaryInput{id: "value:" + id, keys: keys}
}

func dictionaryRaw(id, kind string) dictionaryInput {
	return dictionaryInput{id: "raw:" + id, keys: -1, raw: kind}
}

var dictionaryInventory = []dictionaryType{
	{"StringKeyed", []dictionaryInput{
		dictionaryValue("empty", 0), dictionaryValue("ordinal-case", 2), dictionaryValue("escaped", 2),
		dictionaryValue("null-dictionary", dictionaryNullWire),
		dictionaryRaw("missing", ""), dictionaryRaw("null", ""), dictionaryRaw("empty", ""),
		dictionaryRaw("duplicate-spelling", "duplicate"),
	}},
	{"GuidKeyed", []dictionaryInput{
		dictionaryValue("zero-and-nonzero", 2),
		dictionaryRaw("missing", ""), dictionaryRaw("spellings-collide", "collide"),
	}},
	{"EnumKeyed", []dictionaryInput{
		dictionaryValue("declared", 2), dictionaryValue("unknown-member", 2),
		dictionaryRaw("missing", ""), dictionaryRaw("names-collide", "collide"),
		dictionaryRaw("numeric-name", ""), dictionaryRaw("unknown-name", ""),
	}},
	{"ConceptKeyed", []dictionaryInput{
		dictionaryValue("escaped", 2), dictionaryRaw("missing", ""),
	}},
	{"RecordKeyed", []dictionaryInput{
		dictionaryValue("two-distinct", 2), dictionaryValue("escaped", 2),
		dictionaryRaw("missing", ""), dictionaryRaw("spellings-collide", "collide"),
	}},
	{"NestedKeyed", []dictionaryInput{
		dictionaryValue("nested", 2), dictionaryRaw("missing", ""),
	}},
	{"NullableKeyed", []dictionaryInput{
		dictionaryValue("null-value", 2), dictionaryRaw("missing", ""),
	}},
}

type dictionaryPolicy struct {
	name, short, root string
}

var dictionaryPolicies = []dictionaryPolicy{
	{"Cratis.Serialization.DefaultNamingPolicy", "default", "Values"},
	{"Cratis.Serialization.CamelCaseNamingPolicy", "camelCase", "values"},
}

var dictionarySchemaAPIs = []string{"Generate", "GenerateForReadModel"}

var dictionaryMethods = map[string]string{
	"serialize":                   "EventSerializer.Serialize",
	"parse":                       "JsonNode.Parse",
	"deserialize":                 "EventSerializer.Deserialize",
	"lookup":                      "IDictionary.Contains+indexer",
	"rewrite":                     "EventSerializer.Serialize",
	"expando-read":                "ExpandoObjectConverter.ToExpandoObject",
	"expando-write":               "ExpandoObjectConverter.ToJsonObject",
	"schema/Generate":             "JsonSchemaGenerator.Generate",
	"schema/GenerateForReadModel": "JsonSchemaGenerator.GenerateForReadModel",
}

// dictionaryRawTemplates are the exact stimuli, written from the independent
// intent (not from the captured output). $V is the policy's root name, $F and $S
// the record member names. The record keys are JSON text inside a JSON string.
var dictionaryRawTemplates = map[string]string{
	"StringKeyed/raw:duplicate-spelling": `{"$V":{"a":1,"a":2}}`,
	"GuidKeyed/raw:spellings-collide":    `{"$V":{"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa":"x","AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA":"y"}}`,
	"EnumKeyed/raw:names-collide":        `{"$V":{"One":"x","Alias":"y"}}`,
	"RecordKeyed/raw:spellings-collide":  `{"$V":{"{\u0022$F\u0022:\u0022a\u0022,\u0022$S\u0022:1}":"x","{\u0022$F\u0022:\u0022a\u0022, \u0022$S\u0022:1}":"y"}}`,
}

// dictionaryRewriteTemplates are the exact canonical rewrites: one member, the
// canonical (first-written) key spelling, and the last spelling's value.
var dictionaryRewriteTemplates = map[string]string{
	"StringKeyed/raw:duplicate-spelling": `{"$V":{"a":2}}`,
	"GuidKeyed/raw:spellings-collide":    `{"$V":{"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa":"y"}}`,
	"RecordKeyed/raw:spellings-collide":  `{"$V":{"{\u0022$F\u0022:\u0022a\u0022,\u0022$S\u0022:1}":"y"}}`,
}

func dictionaryExpand(template string, policy dictionaryPolicy) string {
	first, second := "First", "Second"
	if policy.short == "camelCase" {
		first, second = "first", "second"
	}
	return strings.NewReplacer("$V", policy.root, "$F", first, "$S", second).Replace(template)
}

func dictionaryRawText(policy dictionaryPolicy, typ, input string) (string, bool) {
	template, ok := dictionaryRawTemplates[typ+"/"+input]
	return dictionaryExpand(template, policy), ok
}

func dictionaryRewriteText(policy dictionaryPolicy, typ, input string) (string, bool) {
	template, ok := dictionaryRewriteTemplates[typ+"/"+input]
	return dictionaryExpand(template, policy), ok
}
