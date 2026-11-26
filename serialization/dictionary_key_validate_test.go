// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	dictionaryStatusOK           = "ok"
	dictionaryStatusError        = "error"
	dictionaryStatusNotAttempted = "not-attempted"
)

type dictionaryCase struct {
	ID               string   `json:"id"`
	DeclaredType     string   `json:"declaredType"`
	InputID          string   `json:"inputId"`
	Stage            string   `json:"stage"`
	Method           string   `json:"method"`
	SchemaAPI        string   `json:"schemaAPI"`
	DependsOn        []string `json:"dependsOn"`
	ValueDescription *string  `json:"valueDescription"`
	Input            *string  `json:"input"`
	Status           string   `json:"status"`
	Output           *string  `json:"output"`
	Category         *string  `json:"category"`
	ConstructedKeys  *int     `json:"constructedKeyCount"`
}

type dictionaryAssembly struct {
	Name                 string `json:"name"`
	InformationalVersion string `json:"informationalVersion"`
}

type dictionaryProfile struct {
	NamingPolicy string `json:"namingPolicy"`
	EventOptions struct {
		Converters []string `json:"converters"`
	} `json:"eventOptions"`
	Cases []dictionaryCase `json:"cases"`
}

type dictionaryFixture struct {
	Profile     string `json:"profile"`
	GoAdmission string `json:"goAdmission"`
	Runtime     struct {
		Version string `json:"version"`
	} `json:"runtime"`
	Isolation struct {
		RegistryCalls *int `json:"registryCalls"`
	} `json:"isolation"`
	Assemblies []dictionaryAssembly `json:"assemblies"`
	Profiles   []dictionaryProfile  `json:"profiles"`
}

// dictionaryRef names one observation within a naming policy.
type dictionaryRef struct {
	typ, input, stage, api string
}

func (ref dictionaryRef) id(policy dictionaryPolicy) string {
	id := policy.short + "/" + ref.typ + "/" + ref.input + "/" + ref.stage
	if ref.api != "" {
		id += "/" + ref.api
	}
	return id
}

type dictionaryExpectation struct {
	ref  dictionaryRef
	id   string
	deps []string
}

type expectationBuilder struct {
	policy dictionaryPolicy
	out    []dictionaryExpectation
}

func (b *expectationBuilder) add(ref dictionaryRef, deps ...string) string {
	id := ref.id(b.policy)
	b.out = append(b.out, dictionaryExpectation{ref, id, deps})
	return id
}

// dictionaryExpectations derives the full case inventory and dependency graph.
func dictionaryExpectations(policy dictionaryPolicy) []dictionaryExpectation {
	b := &expectationBuilder{policy: policy}
	for _, typ := range dictionaryInventory {
		b.addType(typ)
	}
	return b.out
}

func (b *expectationBuilder) addType(typ dictionaryType) {
	schemas := map[string]string{}
	for _, api := range dictionarySchemaAPIs {
		schemas[api] = b.add(dictionaryRef{typ.name, "type", "schema", api})
	}
	for _, input := range typ.inputs {
		b.addInput(typ.name, input.id, schemas)
	}
}

func (b *expectationBuilder) addInput(typ, input string, schemas map[string]string) {
	stage := func(name, api string, deps ...string) string {
		return b.add(dictionaryRef{typ, input, name, api}, deps...)
	}
	var parseDeps []string
	if strings.HasPrefix(input, "value:") {
		parseDeps = []string{stage("serialize", "")}
	}
	parse := stage("parse", "", parseDeps...)
	read := stage("deserialize", "", parse)
	stage("lookup", "", read)
	stage("rewrite", "", read)
	for _, api := range dictionarySchemaAPIs {
		expando := stage("expando-read", api, parse, schemas[api])
		stage("expando-write", api, expando)
	}
}

func dictionaryMethod(ref dictionaryRef) string {
	if ref.stage == "schema" {
		return dictionaryMethods["schema/"+ref.api]
	}
	return dictionaryMethods[ref.stage]
}

// validateDictionaryStructure checks inventory, identity, dependencies and
// field consistency. Behavior is separately pinned by validateDictionaryBehavior.
func validateDictionaryStructure(fixture dictionaryFixture) error {
	if err := validateDictionaryIdentity(fixture); err != nil {
		return err
	}
	if len(fixture.Profiles) != len(dictionaryPolicies) {
		return errors.New("naming profile count changed")
	}
	for i, policy := range dictionaryPolicies {
		if err := validateDictionaryProfile(fixture.Profiles[i], policy); err != nil {
			return fmt.Errorf("%s: %w", policy.short, err)
		}
	}
	return nil
}

func validateDictionaryIdentity(fixture dictionaryFixture) error {
	if fixture.Profile != "chronicle-19.153.0_fundamentals-7.33.1_net-11.0.0" {
		return errors.New("capture profile changed")
	}
	if !strings.HasPrefix(fixture.GoAdmission, "not-implemented;") {
		return errors.New("admission statement changed")
	}
	if !strings.HasPrefix(fixture.Runtime.Version, "11.0.") {
		return errors.New("runtime is not .NET 11.0")
	}
	if fixture.Isolation.RegistryCalls == nil || *fixture.Isolation.RegistryCalls != 0 {
		return errors.New("capture isolation was not proven")
	}
	return validateDictionaryPins(fixture.Assemblies)
}

func validateDictionaryPins(assemblies []dictionaryAssembly) error {
	pins := map[string]string{
		"Cratis.Chronicle":    "9157c14098688ac0c6946a335bfabf1f44220c7c",
		"Cratis.Fundamentals": "6ae7eb7d133afdb7adaf161ca185eaa189c7372b",
	}
	for name, revision := range pins {
		if !dictionaryHasPin(assemblies, name, revision) {
			return fmt.Errorf("assembly %s does not carry its pinned revision", name)
		}
	}
	return nil
}

func dictionaryHasPin(assemblies []dictionaryAssembly, name, revision string) bool {
	for _, assembly := range assemblies {
		if strings.HasPrefix(assembly.Name, name+",") && strings.Contains(assembly.InformationalVersion, revision) {
			return true
		}
	}
	return false
}

func validateDictionaryProfile(profile dictionaryProfile, policy dictionaryPolicy) error {
	if profile.NamingPolicy != policy.name {
		return errors.New("naming policy order or identity changed")
	}
	if len(profile.EventOptions.Converters) == 0 {
		return errors.New("event converter inventory missing")
	}
	actual, err := indexDictionaryCases(profile.Cases)
	if err != nil {
		return err
	}
	expected := dictionaryExpectations(policy)
	if len(actual) != len(expected) {
		return fmt.Errorf("case count %d, want %d", len(actual), len(expected))
	}
	for _, want := range expected {
		if err := validateDictionaryExpected(want, actual); err != nil {
			return fmt.Errorf("case %s: %w", want.id, err)
		}
	}
	return nil
}

func indexDictionaryCases(cases []dictionaryCase) (map[string]dictionaryCase, error) {
	index := make(map[string]dictionaryCase, len(cases))
	for _, c := range cases {
		if _, exists := index[c.ID]; exists {
			return nil, fmt.Errorf("duplicate case %s", c.ID)
		}
		index[c.ID] = c
	}
	return index, nil
}

func validateDictionaryExpected(want dictionaryExpectation, all map[string]dictionaryCase) error {
	got, ok := all[want.id]
	if !ok {
		return errors.New("missing")
	}
	if err := validateDictionaryShape(got, want); err != nil {
		return err
	}
	if err := validateDictionaryRaw(got, want); err != nil {
		return err
	}
	blocked := false
	for _, dep := range want.deps {
		blocked = blocked || all[dep].Status != dictionaryStatusOK
	}
	if err := validateDictionaryStatus(got, blocked); err != nil {
		return err
	}
	if got.Status != dictionaryStatusOK {
		return nil
	}
	return validateDictionaryOutput(got, want)
}

func validateDictionaryShape(got dictionaryCase, want dictionaryExpectation) error {
	ref := want.ref
	if got.DeclaredType != ref.typ || got.InputID != ref.input || got.Stage != ref.stage || got.SchemaAPI != ref.api {
		return errors.New("identity fields do not match the case ID")
	}
	if got.Method != dictionaryMethod(ref) {
		return errors.New("method is not the actual packaged API")
	}
	if strings.Join(got.DependsOn, "|") != strings.Join(want.deps, "|") {
		return errors.New("dependency list changed")
	}
	return nil
}

// dictionaryStatusRules returns the field consistency violation for a status.
var dictionaryStatusRules = map[string]func(got dictionaryCase, blocked bool) bool{
	dictionaryStatusOK: func(got dictionaryCase, blocked bool) bool {
		return blocked || got.Output == nil || got.Category != nil
	},
	dictionaryStatusError: func(got dictionaryCase, blocked bool) bool {
		return blocked || got.Output != nil || got.Category == nil || *got.Category == ""
	},
	dictionaryStatusNotAttempted: func(got dictionaryCase, blocked bool) bool {
		return !blocked || got.Output != nil || got.Category != nil
	},
}

func validateDictionaryStatus(got dictionaryCase, blocked bool) error {
	violates, known := dictionaryStatusRules[got.Status]
	if !known {
		return errors.New("unknown status")
	}
	if violates(got, blocked) {
		return fmt.Errorf("%s result is inconsistent with its dependencies or fields", got.Status)
	}
	return nil
}

func validateDictionaryOutput(got dictionaryCase, want dictionaryExpectation) error {
	policy := dictionaryPolicyOf(got.ID)
	switch want.ref.stage {
	case "schema":
		return validateDictionarySchema(*got.Output, policy)
	case "serialize":
		return validateDictionaryWire(got, want, policy)
	case "lookup":
		return validateDictionaryLookup(*got.Output)
	}
	return nil
}

func dictionaryPolicyOf(id string) dictionaryPolicy {
	for _, policy := range dictionaryPolicies {
		if strings.HasPrefix(id, policy.short+"/") {
			return policy
		}
	}
	return dictionaryPolicy{}
}

func validateDictionarySchema(output string, policy dictionaryPolicy) error {
	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal([]byte(output), &schema); err != nil || schema.Properties[policy.root] == nil {
		return errors.New("schema role lost the dictionary property")
	}
	return nil
}

func validateDictionaryLookup(output string) error {
	var lookup struct {
		KeyCount json.RawMessage `json:"keyCount"`
	}
	// A null count is a valid observation: the read left the dictionary null.
	if err := json.Unmarshal([]byte(output), &lookup); err != nil || len(lookup.KeyCount) == 0 {
		return errors.New("lookup lost its key count")
	}
	return nil
}

func validateDictionaryWire(got dictionaryCase, want dictionaryExpectation, policy dictionaryPolicy) error {
	input := dictionaryInputFor(want.ref.typ, want.ref.input)
	names, isNull, err := dictionaryMemberNames(*got.Output, policy.root)
	if err != nil {
		return errors.New("wire payload is not an object")
	}
	if input.keys == dictionaryNullWire {
		if !isNull {
			return errors.New("null dictionary wrote members")
		}
		return nil
	}
	if isNull || len(names) != input.keys || dictionaryHasDuplicate(names) {
		return errors.New("distinct key count was lost")
	}
	if got.ConstructedKeys == nil || *got.ConstructedKeys != input.keys {
		return errors.New("constructed key count changed")
	}
	return nil
}

func dictionaryInputFor(typ, id string) dictionaryInput {
	for _, t := range dictionaryInventory {
		for _, input := range t.inputs {
			if t.name == typ && input.id == id {
				return input
			}
		}
	}
	return dictionaryInput{keys: -1}
}

// validateDictionaryRaw requires every raw stimulus to carry its input and, for
// the duplicate and colliding spellings, the exact ordered text written by the
// independent inventory, in both the parse and deserialize copies.
func validateDictionaryRaw(got dictionaryCase, want dictionaryExpectation) error {
	ref := want.ref
	if (ref.stage != "parse" && ref.stage != "deserialize") || !strings.HasPrefix(ref.input, "raw:") {
		return nil
	}
	if got.Input == nil {
		return errors.New("raw stimulus input missing")
	}
	expected, pinned := dictionaryRawText(dictionaryPolicyOf(got.ID), ref.typ, ref.input)
	if !pinned {
		return nil
	}
	if *got.Input != expected || (ref.stage == "parse" && (got.Output == nil || *got.Output != expected)) {
		return errors.New("raw stimulus is not the exact ordered text")
	}
	return nil
}

func dictionaryHasDuplicate(names []string) bool {
	seen := map[string]bool{}
	for _, name := range names {
		if seen[name] {
			return true
		}
		seen[name] = true
	}
	return false
}

// dictionaryMemberNames streams raw tokens so duplicate names are preserved.
// isNull reports a null or omitted dictionary property. The whole document must
// be one complete object: truncation and trailing data are errors.
func dictionaryMemberNames(text, root string) (names []string, isNull bool, err error) {
	decoder := json.NewDecoder(strings.NewReader(text))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return nil, false, errors.New("not an object")
	}
	// An omitted property is an observation equal to null: no members written.
	isNull = true
	seen := false
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return nil, false, err
		}
		if key != root {
			if err := dictionarySkipValue(decoder); err != nil {
				return nil, false, err
			}
			continue
		}
		if seen {
			return nil, false, errors.New("repeated root property")
		}
		seen = true
		if names, isNull, err = dictionaryReadMembers(decoder); err != nil {
			return nil, false, err
		}
	}
	if err := dictionaryExpectEnd(decoder); err != nil {
		return nil, false, err
	}
	return names, isNull, nil
}

func dictionaryExpectEnd(decoder *json.Decoder) error {
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return errors.New("object not closed")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing data after object")
	}
	return nil
}

func dictionaryReadMembers(decoder *json.Decoder) ([]string, bool, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, false, err
	}
	if token == nil {
		return nil, true, nil
	}
	if token != json.Delim('{') {
		return nil, false, errors.New("dictionary is not an object")
	}
	var names []string
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return nil, false, err
		}
		names = append(names, key.(string))
		if err := dictionarySkipValue(decoder); err != nil {
			return nil, false, err
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, false, errors.New("dictionary not closed")
	}
	return names, false, nil
}

// dictionaryDepthChange is +1 for an opening delimiter, -1 for a closing one.
func dictionaryDepthChange(token json.Token) int {
	delim, ok := token.(json.Delim)
	switch {
	case !ok:
		return 0
	case delim == '{' || delim == '[':
		return 1
	}
	return -1
}

func dictionarySkipValue(decoder *json.Decoder) error {
	for depth := 0; ; {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			return io.ErrUnexpectedEOF
		}
		if err != nil {
			return err
		}
		if depth += dictionaryDepthChange(token); depth == 0 {
			return nil
		}
	}
}
