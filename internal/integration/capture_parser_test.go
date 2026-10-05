//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/captures"
	"github.com/cratis/chronicle.go/events"
)

type parserCondition struct {
	Kind       string
	Properties []string
	From, To   *string
}
type parserCapture struct {
	Declaration  string
	Name, Event  string
	Conditions   []parserCondition
	Translations []captures.Translation
	Separators   []string
}

func TestScreenplayCaptureParserPreservesGoDeclarations(t *testing.T) {
	dll := os.Getenv("CHRONICLE_CAPTURE_PARSER_DLL")
	if dll == "" {
		t.Skip("set CHRONICLE_CAPTURE_PARSER_DLL to the built testdata/captureparser/CaptureParser.dll for pinned AST checks")
	}
	event, err := events.Define[IntegrationPublished](events.WithID("IntegrationPublished"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []parserCapture
	for _, name := range []string{"Capture", "_Capture2", "Captureé"} {
		b := new(captures.Builder).From(captures.API("Service", "/items", "1m")).Key("id")
		test := parserCapture{Name: name, Event: "IntegrationPublished"}
		add := func(condition captures.Condition, kind string, properties []string, from, to *string) {
			b.Append(captures.Append(event, condition, nil))
			test.Conditions = append(test.Conditions, parserCondition{kind, properties, from, to})
		}
		for _, property := range []string{"added", "removed", "status"} {
			for _, condition := range []captures.Condition{captures.PropertyChanges(property), captures.AnyOf(property), captures.AllOf(property)} {
				add(condition, "PropertyChanged", []string{property}, nil, nil)
			}
		}
		add(captures.AnyOf("added", "removed"), "LogicalOr", []string{"added", "removed"}, nil, nil)
		add(captures.AllOf("added", "removed"), "LogicalAnd", []string{"added", "removed"}, nil, nil)
		add(captures.Added(), "Added", []string{}, nil, nil)
		add(captures.Removed(), "Removed", []string{}, nil, nil)
		var mappings []captures.Mapping
		for _, literal := range []string{"", `C:\new`, `\d+`, "\"quoted\"\n\r\t雪", `ends\`} {
			from, to := literal, literal
			add(captures.Transition("status", from, to), "ValueTransition", []string{"status"}, &from, &to)
			translation := captures.Translation{From: literal, To: "open"}
			test.Translations = append(test.Translations, translation)
			test.Separators = append(test.Separators, literal)
			mappings = append(mappings, captures.Translate("status", "status", translation), captures.Split("status", literal, "first"))
		}
		b.Map(mappings...)
		definition, err := b.Build(name)
		if err != nil {
			t.Fatal(err)
		}
		test.Declaration = definition.Declaration()
		cases = append(cases, test)
	}
	input, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "dotnet", dll)
	command.Stdin = bytes.NewReader(input)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("pinned Screenplay AST check failed: %v\n%s", err, output)
	}
	t.Log(string(output))
}
