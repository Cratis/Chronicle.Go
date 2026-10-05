// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package captures_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/captures"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
)

type StatusChanged struct{ Status string }

func TestCaptureDeclarationGoldenAndSnapshot(t *testing.T) {
	event, err := events.Define[StatusChanged](events.WithID("InvoiceStatusChanged"))
	if err != nil {
		t.Fatal(err)
	}
	fields := map[string]string{"Status": "$.status"}
	rule := captures.Append(event, captures.PropertyChanges("status"), fields)
	fields["Status"] = "mutated"
	b := new(captures.Builder).From(captures.API("InvoicingApi", "/invoices", "10m")).Key("id").Append(rule)
	d, err := b.Build("InvoiceCapture")
	if err != nil {
		t.Fatal(err)
	}
	want := "capture InvoiceCapture\n  source api\n    api InvoicingApi\n    route /invoices\n    poll 10m\n  key id\n  append InvoiceStatusChanged\n    when status\n    Status = $.status\n"
	if d.Declaration() != want {
		t.Fatalf("declaration =\n%s", d.Declaration())
	}
	b.Key("changed")
	second, err := b.Build("InvoiceCapture")
	if err != nil {
		t.Fatal(err)
	}
	if second.ID() == d.ID() || d.Declaration() != want {
		t.Fatal("build did not snapshot or allocate identity")
	}
	if strings.Contains(fmt.Sprintf("%#v", d), "invoices") {
		t.Fatal("declaration literals exposed")
	}
}
func TestCaptureScopesConditionsAndMappings(t *testing.T) {
	event, err := events.Define[StatusChanged](events.WithID("Changed"))
	if err != nil {
		t.Fatal(err)
	}
	mappings := []captures.Mapping{captures.Rename("old", "new"), captures.Template("name", "${first} ${last}"), captures.Translate("status", "status", captures.Translation{From: "draft", To: "open"}), captures.Split("name", ",", "first", "last")}
	conditions := []captures.Condition{captures.Added(), captures.Removed(), captures.AnyOf("one", "two"), captures.AllOf("one", "two"), captures.Transition("status", "open", "closed"), captures.Expression("$.value > 3")}
	var rules []captures.AppendRule
	for _, condition := range conditions {
		rules = append(rules, captures.Append(event, condition, nil))
	}
	scope := captures.NewScope(mappings, rules...)
	for _, source := range []captures.Source{captures.Webhook("/invoices"), captures.MessageTopic("invoices")} {
		d, err := new(captures.Builder).From(source).Key("id").Map(mappings...).Append(rules...).Nested("address", scope).Children("lines", "number", scope).Build("InvoiceCapture")
		if err != nil {
			t.Fatal(err)
		}
		for _, expected := range []string{"map\n", "new = old", "status = status translate", "split name by \",\"", "when one or two", "when one and two", "when status from \"open\" to \"closed\"", "when added", "when removed", "nested address", "children lines identified by number"} {
			if !strings.Contains(d.Declaration(), expected) {
				t.Error("missing", expected)
			}
		}
	}
}
func TestCaptureValidationRejectsMissingOrInjectedDeclarations(t *testing.T) {
	event, err := events.Define[StatusChanged](events.WithID("Changed"))
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range []*captures.Builder{
		new(captures.Builder), new(captures.Builder).From(captures.API("service", "", "")),
		new(captures.Builder).From(captures.Webhook("/path\n  append Injected")).Key("id"),
		new(captures.Builder).From(captures.API("service", "", "1m")).Key("id").Append(captures.Append(event, captures.Condition{}, nil)),
		new(captures.Builder).From(captures.API("service", "", "1m")).Key("id").Append(captures.Append(event, captures.Added(), map[string]string{"Status": "$.status\n  key injected"})),
	} {
		if _, err := b.Build("Invalid"); !errors.Is(err, faults.ErrInvalidConfiguration) {
			t.Fatal("invalid definition accepted", err)
		}
	}
	dashed, err := events.Define[StatusChanged](events.WithID("persisted-id"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = new(captures.Builder).From(captures.API("service", "", "1m")).Key("id").Append(captures.Append(dashed, captures.Added(), nil)).Build("Invalid"); !errors.Is(err, faults.ErrUnsupported) {
		t.Fatal("persisted ID silently renamed", err)
	}
}

func ExampleBuilder() {
	event, err := events.Define[StatusChanged](events.WithID("InvoiceStatusChanged"))
	if err != nil {
		panic(err)
	}
	definition, err := new(captures.Builder).
		From(captures.API("InvoicingApi", "/invoices", "10m")).
		Key("id").
		Append(captures.Append(event, captures.PropertyChanges("status"), map[string]string{"Status": "$.status"})).
		Build("InvoiceCapture")
	if err != nil {
		panic(err)
	}
	fmt.Print(definition.Declaration())
	// Output:
	// capture InvoiceCapture
	//   source api
	//     api InvoicingApi
	//     route /invoices
	//     poll 10m
	//   key id
	//   append InvoiceStatusChanged
	//     when status
	//     Status = $.status
}
