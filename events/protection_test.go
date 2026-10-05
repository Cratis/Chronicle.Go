// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/conceptfixtures"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type classifiedName = conceptfixtures.Name
type classifiedAddress struct {
	Street string `json:"street"`
	Zip    *int32 `json:"zip"`
}
type classifiedEvent struct {
	Owner    string            `json:"owner" chronicle:"subject"`
	Email    string            `json:"email" chronicle:"pii;compliance-details(value=\"billing; notifications\")"`
	Address  classifiedAddress `json:"address" chronicle:"pii"`
	Names    []classifiedName  `json:"names"`
	Optional *classifiedName   `json:"optional"`
	Numbers  []int32           `json:"numbers" chronicle:"pii"`
	Secret   string            `json:"secret" chronicle:"encrypted(details=\"operational\")"`
	Shared   string            `json:"shared" chronicle:"encrypted(scope=namespace)"`
	Global   string            `json:"global" chronicle:"encrypted(scope=global)"`
}

func TestClassificationSchemaMatchesCSharpCategoriesAndPropagation(t *testing.T) {
	declaration := compliance.For[classifiedName](compliance.Classification{PII: true, Details: "contact"})
	event, err := events.Define[classifiedEvent](events.WithProtection(declaration))
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[classifiedEvent](readmodels.WithProtection(declaration))
	if err != nil {
		t.Fatal(err)
	}
	if event.Descriptor().Schema() != model.Descriptor().Schema() {
		t.Fatal("events and read models diverged")
	}
	var schema map[string]any
	if err := json.Unmarshal([]byte(event.Descriptor().Schema()), &schema); err != nil {
		t.Fatal(err)
	}
	properties := schema["properties"].(map[string]any)
	check := func(node map[string]any, category, kind, details string) {
		t.Helper()
		want := []any{map[string]any{"metadataType": kind, "details": details}}
		if !reflect.DeepEqual(node[category], want) {
			t.Fatalf("metadata = %#v, want %#v", node[category], want)
		}
		other := "security"
		if category == "security" {
			other = "compliance"
		}
		if node[other] != nil {
			t.Fatal("categories mixed")
		}
	}
	property := func(name string) map[string]any { return properties[name].(map[string]any) }
	check(property("email"), "compliance", "PII", "billing; notifications")
	address := property("address")
	if address["compliance"] != nil {
		t.Fatal("composite encrypted as a blob")
	}
	for _, field := range address["properties"].(map[string]any) {
		check(field.(map[string]any), "compliance", "PII", "")
	}
	check(property("names")["items"].(map[string]any), "compliance", "PII", "contact")
	if property("names")["compliance"] != nil {
		t.Fatal("item classification moved to container")
	}
	check(property("optional"), "compliance", "PII", "contact")
	check(property("numbers"), "compliance", "PII", "")
	check(property("secret"), "security", "EncryptedSubject", "operational")
	check(property("shared"), "security", "EncryptedNamespace", "")
	check(property("global"), "security", "EncryptedGlobal", "")
	if property("owner")["compliance"] != nil {
		t.Fatal("subject was classified implicitly")
	}
}

func TestClassificationPrecedenceAndExplicitEmptyDetails(t *testing.T) {
	type Invoice struct {
		First  classifiedName `chronicle:"pii;compliance-details(value=\"field\")"`
		Second classifiedName
		Empty  classifiedName `chronicle:"compliance-details(value=\"\")"`
	}
	event, err := events.Define[Invoice](events.WithProtection(
		compliance.For[Invoice](compliance.Classification{PII: true, Details: "declaring"}),
		compliance.For[classifiedName](compliance.Classification{PII: true, Details: "value"}),
	))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties map[string]struct{ Compliance []struct{ Details string } }
	}
	if err := json.Unmarshal([]byte(event.Descriptor().Schema()), &schema); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"First": "field", "Second": "declaring", "Empty": ""} {
		if got := schema.Properties[name].Compliance; len(got) != 1 || got[0].Details != want {
			t.Fatalf("%s: %#v", name, got)
		}
	}
}

func TestProtectionAlwaysRejectsEventSourceIdentity(t *testing.T) {
	type directPII struct {
		ID events.SourceID `chronicle:"pii"`
	}
	type directEncrypted struct {
		ID events.SourceID `chronicle:"encrypted"`
	}
	type nested struct{ IDs []events.SourceID }
	type inherited struct {
		Value nested `chronicle:"pii"`
	}
	type root struct{ ID events.SourceID }
	cases := []func() error{
		func() error { _, err := events.Define[directPII](); return err },
		func() error { _, err := events.Define[directEncrypted](); return err },
		func() error { _, err := events.Define[inherited](); return err },
		func() error {
			_, err := events.Define[root](events.WithProtection(compliance.For[root](compliance.Classification{Encrypted: true})))
			return err
		},
		func() error {
			_, err := events.Define[root](events.WithProtection(compliance.For[events.SourceID](compliance.Classification{PII: true})))
			return err
		},
		func() error { _, err := readmodels.Define[directPII](); return err },
	}
	for i, define := range cases {
		var declaration *declarations.DeclarationError
		if err := define(); !errors.Is(err, faults.ErrInvalidConfiguration) || !errors.As(err, &declaration) {
			t.Fatalf("case %d: %v", i, err)
		}
	}
}

func TestProtectionConflictsFailBeforeRegistration(t *testing.T) {
	type mixed struct {
		Value string `chronicle:"pii;encrypted(details=\"PRIVATE\")"`
	}
	type nestedMixed struct{ Value mixed }
	type inherited struct {
		Name classifiedName `chronicle:"encrypted"`
	}
	type duplicate struct {
		Value string `chronicle:"pii;pii"`
	}
	for _, define := range []func() error{
		func() error { _, err := events.Define[mixed](); return err },
		func() error { _, err := events.Define[nestedMixed](); return err },
		func() error {
			_, err := events.Define[inherited](events.WithProtection(compliance.For[classifiedName](compliance.Classification{PII: true})))
			return err
		},
		func() error { _, err := events.Define[duplicate](); return err },
		func() error { _, err := readmodels.Define[mixed](); return err },
	} {
		if err := define(); !errors.Is(err, faults.ErrInvalidConfiguration) || strings.Contains(err.Error(), "PRIVATE") {
			t.Fatalf("unsafe conflict: %v", err)
		}
	}
}

func TestClassifiedCollectionsCannotHideCategoryConflicts(t *testing.T) {
	type Batch struct {
		Names []classifiedName `chronicle:"pii"`
	}
	_, err := events.Define[Batch](events.WithProtection(compliance.For[classifiedName](compliance.Classification{Encrypted: true})))
	if !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatal("coarse PII hid an encrypted item classification")
	}
}

func TestProtectionProviderErrorsAreInspectableAndRedacted(t *testing.T) {
	type Message struct{ Name string }
	cause := errors.New("PRIVATE")
	_, err := events.Define[Message](events.WithProtection(compliance.Using(func(compliance.Target) (compliance.Classification, error) { return compliance.Classification{}, cause })))
	if !errors.Is(err, cause) || !errors.Is(err, faults.ErrInvalidConfiguration) || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatalf("unsafe provider failure: %v", err)
	}
}

func TestClassificationCatalogIsolationProviderAndNaming(t *testing.T) {
	type value struct{ Name string }
	plain, err := events.Define[value]()
	if err != nil {
		t.Fatal(err)
	}
	protected, err := events.Define[value](events.WithProtection(compliance.Property("Name", compliance.Classification{PII: true})))
	if err != nil {
		t.Fatal(err)
	}
	renamed, err := protected.Descriptor().WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	roots, err := serialization.ProtectionRoots(renamed.Schema())
	if err != nil || !roots["name"] {
		t.Fatal("classification did not follow field naming")
	}
	if strings.Contains(plain.Descriptor().Schema(), "compliance") {
		t.Fatal("global classification leaked")
	}
	provided, err := events.Define[value](events.WithProtection(compliance.Using(func(target compliance.Target) (compliance.Classification, error) {
		return compliance.Classification{Encrypted: target.Field == "Name", Scope: ""}, nil
	})))
	if err != nil || !strings.Contains(provided.Descriptor().Schema(), "EncryptedSubject") {
		t.Fatalf("provider: %v", err)
	}
}

func TestEmbeddedAndRecursivePathClassificationsAreNotLost(t *testing.T) {
	type PersonalFields struct{ Name string }
	type Message struct{ PersonalFields }
	event, err := events.Define[Message](events.WithProtection(compliance.For[PersonalFields](compliance.Classification{PII: true})))
	if err != nil {
		t.Fatal(err)
	}
	roots, err := serialization.ProtectionRoots(event.Descriptor().Schema())
	if err != nil || !roots["Name"] {
		t.Fatal("embedded type classification was lost")
	}
	type Tree struct {
		Name string
		Next *Tree
	}
	tree, err := events.Define[Tree](events.WithProtection(compliance.Property("Next.Name", compliance.Classification{PII: true})))
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := json.Unmarshal([]byte(tree.Descriptor().Schema()), &schema); err != nil {
		t.Fatal(err)
	}
	properties := schema["properties"].(map[string]any)
	if properties["Name"].(map[string]any)["compliance"] != nil {
		t.Fatal("recursive member classification leaked to root")
	}
	next := properties["Next"].(map[string]any)["properties"].(map[string]any)
	if next["Name"].(map[string]any)["compliance"] == nil {
		t.Fatal("recursive member classification was lost")
	}
}

func TestSerializationPlanCannotSilentlyDropProtectionTags(t *testing.T) {
	type Message struct {
		Name string `chronicle:"pii"`
	}
	plan, err := serialization.Compile(reflect.TypeFor[Message]())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan.Schema(), `"metadataType":"PII"`) {
		t.Fatal("plan silently omitted security metadata")
	}
}

func TestBooleanEventSubjectMatchesCSharpToString(t *testing.T) {
	type Flag struct {
		Owner bool `chronicle:"subject"`
	}
	event, err := events.Define[Flag]()
	if err != nil {
		t.Fatal(err)
	}
	d, err := event.Descriptor().WithNamingPolicy(serialization.PreservePropertyNames)
	if err != nil {
		t.Fatal(err)
	}
	for _, owner := range []bool{false, true} {
		want := events.Subject("False")
		if owner {
			want = "True"
		}
		if got, ok := d.ResolveSubject(Flag{Owner: owner}); !ok || got != want {
			t.Fatal("boolean subject spelling differs")
		}
	}
}

func TestEncryptedMemberOverridesDeclaringAndValueType(t *testing.T) {
	type Message struct {
		Default  classifiedName
		Override classifiedName `chronicle:"encrypted(scope=global,details=\"field\")"`
	}
	event, err := events.Define[Message](events.WithProtection(
		compliance.For[Message](compliance.Classification{Encrypted: true, Scope: compliance.Namespace, Details: "declaring"}),
		compliance.For[classifiedName](compliance.Classification{Encrypted: true, Scope: compliance.Subject, Details: "value"}),
	))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties map[string]struct {
			Security []struct {
				MetadataType string
				Details      string
			}
		}
	}
	if err := json.Unmarshal([]byte(event.Descriptor().Schema()), &schema); err != nil {
		t.Fatal(err)
	}
	for field, want := range map[string]string{"Default": "EncryptedNamespace", "Override": "EncryptedGlobal"} {
		if values := schema.Properties[field].Security; len(values) != 1 || values[0].MetadataType != want {
			t.Fatalf("wrong encryption precedence for %s", field)
		}
	}
}

func TestRecursiveClassificationsPreserveShapeAndGuardCycles(t *testing.T) {
	type tree struct {
		Name     string `chronicle:"pii"`
		Children []*tree
	}
	event, err := events.Define[tree]()
	if err != nil {
		t.Fatal(err)
	}
	roots, err := serialization.ProtectionRoots(event.Descriptor().Schema())
	if err != nil || !roots["Name"] || !roots["Children"] {
		t.Fatalf("recursive protection: %v %v", roots, err)
	}
	if !strings.Contains(event.Descriptor().Schema(), `"$ref"`) {
		t.Fatal("recursive schema was flattened")
	}
}
