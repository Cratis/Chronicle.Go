// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package captures_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/captures"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
)

func captureBuilder() *captures.Builder {
	return new(captures.Builder).From(captures.API("Service", "/items", "1m")).Key("id")
}

func TestCaptureTranslationTargetsUseCDLWordGrammar(t *testing.T) {
	for _, target := range []string{"open", "Open_2", "123", "é_1"} {
		d, err := captureBuilder().Map(captures.Translate("status", "status", captures.Translation{From: "draft", To: target})).Build("Capture")
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(d.Declaration(), "\n      \"draft\" => "+target+"\n") {
			t.Fatal("translation target was quoted or changed")
		}
	}
	for _, target := range []string{"", "in progress", "open.closed", "open-closed", `"open"`, "open\nclosed", `C:\new`, "𐐀"} {
		if _, err := captureBuilder().Map(captures.Translate("status", "status", captures.Translation{From: "draft", To: target})).Build("Capture"); !errors.Is(err, faults.ErrUnsupported) {
			t.Fatalf("unrepresentable translation target accepted: %v", err)
		}
	}
}

func TestCaptureIdentifiersMatchConstructGrammar(t *testing.T) {
	for _, id := range []string{"Changed", "Order_2", "Aé"} {
		event, err := events.Define[StatusChanged](events.WithID(events.TypeID(id)))
		if err != nil {
			t.Fatal(err)
		}
		d, err := captureBuilder().Append(captures.Append(event, captures.Added(), nil)).Build("_Capture2")
		if err != nil || !strings.Contains(d.Declaration(), "append "+id+"\n") {
			t.Fatalf("valid persisted ID not preserved: %v", err)
		}
	}
	for _, id := range []string{"Orders.Changed", "changed", "_Changed", "persisted-id", "1Changed", "Changed𐐀"} {
		event, err := events.Define[StatusChanged](events.WithID(events.TypeID(id)))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := captureBuilder().Append(captures.Append(event, captures.Added(), nil)).Build("Capture"); !errors.Is(err, faults.ErrUnsupported) {
			t.Fatalf("unrepresentable persisted ID accepted: %v", err)
		}
	}
	for _, name := range []string{"Orders.Capture", "1Capture", "Capture-Name"} {
		if _, err := captureBuilder().Build(name); !errors.Is(err, faults.ErrInvalidConfiguration) {
			t.Fatalf("invalid capture name accepted: %v", err)
		}
	}
	for _, mapping := range []captures.Mapping{captures.Rename("source", "nested.target"), captures.Template("Target", "value"), captures.Translate("nested.target", "source", captures.Translation{From: "a", To: "b"})} {
		if _, err := captureBuilder().Map(mapping).Build("Capture"); !errors.Is(err, faults.ErrInvalidConfiguration) {
			t.Fatalf("invalid map target accepted: %v", err)
		}
	}
	if _, err := captureBuilder().Children("nested.children", "id", captures.NewScope(nil)).Build("Capture"); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatalf("invalid children collection accepted: %v", err)
	}
}

func TestCaptureStringLiteralsEscapeOnlyScreenplaySequences(t *testing.T) {
	event, err := events.Define[StatusChanged](events.WithID("Changed"))
	if err != nil {
		t.Fatal(err)
	}
	// Literal backslash-n is not a newline; all five supported escapes are
	// exercised alongside an unknown escape and non-ASCII text.
	value := "C:\\new\\d\"\n\r\t雪"
	d, err := captureBuilder().Map(captures.Split("path", value, "first"), captures.Translate("status", "path", captures.Translation{From: value, To: "open"})).
		Append(captures.Append(event, captures.Transition("path", value, value), nil)).Build("Capture")
	if err != nil {
		t.Fatal(err)
	}
	quoted := `"C:\\new\\d\"\n\r\t雪"`
	for _, clause := range []string{"split path by " + quoted, quoted + " => open", "when path from " + quoted + " to " + quoted} {
		if !strings.Contains(d.Declaration(), clause) {
			t.Fatal("literal escaping differs from Screenplay's escape set")
		}
	}
	if _, err := captureBuilder().Map(captures.Split("path", "\x00", "first")).Build("Capture"); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatal("NUL literal accepted")
	}
}

func TestCapturePropertyNamesCannotBecomeLifecycleConditions(t *testing.T) {
	event, err := events.Define[StatusChanged](events.WithID("Changed"))
	if err != nil {
		t.Fatal(err)
	}
	for _, property := range []string{"added", "removed"} {
		for _, condition := range []captures.Condition{captures.PropertyChanges(property), captures.AnyOf(property), captures.AllOf(property)} {
			d, err := captureBuilder().Append(captures.Append(event, condition, nil)).Build("Capture")
			if err != nil || !strings.Contains(d.Declaration(), "when \""+property+"\"\n") {
				t.Fatalf("property operand not quoted: %v", err)
			}
		}
	}
}
