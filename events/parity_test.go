// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events_test

import (
	"testing"

	"github.com/cratis/chronicle.go/events"
)

type declared struct{ Subject string }
type other struct{ Value string }

func TestSubjectResolverDeclarationAndLookup(t *testing.T) {
	definition, err := events.Define[declared](events.WithID("stable-id"), events.WithGeneration(3), events.WithSubjectResolver(func(value declared) (events.Subject, bool) { return events.Subject(value.Subject), value.Subject != "" }))
	if err != nil {
		t.Fatal(err)
	}
	d := definition.Descriptor()
	for _, value := range []any{declared{Subject: "person"}, &declared{Subject: "person"}} {
		if subject, ok := d.ResolveSubject(value); !ok || subject != "person" {
			t.Fatalf("subject = %q, %v", subject, ok)
		}
	}
	for _, value := range []any{nil, (*declared)(nil), other{}, declared{}} {
		if _, ok := d.ResolveSubject(value); ok {
			t.Fatalf("unexpected subject for %T", value)
		}
	}
	catalog, err := events.NewCatalog(d)
	if err != nil {
		t.Fatal(err)
	}
	byID, ok := catalog.LookupID("stable-id")
	if !ok || byID.Ref() != definition.Ref() || byID.Schema() != d.Schema() || byID.GoType() != d.GoType() {
		t.Fatalf("ID lookup = %+v, %v", byID, ok)
	}
	if _, ok := catalog.LookupRef(definition.Ref()); !ok {
		t.Fatal("exact reference missing")
	}
	for _, ref := range []events.TypeRef{{ID: "missing", Generation: 3}, {ID: "stable-id", Generation: 2}, {ID: "stable-id"}} {
		if _, ok := catalog.LookupRef(ref); ok {
			t.Fatalf("unexpected lookup for %+v", ref)
		}
	}
	if _, ok := catalog.LookupID("missing"); ok {
		t.Fatal("unexpected missing ID")
	}
	if _, err := events.Define[declared](events.WithSubjectResolver[declared](nil)); err == nil {
		t.Fatal("nil resolver accepted")
	}
	if _, err := events.Define[declared](events.WithSubjectResolver(func(other) (events.Subject, bool) { return "", false })); err == nil {
		t.Fatal("mismatched resolver accepted")
	}
}

func TestCanonicalSequences(t *testing.T) {
	if events.UnspecifiedSequence != "[unspecified]" || events.SystemSequence != "system" || events.Outbox != "outbox" || events.Inbox != "inbox" || events.InboxPrefix != "inbox-" || events.UnspecifiedSourceType != "" {
		t.Fatal("canonical sequence values differ from C#")
	}
	if !events.EventLog.IsEventLog() || events.Outbox.IsEventLog() {
		t.Fatal("IsEventLog")
	}
}
