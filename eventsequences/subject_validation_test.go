// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
	"github.com/cratis/chronicle.go/transactions"
)

type protectedPerson struct {
	Owner string `chronicle:"subject"`
	Name  string `chronicle:"pii"`
}

func TestReservedSubjectsFailBeforeAdmissionOnEveryAppendPath(t *testing.T) {
	for _, reserved := range []string{"$chronicle-encrypted-value$namespace$", "$chronicle-encrypted-value$subject$PRIVATE", "$chronicle-encrypted-value$global$"} {
		for _, subjectFrom := range []string{"explicit", "tag", "resolver", "source"} {
			for _, path := range []string{"single", "many", "batch", "prepared", "unit"} {
				t.Run(subjectFrom+"/"+path, func(t *testing.T) {
					value := protectedPerson{Owner: "ordinary", Name: "PRIVATE"}
					source := events.SourceID("ordinary-source")
					var subject *events.Subject
					var options []events.TypeOption
					switch subjectFrom {
					case "explicit":
						s := events.Subject(reserved)
						subject = &s
					case "tag":
						value.Owner = reserved
					case "resolver":
						options = append(options, events.WithSubjectResolver(func(protectedPerson) (events.Subject, bool) { return events.Subject(reserved), true }))
					case "source":
						source, value.Owner = events.SourceID(reserved), ""
					}
					definition, err := events.Define[protectedPerson](options...)
					if err != nil {
						t.Fatal(err)
					}
					descriptor, err := definition.Descriptor().WithNamingPolicy(serialization.PreservePropertyNames)
					if err != nil {
						t.Fatal(err)
					}
					catalog, err := events.NewCatalog(descriptor)
					if err != nil {
						t.Fatal(err)
					}
					sequence, calls := parityFixture(t, nil, catalog, eventsequences.ConcurrencyPolicy{})
					ctx := testContext(t)
					var appendOptions []eventsequences.AppendOption
					if subject != nil {
						appendOptions = append(appendOptions, eventsequences.WithSubject(*subject))
					}
					entries := []eventsequences.Entry{{Source: "valid", Event: protectedPerson{Owner: "valid"}}, {Source: source, Event: value, Subject: subject}}
					switch path {
					case "single":
						_, err = sequence.Append(ctx, source, value, appendOptions...)
					case "many":
						_, err = sequence.AppendMany(ctx, source, []any{protectedPerson{Owner: "valid"}, value}, appendOptions...)
					case "batch":
						_, err = sequence.AppendBatch(ctx, entries)
					case "prepared":
						_, err = sequence.PrepareBatch(ctx, entries)
					case "unit":
						unit, _, beginErr := transactions.Begin(ctx, sequence)
						if beginErr != nil {
							t.Fatal(beginErr)
						}
						err = unit.Stage(ctx, entries)
					}
					var typed *compliance.InvalidSubjectError
					if !errors.As(err, &typed) || !typed.Reserved || !errors.Is(err, faults.ErrInvalidConfiguration) {
						t.Fatalf("expected reserved subject error: %v", err)
					}
					if strings.Contains(err.Error(), "PRIVATE") || calls.Load() != 0 {
						t.Fatal("invalid subject leaked or reached I/O")
					}
				})
			}
		}
	}
}
