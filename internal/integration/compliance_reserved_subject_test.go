//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/google/uuid"
)

type ReservedSubjectNoted struct {
	Note string `json:"note"`
}

// Chronicle#4553 (fixed in 19.32.2): the kernel refuses PII for a reserved
// confidentiality identifier. It still accepts that identifier as the subject
// of an unclassified event, which can later reach a protected read model, so
// the SDK keeps rejecting reserved subjects at admission for every event.
func TestKernelReservedPIISubjectsAndSDKAdmission(t *testing.T) {
	f := newKernelFixture(t)
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[CompliancePersonRegistered](registry); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[ReservedSubjectNoted](registry); err != nil {
		t.Fatal(err)
	}
	store, err := f.client(registry).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	const reserved = "$chronicle-encrypted-value$namespace$"
	rawAppend := func(eventType events.TypeID, subject string, content any) (bool, error) {
		t.Helper()
		payload, err := json.Marshal(content)
		if err != nil {
			t.Fatal(err)
		}
		response, err := sequences.NewEventSequencesClient(f.conn).Append(f.ctx, &sequences.AppendRequest{
			EventStore: string(f.storeName), Namespace: string(chronicle.DefaultNamespace), EventSequenceId: "event-log", EventSourceId: uuid.NewString(),
			EventType: &sequences.EventType{Id: string(eventType), Generation: 1}, Content: string(payload), Subject: subject, CorrelationId: wire.Guid(metadata.CorrelationID(uuid.New())),
			Occurred: &sequences.SerializableDateTimeOffset{Value: wire.DateTimeOffset(time.Now().UTC())}, CausedBy: &sequences.Identity{}, ConcurrencyScope: &sequences.ConcurrencyScope{SequenceNumber: uint64(events.Unavailable)},
		})
		if err != nil {
			return false, err
		}
		if err := wire.CheckEnvelope(response); err != nil {
			return false, err
		}
		return response.Response != nil && response.Response.IsSuccess, nil
	}
	// Control: the raw path appends PII for an ordinary subject.
	if ok, err := rawAppend("CompliancePersonRegistered", uuid.NewString(), CompliancePersonRegistered{Name: "fixture"}); err != nil || !ok {
		t.Fatalf("ordinary raw PII append: %v %v", ok, err)
	}
	// Kernel: PII for a reserved identifier is refused before any key exists.
	if ok, _ := rawAppend("CompliancePersonRegistered", reserved, CompliancePersonRegistered{Name: "fixture"}); ok {
		t.Fatal("kernel accepted PII for a reserved confidentiality identifier")
	}
	// Kernel: an unclassified event keeps the reserved subject.
	if ok, err := rawAppend("ReservedSubjectNoted", reserved, ReservedSubjectNoted{Note: "plain"}); err != nil || !ok {
		t.Fatalf("kernel now refuses reserved subjects without PII (%v %v); reconsider the SDK check", ok, err)
	}
	// SDK: both shapes are rejected before dispatch.
	for _, value := range []any{CompliancePersonRegistered{Name: "fixture"}, ReservedSubjectNoted{Note: "plain"}} {
		_, err := store.EventLog().Append(f.ctx, events.SourceID(uuid.NewString()), value, eventsequences.WithSubject(reserved))
		var invalid *compliance.InvalidSubjectError
		if !errors.As(err, &invalid) || !invalid.Reserved {
			t.Fatalf("SDK admitted reserved subject for %T: %v", value, err)
		}
	}
}
