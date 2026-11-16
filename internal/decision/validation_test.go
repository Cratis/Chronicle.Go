// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package decision

import (
	"testing"

	"github.com/cratis/chronicle.go/events"
)

func validationEvidence(t *testing.T) Evidence {
	t.Helper()
	catalog := NewCatalog(nil, nil)
	return Evidence{Target: Target{Client: t, Sequence: string(events.EventLog)}, Model: "model", Key: "key",
		Types: []events.TypeRef{{ID: "changed", Generation: 1}}, Catalog: catalog, Epoch: catalog.ExpectedEpoch,
		Check: func() error { return nil }}
}

func TestValidateRejectsMissingEvidenceBeforeCheckingFreshness(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Evidence)
	}{
		{"client", func(e *Evidence) { e.Target.Client = nil }},
		{"sequence", func(e *Evidence) { e.Target.Sequence = "other" }},
		{"model", func(e *Evidence) { e.Model = "" }},
		{"key", func(e *Evidence) { e.Key = "" }},
		{"types", func(e *Evidence) { e.Types = nil }},
		{"catalog", func(e *Evidence) { e.Catalog = nil }},
		{"check", func(e *Evidence) { e.Check = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := validationEvidence(t)
			e.Epoch++
			e.Check = func() error { t.Fatal("checked freshness of invalid evidence"); return nil }
			tc.change(&e)
			if err := validate(e); err != ErrInvalid {
				t.Fatalf("validate = %v, want %v", err, ErrInvalid)
			}
		})
	}
}

func TestValidateChecksEpochsBeforeConnection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Evidence)
	}{
		{"expected", func(e *Evidence) { e.Epoch++ }},
		{"current", func(e *Evidence) { e.Catalog.Epoch.Add(1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := validationEvidence(t)
			e.Check = func() error { t.Fatal("checked connection after epoch mismatch"); return nil }
			tc.change(&e)
			if err := validate(e); err != ErrStale {
				t.Fatalf("validate = %v, want %v", err, ErrStale)
			}
		})
	}
}

func TestValidateChecksConnectionAfterMatchingEpochs(t *testing.T) {
	e := validationEvidence(t)
	calls := 0
	e.Check = func() error { calls++; return ErrTarget }
	if err := validate(e); err != ErrStale {
		t.Fatalf("validate = %v, want %v", err, ErrStale)
	}
	if calls != 1 {
		t.Fatalf("connection checks = %d, want 1", calls)
	}
}
