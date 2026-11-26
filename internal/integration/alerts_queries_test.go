//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/alerts"
	"github.com/google/uuid"
)

func readyHostedAlertPage(t *testing.T, ctx context.Context, service *alerts.Service) alerts.Page {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		page, err := service.GetOpenIncidents(ctx, alerts.Query{Limit: 1})
		if err != nil {
			t.Fatal(err)
		}
		if page.Readiness == alerts.Ready {
			return page
		}
		select {
		case <-ctx.Done():
			t.Fatalf("alert queries never became authoritative: readiness=%v error=%v", page.Readiness, ctx.Err())
		case <-ticker.C:
		}
	}
}

func assertHostedEmptyAlerts(t *testing.T, f *kernelFixture, service *alerts.Service) {
	t.Helper()
	page := readyHostedAlertPage(t, f.ctx, service)
	if len(page.Incidents) != 0 || page.Next != nil {
		t.Fatalf("empty store has open incidents: %+v", page)
	}
	lookup, err := service.GetOpenIncident(f.ctx, uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	if lookup.Readiness != alerts.Ready || lookup.Incident != nil {
		t.Fatalf("missing incident lookup = %+v", lookup)
	}
	counts, err := service.GetOpenIncidentCounts(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Readiness != alerts.Ready || len(counts.Buckets) != 0 {
		t.Fatalf("empty incident counts = %+v", counts)
	}
}

func TestKernelAlertQueriesReportAuthoritativeEmptyAndMissingResults(t *testing.T) {
	f, store := newHostedSourceStore(t)
	assertHostedEmptyAlerts(t, f, store.Alerts())
	other, err := f.client(integrationRegistry[HostedSourceEvent](t)).EventStore(f.ctx, f.storeName, chronicle.WithNamespace("other"))
	if err != nil {
		t.Fatal(err)
	}
	assertHostedEmptyAlerts(t, f, other.Alerts())
	// Nonempty continuation/count/isolation witnesses need a deterministic alert
	// fixture. Ordinary failed partitions have a five-minute default raise grace,
	// beyond this fixture's one-minute lifetime; do not treat emptiness as that proof.
}
