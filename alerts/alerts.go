// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package alerts reads open observer alert incidents through the kernel's unary
// Alerts queries. Readiness is reported separately from RPC success: a
// CatchingUp or Degraded result is data, never proof that no incident exists.
// The pinned kernel exposes no alert subscription, so Subscribe is unsupported.
package alerts

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/alerts"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

// IncidentID identifies an alert incident using an RFC UUID with .NET Guid wire conversion.
type IncidentID = uuid.UUID

// Readiness is the alert read-model state. Unknown numeric values are preserved.
type Readiness int32

const (
	// CatchingUp means the incident read model is still catching up; results may be incomplete.
	CatchingUp Readiness = 0
	// Ready means the incident read model is current.
	Ready Readiness = 1
	// Degraded means the incident read model is impaired; results may be incomplete.
	Degraded Readiness = 2
)

// Severity is an alert severity. Unknown numeric values are preserved on output.
type Severity int32

const (
	// Warning is the lowest severity.
	Warning Severity = 0
	// Critical is the highest severity.
	Critical Severity = 1
)

// FailureKind classifies the evidence behind an incident. Unknown numeric values are preserved.
type FailureKind int32

const (
	// UnknownFailure means the kind is not known.
	UnknownFailure FailureKind = 0
	// Handling means the handler failed.
	Handling FailureKind = 1
	// Timeout means handling timed out.
	Timeout FailureKind = 2
	// Disconnected means the handler was disconnected.
	Disconnected FailureKind = 3
	// WaitingForHandler means no handler was available.
	WaitingForHandler FailureKind = 4
)

// Target identifies what an incident is about.
type Target struct {
	Store         metadata.StoreName
	Namespace     metadata.Namespace
	Observer      string
	EventSequence string
	Partition     string
}

// Evidence describes the failures behind an incident.
type Evidence struct {
	AttemptCount int32
	FirstFailure time.Time
	LastFailure  time.Time
	Kind         FailureKind
	Message      string
}

// Incident is an immutable snapshot of an open alert incident.
type Incident struct {
	ID                           IncidentID
	Target                       Target
	Condition                    string
	Severity                     Severity
	Evidence                     Evidence
	RaisedAt                     time.Time
	LastChangedAt                time.Time
	RaisedSequenceNumber         uint64
	LastTransitionSequenceNumber uint64
}

// Count is the number of open incidents for a namespace, condition and severity.
type Count struct {
	Namespace metadata.Namespace
	Condition string
	Severity  Severity
	Count     int64
}

// Continuation is the paired position after which the next page starts.
type Continuation struct {
	RaisedSequenceNumber uint64
	IncidentID           IncidentID
}

// Query filters open incidents. Zero fields apply the kernel defaults.
type Query struct {
	// Observer filters by observer identifier when not empty.
	Observer string
	// Condition filters by condition when not empty.
	Condition string
	// MinimumSeverity excludes lower severities; Warning includes everything.
	MinimumSeverity Severity
	// Limit is the page size; zero lets the kernel choose its default and the
	// kernel clamps it to its own maximum. Negative values are invalid.
	Limit int
	// After continues after a previous page's Next. A nil value or a zero
	// IncidentID starts at the beginning; a nonzero IncidentID requires a
	// RaisedSequenceNumber that fits a signed 64-bit position.
	After *Continuation
}

// Lookup is the result of a single incident lookup.
type Lookup struct {
	Readiness Readiness
	// Incident is nil when no open incident has the requested identifier. With a
	// Readiness other than Ready, nil does not prove the incident does not exist.
	Incident *Incident
}

// Page is one page of open incidents.
type Page struct {
	Readiness Readiness
	Incidents []Incident
	// Next is nil on the last page; it is not sequence number zero.
	Next *Continuation
}

// Counts is the open incident count buckets.
type Counts struct {
	Readiness Readiness
	Buckets   []Count
}

// Service is an immutable, concurrent-safe store/namespace handle. It borrows
// its connection, starts no goroutines and never retries.
type Service struct {
	store     metadata.StoreName
	namespace metadata.Namespace
	client    contracts.AlertsClient
}

// New constructs a low-level service without I/O. Prefer EventStore.Alerts for
// registration/lifecycle barriers. The caller owns the connection.
func New(store metadata.StoreName, namespace metadata.Namespace, conn grpc.ClientConnInterface) (*Service, error) {
	if strings.TrimSpace(string(store)) == "" || strings.TrimSpace(string(namespace)) == "" || conn == nil {
		return nil, invalid("store, namespace and connection required")
	}
	return &Service{store, namespace, contracts.NewAlertsClient(conn)}, nil
}

// Store returns this service's immutable store coordinate.
func (s *Service) Store() metadata.StoreName { return s.store }

// Namespace returns this service's immutable namespace coordinate.
func (s *Service) Namespace() metadata.Namespace { return s.namespace }

// GetOpenIncident reads one open incident. A zero identifier is invalid.
func (s *Service) GetOpenIncident(ctx context.Context, id IncidentID) (Lookup, error) {
	if err := ctx.Err(); err != nil {
		return Lookup{}, err
	}
	if id == uuid.Nil {
		return Lookup{}, invalid("incident ID required")
	}
	response, err := s.client.GetOpenIncident(ctx, &contracts.GetOpenIncidentRequest{EventStore: string(s.store), Namespace: string(s.namespace), IncidentId: wire.Guid(metadata.CorrelationID(id))})
	if err != nil {
		return Lookup{}, wire.RPCError(err)
	}
	if err = wire.CheckEnvelope(response); err != nil {
		return Lookup{}, err
	}
	if response.Data == nil {
		return Lookup{}, faults.ErrProtocol
	}
	lookup := Lookup{Readiness: Readiness(response.Data.Status)}
	if response.Data.Incident != nil {
		incident, err := decodeIncident(response.Data.Incident)
		if err != nil {
			return Lookup{}, err
		}
		lookup.Incident = &incident
	}
	return lookup, nil
}

// GetOpenIncidents reads one page of open incidents in this namespace.
func (s *Service) GetOpenIncidents(ctx context.Context, query Query) (Page, error) {
	if err := ctx.Err(); err != nil {
		return Page{}, err
	}
	request, err := s.pageRequest(query)
	if err != nil {
		return Page{}, err
	}
	response, err := s.client.GetOpenIncidents(ctx, request)
	if err != nil {
		return Page{}, wire.RPCError(err)
	}
	if err = wire.CheckEnvelope(response); err != nil {
		return Page{}, err
	}
	if response.Data == nil {
		return Page{}, faults.ErrProtocol
	}
	page := Page{Readiness: Readiness(response.Data.Status), Incidents: make([]Incident, 0, len(response.Data.Items))}
	seen := map[IncidentID]bool{}
	for _, value := range response.Data.Items {
		incident, err := decodeIncident(value)
		if err != nil {
			return Page{}, err
		}
		if seen[incident.ID] {
			return Page{}, faults.ErrProtocol
		}
		seen[incident.ID] = true
		page.Incidents = append(page.Incidents, incident)
	}
	if next := response.Data.Next; next != nil {
		id := IncidentID(wire.Correlation(next.IncidentId))
		if id == uuid.Nil || next.RaisedSequenceNumber > math.MaxInt64 {
			return Page{}, faults.ErrProtocol
		}
		page.Next = &Continuation{RaisedSequenceNumber: next.RaisedSequenceNumber, IncidentID: id}
	}
	return page, nil
}

// GetOpenIncidentCounts reads open incident counts for this namespace.
func (s *Service) GetOpenIncidentCounts(ctx context.Context) (Counts, error) {
	if err := ctx.Err(); err != nil {
		return Counts{}, err
	}
	response, err := s.client.GetOpenIncidentCounts(ctx, &contracts.GetOpenIncidentCountsRequest{EventStore: string(s.store), Namespace: string(s.namespace)})
	if err != nil {
		return Counts{}, wire.RPCError(err)
	}
	if err = wire.CheckEnvelope(response); err != nil {
		return Counts{}, err
	}
	if response.Data == nil {
		return Counts{}, faults.ErrProtocol
	}
	counts := Counts{Readiness: Readiness(response.Data.Status), Buckets: make([]Count, 0, len(response.Data.Counts))}
	for _, value := range response.Data.Counts {
		if value == nil || value.Count < 0 {
			return Counts{}, faults.ErrProtocol
		}
		counts.Buckets = append(counts.Buckets, Count{Namespace: metadata.Namespace(value.Namespace), Condition: value.Condition, Severity: Severity(value.Severity), Count: value.Count})
	}
	return counts, nil
}

// Subscribe is unsupported: the pinned kernel has no alert subscription and
// the SDK does not fake one by polling. No connection or goroutine is used.
func (s *Service) Subscribe(context.Context, func(Incident)) error {
	return fmt.Errorf("%w: alert subscriptions", faults.ErrUnsupported)
}

func (s *Service) pageRequest(query Query) (*contracts.GetOpenIncidentsRequest, error) {
	if query.Limit < 0 || query.Limit > math.MaxInt32 {
		return nil, invalid("limit out of range")
	}
	if query.MinimumSeverity != Warning && query.MinimumSeverity != Critical {
		return nil, invalid("unknown minimum severity")
	}
	request := &contracts.GetOpenIncidentsRequest{EventStore: string(s.store), Namespace: string(s.namespace), ObserverId: query.Observer, Condition: query.Condition, MinimumSeverity: contracts.AlertSeverity(query.MinimumSeverity), Limit: int32(query.Limit)}
	if after := query.After; after != nil && after.IncidentID != uuid.Nil {
		if after.RaisedSequenceNumber > math.MaxInt64 {
			return nil, invalid("continuation position exceeds the supported range")
		}
		request.AfterRaisedSequenceNumber = after.RaisedSequenceNumber
		request.AfterIncidentId = wire.Guid(metadata.CorrelationID(after.IncidentID))
	}
	return request, nil
}

func decodeIncident(value *contracts.AlertIncidentDetails) (Incident, error) {
	if value == nil || value.Target == nil || value.Evidence == nil || value.Id == nil {
		return Incident{}, faults.ErrProtocol
	}
	id := IncidentID(wire.Correlation(value.Id))
	if id == uuid.Nil {
		return Incident{}, faults.ErrProtocol
	}
	times, err := parseTimes(value.Evidence.FirstFailure, value.Evidence.LastFailure, value.RaisedAt, value.LastChangedAt)
	if err != nil {
		return Incident{}, err
	}
	return Incident{
		ID:                           id,
		Target:                       decodeTarget(value.Target),
		Condition:                    value.Condition,
		Severity:                     Severity(value.Severity),
		Evidence:                     decodeEvidence(value.Evidence, times[0], times[1]),
		RaisedAt:                     times[2],
		LastChangedAt:                times[3],
		RaisedSequenceNumber:         value.RaisedSequenceNumber,
		LastTransitionSequenceNumber: value.LastTransitionSequenceNumber,
	}, nil
}

func decodeTarget(target *contracts.AlertTarget) Target {
	return Target{Store: metadata.StoreName(target.EventStore), Namespace: metadata.Namespace(target.Namespace), Observer: target.ObserverId, EventSequence: target.EventSequenceId, Partition: target.Partition}
}

func decodeEvidence(evidence *contracts.AlertEvidence, first, last time.Time) Evidence {
	return Evidence{AttemptCount: evidence.AttemptCount, FirstFailure: first, LastFailure: last, Kind: FailureKind(evidence.FailureKind), Message: evidence.Message}
}

func parseTimes(values ...*contracts.SerializableDateTimeOffset) ([]time.Time, error) {
	result := make([]time.Time, len(values))
	for i, value := range values {
		parsed, err := parseTime(value)
		if err != nil {
			return nil, err
		}
		result[i] = parsed
	}
	return result, nil
}

func parseTime(value *contracts.SerializableDateTimeOffset) (time.Time, error) {
	if value == nil {
		return time.Time{}, faults.ErrProtocol
	}
	parsed, err := time.Parse(time.RFC3339Nano, value.Value)
	if err != nil {
		return time.Time{}, faults.ErrProtocol
	}
	return parsed, nil
}

func invalid(message string) error {
	return fmt.Errorf("%w: %s", faults.ErrInvalidConfiguration, message)
}
