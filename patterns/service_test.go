// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package patterns_test

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/contracts/bcl"
	contracts "github.com/cratis/chronicle.go/contracts/patterns"
	"github.com/cratis/chronicle.go/patterns"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type transport struct {
	invoke func(context.Context, string, any, any) error
}

func (c *transport) Invoke(ctx context.Context, method string, request, reply any, _ ...grpc.CallOption) error {
	return c.invoke(ctx, method, request, reply)
}
func (*transport) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("unexpected stream")
}

func service(t *testing.T, conn *transport) *patterns.Service {
	t.Helper()
	result, err := patterns.New("store", "namespace", conn)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func success(reply any) {
	switch result := reply.(type) {
	case *contracts.QueryResult_IEnumerable_BehaviorPatternDetailsResponse:
		result.IsAuthorized = true
	case *contracts.QueryResult_IEnumerable_PatternScopeResponse:
		result.IsAuthorized = true
	}
}

// Hand-derived golden requests, not .NET-captured bytes. Authority:
// Chronicle 2e31b0dfb DotNET/Patterns/Patterns.cs and its GetPatternsAt specs.
func TestCSharpQueryRequestGoldens(t *testing.T) {
	confidence, limit := patterns.Confidence(0.75), int32(23)
	facets := patterns.NewFacetSet(map[patterns.FacetName]patterns.FacetValue{patterns.AggregateType: "Invoice", "FutureFact": "0", "Unspecified": ""})
	options := patterns.QueryOptions{MinimumConfidence: &confidence, MaximumResults: &limit}
	moment := time.Date(2024, 1, 15, 9, 30, 0, 123456789, time.FixedZone("", 14*3600))
	cases := []struct {
		name   string
		method string
		want   proto.Message
		call   func(*patterns.Service) error
	}{
		{"contextual", contracts.Patterns_MatchingPatterns_FullMethodName, &contracts.MatchingPatternsRequest{EventStore: "store", Namespace: "namespace", GroupingKey: " user ", Context: map[string]string{"AggregateType": "Invoice", "FutureFact": "0"}, MinimumConfidence: 0.75, MaximumResults: 23}, func(s *patterns.Service) error {
			_, err := s.GetPatterns(t.Context(), " user ", facets, options)
			return err
		}},
		{"usual-actions", contracts.Patterns_UsualActions_FullMethodName, &contracts.UsualActionsRequest{EventStore: "store", Namespace: "namespace", GroupingKey: "user", Context: map[string]string{"AggregateType": "Invoice", "FutureFact": "0"}, MinimumConfidence: 0.75, MaximumResults: 23}, func(s *patterns.Service) error {
			_, err := s.GetUsualActions(t.Context(), "user", facets, options)
			return err
		}},
		{"at-offset-not-UTC", contracts.Patterns_UsualActions_FullMethodName, &contracts.UsualActionsRequest{EventStore: "store", Namespace: "namespace", GroupingKey: "user", Context: map[string]string{"AggregateType": "Invoice", "FutureFact": "0", "Day": "Monday", "TimeBucket": "Morning"}, MinimumConfidence: 0.75, MaximumResults: 23}, func(s *patterns.Service) error {
			_, err := s.GetPatternsAt(t.Context(), "user", &moment, facets.With(patterns.Day, "Tuesday").With(patterns.TimeBucket, "Night"), options)
			return err
		}},
		{"scope", contracts.Patterns_PatternsForScope_FullMethodName, &contracts.PatternsForScopeRequest{EventStore: "store", Namespace: "namespace", GroupingKey: "not-a-uuid"}, func(s *patterns.Service) error {
			_, err := s.GetPatternsForScope(t.Context(), "not-a-uuid")
			return err
		}},
		{"scopes", contracts.Patterns_AllPatternScopes_FullMethodName, &contracts.AllPatternScopesRequest{EventStore: "store", Namespace: "namespace"}, func(s *patterns.Service) error { _, err := s.GetScopes(t.Context()); return err }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			conn := &transport{invoke: func(_ context.Context, method string, request, reply any) error {
				calls++
				if method != tc.method || !proto.Equal(request.(proto.Message), tc.want) {
					t.Fatalf("request = %s %v, want %s %v", method, request, tc.method, tc.want)
				}
				success(reply)
				return nil
			}}
			if err := tc.call(service(t, conn)); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatalf("calls = %d", calls)
			}
		})
	}
}

func TestCriteriaDefaultsZerosAndNoInventedValidation(t *testing.T) {
	for _, confidence := range []float64{0, -1, 1.5, math.NaN(), math.Inf(1), math.Inf(-1)} {
		for _, limit := range []int32{0, -1, 1, math.MaxInt32} {
			c, l := patterns.Confidence(confidence), limit
			conn := &transport{invoke: func(_ context.Context, _ string, request, reply any) error {
				r := request.(*contracts.MatchingPatternsRequest)
				if math.Float64bits(r.MinimumConfidence) != math.Float64bits(confidence) || r.MaximumResults != limit || len(r.Context) != 0 || r.GroupingKey != "" {
					t.Fatalf("criteria changed: %v", r)
				}
				success(reply)
				return nil
			}}
			if _, err := service(t, conn).GetPatterns(t.Context(), "", patterns.FacetSet{}, patterns.QueryOptions{MinimumConfidence: &c, MaximumResults: &l}); err != nil {
				t.Fatal(err)
			}
		}
	}
	conn := &transport{invoke: func(_ context.Context, _ string, request, reply any) error {
		r := request.(*contracts.UsualActionsRequest)
		if r.MinimumConfidence != 0 || r.MaximumResults != 0 {
			t.Fatalf("invented SDK defaults: %v", r)
		}
		success(reply)
		return nil
	}}
	if result, err := service(t, conn).GetUsualActions(t.Context(), "", patterns.FacetSet{}, patterns.QueryOptions{}); err != nil || result.Data == nil || len(result.Data) != 0 {
		t.Fatalf("empty answer = %+v, %v", result, err)
	}
}

func TestTimeBucketsAndExplicitZeroMoment(t *testing.T) {
	for hour, want := range map[int]string{0: "Night", 4: "Night", 5: "EarlyMorning", 7: "EarlyMorning", 8: "Morning", 10: "Morning", 11: "Midday", 13: "Midday", 14: "Afternoon", 16: "Afternoon", 17: "Evening", 21: "Evening", 22: "Night", 23: "Night"} {
		moment := time.Date(2024, 1, 15, hour, 0, 0, 0, time.FixedZone("", -7*3600))
		conn := &transport{invoke: func(_ context.Context, _ string, request, reply any) error {
			if got := request.(*contracts.UsualActionsRequest).Context; got["TimeBucket"] != want || got["Day"] != "Monday" {
				t.Fatalf("hour %d: %v", hour, got)
			}
			success(reply)
			return nil
		}}
		if _, err := service(t, conn).GetPatternsAt(t.Context(), "scope", &moment, patterns.FacetSet{}, patterns.QueryOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	zero := time.Time{}
	conn := &transport{invoke: func(_ context.Context, _ string, request, reply any) error {
		if got := request.(*contracts.UsualActionsRequest).Context; got["Day"] != "Monday" || got["TimeBucket"] != "Night" {
			t.Fatalf("explicit zero moment lost: %v", got)
		}
		success(reply)
		return nil
	}}
	if _, err := service(t, conn).GetPatternsAt(t.Context(), "scope", &zero, patterns.FacetSet{}, patterns.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestOmittedMomentUsesLocalNowAndRejectsUnrepresentableMoment(t *testing.T) {
	before := time.Now()
	var got map[string]string
	conn := &transport{invoke: func(_ context.Context, _ string, request, reply any) error {
		got = request.(*contracts.UsualActionsRequest).Context
		success(reply)
		return nil
	}}
	if _, err := service(t, conn).GetPatternsAt(t.Context(), "scope", nil, patterns.FacetSet{}, patterns.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	after := time.Now()
	// Checking a bounded interval avoids a flaky midnight/bucket-boundary assertion.
	matches := func(moment time.Time) bool {
		hour := moment.Hour()
		bucket := "Night"
		switch {
		case hour >= 5 && hour < 8:
			bucket = "EarlyMorning"
		case hour >= 8 && hour < 11:
			bucket = "Morning"
		case hour >= 11 && hour < 14:
			bucket = "Midday"
		case hour >= 14 && hour < 17:
			bucket = "Afternoon"
		case hour >= 17 && hour < 22:
			bucket = "Evening"
		}
		return got["Day"] == moment.Weekday().String() && got["TimeBucket"] == bucket
	}
	if !matches(before) && !matches(after) {
		t.Fatalf("now context = %v", got)
	}
	for _, moment := range []time.Time{time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 1, 1, 0, 0, 0, 0, time.FixedZone("", 15*3600)), time.Date(2024, 1, 1, 0, 0, 0, 0, time.FixedZone("", 1))} {
		got = nil
		if _, err := service(t, conn).GetPatternsAt(t.Context(), "scope", &moment, patterns.FacetSet{}, patterns.QueryOptions{}); !errors.Is(err, patterns.ErrInvalidConfiguration) || got != nil {
			t.Fatalf("invalid moment dispatched: %v", err)
		}
	}
}

func TestResponseGoldensAndSnapshotOwnership(t *testing.T) {
	facts := map[patterns.FacetName]patterns.FacetValue{patterns.Day: "Monday", patterns.CommandType: "Approve"}
	facets := patterns.NewFacetSet(facts)
	facts[patterns.Day] = "Tuesday"
	copy := facets.Facts()
	copy[patterns.Day] = "Wednesday"
	confidence, limit := patterns.Confidence(0.5), int32(8)
	date := "2024-01-15T09:30:00.1234567+02:00"
	response := &contracts.QueryResult_IEnumerable_BehaviorPatternDetailsResponse{IsAuthorized: true, CorrelationId: &bcl.Guid{Lo: 0x7766554433221100, Hi: 0xffeeddccbbaa9988}, Data: []*contracts.BehaviorPatternDetailsResponse{{Id: "CommandType=Approve;Day=Monday", GroupingKey: "scope", Facets: map[string]string{"CommandType": "Approve", "Day": "Monday"}, Confidence: 0, Support: 0.25, Occurrences: 12, Weight: 8.5, Specificity: 99, FirstSeen: &contracts.SerializableDateTimeOffset{Value: date}, LastSeen: &contracts.SerializableDateTimeOffset{Value: date}}}}
	conn := &transport{invoke: func(_ context.Context, _ string, request, reply any) error {
		r := request.(*contracts.MatchingPatternsRequest)
		confidence, limit = 1, 1
		if r.MinimumConfidence != 0.5 || r.MaximumResults != 8 || r.Context["Day"] != "Monday" {
			t.Fatalf("request aliases input: %v", r)
		}
		r.Context["Day"] = "mutated transport snapshot"
		raw := reply.(*contracts.QueryResult_IEnumerable_BehaviorPatternDetailsResponse)
		raw.IsAuthorized, raw.CorrelationId, raw.Data = response.IsAuthorized, response.CorrelationId, response.Data
		return nil
	}}
	result, err := service(t, conn).GetPatterns(t.Context(), "scope", facets, patterns.QueryOptions{MinimumConfidence: &confidence, MaximumResults: &limit})
	if err != nil {
		t.Fatal(err)
	}
	response.Data[0].Facets["Day"] = "Thursday"
	if facets.ValueOf(patterns.Day) != "Monday" || result.Data[0].Facets.ValueOf(patterns.Day) != "Monday" || result.Data[0].Action() != "Approve" || result.Data[0].Confidence != 0 || result.Data[0].Occurrences != 12 || result.Data[0].Support != 0.25 || result.Data[0].Weight != 8.5 || result.Data[0].Specificity != 2 || result.Data[0].ID != "CommandType=Approve;Day=Monday" {
		t.Fatalf("response = %+v", result)
	}
	if uuid.UUID(result.CorrelationID).String() != "33221100-5544-7766-8899-aabbccddeeff" {
		t.Fatalf("Guid ordering: %s", result.CorrelationID)
	}
	if result.Data[0].FirstSeen.Format("2006-01-02T15:04:05.0000000-07:00") != date || result.Data[0].LastSeen != result.Data[0].FirstSeen {
		t.Fatal("offset/tick precision changed")
	}
	copy = result.Data[0].Facets.Facts()
	copy[patterns.Day] = "Friday"
	if result.Data[0].Facets.ValueOf(patterns.Day) != "Monday" {
		t.Fatal("response facets mutable")
	}
}

func TestEnvelopeFailuresRedactedAndUnknownSeverities(t *testing.T) {
	for _, severity := range []int32{0, 1, 2, 3, 4, -1} {
		members := []string{"secret-member"}
		conn := &transport{invoke: func(_ context.Context, _ string, _, reply any) error {
			r := reply.(*contracts.QueryResult_IEnumerable_BehaviorPatternDetailsResponse)
			r.IsAuthorized = true
			r.ValidationResults = []*contracts.ValidationResult{{Severity: contracts.ValidationResultSeverity(severity), Message: "secret-PII", Members: members}}
			return nil
		}}
		result, err := service(t, conn).GetPatternsForScope(t.Context(), "scope")
		var envelope *patterns.EnvelopeError
		if !errors.As(err, &envelope) || result.Data != nil || envelope.ValidationResults[0].Severity != severity || strings.Contains(err.Error(), "secret") {
			t.Fatalf("severity %d: %+v, %v", severity, result, err)
		}
		members[0] = "changed"
		if envelope.ValidationResults[0].Members[0] != "secret-member" {
			t.Fatal("diagnostic aliases response")
		}
	}
	for _, authorized := range []bool{false, true} {
		conn := &transport{invoke: func(_ context.Context, _ string, _, reply any) error {
			r := reply.(*contracts.QueryResult_IEnumerable_PatternScopeResponse)
			r.IsAuthorized = authorized
			if authorized {
				r.ExceptionMessages = []string{"secret-PII"}
			}
			return nil
		}}
		result, err := service(t, conn).GetScopes(t.Context())
		var envelope *patterns.EnvelopeError
		if !errors.As(err, &envelope) || envelope.Authorized != authorized || result.Data != nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("envelope: %v", err)
		}
	}
}

func TestMalformedResponsesFailAtomically(t *testing.T) {
	valid := &contracts.BehaviorPatternDetailsResponse{FirstSeen: &contracts.SerializableDateTimeOffset{Value: "2024-01-15T00:00:00.0000000+00:00"}, LastSeen: &contracts.SerializableDateTimeOffset{Value: "2024-01-15T00:00:00.0000000+00:00"}}
	for _, malformed := range []*contracts.BehaviorPatternDetailsResponse{nil, {}, {FirstSeen: valid.FirstSeen}, {FirstSeen: &contracts.SerializableDateTimeOffset{Value: "not-a-date"}, LastSeen: valid.LastSeen}, {FirstSeen: &contracts.SerializableDateTimeOffset{Value: "2024-01-15T00:00:00.000000001Z"}, LastSeen: valid.LastSeen}} {
		conn := &transport{invoke: func(_ context.Context, _ string, _, reply any) error {
			r := reply.(*contracts.QueryResult_IEnumerable_BehaviorPatternDetailsResponse)
			r.IsAuthorized = true
			r.Data = []*contracts.BehaviorPatternDetailsResponse{valid, malformed}
			return nil
		}}
		result, err := service(t, conn).GetPatternsForScope(t.Context(), "scope")
		if !errors.Is(err, patterns.ErrProtocol) || result.Data != nil {
			t.Fatalf("malformed result became partial success: %+v %v", result, err)
		}
	}
	conn := &transport{invoke: func(_ context.Context, _ string, _, reply any) error {
		r := reply.(*contracts.QueryResult_IEnumerable_PatternScopeResponse)
		r.IsAuthorized = true
		r.Data = []*contracts.PatternScopeResponse{nil}
		return nil
	}}
	if _, err := service(t, conn).GetScopes(t.Context()); !errors.Is(err, patterns.ErrProtocol) {
		t.Fatal(err)
	}
}

func TestResponseTimestampGrammar(t *testing.T) {
	// SerializableDateTimeOffset.cs at 2e31b0dfb emits seven digits using "O".
	// The Go response profile also permits short or absent fractional seconds.
	valid := []struct {
		name, value string
		nanosecond  int
		offset      int
	}{
		{"seven-digits", "2024-01-15T09:30:00.1234567+02:00", 123456700, 2 * 3600},
		{"short-fraction", "2024-01-15T09:30:00.1Z", 100000000, 0},
		{"zero-fraction", "2024-01-15T09:30:00.0000000Z", 0, 0},
		{"absent-fraction", "2024-01-15T09:30:00Z", 0, 0},
		{"positive-minute-59", "2024-01-15T09:30:00+13:59", 0, 13*3600 + 59*60},
		{"negative-minute-59", "2024-01-15T09:30:00-13:59", 0, -13*3600 - 59*60},
		{"maximum-positive-offset", "2024-01-15T09:30:00+14:00", 0, 14 * 3600},
		{"maximum-negative-offset", "2024-01-15T09:30:00-14:00", 0, -14 * 3600},
		{"negative-zero-offset", "2024-01-15T09:30:00-00:00", 0, 0},
		{"minimum-date", "0001-01-01T00:00:00Z", 0, 0},
		{"maximum-date", "9999-12-31T23:59:59.9999999Z", 999999900, 0},
		{"gregorian-leap-day", "2000-02-29T09:30:00Z", 0, 0},
	}
	for _, tc := range valid {
		t.Run(tc.name, func(t *testing.T) {
			conn := &transport{invoke: func(_ context.Context, _ string, _, reply any) error {
				r := reply.(*contracts.QueryResult_IEnumerable_BehaviorPatternDetailsResponse)
				r.IsAuthorized = true
				r.Data = []*contracts.BehaviorPatternDetailsResponse{{FirstSeen: &contracts.SerializableDateTimeOffset{Value: tc.value}, LastSeen: &contracts.SerializableDateTimeOffset{Value: tc.value}}}
				return nil
			}}
			result, err := service(t, conn).GetPatternsForScope(t.Context(), "scope")
			if err != nil || len(result.Data) != 1 {
				t.Fatalf("valid timestamp rejected: %+v %v", result, err)
			}
			for _, date := range []time.Time{result.Data[0].FirstSeen, result.Data[0].LastSeen} {
				_, offset := date.Zone()
				if date.Nanosecond() != tc.nanosecond || offset != tc.offset {
					t.Fatalf("timestamp changed: %v, want nanosecond=%d offset=%d", date, tc.nanosecond, tc.offset)
				}
			}
		})
	}
}

func TestMalformedTimestampResponsesFailAtomically(t *testing.T) {
	invalid := []struct{ name, value string }{
		{"normalized-offset-minute", "2024-01-15T09:30:00+00:60"},
		{"normalized-negative-offset-minute", "2024-01-15T09:30:00-00:60"},
		{"truncated-fraction", "2024-01-15T09:30:00.1234567001Z"},
		{"eight-fraction-digits", "2024-01-15T09:30:00.12345670Z"},
		{"excess-zero-precision", "2024-01-15T09:30:00.00000000Z"},
		{"sub-tick-precision", "2024-01-15T09:30:00.123456701Z"},
		{"positive-offset-past-maximum", "2024-01-15T09:30:00+14:01"},
		{"negative-offset-past-maximum", "2024-01-15T09:30:00-14:01"},
		{"offset-hour-past-maximum", "2024-01-15T09:30:00+15:00"},
		{"offset-missing-sign", "2024-01-15T09:30:0002:00"},
		{"offset-minute-not-two-digits", "2024-01-15T09:30:00+02:9"},
		{"offset-minute-not-numeric", "2024-01-15T09:30:00+02:xx"},
		{"empty-fraction", "2024-01-15T09:30:00.Z"},
		{"comma-fraction", "2024-01-15T09:30:00,1234567Z"},
		{"hour-not-two-digits", "2024-01-15T9:30:00Z"},
		{"invalid-hour", "2024-01-15T24:30:00Z"},
		{"invalid-minute", "2024-01-15T09:60:00Z"},
		{"invalid-second", "2024-01-15T09:30:60Z"},
		{"invalid-month", "2024-13-15T09:30:00Z"},
		{"invalid-day", "2024-04-31T09:30:00Z"},
		{"gregorian-non-leap-day", "1900-02-29T09:30:00Z"},
		{"year-zero", "0000-01-15T09:30:00Z"},
		{"utc-before-minimum", "0001-01-01T00:00:00+00:01"},
		{"utc-after-maximum", "9999-12-31T23:59:59-00:01"},
		{"missing-offset", "2024-01-15T09:30:00"},
		{"malformed-TimeUTC", "TimeUTC"},
		{"trailing-data", "2024-01-15T09:30:00Z\n"},
	}
	for _, tc := range invalid {
		for _, field := range []string{"FirstSeen", "LastSeen"} {
			t.Run(tc.name+"/"+field, func(t *testing.T) {
				validDate := &contracts.SerializableDateTimeOffset{Value: "2024-01-15T09:30:00.1234567+02:00"}
				valid := &contracts.BehaviorPatternDetailsResponse{FirstSeen: validDate, LastSeen: validDate}
				malformed := &contracts.BehaviorPatternDetailsResponse{FirstSeen: validDate, LastSeen: validDate}
				if field == "FirstSeen" {
					malformed.FirstSeen = &contracts.SerializableDateTimeOffset{Value: tc.value}
				} else {
					malformed.LastSeen = &contracts.SerializableDateTimeOffset{Value: tc.value}
				}
				conn := &transport{invoke: func(_ context.Context, _ string, _, reply any) error {
					r := reply.(*contracts.QueryResult_IEnumerable_BehaviorPatternDetailsResponse)
					r.IsAuthorized = true
					r.Data = []*contracts.BehaviorPatternDetailsResponse{valid, malformed}
					return nil
				}}
				result, err := service(t, conn).GetPatternsForScope(t.Context(), "scope")
				if !errors.Is(err, patterns.ErrProtocol) || !reflect.DeepEqual(result, patterns.QueryResult[patterns.BehaviorPattern]{}) {
					t.Fatalf("malformed timestamp became partial success: %+v %v", result, err)
				}
				if err != patterns.ErrProtocol || strings.Contains(err.Error(), tc.value) {
					t.Fatalf("protocol error exposed timestamp or parse error: %v", err)
				}
			})
		}
	}
}

func TestTransportErrorsKeepOriginalGraphWithoutSensitiveFormatting(t *testing.T) {
	ordinary := errors.New("secret-local-cause")
	for _, code := range []codes.Code{codes.Unimplemented, codes.Canceled, codes.DeadlineExceeded, codes.Unavailable} {
		cause := errors.Join(ordinary, status.Error(code, "secret-server-text"))
		conn := &transport{invoke: func(context.Context, string, any, any) error { return cause }}
		result, err := service(t, conn).GetPatternsForScope(t.Context(), "scope")
		if result.Data != nil || !errors.Is(err, ordinary) || strings.Contains(err.Error(), "secret") || !errors.Is(err, cause) {
			t.Fatalf("lost/redacted cause: %v", err)
		}
		switch code {
		case codes.Unimplemented:
			var typed *patterns.UnsupportedError
			if !errors.Is(err, patterns.ErrUnsupported) || !errors.As(err, &typed) {
				t.Fatal(err)
			}
		case codes.Canceled:
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		case codes.DeadlineExceeded:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
		}
	}
}

func TestCancellationDeadlineAndConstruction(t *testing.T) {
	calls := 0
	conn := &transport{invoke: func(ctx context.Context, _ string, _, _ any) error { calls++; <-ctx.Done(); return ctx.Err() }}
	s := service(t, conn)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.GetPatterns(ctx, "scope", patterns.FacetSet{}, patterns.QueryOptions{}); !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatal("pre-cancellation dispatched", err)
	}
	ctx, cancel = context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	if _, err := s.GetUsualActions(ctx, "scope", patterns.FacetSet{}, patterns.QueryOptions{}); !errors.Is(err, context.DeadlineExceeded) || calls != 0 {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(t.Context())
	entered := make(chan struct{})
	conn.invoke = func(ctx context.Context, _ string, _, _ any) error { close(entered); <-ctx.Done(); return ctx.Err() }
	go func() { <-entered; cancel() }()
	if _, err := s.GetScopes(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if s.Store() != "store" || s.Namespace() != "namespace" {
		t.Fatal("coordinates lost")
	}
	for _, c := range []grpc.ClientConnInterface{nil, (*transport)(nil)} {
		if _, err := patterns.New("store", "namespace", c); !errors.Is(err, patterns.ErrInvalidConfiguration) {
			t.Fatal(err)
		}
	}
	if _, err := patterns.New("", "namespace", conn); !errors.Is(err, patterns.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
}

func TestScopeResponseLabelsAndEmptyFacts(t *testing.T) {
	conn := &transport{invoke: func(_ context.Context, _ string, _, reply any) error {
		r := reply.(*contracts.QueryResult_IEnumerable_PatternScopeResponse)
		r.IsAuthorized = true
		r.Data = []*contracts.PatternScopeResponse{{Id: "scope", Name: "Label", UserName: "name"}}
		return nil
	}}
	result, err := service(t, conn).GetScopes(t.Context())
	if err != nil || !reflect.DeepEqual(result.Data, []patterns.Scope{{ID: "scope", Name: "Label", UserName: "name"}}) {
		t.Fatalf("scopes=%+v %v", result, err)
	}
	facets := patterns.NewFacetSet(map[patterns.FacetName]patterns.FacetValue{patterns.Year: "0", patterns.Day: ""})
	if facets.Specificity() != 1 || facets.ValueOf(patterns.Year) != "0" || facets.With(patterns.Year, "").Specificity() != 0 {
		t.Fatal("zero/absent criteria conflated")
	}
}
