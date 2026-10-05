// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package patterns

import (
	"context"
	"reflect"
	"slices"
	"strings"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/patterns"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc"
)

// Service is an immutable, concurrent-safe store/namespace query facade. It
// borrows its connection, owns no goroutines and keeps no response cache.
type Service struct {
	store     metadata.StoreName
	namespace metadata.Namespace
	client    contracts.PatternsClient
}

// New constructs a facade without I/O. The caller owns the connection and must
// supply authentication/metadata as appropriate. Prefer EventStore.Patterns for
// the existing client's authentication, registration and cancellation barriers.
func New(store metadata.StoreName, namespace metadata.Namespace, conn grpc.ClientConnInterface) (*Service, error) {
	if strings.TrimSpace(string(store)) == "" || strings.TrimSpace(string(namespace)) == "" || conn == nil {
		return nil, invalid("store, namespace and connection required")
	}
	v := reflect.ValueOf(conn)
	if (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) && v.IsNil() {
		return nil, invalid("connection required")
	}
	return &Service{store: store, namespace: namespace, client: contracts.NewPatternsClient(conn)}, nil
}

// Store returns the immutable selected store.
func (s *Service) Store() metadata.StoreName { return s.store }

// Namespace returns the immutable selected namespace.
func (s *Service) Namespace() metadata.Namespace { return s.namespace }

// GetPatterns asks which patterns describe a context. Results keep server order;
// the SDK neither ranks nor filters. Empty facets match the C# empty FacetSet.
func (s *Service) GetPatterns(ctx context.Context, key GroupingKey, facets FacetSet, options QueryOptions) (QueryResult[BehaviorPattern], error) {
	if err := checkContext(ctx); err != nil {
		return QueryResult[BehaviorPattern]{}, err
	}
	confidence, limit := criteria(options)
	response, err := s.client.MatchingPatterns(ctx, &contracts.MatchingPatternsRequest{EventStore: string(s.store), Namespace: string(s.namespace), GroupingKey: string(key), Context: facts(facets), MinimumConfidence: confidence, MaximumResults: limit}, grpc.ForceCodec(queryCodec{}))
	if err != nil {
		return QueryResult[BehaviorPattern]{}, callError(err)
	}
	return decodePatterns(response)
}

// GetUsualActions asks what usually happens in the context, at most one answer
// per action according to the server. Read each answer's Action and Confidence.
func (s *Service) GetUsualActions(ctx context.Context, key GroupingKey, facets FacetSet, options QueryOptions) (QueryResult[BehaviorPattern], error) {
	if err := checkContext(ctx); err != nil {
		return QueryResult[BehaviorPattern]{}, err
	}
	confidence, limit := criteria(options)
	response, err := s.client.UsualActions(ctx, &contracts.UsualActionsRequest{EventStore: string(s.store), Namespace: string(s.namespace), GroupingKey: string(key), Context: facts(facets), MinimumConfidence: confidence, MaximumResults: limit}, grpc.ForceCodec(queryCodec{}))
	if err != nil {
		return QueryResult[BehaviorPattern]{}, callError(err)
	}
	return decodePatterns(response)
}

// GetPatternsAt asks what usually happens at a moment. Nil means local now;
// a non-nil zero time is an explicit year-1 moment, not omission. Day and
// TimeBucket replace those facets in alsoConstraining, preserving all others.
// Bucketing uses the moment's own offset, never normalization to UTC. Moments
// must be DateTimeOffset-representable; sub-100ns precision is truncated.
func (s *Service) GetPatternsAt(ctx context.Context, key GroupingKey, moment *time.Time, alsoConstraining FacetSet, options QueryOptions) (QueryResult[BehaviorPattern], error) {
	if err := checkContext(ctx); err != nil {
		return QueryResult[BehaviorPattern]{}, err
	}
	at := time.Now()
	if moment != nil {
		at = *moment
	}
	_, offset := at.Zone()
	utcYear := at.UTC().Year()
	if at.Year() < 1 || at.Year() > 9999 || utcYear < 1 || utcYear > 9999 || offset%60 != 0 || offset < -14*3600 || offset > 14*3600 {
		return QueryResult[BehaviorPattern]{}, invalid("moment is not representable as DateTimeOffset")
	}
	at = at.Truncate(100 * time.Nanosecond)
	context := alsoConstraining.With(Day, FacetValue(at.Weekday().String())).With(TimeBucket, bucket(at.Hour()))
	return s.GetUsualActions(ctx, key, context, options)
}

// GetPatternsForScope returns all server-established behavior for the scope.
func (s *Service) GetPatternsForScope(ctx context.Context, key GroupingKey) (QueryResult[BehaviorPattern], error) {
	if err := checkContext(ctx); err != nil {
		return QueryResult[BehaviorPattern]{}, err
	}
	response, err := s.client.PatternsForScope(ctx, &contracts.PatternsForScopeRequest{EventStore: string(s.store), Namespace: string(s.namespace), GroupingKey: string(key)}, grpc.ForceCodec(queryCodec{}))
	if err != nil {
		return QueryResult[BehaviorPattern]{}, callError(err)
	}
	return decodePatterns(response)
}

// GetScopes returns scope IDs with optional display metadata in server order.
// C# returns only the grouping keys; Go additionally preserves the DTO labels.
func (s *Service) GetScopes(ctx context.Context) (QueryResult[Scope], error) {
	var empty QueryResult[Scope]
	if err := checkContext(ctx); err != nil {
		return empty, err
	}
	response, err := s.client.AllPatternScopes(ctx, &contracts.AllPatternScopesRequest{EventStore: string(s.store), Namespace: string(s.namespace)}, grpc.ForceCodec(queryCodec{}))
	if err != nil {
		return empty, callError(err)
	}
	if response == nil {
		return empty, ErrProtocol
	}
	correlation := wire.Correlation(response.CorrelationId)
	validation, err := diagnostics(response.ValidationResults)
	if err != nil {
		return empty, err
	}
	if err = envelope(response.IsAuthorized, correlation, validation, slices.Clone(response.ExceptionMessages)); err != nil {
		return empty, err
	}
	result := QueryResult[Scope]{CorrelationID: correlation, Data: make([]Scope, 0, len(response.Data))}
	for _, value := range response.Data {
		if value == nil {
			return empty, ErrProtocol
		}
		result.Data = append(result.Data, Scope{ID: GroupingKey(value.Id), Name: value.Name, UserName: value.UserName})
	}
	return result, nil
}

func criteria(options QueryOptions) (float64, int32) {
	var confidence float64
	var limit int32
	if options.MinimumConfidence != nil {
		confidence = float64(*options.MinimumConfidence)
	}
	if options.MaximumResults != nil {
		limit = *options.MaximumResults
	}
	return confidence, limit
}

func facts(facets FacetSet) map[string]string {
	result := make(map[string]string, facets.Specificity())
	for name, value := range facets.values {
		result[string(name)] = string(value)
	}
	return result
}

func bucket(hour int) FacetValue {
	switch {
	case hour >= 5 && hour < 8:
		return "EarlyMorning"
	case hour >= 8 && hour < 11:
		return "Morning"
	case hour >= 11 && hour < 14:
		return "Midday"
	case hour >= 14 && hour < 17:
		return "Afternoon"
	case hour >= 17 && hour < 22:
		return "Evening"
	default:
		return "Night"
	}
}
