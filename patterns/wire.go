// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package patterns

import (
	"slices"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/patterns"
	"github.com/cratis/chronicle.go/internal/wire"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// queryCodec is local to this facade. The generated proto3 getters lose the C#
// [DefaultValue(true)] authorization and [DefaultValue(Error)] severity defaults.
// Inspect actual field presence before applying defaults: explicit false/Unknown
// must not become true/Error. No global codec registration or generated edits.
type queryCodec struct{}

func (queryCodec) Name() string { return "proto" }
func (queryCodec) Marshal(value any) ([]byte, error) {
	message, ok := value.(proto.Message)
	if !ok {
		return nil, ErrProtocol
	}
	return proto.Marshal(message)
}
func (queryCodec) Unmarshal(data []byte, value any) error {
	message, ok := value.(proto.Message)
	if !ok {
		return ErrProtocol
	}
	if err := proto.Unmarshal(data, message); err != nil {
		return err
	}
	fields, err := wireFields(data)
	if err != nil {
		return err
	}
	var authorized *bool
	var validation []*contracts.ValidationResult
	switch result := value.(type) {
	case *contracts.QueryResult_IEnumerable_BehaviorPatternDetailsResponse:
		authorized, validation = &result.IsAuthorized, result.ValidationResults
	case *contracts.QueryResult_IEnumerable_PatternScopeResponse:
		authorized, validation = &result.IsAuthorized, result.ValidationResults
	default:
		return ErrProtocol
	}
	if len(fields[2]) == 0 {
		*authorized = true
	}
	// Each nested ValidationResult owns its independent proto2-style default.
	if len(fields[3]) != len(validation) {
		return ErrProtocol
	}
	for i, item := range fields[3] {
		nested, err := wireFields(item)
		if err != nil {
			return err
		}
		if len(nested[1]) == 0 {
			validation[i].Severity = contracts.ValidationResultSeverity_Error
		}
	}
	return nil
}

// wireFields records presence, preserving repeated length-delimited submessages.
func wireFields(data []byte) (map[protowire.Number][][]byte, error) {
	fields := map[protowire.Number][][]byte{}
	for len(data) > 0 {
		number, kind, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, ErrProtocol
		}
		data = data[n:]
		var payload []byte
		if kind == protowire.BytesType {
			payload, n = protowire.ConsumeBytes(data)
		} else {
			n = protowire.ConsumeFieldValue(number, kind, data)
		}
		if n < 0 {
			return nil, ErrProtocol
		}
		fields[number] = append(fields[number], payload)
		data = data[n:]
	}
	return fields, nil
}

func diagnostics(values []*contracts.ValidationResult) ([]ValidationResult, error) {
	result := make([]ValidationResult, 0, len(values))
	for _, value := range values {
		if value == nil {
			return nil, ErrProtocol
		}
		result = append(result, ValidationResult{Severity: int32(value.Severity), Message: value.Message, Members: slices.Clone(value.Members)})
	}
	return result, nil
}

func decodePatterns(response *contracts.QueryResult_IEnumerable_BehaviorPatternDetailsResponse) (QueryResult[BehaviorPattern], error) {
	var empty QueryResult[BehaviorPattern]
	if response == nil {
		return empty, ErrProtocol
	}
	correlation := wire.Correlation(response.CorrelationId)
	validation, err := diagnostics(response.ValidationResults)
	if err != nil {
		return empty, err
	}
	if err := envelope(response.IsAuthorized, correlation, validation, slices.Clone(response.ExceptionMessages)); err != nil {
		return empty, err
	}
	result := QueryResult[BehaviorPattern]{CorrelationID: correlation, Data: make([]BehaviorPattern, 0, len(response.Data))}
	// Repeated data has no wire presence: nil and empty both mean the C# empty
	// enumerable. A nil message or a missing required date inside data is an error.
	for _, value := range response.Data {
		if value == nil || value.FirstSeen == nil || value.LastSeen == nil {
			return empty, ErrProtocol
		}
		first, err := parseDate(value.FirstSeen.Value)
		if err != nil {
			return empty, err
		}
		last, err := parseDate(value.LastSeen.Value)
		if err != nil {
			return empty, err
		}
		facts := make(map[FacetName]FacetValue, len(value.Facets))
		for name, fact := range value.Facets {
			facts[FacetName(name)] = FacetValue(fact)
		}
		facets := NewFacetSet(facts)
		result.Data = append(result.Data, BehaviorPattern{ID: value.Id, GroupingKey: GroupingKey(value.GroupingKey), Facets: facets, Confidence: Confidence(value.Confidence), Support: value.Support, Occurrences: value.Occurrences, Weight: value.Weight, Specificity: facets.Specificity(), FirstSeen: first, LastSeen: last})
	}
	return result, nil
}

func parseDate(value string) (time.Time, error) {
	result, err := time.Parse(time.RFC3339Nano, value)
	// DateTimeOffset cannot carry sub-tick precision or sub-minute offsets.
	_, offset := result.Zone()
	if err != nil || result.Year() < 1 || result.Year() > 9999 || result.UTC().Year() < 1 || result.UTC().Year() > 9999 || result.Nanosecond()%100 != 0 || offset%60 != 0 || offset < -14*3600 || offset > 14*3600 {
		return time.Time{}, ErrProtocol
	}
	return result, nil
}
