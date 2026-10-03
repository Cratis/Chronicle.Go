// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/decision"
	"github.com/google/uuid"
)

// ErrDecisionReadRefused identifies a decision that cannot safely issue a token.
var ErrDecisionReadRefused = errors.New("chronicle: decision read refused")

// DecisionReadRefusalReason is a stable admission or read refusal category.
type DecisionReadRefusalReason string

const (
	DecisionUnavailable          DecisionReadRefusalReason = "unavailable"            // DecisionUnavailable means no SDK decision provider.
	DecisionReducer              DecisionReadRefusalReason = "reducer"                // DecisionReducer excludes reducer-backed models.
	DecisionAmbiguousProjection  DecisionReadRefusalReason = "ambiguous-projection"   // DecisionAmbiguousProjection requires exactly one projection.
	DecisionNotEventLog          DecisionReadRefusalReason = "not-event-log"          // DecisionNotEventLog excludes other sequences.
	DecisionJoin                 DecisionReadRefusalReason = "join"                   // DecisionJoin excludes joins and removal joins.
	DecisionHierarchy            DecisionReadRefusalReason = "hierarchy"              // DecisionHierarchy excludes children and nested projections.
	DecisionOpenEndedEventTypes  DecisionReadRefusalReason = "open-ended-event-types" // DecisionOpenEndedEventTypes excludes all-event subscriptions.
	DecisionDerivatives          DecisionReadRefusalReason = "derivatives"            // DecisionDerivatives excludes FromEvery derivatives.
	DecisionFromEventProperty    DecisionReadRefusalReason = "from-event-property"    // DecisionFromEventProperty excludes property-derived input.
	DecisionNotEventSourceKeyed  DecisionReadRefusalReason = "not-event-source-keyed" // DecisionNotEventSourceKeyed requires source keys, including parents and All.
	DecisionNoEventTypes         DecisionReadRefusalReason = "no-event-types"         // DecisionNoEventTypes requires a finite nonempty dependency set.
	DecisionUnsupportedEventType DecisionReadRefusalReason = "unsupported-event-type" // DecisionUnsupportedEventType rejects unresolved or comma-containing IDs.
	DecisionKeyConversion        DecisionReadRefusalReason = "key-conversion"         // DecisionKeyConversion requires a string or UUID schema key.
	DecisionInvalidKey           DecisionReadRefusalReason = "invalid-key"            // DecisionInvalidKey rejects noncanonical and wildcard keys.
	DecisionDefinitionMismatch   DecisionReadRefusalReason = "definition-mismatch"    // DecisionDefinitionMismatch means the server disagrees with admission.
	DecisionFoldIncomplete       DecisionReadRefusalReason = "fold-incomplete"        // DecisionFoldIncomplete exhausts three fresh fold attempts.
	DecisionFoldAhead            DecisionReadRefusalReason = "fold-ahead"             // DecisionFoldAhead exhausts three fresh pre-fold boundaries.
)

// DecisionReadAdmission reports local shape admission without network I/O. It
// does not establish server agreement, capabilities or a protected read.
type DecisionReadAdmission struct {
	IsAdmitted bool                      // IsAdmitted means the frozen local shape is supported.
	Reason     DecisionReadRefusalReason // Reason is empty on admission.
}

// DecisionReadRefused identifies the model and stable reason; it never contains
// the potentially sensitive instance key or model content.
type DecisionReadRefused struct {
	Model  Identifier                // Model identifies the refused model.
	Reason DecisionReadRefusalReason // Reason describes admission/agreement/fold failure.
}

func (e *DecisionReadRefused) Error() string {
	return fmt.Sprintf("%v: %s (%s)", ErrDecisionReadRefused, e.Model, e.Reason)
}
func (e *DecisionReadRefused) Unwrap() error { return ErrDecisionReadRefused }

// DecisionReader is a concurrency-safe admitted optimistic reader. Construct it
// with DecisionsFor; its zero value is invalid. It owns no client resources.
type DecisionReader[T any] struct{ reader *Reader[T] }

// DecisionsFor selects a typed decision reader without I/O. Only EventStore's
// frozen projection catalogs support issuance; low-level/test substitutes do not.
func DecisionsFor[T any](service *Service, model Model[T]) *DecisionReader[T] {
	return &DecisionReader[T]{reader: For(service, model)}
}

type admittedDecision struct {
	descriptor Descriptor
	projection *contracts.ProjectionDefinition
	types      []events.TypeRef
	key        keyShape
	catalog    *decision.Catalog
	epoch      uint64
}
type keyShape struct {
	name, format string
	nullable     bool
}

// Admit inspects the frozen catalog only. Refused declarations never issue RPCs.
func (r *DecisionReader[T]) Admit() DecisionReadAdmission {
	_, err := r.assess()
	if err == nil {
		return DecisionReadAdmission{IsAdmitted: true}
	}
	var refusal *DecisionReadRefused
	if errors.As(err, &refusal) {
		return DecisionReadAdmission{Reason: refusal.Reason}
	}
	return DecisionReadAdmission{Reason: DecisionUnavailable}
}

func (r *DecisionReader[T]) assess() (admittedDecision, error) {
	if r == nil || r.reader == nil {
		return admittedDecision{}, notRegistered()
	}
	d, err := r.reader.descriptor()
	if err != nil {
		return admittedDecision{}, err
	}
	refuse := func(reason DecisionReadRefusalReason) (admittedDecision, error) {
		return admittedDecision{}, &DecisionReadRefused{Model: d.Identifier(), Reason: reason}
	}
	if kind, _ := d.Observer(); kind == Reducer {
		return refuse(DecisionReducer)
	}
	s := r.reader.service
	if s.decisions == nil || s.decisions.DecisionCatalog() == nil {
		return refuse(DecisionUnavailable)
	}
	catalog := s.decisions.DecisionCatalog()
	var matches []*contracts.ProjectionDefinition
	for _, definition := range catalog.Projections {
		if definition.GetReadModel() == string(d.Identifier()) {
			matches = append(matches, definition)
		}
	}
	if len(matches) != 1 {
		return refuse(DecisionAmbiguousProjection)
	}
	types, reason := assessProjection(matches[0], catalog.Events)
	if reason != "" {
		return refuse(reason)
	}
	key, ok := decisionKeySchema(d.Schema())
	if !ok {
		return refuse(DecisionKeyConversion)
	}
	return admittedDecision{descriptor: d, projection: matches[0], types: types, key: key, catalog: catalog, epoch: catalog.Epoch.Load()}, nil
}

func assessProjection(p *contracts.ProjectionDefinition, catalog *events.Catalog) ([]events.TypeRef, DecisionReadRefusalReason) {
	if p == nil || p.Identifier == "" {
		return nil, DecisionAmbiguousProjection
	}
	if p.EventSequenceId != string(events.EventLog) {
		return nil, DecisionNotEventLog
	}
	if len(p.Join)+len(p.RemovedWithJoin) != 0 {
		return nil, DecisionJoin
	}
	if len(p.Children)+len(p.Nested) != 0 {
		return nil, DecisionHierarchy
	}
	if p.SubscribesToAllEvents {
		return nil, DecisionOpenEndedEventTypes
	}
	if len(p.FromEvery) != 0 {
		return nil, DecisionDerivatives
	}
	if p.FromEventProperty != nil {
		return nil, DecisionFromEventProperty
	}
	if !sourceExpression(p.All.GetKey()) {
		return nil, DecisionNotEventSourceKeyed
	}
	var types []events.TypeRef
	add := func(event *contracts.EventType) bool {
		if event == nil || strings.TrimSpace(event.Id) == "" || strings.Contains(event.Id, ",") || event.Generation == 0 || catalog == nil {
			return false
		}
		ref := events.TypeRef{ID: events.TypeID(event.Id), Generation: events.Generation(event.Generation)}
		if _, found := catalog.LookupRef(ref); !found {
			return false
		}
		types = append(types, ref)
		return true
	}
	for _, from := range p.From {
		if from == nil || from.Value == nil || !add(from.Key) {
			return nil, DecisionUnsupportedEventType
		}
		if !sourceExpression(from.Value.Key) || !sourceExpression(from.Value.ParentKey) {
			return nil, DecisionNotEventSourceKeyed
		}
	}
	for _, removal := range p.RemovedWith {
		if removal == nil || removal.Value == nil || !add(removal.Key) {
			return nil, DecisionUnsupportedEventType
		}
		if !sourceExpression(removal.Value.Key) || !sourceExpression(removal.Value.ParentKey) {
			return nil, DecisionNotEventSourceKeyed
		}
	}
	if len(types) == 0 {
		return nil, DecisionNoEventTypes
	}
	sort.Slice(types, func(i, j int) bool { return types[i].ID < types[j].ID })
	return slices.CompactFunc(types, func(a, b events.TypeRef) bool { return a.ID == b.ID }), ""
}
func sourceExpression(s string) bool { return s == "" || s == "$eventSourceId" }

// The pinned kernel's actual key is schema property id, then Id. A Go Key tag
// alone cannot change that wire convention. Follow local references boundedly.
func decisionKeySchema(schema string) (keyShape, bool) {
	var root map[string]any
	if json.Unmarshal([]byte(schema), &root) != nil {
		return keyShape{}, false
	}
	properties, _ := root["properties"].(map[string]any)
	name := "id"
	if _, ok := properties[name]; !ok {
		name = "Id"
	}
	node, _ := properties[name].(map[string]any)
	for range 16 {
		if node == nil {
			return keyShape{}, false
		}
		ref, reference := node["$ref"].(string)
		if !reference {
			format, validFormat := node["format"].(string)
			if _, present := node["format"]; present && !validFormat {
				return keyShape{}, false
			}
			typ, _ := node["type"].(string)
			nullable := false
			if types, ok := node["type"].([]any); ok && len(types) == 2 {
				first, _ := types[0].(string)
				second, _ := types[1].(string)
				if (first == "string" && second == "null") || (first == "null" && second == "string") {
					typ, nullable = "string", true
				}
			}
			return keyShape{name: name, format: format, nullable: nullable}, typ == "string" && (format == "" || format == "uuid" || format == "guid")
		}
		if !strings.HasPrefix(ref, "#/definitions/") {
			return keyShape{}, false
		}
		definitions, _ := root["definitions"].(map[string]any)
		node, _ = definitions[strings.TrimPrefix(ref, "#/definitions/")].(map[string]any)
	}
	return keyShape{}, false
}

func validDecisionKey(key Key, shape keyShape) bool {
	value := string(key)
	if value == "" || value != strings.TrimSpace(value) || value == "*" || strings.Contains(value, "#") {
		return false
	}
	if shape.format == "" {
		return true
	}
	id, err := uuid.Parse(value)
	return err == nil && id.String() == value
}
