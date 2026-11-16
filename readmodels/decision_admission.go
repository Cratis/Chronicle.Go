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

// DecisionProtectedModel formerly excluded every classified model from
// session-based decisions.
//
// Deprecated: no longer produced. Kernel 19.32.2 and later release decision
// session folds with each value's original subject (Chronicle#4561), so
// classified models are admitted like C#.
const DecisionProtectedModel DecisionReadRefusalReason = "protected-model"

// DecisionProtectionMetadata formerly meant model protection could not be
// determined.
//
// Deprecated: no longer produced; admission does not inspect protection metadata.
const DecisionProtectionMetadata DecisionReadRefusalReason = "protection-metadata"

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
	catalog, err := r.reader.service.decisionCatalogFor(d.Identifier())
	if err != nil {
		return admittedDecision{}, err
	}
	return assessDecisionModel(d, catalog)
}

func (s *Service) decisionCatalogFor(model Identifier) (*decision.Catalog, error) {
	if s.decisions == nil || s.decisions.DecisionCatalog() == nil {
		return nil, &DecisionReadRefused{Model: model, Reason: DecisionUnavailable}
	}
	catalog := s.decisions.DecisionCatalog()
	if catalog.ExpectedEpoch != catalog.Epoch.Load() {
		return nil, decision.ErrStale
	}
	return catalog, nil
}

func assessDecisionModel(d Descriptor, catalog *decision.Catalog) (admittedDecision, error) {
	refuse := func(reason DecisionReadRefusalReason) (admittedDecision, error) {
		return admittedDecision{}, &DecisionReadRefused{Model: d.Identifier(), Reason: reason}
	}
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
	return admittedDecision{descriptor: d, projection: matches[0], types: types, key: key, catalog: catalog, epoch: catalog.ExpectedEpoch}, nil
}

func assessProjection(p *contracts.ProjectionDefinition, catalog *events.Catalog) ([]events.TypeRef, DecisionReadRefusalReason) {
	if p == nil || p.Identifier == "" {
		return nil, DecisionAmbiguousProjection
	}
	if reason := assessProjectionShape(p); reason != "" {
		return nil, reason
	}
	types, reason := decisionProjectionEvents(p, catalog)
	if reason != "" {
		return nil, reason
	}
	if len(types) == 0 {
		return nil, DecisionNoEventTypes
	}
	sort.Slice(types, func(i, j int) bool { return types[i].ID < types[j].ID })
	return slices.CompactFunc(types, func(a, b events.TypeRef) bool { return a.ID == b.ID }), ""
}
func assessProjectionShape(p *contracts.ProjectionDefinition) DecisionReadRefusalReason {
	if p.EventSequenceId != string(events.EventLog) {
		return DecisionNotEventLog
	}
	if len(p.Join)+len(p.RemovedWithJoin) != 0 {
		return DecisionJoin
	}
	if len(p.Children)+len(p.Nested) != 0 {
		return DecisionHierarchy
	}
	if p.SubscribesToAllEvents {
		return DecisionOpenEndedEventTypes
	}
	if len(p.FromEvery) != 0 {
		return DecisionDerivatives
	}
	if p.FromEventProperty != nil {
		return DecisionFromEventProperty
	}
	if !sourceExpression(p.All.GetKey()) {
		return DecisionNotEventSourceKeyed
	}
	return ""
}

func decisionProjectionEvents(p *contracts.ProjectionDefinition, catalog *events.Catalog) ([]events.TypeRef, DecisionReadRefusalReason) {
	var types []events.TypeRef
	for _, from := range p.From {
		ref, reason := decisionFromEvent(from, catalog)
		if reason != "" {
			return nil, reason
		}
		types = append(types, ref)
	}
	return appendDecisionRemovals(types, p.RemovedWith, catalog)
}

func appendDecisionRemovals(types []events.TypeRef, removals []*contracts.KeyValuePair_EventType_RemovedWithDefinition, catalog *events.Catalog) ([]events.TypeRef, DecisionReadRefusalReason) {
	for _, removal := range removals {
		ref, reason := decisionRemovalEvent(removal, catalog)
		if reason != "" {
			return nil, reason
		}
		types = append(types, ref)
	}
	return types, ""
}

func decisionFromEvent(from *contracts.KeyValuePair_EventType_FromDefinition, catalog *events.Catalog) (events.TypeRef, DecisionReadRefusalReason) {
	if from == nil || from.Value == nil {
		return events.TypeRef{}, DecisionUnsupportedEventType
	}
	return decisionInputEvent(from.Key, from.Value.Key, from.Value.ParentKey, catalog)
}

func decisionRemovalEvent(removal *contracts.KeyValuePair_EventType_RemovedWithDefinition, catalog *events.Catalog) (events.TypeRef, DecisionReadRefusalReason) {
	if removal == nil || removal.Value == nil {
		return events.TypeRef{}, DecisionUnsupportedEventType
	}
	return decisionInputEvent(removal.Key, removal.Value.Key, removal.Value.ParentKey, catalog)
}

func decisionInputEvent(event *contracts.EventType, key, parentKey string, catalog *events.Catalog) (events.TypeRef, DecisionReadRefusalReason) {
	ref, ok := decisionEventRef(event, catalog)
	if !ok {
		return events.TypeRef{}, DecisionUnsupportedEventType
	}
	if !sourceExpression(key) || !sourceExpression(parentKey) {
		return events.TypeRef{}, DecisionNotEventSourceKeyed
	}
	return ref, ""
}

func decisionEventRef(event *contracts.EventType, catalog *events.Catalog) (events.TypeRef, bool) {
	if event == nil || catalog == nil {
		return events.TypeRef{}, false
	}
	if !decisionEventIdentity(event) {
		return events.TypeRef{}, false
	}
	ref := events.TypeRef{ID: events.TypeID(event.Id), Generation: events.Generation(event.Generation)}
	_, found := catalog.LookupRef(ref)
	return ref, found
}

func decisionEventIdentity(event *contracts.EventType) bool {
	return strings.TrimSpace(event.Id) != "" && !strings.Contains(event.Id, ",") && event.Generation != 0
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
	node = decisionSchemaNode(root, node)
	if node == nil {
		return keyShape{}, false
	}
	return decisionPrimitiveKey(name, node)
}

func decisionSchemaNode(root, node map[string]any) map[string]any {
	for range 16 {
		ref, reference := node["$ref"].(string)
		if !reference {
			return node
		}
		node = decisionSchemaTarget(root, ref)
	}
	return nil
}

func decisionSchemaTarget(root map[string]any, ref string) map[string]any {
	if !strings.HasPrefix(ref, "#/definitions/") {
		return nil
	}
	definitions, _ := root["definitions"].(map[string]any)
	node, _ := definitions[strings.TrimPrefix(ref, "#/definitions/")].(map[string]any)
	return node
}

func decisionPrimitiveKey(name string, node map[string]any) (keyShape, bool) {
	format, validFormat := node["format"].(string)
	if _, present := node["format"]; present && !validFormat {
		return keyShape{}, false
	}
	typ, _ := node["type"].(string)
	nullable := nullableStringSchema(node["type"])
	if nullable {
		typ = "string"
	}
	return keyShape{name: name, format: format, nullable: nullable}, typ == "string" && decisionKeyFormat(format)
}

func nullableStringSchema(raw any) bool {
	types, ok := raw.([]any)
	if !ok || len(types) != 2 {
		return false
	}
	first, _ := types[0].(string)
	second, _ := types[1].(string)
	return first == "string" && second == "null" || first == "null" && second == "string"
}

func decisionKeyFormat(format string) bool {
	return format == "" || format == "uuid" || format == "guid"
}

func validDecisionKey(key Key, shape keyShape) bool {
	value := string(key)
	if !decisionKeyText(value) {
		return false
	}
	if shape.format == "" {
		return true
	}
	id, err := uuid.Parse(value)
	return err == nil && id.String() == value
}

func decisionKeyText(value string) bool {
	return value != "" && value == strings.TrimSpace(value) && value != "*" && !strings.Contains(value, "#")
}
