// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package graphs

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

// Types returns the Go types of the nodes and edges, so a registry can refuse a
// type that is both a graph element and a read model.
func (d Declaration) Types() []reflect.Type {
	if d.data == nil {
		return nil
	}
	var result []reflect.Type
	for _, node := range d.data.nodes {
		result = append(result, node.typ)
	}
	for _, edge := range d.data.edges {
		result = append(result, edge.typ)
	}
	return result
}

// Compile resolves the declaration against the frozen event catalog and the
// client's naming policy. defaultSink applies when the graph names no sink. It
// performs no I/O and invokes no application code.
func (d Declaration) Compile(catalog *events.Catalog, policy serialization.NamingPolicy, defaultSink SinkType) (Definition, error) {
	if d.data == nil || catalog == nil {
		return Definition{}, refuse("", "", ErrInvalidGraph, "a graph declaration and an event catalog are required")
	}
	if err := policy.Validate(); err != nil {
		return Definition{}, err
	}
	c := &compiler{catalog: catalog, policy: policy, graph: d.data.name, sink: d.data.sink, eventShapes: make(map[events.TypeRef]*eventShape)}
	if c.sink == "" {
		c.sink = defaultSink
	}
	if c.sink == "" {
		c.sink = Neo4j
	}
	return c.compile(d.data)
}

type compiler struct {
	catalog     *events.Catalog
	policy      serialization.NamingPolicy
	graph       string
	sink        SinkType
	eventShapes map[events.TypeRef]*eventShape
	used        []events.TypeRef
}

// eventShape is what a graph can read from an event: its primitive properties by
// serialized path, and the lists of primitives by name.
type eventShape struct {
	ref    events.TypeRef
	byPath map[string]serialization.Field
	top    []serialization.Field
}

func (c *compiler) event(element string, event Event) (*eventShape, error) {
	declared := event.Descriptor()
	ref := declared.Ref()
	if known, ok := c.eventShapes[ref]; ok {
		return known, nil
	}
	registered, ok := c.catalog.LookupRef(ref)
	if !ok || registered.GoType() != declared.GoType() {
		return nil, refuse(c.graph, element, faults.ErrNotRegistered, "event type %s is not registered with this client", ref.ID)
	}
	if registered.SourceStore() != "" {
		return nil, refuse(c.graph, element, faults.ErrUnsupported, "event type %s comes from another event store; a graph reads its own event log", ref.ID)
	}
	shape := &eventShape{ref: ref, byPath: make(map[string]serialization.Field)}
	for _, field := range registered.Fields() {
		if field.Collection {
			continue
		}
		if !strings.Contains(field.Path, ".") {
			shape.top = append(shape.top, field)
		}
		if field.Scalar.IsPrimitive() {
			shape.byPath[field.Path] = field
		}
	}
	c.eventShapes[ref] = shape
	c.used = append(c.used, ref)
	return shape, nil
}

// resolvePath validates a serialized event path that supplies a primitive value.
func (c *compiler) resolvePath(element, what, path string, event *eventShape) (serialization.Field, error) {
	field, ok := event.byPath[path]
	if !ok {
		return serialization.Field{}, refuse(c.graph, element, ErrUnknownProperty, "%s %q is not a primitive property of event type %s", what, path, event.ref.ID)
	}
	return field, nil
}

func (c *compiler) compile(d *declaration) (Definition, error) {
	var nodes []*compiledNode
	var edges []*compiledEdge
	labels := make(map[*Node]string)
	for _, node := range d.nodes {
		compiled, err := c.node(node)
		if err != nil {
			return Definition{}, err
		}
		nodes = append(nodes, compiled)
		labels[node] = node.label
	}
	for _, edge := range d.edges {
		compiled, err := c.edge(edge, labels)
		if err != nil {
			return Definition{}, err
		}
		edges = append(edges, compiled)
	}
	views, err := d.checkViews(c.policy)
	if err != nil {
		return Definition{}, err
	}
	definition, err := c.definition(nodes, edges)
	if err != nil {
		return Definition{}, err
	}
	definition.data.views = views
	return definition, nil
}

func eventKey(ref events.TypeRef) string { return fmt.Sprintf("%s+%d", ref.ID, ref.Generation) }

// keyExpression resolves the key of a node from an event: the event source id,
// unless an event property names it.
func (c *compiler) keyExpression(element, what string, path string, set bool, event *eventShape) (string, error) {
	if !set {
		return "$eventSourceId", nil
	}
	if _, err := c.resolvePath(element, what, path, event); err != nil {
		return "", err
	}
	return path, nil
}

// mappings resolves the explicit mappings of one event and adds the properties
// the event has under the same name. excluded is the key or identifying property,
// which is never mapped automatically.
func (c *compiler) mappings(element string, target *shape, excluded string, event *eventShape, explicit []mapping) (map[string]string, error) {
	result := make(map[string]string)
	for _, m := range explicit {
		property, ok := target.byName[m.property]
		if !ok {
			return nil, refuse(c.graph, element, ErrUnknownProperty, "property %q is not a property of the node or edge type", m.property)
		}
		expression, err := c.expression(element, *property, m, event)
		if err != nil {
			return nil, err
		}
		result[m.property] = expression
	}
	for _, property := range target.properties {
		if _, mapped := result[property.name]; mapped || property.name == excluded {
			continue
		}
		for _, field := range event.top {
			if field.Name != property.name {
				continue
			}
			if automatic(property, field) {
				result[property.name] = field.Path
			}
			break
		}
	}
	return result, nil
}

// automatic reports whether an event property maps to a property of the same name
// without being asked to. A property the event has under the same name with
// another representation is left alone, as the other clients do for objects.
func automatic(target property, source serialization.Field) bool {
	if target.list {
		return source.Scalar == serialization.NotScalar && isPrimitiveList(source)
	}
	return source.Scalar.IsPrimitive() && assignable(target.scalar, source.Scalar)
}

func isPrimitiveList(field serialization.Field) bool {
	kind := field.Type
	for kind.Kind() == reflect.Pointer {
		kind = kind.Elem()
	}
	if kind.Kind() != reflect.Slice && kind.Kind() != reflect.Array {
		return false
	}
	element, ok := field.Element()
	return ok && element.Scalar.IsPrimitive()
}

func (c *compiler) expression(element string, target property, m mapping, event *eventShape) (string, error) {
	switch m.kind {
	case setFromEvent:
		if target.list {
			return "", refuse(c.graph, element, ErrInvalidMapping, "property %s is a list; map it from the event property of the same name", m.property)
		}
		field, err := c.resolvePath(element, "event property", m.source, event)
		if err != nil {
			return "", err
		}
		if !assignable(target.scalar, field.Scalar) {
			return "", refuse(c.graph, element, ErrInvalidMapping, "event property %q cannot be stored in property %s", m.source, m.property)
		}
		return field.Path, nil
	case setFromContext:
		scalar, ok := contextScalars[m.source]
		if !ok {
			return "", refuse(c.graph, element, ErrUnknownProperty, "event context property %q is unknown or not a primitive", m.source)
		}
		if target.list || !assignable(target.scalar, scalar) {
			return "", refuse(c.graph, element, ErrInvalidMapping, "event context property %q cannot be stored in property %s", m.source, m.property)
		}
		return "$eventContext(" + m.source + ")", nil
	case setFromEventSourceID:
		if target.list || target.scalar != serialization.String {
			return "", refuse(c.graph, element, ErrInvalidMapping, "the event source id can only be stored in a string property, not %s", m.property)
		}
		return "$eventSourceId", nil
	case setLiteral:
		if target.list {
			return "", refuse(c.graph, element, ErrInvalidMapping, "property %s is a list and cannot take a constant", m.property)
		}
		return m.literal, nil
	case addFromEvent, subtractFromEvent:
		field, err := c.resolvePath(element, "event property", m.source, event)
		if err != nil {
			return "", err
		}
		if !numeric(target.scalar) || !numeric(field.Scalar) || target.list {
			return "", refuse(c.graph, element, ErrInvalidMapping, "adding and subtracting need numeric properties: %s and %q", m.property, m.source)
		}
		if !arithmeticPath.MatchString(field.Path) {
			return "", refuse(c.graph, element, ErrInvalidMapping, "event property %q cannot be used in an arithmetic expression", m.source)
		}
		if m.kind == addFromEvent {
			return "$add(" + field.Path + ")", nil
		}
		return "$subtract(" + field.Path + ")", nil
	case increment, decrement, count:
		if target.list || !numeric(target.scalar) {
			return "", refuse(c.graph, element, ErrInvalidMapping, "property %s must be numeric to be incremented, decremented or counted", m.property)
		}
		return map[mappingKind]string{increment: "$increment", decrement: "$decrement", count: "$count"}[m.kind], nil
	case clearProperty:
		return "$null", nil
	}
	return "", refuse(c.graph, element, ErrInvalidMapping, "unknown mapping")
}
