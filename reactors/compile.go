// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reactors

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"unicode/utf16"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
)

var contextType = reflect.TypeFor[context.Context]()
var errorType = reflect.TypeFor[error]()
var eventContextType = reflect.TypeFor[events.Context]()
var deliveryType = reflect.TypeFor[Delivery]()
var scopeType = reflect.TypeFor[Scope]()
var middlewareType = reflect.TypeFor[Middleware]()

type argument struct {
	typ   reflect.Type
	model readmodels.Descriptor
}
type call struct {
	name         string
	fn           reflect.Value
	receiver     bool
	context      bool
	event        reflect.Type
	args         []argument
	returnsEvent bool
	returnsError bool
}
type constructor struct {
	typ      reflect.Type
	fn       reflect.Value
	context  bool
	args     []reflect.Type
	borrowed bool
}

// Plan is a catalog-isolated, immutable dispatch plan shared by both authoring
// paths. Build it with Compile; reconnect reuses it without rediscovery.
type Plan struct {
	declaration Declaration
	handlers    map[events.TypeID]call
	descriptors map[events.TypeID]events.Descriptor
	ordered     []events.TypeRef
	factory     constructor
	middlewares []constructor
	services    ScopeFactory
	shadows     []Shadow
}

// Shadow diagnoses a method hidden by C# richest-signature/name precedence.
type Shadow struct {
	Event  events.TypeRef
	Winner string
	Hidden string
}

// Shadows returns a detached list of hidden handler methods.
func (p *Plan) Shadows() []Shadow { return slices.Clone(p.shadows) }

// Identifier returns the observer identity.
func (p *Plan) Identifier() ID { return p.declaration.Identifier() }

// EventSequence returns the observed sequence.
func (p *Plan) EventSequence() events.SequenceID { return p.declaration.config.sequence }

// EventTypes returns the subscribed types in catalog registration order.
func (p *Plan) EventTypes() []events.TypeRef { return slices.Clone(p.ordered) }

// PerEvent reports the explicit activation override.
func (p *Plan) PerEvent() bool { return p.declaration.config.perEvent }

// Compile validates every signature without activating user code. Services may
// be nil for the zero-container default. No cache is shared across catalogs.
func Compile(d Declaration, catalog *events.Catalog, models *readmodels.Catalog, services ScopeFactory) (*Plan, error) {
	if d.Identifier() == "" || catalog == nil || models == nil {
		return nil, invalid("reactor and catalogs required")
	}
	if services == nil {
		services = DefaultScopeFactory()
	}
	if nilLike(services) {
		return nil, invalid("nil scope factory")
	}
	p := &Plan{declaration: d, handlers: map[events.TypeID]call{}, descriptors: map[events.TypeID]events.Descriptor{}, services: services}
	fail := func(method string, t reflect.Type, err error) (*Plan, error) {
		var detail *DeclarationError
		if errors.As(err, &detail) {
			detail.Reactor = d.Identifier()
			return nil, detail
		}
		return nil, &DeclarationError{Reactor: d.Identifier(), Method: method, Parameter: t, Cause: err}
	}
	if !d.explicit {
		factory, err := compileConstructor(d.typ, d.factory, services)
		if err != nil {
			return fail("constructor", d.typ, err)
		}
		p.factory = factory
		methods := make([]reflect.Method, d.typ.NumMethod())
		for i := range methods {
			methods[i] = d.typ.Method(i)
		}
		slices.SortFunc(methods, func(a, b reflect.Method) int {
			if a.Type.NumIn() != b.Type.NumIn() {
				return b.Type.NumIn() - a.Type.NumIn()
			}
			return slices.Compare(utf16.Encode([]rune(a.Name)), utf16.Encode([]rune(b.Name)))
		})
		for _, method := range methods {
			t := method.Type
			first := 1
			if t.NumIn() > first && t.In(first) == contextType {
				first++
			}
			if t.NumIn() <= first {
				continue
			}
			matching := matchingEvents(t.In(first), catalog)
			if len(matching) == 0 {
				continue
			}
			handler, err := compileCall(method.Name, method.Func, true, catalog, models, services)
			if err != nil {
				return fail(method.Name, t.In(first), err)
			}
			for _, descriptor := range matching {
				id := descriptor.Ref().ID
				if previous, exists := p.handlers[id]; exists {
					p.shadows = append(p.shadows, Shadow{descriptor.Ref(), previous.name, method.Name})
					d.config.logger.Warn("reactor handler shadowed", "reactor", d.Identifier(), "event", id, "winner", previous.name, "hidden", method.Name)
					continue
				}
				p.handlers[id] = handler
				p.descriptors[id] = descriptor
			}
		}
	}
	for i, explicit := range d.config.handlers {
		name := fmt.Sprintf("callback[%d]", i)
		if nilLike(explicit.function) || explicit.event == nil {
			return fail(name, nil, invalid("nil callback"))
		}
		matching := matchingEvents(explicit.event, catalog)
		if len(matching) == 0 {
			return fail(name, explicit.event, invalid("callback event is not registered"))
		}
		handler, err := compileCall(name, reflect.ValueOf(explicit.function), false, catalog, models, services)
		if err != nil {
			return fail(name, explicit.event, err)
		}
		for _, descriptor := range matching {
			id := descriptor.Ref().ID
			if _, exists := p.handlers[id]; exists {
				return fail(name, explicit.event, invalid("duplicate explicit event binding"))
			}
			p.handlers[id] = handler
			p.descriptors[id] = descriptor
		}
	}
	if len(p.handlers) == 0 {
		return fail("", nil, invalid("no registered event handlers"))
	}
	for _, descriptor := range catalog.Descriptors() {
		if _, ok := p.handlers[descriptor.Ref().ID]; ok {
			p.ordered = append(p.ordered, descriptor.Ref())
		}
	}
	for _, factory := range d.config.middlewares {
		if nilLike(factory) {
			return fail("middleware", nil, invalid("nil middleware factory"))
		}
		typ := reflect.TypeOf(factory)
		if typ.Kind() != reflect.Func || typ.NumOut() < 1 || !typ.Out(0).Implements(middlewareType) {
			return fail("middleware", typ, invalid("factory must return Middleware"))
		}
		compiled, err := compileConstructor(typ.Out(0), factory, services)
		if err != nil {
			return fail("middleware", typ, err)
		}
		p.middlewares = append(p.middlewares, compiled)
	}
	return p, nil
}
func matchingEvents(typ reflect.Type, catalog *events.Catalog) []events.Descriptor {
	if typ.Kind() == reflect.Interface && typ.NumMethod() == 0 {
		return nil
	}
	var result []events.Descriptor
	for _, d := range catalog.Descriptors() {
		if d.GoType().AssignableTo(typ) || reflect.PointerTo(d.GoType()).AssignableTo(typ) {
			result = append(result, d)
		}
	}
	return result
}
func compileCall(name string, fn reflect.Value, receiver bool, catalog *events.Catalog, models *readmodels.Catalog, services ScopeFactory) (call, error) {
	t := fn.Type()
	c := call{name: name, fn: fn, receiver: receiver}
	first := 0
	if receiver {
		first++
	}
	if t.IsVariadic() {
		return c, invalid("variadic handler")
	}
	if t.In(first) == contextType {
		c.context = true
		first++
	}
	c.event = t.In(first)
	for i := first + 1; i < t.NumIn(); i++ {
		typ := t.In(i)
		arg := argument{typ: typ}
		if typ != eventContextType && typ != deliveryType {
			if model, ok := models.LookupType(typ); ok {
				arg.model = model
			} else if err := validateService(services, typ); err != nil {
				return c, &DeclarationError{Method: name, Parameter: typ, Cause: err}
			}
		}
		c.args = append(c.args, arg)
	}
	n := t.NumOut()
	if n > 0 && t.Out(n-1) == errorType {
		c.returnsError = true
		n--
	}
	if n > 1 || (n == 1 && len(matchingEvents(t.Out(0), catalog)) == 0) {
		return c, invalid("unsupported result: expected no result, error, registered event, or event plus error")
	}
	c.returnsEvent = n == 1
	return c, nil
}
func validateService(factory ScopeFactory, typ reflect.Type) error {
	if typ == scopeType {
		return nil
	}
	if catalog, ok := factory.(Catalog); ok && !catalog.Contains(typ) {
		return invalid("unresolvable service parameter: " + typ.String())
	}
	return nil
}
func compileConstructor(typ reflect.Type, factory any, services ScopeFactory) (constructor, error) {
	c := constructor{typ: typ}
	_, plain := services.(defaultFactory)
	catalog, hasCatalog := services.(Catalog)
	if factory == nil || (!plain && hasCatalog && catalog.Contains(typ)) {
		c.borrowed = true
		return c, validateService(services, typ)
	}
	if nilLike(factory) {
		return c, invalid("nil constructor")
	}
	t := reflect.TypeOf(factory)
	if t.Kind() != reflect.Func || t.IsVariadic() || t.NumOut() < 1 || t.NumOut() > 2 || t.Out(0) != typ || (t.NumOut() == 2 && t.Out(1) != errorType) {
		return c, invalid("constructor must return the registered type, optionally followed by error")
	}
	c.fn = reflect.ValueOf(factory)
	for i := 0; i < t.NumIn(); i++ {
		arg := t.In(i)
		if i == 0 && arg == contextType {
			c.context = true
			continue
		}
		if err := validateService(services, arg); err != nil {
			return c, &DeclarationError{Method: "constructor", Parameter: arg, Cause: err}
		}
		c.args = append(c.args, arg)
	}
	return c, nil
}
