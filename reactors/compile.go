// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reactors

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/artifacts"
	"github.com/cratis/chronicle.go/internal/discovery"
	"github.com/cratis/chronicle.go/readmodels"
)

var contextType = reflect.TypeFor[context.Context]()
var errorType = reflect.TypeFor[error]()
var eventContextType = reflect.TypeFor[events.Context]()
var deliveryType = reflect.TypeFor[Delivery]()
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
	onceOnly     bool
}
type constructor = artifacts.Constructor

// Plan is a catalog-isolated, immutable dispatch plan shared by both authoring
// paths. Build it with Compile; reconnect reuses it without rediscovery.
type Plan struct {
	declaration    Declaration
	handlers       map[events.TypeID]call
	replayHandlers map[events.TypeID]call
	descriptors    map[events.TypeID]events.Descriptor
	ordered        []events.TypeRef
	factory        constructor
	middlewares    []constructor
	services       ScopeFactory
	shadows        []Shadow
	catalog        *events.Catalog
	sideEffects    []SideEffectHandler
}

// Shadow diagnoses a method hidden by C# richest-signature/name precedence.
type Shadow struct {
	Event  events.TypeRef
	Winner string
	Hidden string
}

// Shadows returns a detached list of hidden handler methods.
func (p *Plan) Shadows() []Shadow { return slices.Clone(p.shadows) }

// GoType returns the registered artifact type, or nil for explicit callbacks.
func (p *Plan) GoType() reflect.Type { return p.declaration.GoType() }

// Identifier returns the observer identity.
func (p *Plan) Identifier() ID { return p.declaration.Identifier() }

// EventSequence returns the observed sequence.
func (p *Plan) EventSequence() events.SequenceID { return p.declaration.config.sequence }

// EventTypes returns the union of live and replay subscriptions in catalog order.
func (p *Plan) EventTypes() []events.TypeRef { return slices.Clone(p.ordered) }

// PerEvent reports the explicit activation override.
func (p *Plan) PerEvent() bool { return p.declaration.config.perEvent }

// IsReplayable reports whether the kernel may replay this reactor.
func (p *Plan) IsReplayable() bool { return p.declaration.config.replayable }

// Tags returns detached artifact labels.
func (p *Plan) Tags() []string { return slices.Clone(p.declaration.config.tags) }

// FilterTags returns detached event tag filters.
func (p *Plan) FilterTags() []string { return slices.Clone(p.declaration.config.filterTags) }

// EventSourceType returns the source-type filter, empty for all source types.
func (p *Plan) EventSourceType() events.SourceType { return p.declaration.config.sourceType }

// EventStreamType returns the stream-type filter, All by default.
func (p *Plan) EventStreamType() events.StreamType { return p.declaration.config.streamType }

// Compile validates every signature without activating user code. Services may
// be nil for the zero-container default. No cache is shared across catalogs.
func Compile(d Declaration, catalog *events.Catalog, models *readmodels.Catalog, services ScopeFactory) (*Plan, error) {
	return CompileWithMiddleware(d, catalog, models, services, nil)
}

// CompileWithMiddleware compiles a declaration with registry middleware factories
// preceding its per-reactor middleware. The input slice is not retained; factories
// follow WithMiddleware's activation and ownership contract.
func CompileWithMiddleware(d Declaration, catalog *events.Catalog, models *readmodels.Catalog, services ScopeFactory, middlewareFactories []any) (*Plan, error) {
	return CompileWithExtensions(d, catalog, models, services, middlewareFactories, nil)
}

// CompileWithExtensions compiles registry middleware and borrowed side-effect
// handlers before per-reactor extensions. Slices are copied; handler instances
// must be concurrency-safe. No constructors or Handle methods run at compilation.
func CompileWithExtensions(d Declaration, catalog *events.Catalog, models *readmodels.Catalog, services ScopeFactory, middlewareFactories []any, sideEffects []SideEffectHandler) (*Plan, error) {
	if d.Identifier() == "" || catalog == nil || models == nil {
		return nil, invalid("reactor and catalogs required")
	}
	if services == nil {
		services = DefaultScopeFactory()
	}
	if nilLike(services) {
		return nil, invalid("nil scope factory")
	}
	p := &Plan{declaration: d, handlers: map[events.TypeID]call{}, replayHandlers: map[events.TypeID]call{}, descriptors: map[events.TypeID]events.Descriptor{}, services: services, catalog: catalog}
	p.sideEffects = append(slices.Clone(sideEffects), d.config.sideEffects...)
	for _, handler := range p.sideEffects {
		if nilLike(handler) {
			return nil, invalid("nil side-effect handler")
		}
	}
	fail := func(method string, t reflect.Type, err error) (*Plan, error) {
		var detail *DeclarationError
		if errors.As(err, &detail) {
			detail.Reactor = d.Identifier()
			return nil, detail
		}
		return nil, &DeclarationError{Reactor: d.Identifier(), Method: method, Parameter: t, Cause: err}
	}
	selected := map[string]bool{}
	if !d.explicit {
		factory, err := compileConstructor(d.typ, d.factory, services)
		if err != nil {
			return fail("constructor", d.typ, err)
		}
		p.factory = factory
		for _, method := range discovery.Methods(d.typ) {
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
			handler, err := compileCall(method.Name, method.Func, true, catalog, models, services, p.sideEffects)
			if err != nil {
				return fail(method.Name, t.In(first), err)
			}
			selected[method.Name] = true
			handler.onceOnly = slices.Contains(d.config.onceMethods, method.Name)
			handlers := p.handlers
			if slices.Contains(d.config.replayMethods, method.Name) {
				handlers = p.replayHandlers
			}
			for _, descriptor := range matching {
				id := descriptor.Ref().ID
				if previous, exists := handlers[id]; exists {
					p.shadows = append(p.shadows, Shadow{descriptor.Ref(), previous.name, method.Name})
					d.config.logger.Warn("reactor handler shadowed", "reactor", d.Identifier(), "event", id, "winner", previous.name, "hidden", method.Name)
					continue
				}
				handlers[id] = handler
				p.descriptors[id] = descriptor
			}
		}
	}
	for _, name := range append(slices.Clone(d.config.replayMethods), d.config.onceMethods...) {
		if !selected[name] {
			return fail(name, nil, invalid("policy must select a discovered exported event handler"))
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
		handler, err := compileCall(name, reflect.ValueOf(explicit.function), false, catalog, models, services, p.sideEffects)
		if err != nil {
			return fail(name, explicit.event, err)
		}
		handler.onceOnly = explicit.onceOnly
		handlers := p.handlers
		if explicit.replay {
			handlers = p.replayHandlers
		}
		for _, descriptor := range matching {
			id := descriptor.Ref().ID
			if _, exists := handlers[id]; exists {
				return fail(name, explicit.event, invalid("duplicate explicit event binding"))
			}
			handlers[id] = handler
			p.descriptors[id] = descriptor
		}
	}
	if len(p.descriptors) == 0 {
		return fail("", nil, invalid("no registered event handlers"))
	}
	for _, descriptor := range catalog.Descriptors() {
		if _, ok := p.descriptors[descriptor.Ref().ID]; ok {
			p.ordered = append(p.ordered, descriptor.Ref())
		}
	}
	for _, factory := range append(slices.Clone(middlewareFactories), d.config.middlewares...) {
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
	return discovery.Events(typ, catalog)
}
func compileCall(name string, fn reflect.Value, receiver bool, catalog *events.Catalog, models *readmodels.Catalog, services ScopeFactory, sideEffects []SideEffectHandler) (call, error) {
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
	if n > 1 || (n == 1 && !supportsResult(t.Out(0), catalog, sideEffects)) {
		return c, invalid("unsupported result: expected no result, error, event/effect or claimed custom result, optionally plus error")
	}
	c.returnsEvent = n == 1
	return c, nil
}
func compileConstructor(typ reflect.Type, factory any, services ScopeFactory) (constructor, error) {
	c, err := artifacts.CompileConstructor(typ, factory, services)
	var parameter *artifacts.ParameterError
	if errors.As(err, &parameter) {
		err = &DeclarationError{Method: "constructor", Parameter: parameter.Type, Cause: parameter.Cause}
	}
	return c, err
}
