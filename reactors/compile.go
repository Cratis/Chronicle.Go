// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reactors

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"slices"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/artifacts"
	"github.com/cratis/chronicle.go/internal/diagnostics"
	"github.com/cratis/chronicle.go/internal/discovery"
	"github.com/cratis/chronicle.go/observation"
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
	sourceStore    string
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
	batches        []batchCall
}

// Logger returns the frozen borrowed diagnostic logger. Chronicle never closes it.
func (p *Plan) Logger() *slog.Logger { return p.declaration.config.logger }

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

// StartsAtTail reports whether the reactor begins at the tail of the event sequence when
// first registered.
func (p *Plan) StartsAtTail() bool { return p.declaration.config.startAtTail }

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
	if !hasCompileInputs(d, catalog, models) {
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
	pc := planCompiler{plan: p, models: models, selected: map[string]bool{}, batchSelected: map[string]bool{}}
	if err := pc.validateSideEffects(); err != nil {
		return nil, err
	}
	if err := pc.compileHandlers(); err != nil {
		return nil, err
	}
	if err := pc.compileMiddlewares(middlewareFactories); err != nil {
		return nil, err
	}
	if err := p.inferSource(); err != nil {
		return nil, pc.fail("", nil, err)
	}
	return p.ForStore(""), nil
}

func hasCompileInputs(d Declaration, catalog *events.Catalog, models *readmodels.Catalog) bool {
	if d.Identifier() == "" {
		return false
	}
	return catalog != nil && models != nil
}

type planCompiler struct {
	plan          *Plan
	models        *readmodels.Catalog
	selected      map[string]bool
	batchSelected map[string]bool
}

func (pc *planCompiler) fail(method string, typ reflect.Type, err error) error {
	var detail *DeclarationError
	if errors.As(err, &detail) {
		detail.Reactor = pc.plan.Identifier()
		return detail
	}
	return &DeclarationError{Reactor: pc.plan.Identifier(), Method: method, Parameter: typ, Cause: err}
}

func (pc *planCompiler) validateSideEffects() error {
	for _, handler := range pc.plan.sideEffects {
		if nilLike(handler) {
			return invalid("nil side-effect handler")
		}
	}
	return nil
}

func (pc *planCompiler) compileHandlers() error {
	if err := pc.compileMethods(); err != nil {
		return err
	}
	if err := pc.validateEventSelectors(); err != nil {
		return err
	}
	if err := pc.validateBatchSelectors(); err != nil {
		return err
	}
	for i, explicit := range pc.plan.declaration.config.handlers {
		if err := pc.compileCallback(fmt.Sprintf("callback[%d]", i), explicit); err != nil {
			return err
		}
	}
	if err := pc.plan.checkExclusive(); err != nil {
		return pc.fail("", nil, err)
	}
	if len(pc.plan.descriptors) == 0 {
		return pc.fail("", nil, invalid("no registered event handlers"))
	}
	pc.orderEventTypes()
	return nil
}

func (pc *planCompiler) compileMethods() error {
	d := pc.plan.declaration
	if d.explicit {
		return nil
	}
	factory, err := compileConstructor(d.typ, d.factory, pc.plan.services)
	if err != nil {
		return pc.fail("constructor", d.typ, err)
	}
	pc.plan.factory = factory
	for _, method := range discovery.Methods(d.typ) {
		if err := pc.compileMethod(method); err != nil {
			return err
		}
	}
	return nil
}

func (pc *planCompiler) compileMethod(method reflect.Method) error {
	t := method.Type
	if index, ok := takesBatch(t, true); ok {
		return pc.compileBatchMethod(method, index)
	}
	first := 1
	if t.NumIn() > first && t.In(first) == contextType {
		first++
	}
	if t.NumIn() <= first {
		return nil
	}
	matching := matchingEvents(t.In(first), pc.plan.catalog)
	if len(matching) == 0 {
		return nil
	}
	handler, err := pc.compileCall(method.Name, method.Func, true)
	if err != nil {
		return pc.fail(method.Name, t.In(first), err)
	}
	pc.selected[method.Name] = true
	config := pc.plan.declaration.config
	handler.onceOnly = slices.Contains(config.onceMethods, method.Name)
	return pc.bindHandlers(handler, matching, false, slices.Contains(config.replayMethods, method.Name))
}

func (pc *planCompiler) compileBatchMethod(method reflect.Method, index int) error {
	p := pc.plan
	batch, matching, err := compileBatch(method.Name, method.Func, true, p.declaration.config.batchMethods[method.Name], p.catalog, pc.models, p.services)
	if err == nil {
		err = p.bindBatch(batch, matching)
	}
	if err != nil {
		return pc.fail(method.Name, method.Type.In(index), err)
	}
	pc.batchSelected[method.Name] = true
	return nil
}

func (pc *planCompiler) validateEventSelectors() error {
	config := pc.plan.declaration.config
	for _, name := range append(slices.Clone(config.replayMethods), config.onceMethods...) {
		if !pc.selected[name] {
			return pc.fail(name, nil, invalid("policy must select a discovered exported event handler"))
		}
	}
	return nil
}

func (pc *planCompiler) validateBatchSelectors() error {
	for _, name := range slices.Sorted(maps.Keys(pc.plan.declaration.config.batchMethods)) {
		if !pc.batchSelected[name] {
			return pc.fail(name, nil, observation.ErrObservesOnlyOnBatchMethods)
		}
	}
	return nil
}

func (pc *planCompiler) compileCallback(name string, explicit Handler) error {
	if explicit.batch {
		return pc.compileBatchCallback(name, explicit)
	}
	if nilLike(explicit.function) || explicit.event == nil {
		return pc.fail(name, nil, invalid("nil callback"))
	}
	matching := matchingEvents(explicit.event, pc.plan.catalog)
	if len(matching) == 0 {
		return pc.fail(name, explicit.event, invalid("callback event is not registered"))
	}
	handler, err := pc.compileCall(name, reflect.ValueOf(explicit.function), false)
	if err != nil {
		return pc.fail(name, explicit.event, err)
	}
	handler.onceOnly = explicit.onceOnly
	return pc.bindHandlers(handler, matching, true, explicit.replay)
}

func (pc *planCompiler) compileBatchCallback(name string, explicit Handler) error {
	if nilLike(explicit.function) {
		return pc.fail(name, nil, invalid("nil callback"))
	}
	p := pc.plan
	batch, matching, err := compileBatch(name, reflect.ValueOf(explicit.function), false, explicit.observes, p.catalog, pc.models, p.services)
	if err == nil {
		err = p.bindBatch(batch, matching)
	}
	if err != nil {
		return pc.fail(name, eventBatchPointerType, err)
	}
	return nil
}

func (pc *planCompiler) bindHandlers(handler call, matching []events.Descriptor, explicit, replay bool) error {
	handlers := pc.plan.handlers
	if replay {
		handlers = pc.plan.replayHandlers
	}
	for _, descriptor := range matching {
		if err := pc.bindEvent(handler, descriptor, handlers, explicit); err != nil {
			return err
		}
	}
	return nil
}

func (pc *planCompiler) bindEvent(handler call, descriptor events.Descriptor, handlers map[events.TypeID]call, explicit bool) error {
	p := pc.plan
	id := descriptor.Ref().ID
	if selected, exists := p.descriptors[id]; exists && selected.Ref() != descriptor.Ref() {
		return pc.fail(handler.name, handler.event, invalid("a reactor must select one generation per event ID; use separate reactors for different generations"))
	}
	if previous, exists := handlers[id]; exists {
		if explicit {
			return pc.fail(handler.name, handler.event, invalid("duplicate explicit event binding"))
		}
		p.shadows = append(p.shadows, Shadow{descriptor.Ref(), previous.name, handler.name})
		diagnostics.Log(context.Background(), p.declaration.config.logger, slog.LevelWarn, "reactor handler shadowed", "reactor", "compile", nil)
		return nil
	}
	handlers[id] = handler
	p.descriptors[id] = descriptor
	return nil
}

func (pc *planCompiler) orderEventTypes() {
	for _, descriptor := range pc.plan.catalog.Descriptors() {
		if selected, ok := pc.plan.descriptors[descriptor.Ref().ID]; ok && selected.Ref() == descriptor.Ref() {
			pc.plan.ordered = append(pc.plan.ordered, descriptor.Ref())
		}
	}
}

func (pc *planCompiler) compileMiddlewares(factories []any) error {
	for _, factory := range append(slices.Clone(factories), pc.plan.declaration.config.middlewares...) {
		if err := pc.compileMiddleware(factory); err != nil {
			return err
		}
	}
	return nil
}

func (pc *planCompiler) compileMiddleware(factory any) error {
	if nilLike(factory) {
		return pc.fail("middleware", nil, invalid("nil middleware factory"))
	}
	typ := reflect.TypeOf(factory)
	if !returnsMiddleware(typ) {
		return pc.fail("middleware", typ, invalid("factory must return Middleware"))
	}
	compiled, err := compileConstructor(typ.Out(0), factory, pc.plan.services)
	if err != nil {
		return pc.fail("middleware", typ, err)
	}
	pc.plan.middlewares = append(pc.plan.middlewares, compiled)
	return nil
}

func returnsMiddleware(typ reflect.Type) bool {
	if typ.Kind() != reflect.Func || typ.NumOut() < 1 {
		return false
	}
	return typ.Out(0).Implements(middlewareType)
}
func matchingEvents(typ reflect.Type, catalog *events.Catalog) []events.Descriptor {
	return discovery.Events(typ, catalog)
}
func (pc *planCompiler) compileCall(name string, fn reflect.Value, receiver bool) (call, error) {
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
		arg, err := pc.compileArgument(name, t.In(i))
		if err != nil {
			return c, err
		}
		c.args = append(c.args, arg)
	}
	err := pc.compileResults(&c)
	return c, err
}

func (pc *planCompiler) compileArgument(name string, typ reflect.Type) (argument, error) {
	arg := argument{typ: typ}
	if typ == batchContextType {
		return arg, observation.ErrBatchContextOnlyForBatchMethods
	}
	if typ == eventContextType || typ == deliveryType {
		return arg, nil
	}
	if model, ok := pc.models.LookupType(typ); ok {
		arg.model = model
		return arg, nil
	}
	if err := validateService(pc.plan.services, typ); err != nil {
		return arg, &DeclarationError{Method: name, Parameter: typ, Cause: err}
	}
	return arg, nil
}

func (pc *planCompiler) compileResults(c *call) error {
	t := c.fn.Type()
	n := t.NumOut()
	if n > 0 && t.Out(n-1) == errorType {
		c.returnsError = true
		n--
	}
	if !pc.supportsResults(t, n) {
		return invalid("unsupported result: expected no result, error, event/effect or claimed custom result, optionally plus error")
	}
	c.returnsEvent = n == 1
	return nil
}

func (pc *planCompiler) supportsResults(typ reflect.Type, count int) bool {
	if count > 1 {
		return false
	}
	if count == 0 {
		return true
	}
	return supportsResult(typ.Out(0), pc.plan.catalog, pc.plan.sideEffects)
}
func compileConstructor(typ reflect.Type, factory any, services ScopeFactory) (constructor, error) {
	c, err := artifacts.CompileConstructor(typ, factory, services)
	var parameter *artifacts.ParameterError
	if errors.As(err, &parameter) {
		err = &DeclarationError{Method: "constructor", Parameter: parameter.Type, Cause: parameter.Cause}
	}
	return c, err
}
