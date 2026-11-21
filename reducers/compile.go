// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reducers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

type fold struct {
	name                                                        string
	function                                                    reflect.Value
	receiver, context, eventContext, valueCurrent, returnsError bool
	event                                                       reflect.Type
}

// Plan is immutable and safe to share across stores. Compile once per catalog;
// registration/reconnect never executes constructors or repeats discovery.
type Plan struct {
	declaration Declaration
	sourceStore string
	model       readmodels.Descriptor
	factory     artifacts.Constructor
	services    ScopeFactory
	folds       map[events.TypeID]fold
	descriptors map[events.TypeID]events.Descriptor
	ordered     []events.TypeRef
	shadows     []Shadow
	hash        string
	replay      map[ReplayState]bool
	batch       *batchFold
}

// Logger returns the frozen borrowed diagnostic logger. Chronicle never closes it.
func (p *Plan) Logger() *slog.Logger { return p.declaration.config.logger }

// Shadow describes a handler hidden by richest-signature then ordinal precedence.
type Shadow struct {
	Event          events.TypeRef
	Winner, Hidden string
}

// Shadows returns detached discovery diagnostics.
func (p *Plan) Shadows() []Shadow { return slices.Clone(p.shadows) }

// Identifier returns the persisted observer identity.
func (p *Plan) Identifier() ID { return p.declaration.Identifier() }

// Model returns the bound, naming-policy-aware model descriptor.
func (p *Plan) Model() readmodels.Descriptor { return p.model }

// EventSequence returns the source sequence.
func (p *Plan) EventSequence() events.SequenceID { return p.declaration.config.sequence }

// EventTypes returns subscriptions in catalog order.
func (p *Plan) EventTypes() []events.TypeRef { return slices.Clone(p.ordered) }

// IsActive indicates kernel materialization; passive models always return false.
func (p *Plan) IsActive() bool { return p.declaration.config.active && !p.IsPassive() }

// IsPassive indicates local on-demand folding without a sink.
func (p *Plan) IsPassive() bool { return p.model.Sink().Type == readmodels.NoSink }

// Fingerprint returns SHA-256 of the canonical plan shape and explicit version.
// Unlike C# IL hashing, changes inside function bodies require WithVersion.
func (p *Plan) Fingerprint() string { return p.hash }

// Tags returns detached artifact labels.
func (p *Plan) Tags() []string { return slices.Clone(p.declaration.config.tags) }

// FilterTags returns detached any-match appended tag filters.
func (p *Plan) FilterTags() []string { return slices.Clone(p.declaration.config.filterTags) }

// EventSourceType returns the appended source filter, empty for all.
func (p *Plan) EventSourceType() events.SourceType { return p.declaration.config.sourceType }

// EventStreamType returns the appended stream filter, All by default.
func (p *Plan) EventStreamType() events.StreamType { return p.declaration.config.streamType }

// Descriptor resolves the expected event generation and serialization metadata.
func (p *Plan) Descriptor(id events.TypeID) (events.Descriptor, bool) {
	d, ok := p.descriptors[id]
	return d, ok
}

// Compile validates all fold signatures and bindings before any activation. Only
// explicitly registered event types or their nonempty interface families qualify.
// Model must belong to the supplied catalog; Chronicle additionally checks handles.
func Compile(d Declaration, catalog *events.Catalog, models *readmodels.Catalog, services ScopeFactory) (*Plan, error) {
	p, err := preparePlan(d, catalog, models, services)
	if err != nil {
		return nil, err
	}
	if err := p.compileReplay(catalog); err != nil {
		return nil, err
	}
	if err := p.compileBindings(catalog); err != nil {
		return nil, err
	}
	return p.finishCompilation(catalog)
}

func validCompilationInput(d Declaration, catalog *events.Catalog, models *readmodels.Catalog) bool {
	return d.Identifier() != "" && catalog != nil && models != nil && d.Model().GoType() != nil
}

func preparePlan(d Declaration, catalog *events.Catalog, models *readmodels.Catalog, services ScopeFactory) (*Plan, error) {
	fail := func(method string, typ reflect.Type, err error) (*Plan, error) {
		return nil, &DeclarationError{d.Identifier(), method, typ, err}
	}
	if !validCompilationInput(d, catalog, models) {
		return fail("", nil, invalid("declaration and catalogs required"))
	}
	if services == nil {
		services = artifacts.DefaultScopeFactory()
	}
	if artifacts.NilLike(services) {
		return fail("constructor", nil, invalid("nil scope factory"))
	}
	model, ok := models.LookupIdentifier(d.model.Identifier())
	if !ok || model.GoType() != d.model.GoType() {
		return fail("", nil, invalid("model is not registered"))
	}
	return &Plan{declaration: d, model: model, services: services, folds: map[events.TypeID]fold{}, descriptors: map[events.TypeID]events.Descriptor{}}, nil
}

func (p *Plan) compileBindings(catalog *events.Catalog) error {
	if !p.declaration.explicit {
		if err := p.compileMethods(catalog); err != nil {
			return err
		}
	}
	if err := p.validateBatchMethods(); err != nil {
		return err
	}
	if err := p.compileCallbacks(catalog); err != nil {
		return err
	}
	return p.validateBindings()
}

func (p *Plan) validateBatchMethods() error {
	for _, name := range slices.Sorted(maps.Keys(p.declaration.config.batchMethods)) {
		if p.batch == nil || p.batch.name != name {
			return p.declarationError(name, nil, observation.ErrObservesOnlyOnBatchMethods)
		}
	}
	return nil
}

func (p *Plan) compileCallbacks(catalog *events.Catalog) error {
	for i, h := range p.declaration.config.handlers {
		if err := p.compileCallback(fmt.Sprintf("callback[%d]", i), h, catalog); err != nil {
			return err
		}
	}
	return nil
}

func (p *Plan) validateBindings() error {
	if p.batch != nil && len(p.folds) > 0 {
		return p.declarationError(p.batch.name, nil, observation.ErrReducerMixesBatchAndPerEventMethods)
	}
	if len(p.folds) == 0 && p.batch == nil {
		return p.declarationError("", nil, invalid("no registered folds"))
	}
	return nil
}

func (p *Plan) finishCompilation(catalog *events.Catalog) (*Plan, error) {
	for _, descriptor := range catalog.Descriptors() {
		if selected, ok := p.descriptors[descriptor.Ref().ID]; ok && selected.Ref() == descriptor.Ref() {
			p.ordered = append(p.ordered, descriptor.Ref())
		}
	}
	if err := p.validateGenerations(catalog); err != nil {
		return nil, err
	}
	if err := p.inferSource(); err != nil {
		return nil, p.declarationError("", nil, err)
	}
	bound, err := p.ForStore("")
	if err != nil {
		return nil, p.declarationError("", nil, err)
	}
	return bound, nil
}
func (p *Plan) declarationError(method string, typ reflect.Type, err error) error {
	return &DeclarationError{p.Identifier(), method, typ, err}
}

func (p *Plan) compileConstructor() error {
	d := p.declaration
	factory, err := artifacts.CompileConstructor(d.typ, d.factory, p.services)
	p.factory = factory
	if err != nil {
		typ := d.typ
		var parameter *artifacts.ParameterError
		if errors.As(err, &parameter) {
			typ = parameter.Type
		}
		return p.declarationError("constructor", typ, err)
	}
	return nil
}

func (p *Plan) compileMethods(catalog *events.Catalog) error {
	if err := p.compileConstructor(); err != nil {
		return err
	}
	for _, method := range discovery.Methods(p.declaration.typ) {
		if p.replay[ReplayState(slices.Index(replayNames, method.Name)+1)] {
			continue
		}
		if err := p.compileMethod(method, catalog); err != nil {
			return err
		}
	}
	return nil
}

func (p *Plan) compileMethod(method reflect.Method, catalog *events.Catalog) error {
	t := method.Type
	if index, ok := takesBatch(t, true); ok {
		return p.compileBatchMethod(method, index, catalog)
	}
	first := methodEventParameter(t)
	if t.NumIn() <= first {
		return nil
	}
	matching := discovery.Events(t.In(first), catalog)
	if len(matching) == 0 {
		// A method accepting this model in the current-state slot is clearly a
		// fold, not an unrelated helper: fail unknown events rather than dropping it.
		if t.NumIn() > first+1 && modelParameter(t.In(first+1), p.model.GoType()) {
			return p.declarationError(method.Name, t.In(first), invalid("fold event is not registered"))
		}
		return nil
	}
	f, err := compileFold(method.Name, method.Func, true, p.model.GoType())
	if err != nil {
		return p.declarationError(method.Name, t.In(first), err)
	}
	return p.bindDiscoveredFold(f, matching)
}

func methodEventParameter(t reflect.Type) int {
	if t.NumIn() > 1 && t.In(1) == contextType {
		return 2
	}
	return 1
}

func (p *Plan) compileBatchMethod(method reflect.Method, index int, catalog *events.Catalog) error {
	f, err := compileBatchFold(method.Name, method.Func, true, p.model.GoType())
	observes, named := p.declaration.config.batchMethods[method.Name]
	if err == nil && !named {
		err = observation.ErrBatchMethodObservesNothing
	}
	if err == nil {
		err = p.bindBatch(f, observes, catalog)
	}
	if err != nil {
		return p.declarationError(method.Name, method.Type.In(index), err)
	}
	return nil
}

func (p *Plan) bindDiscoveredFold(f fold, matching []events.Descriptor) error {
	for _, descriptor := range matching {
		id := descriptor.Ref().ID
		if selected, exists := p.descriptors[id]; exists && selected.Ref() != descriptor.Ref() {
			return p.declarationError(f.name, f.event, invalid("a reducer must select one generation per event ID; use separate reducers for different generations"))
		}
		if previous, exists := p.folds[id]; exists {
			p.shadows = append(p.shadows, Shadow{descriptor.Ref(), previous.name, f.name})
			diagnostics.Log(context.Background(), p.declaration.config.logger, slog.LevelWarn, "reducer fold shadowed", "reducer", "compile", nil)
			continue
		}
		p.folds[id], p.descriptors[id] = f, descriptor
	}
	return nil
}

func (p *Plan) compileCallback(name string, h Handler, catalog *events.Catalog) error {
	if h.batch {
		return p.compileBatchCallback(name, h, catalog)
	}
	if !h.validFoldModel(p.model.GoType()) {
		return p.declarationError(name, h.model, invalid("callback model mismatch or nil callback"))
	}
	matching := discovery.Events(h.event, catalog)
	if len(matching) == 0 {
		return p.declarationError(name, h.event, invalid("callback event is not registered"))
	}
	f, err := compileFold(name, reflect.ValueOf(h.function), false, p.model.GoType())
	if err != nil {
		return p.declarationError(name, h.event, err)
	}
	for _, descriptor := range matching {
		id := descriptor.Ref().ID
		if _, exists := p.folds[id]; exists {
			return p.declarationError(name, h.event, invalid("duplicate explicit fold binding"))
		}
		p.folds[id], p.descriptors[id] = f, descriptor
	}
	return nil
}

func (h Handler) validFoldModel(model reflect.Type) bool {
	return !artifacts.NilLike(h.function) && h.model == model && h.event != nil
}

func (p *Plan) compileBatchCallback(name string, h Handler, catalog *events.Catalog) error {
	if artifacts.NilLike(h.function) || h.model != p.model.GoType() {
		return p.declarationError(name, h.model, invalid("callback model mismatch or nil callback"))
	}
	f, err := compileBatchFold(name, reflect.ValueOf(h.function), false, p.model.GoType())
	if err == nil {
		err = p.bindBatch(f, h.observes, catalog)
	}
	if err != nil {
		return p.declarationError(name, eventBatchPointerType, err)
	}
	return nil
}

func modelParameter(typ, model reflect.Type) bool {
	return typ == model || typ == reflect.PointerTo(model)
}

func compileFold(name string, function reflect.Value, receiver bool, model reflect.Type) (fold, error) {
	f := fold{name: name, function: function, receiver: receiver}
	t := function.Type()
	if t.IsVariadic() {
		return f, invalid("variadic fold")
	}
	if err := f.compileInputs(model); err != nil {
		return f, err
	}
	if !foldReturnsModel(t, model) {
		return f, invalid("fold must return model or *model, optionally followed by error")
	}
	f.returnsError = t.NumOut() == 2
	return f, nil
}

func (f *fold) firstEventInput() int {
	t := f.function.Type()
	first := 0
	if f.receiver {
		first++
	}
	if t.NumIn() > first && t.In(first) == contextType {
		f.context = true
		first++
	}
	return first
}

func (f *fold) compileInputs(model reflect.Type) error {
	t := f.function.Type()
	first := f.firstEventInput()
	n := t.NumIn() - first
	if n < 2 || n > 3 {
		return invalid("fold requires event, current model, optional event context; services belong in constructors")
	}
	f.event = t.In(first)
	current := t.In(first + 1)
	if !modelParameter(current, model) {
		return invalid("current must be the associated model or pointer")
	}
	f.valueCurrent = current == model
	if n == 3 {
		if t.In(first+2) == batchContextType {
			return observation.ErrBatchContextOnlyForBatchMethods
		}
		if t.In(first+2) != eventContextType {
			return invalid("third fold argument must be events.Context")
		}
		f.eventContext = true
	}
	return nil
}

func foldReturnsModel(t, model reflect.Type) bool {
	if t.NumOut() < 1 || t.NumOut() > 2 {
		return false
	}
	if !modelParameter(t.Out(0), model) {
		return false
	}
	return t.NumOut() != 2 || t.Out(1) == errorType
}

func (p *Plan) fingerprint() string {
	// Names, receiver and registration order are authoring details. Sort by stable
	// event ID so equivalent explicit and reflective plans have the same hash.
	ids := make([]string, 0, len(p.folds))
	for id := range p.folds {
		ids = append(ids, string(id))
	}
	slices.Sort(ids)
	shape := []any{"chronicle-go-fold-v1", p.model.Identifier(), p.declaration.config.version, p.EventSequence(), p.IsActive(), p.IsPassive(), p.Tags(), p.FilterTags(), p.EventSourceType(), p.EventStreamType()}
	for _, id := range ids {
		f := p.folds[events.TypeID(id)]
		d := p.descriptors[events.TypeID(id)]
		shape = append(shape, []any{id, d.Ref().Generation, d.GoType().PkgPath() + "." + d.GoType().Name(), f.event.PkgPath(), f.event.String(), f.context, f.eventContext, f.valueCurrent, f.returnsError, f.function.Type().Out(0).Kind() == reflect.Pointer})
	}
	shape = append(shape, p.batchFingerprintShape()...)
	for i, callback := range p.declaration.config.replay.values() {
		if p.replay[ReplayState(i+1)] || !reflect.ValueOf(callback).IsNil() {
			shape = append(shape, replayNames[i])
		}
	}
	encoded, _ := json.Marshal(shape) // Only closed scalar/slice metadata above.
	hash := sha256.Sum256(encoded)
	return hex.EncodeToString(hash[:])
}

func (p *Plan) batchFingerprintShape() []any {
	b := p.batch
	if b == nil {
		return nil
	}
	batchIDs := make([]string, 0, len(p.descriptors))
	for id := range p.descriptors {
		batchIDs = append(batchIDs, string(id))
	}
	slices.Sort(batchIDs)
	var shape []any
	for _, id := range batchIDs {
		d := p.descriptors[events.TypeID(id)]
		shape = append(shape, []any{"batch", id, d.Ref().Generation, d.GoType().PkgPath() + "." + d.GoType().Name()})
	}
	return append(shape, []any{"batch-signature", b.context, b.batchContext, b.valueCurrent, b.returnsError, b.function.Type().Out(0).Kind() == reflect.Pointer})
}
