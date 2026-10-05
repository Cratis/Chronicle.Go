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
	"reflect"
	"slices"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/artifacts"
	"github.com/cratis/chronicle.go/internal/diagnostics"
	"github.com/cratis/chronicle.go/internal/discovery"
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
	fail := func(method string, typ reflect.Type, err error) (*Plan, error) {
		return nil, &DeclarationError{d.Identifier(), method, typ, err}
	}
	if d.Identifier() == "" || catalog == nil || models == nil || d.Model().GoType() == nil {
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
	var err error
	p := &Plan{declaration: d, model: model, services: services, folds: map[events.TypeID]fold{}, descriptors: map[events.TypeID]events.Descriptor{}}
	if err := p.compileReplay(catalog); err != nil {
		return nil, err
	}
	if !d.explicit {
		p.factory, err = artifacts.CompileConstructor(d.typ, d.factory, services)
		if err != nil {
			typ := d.typ
			var parameter *artifacts.ParameterError
			if errors.As(err, &parameter) {
				typ = parameter.Type
			}
			return fail("constructor", typ, err)
		}
		for _, method := range discovery.Methods(d.typ) {
			if p.replay[ReplayState(slices.Index(replayNames, method.Name)+1)] {
				continue
			}
			t := method.Type
			first := 1
			if t.NumIn() > first && t.In(first) == contextType {
				first++
			}
			if t.NumIn() <= first {
				continue
			}
			matching := discovery.Events(t.In(first), catalog)
			if len(matching) == 0 {
				// A method accepting this model in the current-state slot is clearly a
				// fold, not an unrelated helper: fail unknown events rather than dropping it.
				if t.NumIn() > first+1 && (t.In(first+1) == model.GoType() || t.In(first+1) == reflect.PointerTo(model.GoType())) {
					return fail(method.Name, t.In(first), invalid("fold event is not registered"))
				}
				continue
			}
			f, err := compileFold(method.Name, method.Func, true, model.GoType())
			if err != nil {
				return fail(method.Name, t.In(first), err)
			}
			for _, descriptor := range matching {
				id := descriptor.Ref().ID
				if selected, exists := p.descriptors[id]; exists && selected.Ref() != descriptor.Ref() {
					return fail(method.Name, t.In(first), invalid("a reducer must select one generation per event ID; use separate reducers for different generations"))
				}
				if previous, exists := p.folds[id]; exists {
					p.shadows = append(p.shadows, Shadow{descriptor.Ref(), previous.name, method.Name})
					diagnostics.Log(context.Background(), d.config.logger, slog.LevelWarn, "reducer fold shadowed", "reducer", "compile", nil)
					continue
				}
				p.folds[id], p.descriptors[id] = f, descriptor
			}
		}
	}
	for i, h := range d.config.handlers {
		name := fmt.Sprintf("callback[%d]", i)
		if artifacts.NilLike(h.function) || h.model != model.GoType() || h.event == nil {
			return fail(name, h.model, invalid("callback model mismatch or nil callback"))
		}
		matching := discovery.Events(h.event, catalog)
		if len(matching) == 0 {
			return fail(name, h.event, invalid("callback event is not registered"))
		}
		f, err := compileFold(name, reflect.ValueOf(h.function), false, model.GoType())
		if err != nil {
			return fail(name, h.event, err)
		}
		for _, descriptor := range matching {
			id := descriptor.Ref().ID
			if _, exists := p.folds[id]; exists {
				return fail(name, h.event, invalid("duplicate explicit fold binding"))
			}
			p.folds[id], p.descriptors[id] = f, descriptor
		}
	}
	if len(p.folds) == 0 {
		return fail("", nil, invalid("no registered folds"))
	}
	for _, descriptor := range catalog.Descriptors() {
		if selected, ok := p.descriptors[descriptor.Ref().ID]; ok && selected.Ref() == descriptor.Ref() {
			p.ordered = append(p.ordered, descriptor.Ref())
		}
	}
	if err := p.inferSource(); err != nil {
		return fail("", nil, err)
	}
	bound, err := p.ForStore("")
	if err != nil {
		return fail("", nil, err)
	}
	return bound, nil
}
func compileFold(name string, function reflect.Value, receiver bool, model reflect.Type) (fold, error) {
	f := fold{name: name, function: function, receiver: receiver}
	t := function.Type()
	first := 0
	if receiver {
		first++
	}
	if t.IsVariadic() {
		return f, invalid("variadic fold")
	}
	if t.NumIn() > first && t.In(first) == contextType {
		f.context = true
		first++
	}
	n := t.NumIn() - first
	if n < 2 || n > 3 {
		return f, invalid("fold requires event, current model, optional event context; services belong in constructors")
	}
	f.event = t.In(first)
	current := t.In(first + 1)
	if current != reflect.PointerTo(model) && current != model {
		return f, invalid("current must be the associated model or pointer")
	}
	f.valueCurrent = current == model
	if n == 3 {
		if t.In(first+2) != eventContextType {
			return f, invalid("third fold argument must be events.Context")
		}
		f.eventContext = true
	}
	if t.NumOut() < 1 || t.NumOut() > 2 || (t.Out(0) != model && t.Out(0) != reflect.PointerTo(model)) || (t.NumOut() == 2 && t.Out(1) != errorType) {
		return f, invalid("fold must return model or *model, optionally followed by error")
	}
	f.returnsError = t.NumOut() == 2
	return f, nil
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
	for i, callback := range p.declaration.config.replay.values() {
		if p.replay[ReplayState(i+1)] || !reflect.ValueOf(callback).IsNil() {
			shape = append(shape, replayNames[i])
		}
	}
	encoded, _ := json.Marshal(shape) // Only closed scalar/slice metadata above.
	hash := sha256.Sum256(encoded)
	return hex.EncodeToString(hash[:])
}
