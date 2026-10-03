// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reactors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/artifacts"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/readmodels"
)

// ReadModelOption configures a best-effort read-model reactor. Scalar options are
// last-wins; callbacks append in registration order. No durable observer is registered.
type ReadModelOption func(*readModelConfig)
type readModelConfig struct {
	id           ID
	materialized bool
	window       *readmodels.Window
	handlers     []ReadModelHandler
	onError      func(context.Context, error)
	watch        []readmodels.WatchOption
}

// WithReadModelReactorID sets a stable local reactor identity (default: full Go type name).
func WithReadModelReactorID(id ID) ReadModelOption { return func(c *readModelConfig) { c.id = id } }

// Materialized selects snapshot-window observation instead of projection changes,
// matching C# [Materialized]. Nil selects the default 0/50 window. The argument is
// copied; leaving this window means Removed, not necessarily document deletion.
func Materialized(window *readmodels.Window) ReadModelOption {
	var copy *readmodels.Window
	if window != nil {
		value := *window
		copy = &value
	}
	return func(c *readModelConfig) { c.materialized, c.window = true, copy }
}

// WithReadModelErrorHandler reports activation, dispatch, cleanup, effect and
// terminal watch failures. It must honor cancellation and must not block on client
// shutdown from inside itself. Calls are serial per reactor, outside internal locks.
// Nil is invalid. Without this option errors are logged through slog.Default.
func WithReadModelErrorHandler(handler func(context.Context, error)) ReadModelOption {
	return func(c *readModelConfig) { c.onError = handler }
}

// WithReadModelWatchOptions configures the bounded watch feeding this reactor.
// The slice is copied. Saturation terminates the subscription, never drops changes.
func WithReadModelWatchOptions(options ...readmodels.WatchOption) ReadModelOption {
	copy := slices.Clone(options)
	return func(c *readModelConfig) { c.watch = append(c.watch, copy...) }
}

// ReadModelHandler is an explicit callback admitted with ReadModelOn. Its zero
// value is invalid. Multiple callbacks for the same change each own a fresh scope.
type ReadModelHandler struct {
	kind readmodels.ChangeType
	fn   any
}

// ReadModelOn admits a callback for later signature validation at NewClient.
// Like methods, callbacks take optional context.Context then M, *M, []M or []*M,
// then events.Context and/or resolved services. Results may be empty, error,
// a supported event effect, or (effect,error).
func ReadModelOn(kind readmodels.ChangeType, callback any) ReadModelHandler {
	return ReadModelHandler{kind, callback}
}

// WithReadModelHandler appends a callback in addition to discovered methods.
func WithReadModelHandler(handler ReadModelHandler) ReadModelOption {
	return func(c *readModelConfig) { c.handlers = append(c.handlers, handler) }
}

// ReadModelDeclaration is immutable authoring metadata frozen by NewClient.
type ReadModelDeclaration struct {
	typ     reflect.Type
	factory any
	model   readmodels.Descriptor
	config  readModelConfig
}

// Identifier returns the local registration identity.
func (d ReadModelDeclaration) Identifier() ID { return d.config.id }

// DefineReadModel admits a named reactor type and a registered model. Exact
// Added/Modified/Removed method names are discovered at CompileReadModel; no
// constructor, dependency resolution, goroutine or I/O occurs during registration.
func DefineReadModel[R, M any](model readmodels.Model[M], factory any, options ...ReadModelOption) (ReadModelDeclaration, error) {
	typ := reflect.TypeFor[R]()
	base := typ
	if base.Kind() == reflect.Pointer {
		base = base.Elem()
	}
	if base.Kind() != reflect.Struct || base.Name() == "" {
		return ReadModelDeclaration{}, invalid("named read-model reactor required")
	}
	return defineReadModel(typ, model.Descriptor(), factory, ID(base.PkgPath()+"."+base.Name()), options)
}

// DefineReadModelHandlers admits an explicit callback-only reactor with no artifact.
func DefineReadModelHandlers[M any](id ID, model readmodels.Model[M], handlers []ReadModelHandler, options ...ReadModelOption) (ReadModelDeclaration, error) {
	initial := make([]ReadModelOption, 0, len(handlers)+len(options))
	for _, handler := range handlers {
		initial = append(initial, WithReadModelHandler(handler))
	}
	return defineReadModel(nil, model.Descriptor(), nil, id, append(initial, options...))
}
func defineReadModel(typ reflect.Type, model readmodels.Descriptor, factory any, id ID, options []ReadModelOption) (ReadModelDeclaration, error) {
	config := readModelConfig{id: id, onError: func(ctx context.Context, err error) {
		slog.ErrorContext(ctx, "read-model reactor failed (not retried)", "error", err)
	}}
	for _, option := range options {
		if option == nil {
			return ReadModelDeclaration{}, invalid("nil read-model reactor option")
		}
		option(&config)
	}
	if model.GoType() == nil || strings.TrimSpace(string(config.id)) == "" || config.onError == nil {
		return ReadModelDeclaration{}, invalid("model, reactor ID and error handler required")
	}
	return ReadModelDeclaration{typ, factory, model, config}, nil
}

type modelCall struct {
	call
	kind readmodels.ChangeType
}

// ReadModelPlan is a frozen, catalog-isolated convention/callback dispatch plan.
// Reconnect reuses it without rediscovery. Runtime adapters normally use the root
// registration API rather than compiling or dispatching plans themselves.
type ReadModelPlan struct {
	declaration ReadModelDeclaration
	model       readmodels.Descriptor
	factory     artifacts.Constructor
	services    ScopeFactory
	calls       []modelCall
	effects     *Plan
}

// Identifier returns the local reactor identity.
func (p *ReadModelPlan) Identifier() ID { return p.declaration.Identifier() }

// Model returns the frozen model declaration.
func (p *ReadModelPlan) Model() readmodels.Descriptor { return p.model }

// IsMaterialized reports whether to use the snapshot-window RPC.
func (p *ReadModelPlan) IsMaterialized() bool { return p.declaration.config.materialized }

// Window returns a copied selection, or nil for 0/50.
func (p *ReadModelPlan) Window() *readmodels.Window {
	if p.declaration.config.window == nil {
		return nil
	}
	value := *p.declaration.config.window
	return &value
}

// WatchOptions returns a detached option list.
func (p *ReadModelPlan) WatchOptions() []readmodels.WatchOption {
	return slices.Clone(p.declaration.config.watch)
}

// CompileReadModel validates methods, callbacks, factory, dependencies and effects
// without invoking them. It reuses event reactors' scope and return-value contracts.
func CompileReadModel(d ReadModelDeclaration, catalog *events.Catalog, models *readmodels.Catalog, services ScopeFactory, effects []SideEffectHandler) (*ReadModelPlan, error) {
	if models == nil || catalog == nil || d.model.GoType() == nil || d.Identifier() == "" {
		return nil, invalid("read-model reactor and catalogs required")
	}
	model, ok := models.LookupIdentifier(d.model.Identifier())
	if !ok || model.GoType() != d.model.GoType() {
		return nil, invalid("read-model reactor requires a model from this registry")
	}
	if services == nil {
		services = DefaultScopeFactory()
	}
	if err := readmodels.ValidateWatchOptions(d.config.watch...); err != nil {
		return nil, err
	}
	p := &ReadModelPlan{declaration: d, model: model, services: services}
	var err error
	if d.typ != nil {
		p.factory, err = artifacts.CompileConstructor(d.typ, d.factory, services)
		if err != nil {
			return nil, err
		}
	}
	for _, extension := range effects {
		if nilLike(extension) {
			return nil, invalid("nil side-effect handler")
		}
	}
	p.effects = &Plan{catalog: catalog, sideEffects: slices.Clone(effects), declaration: Declaration{config: configuration{id: d.config.id, sequence: model.EventSequence(), streamType: events.AllStreamTypes, logger: slog.Default()}}}
	if d.typ != nil {
		for _, entry := range []struct {
			name string
			kind readmodels.ChangeType
		}{{"Added", readmodels.Added}, {"Modified", readmodels.Modified}, {"Removed", readmodels.Removed}} {
			if method, ok := d.typ.MethodByName(entry.name); ok {
				if err := p.addCall(entry.name, method.Func, true, entry.kind); err != nil {
					return nil, err
				}
			}
		}
	}
	for _, handler := range d.config.handlers {
		if nilLike(handler.fn) {
			return nil, invalid("nil read-model callback")
		}
		if err := p.addCall("callback", reflect.ValueOf(handler.fn), false, handler.kind); err != nil {
			return nil, err
		}
	}
	if len(p.calls) == 0 {
		return nil, invalid("no Added/Modified/Removed handlers")
	}
	kind, observer := model.Observer()
	if !d.config.materialized && ((kind != readmodels.Projection && kind != readmodels.Reducer) || observer == "") {
		return nil, invalid("change reactor requires a projection or reducer")
	}
	if d.config.materialized && model.Sink().Type == readmodels.NoSink {
		return nil, invalid("passive model has no materialized window")
	}
	return p, nil
}

func (p *ReadModelPlan) addCall(name string, fn reflect.Value, receiver bool, kind readmodels.ChangeType) error {
	t := fn.Type()
	if t.Kind() != reflect.Func || t.IsVariadic() || kind > readmodels.Removed {
		return invalid("invalid read-model handler")
	}
	c := call{name: name, fn: fn, receiver: receiver}
	index := 0
	if receiver {
		index++
	}
	if index < t.NumIn() && t.In(index) == contextType {
		c.context = true
		index++
	}
	if index >= t.NumIn() {
		return invalid("read-model handler requires a model parameter")
	}
	c.event = t.In(index)
	base := c.event
	if base.Kind() == reflect.Slice {
		base = base.Elem()
	}
	if base.Kind() == reflect.Pointer {
		base = base.Elem()
	}
	if base != p.model.GoType() {
		return invalid("handler first parameter must be the registered model or collection")
	}
	for index++; index < t.NumIn(); index++ {
		typ := t.In(index)
		if typ != eventContextType {
			if err := artifacts.ValidateService(p.services, typ); err != nil {
				return err
			}
		}
		c.args = append(c.args, argument{typ: typ})
	}
	outputs := t.NumOut()
	if outputs > 0 && t.Out(outputs-1) == errorType {
		c.returnsError = true
		outputs--
	}
	if outputs > 1 || (outputs == 1 && !supportsResult(t.Out(0), p.effects.catalog, p.effects.sideEffects)) {
		return invalid("unsupported read-model reactor result")
	}
	c.returnsEvent = outputs == 1
	p.calls = append(p.calls, modelCall{c, kind})
	return nil
}

// Report delivers a best-effort failure; reporter panics are logged rather than
// escaping an owned worker. No failed callback or ambiguous effect is retried.
func (p *ReadModelPlan) Report(ctx context.Context, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.ErrorContext(ctx, "read-model error reporter panicked")
		}
	}()
	p.declaration.config.onError(ctx, fmt.Errorf("read-model reactor %q: %w", p.Identifier(), err))
}

// Dispatch invokes all matching callbacks serially, with one scope/activation per
// callback (C# ReadModelReactors.Dispatch), then effects and cleanup. Failures are
// reported and joined, never retried. Callbacks must honor cancellation. Missing
// values become nil pointers or empty collections; nonnullable values fail closed.
func (p *ReadModelPlan) Dispatch(ctx context.Context, change readmodels.Change[json.RawMessage], runtime Runtime) error {
	var failures error
	for _, handler := range p.calls {
		if handler.kind != change.Type {
			continue
		}
		if err := ctx.Err(); err != nil {
			return errors.Join(failures, err)
		}
		err := p.dispatch(ctx, handler, change, runtime)
		if err != nil {
			p.Report(ctx, err)
			failures = errors.Join(failures, err)
		}
	}
	return failures
}
func (p *ReadModelPlan) dispatch(ctx context.Context, handler modelCall, change readmodels.Change[json.RawMessage], runtime Runtime) (err error) {
	ctx = metadata.WithCorrelation(ctx, change.Context.CorrelationID)
	var lease *artifacts.Lease
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("read-model dispatch panic: %v", recovered)
		}
		if lease != nil {
			err = errors.Join(err, lease.Close(ctx))
		}
	}()
	lease, err = artifacts.Open(ctx, p.services)
	if err != nil {
		return err
	}
	var instance any
	if p.declaration.typ != nil {
		instance, err = lease.Construct(ctx, p.factory)
		if err != nil {
			return err
		}
	}
	args := []reflect.Value{}
	if handler.receiver {
		args = append(args, reflect.ValueOf(instance))
	}
	if handler.context {
		args = append(args, reflect.ValueOf(ctx))
	}
	value, err := p.modelArgument(handler.event, change)
	if err != nil {
		return err
	}
	args = append(args, value)
	for _, arg := range handler.args {
		var value any
		if arg.typ == eventContextType {
			value = change.Context
		} else {
			value, err = artifacts.Resolve(ctx, lease.Scope, arg.typ)
		}
		if err != nil {
			return err
		}
		args = append(args, reflect.ValueOf(value))
	}
	result := handler.fn.Call(args)
	if handler.returnsError && !result[len(result)-1].IsNil() {
		return result[len(result)-1].Interface().(error)
	}
	if handler.returnsEvent {
		l := &Lease{plan: p.effects, scope: lease.Scope, instance: instance, resources: lease}
		invocation := Invocation{Context: change.Context, Event: value.Interface(), Scope: lease.Scope}
		invocation.Delivery = Delivery{p.Identifier(), change.Context.Store, change.Context.Namespace, p.model.EventSequence(), change.Context.SourceID, change.Context.SequenceNumber}
		return l.handleEffect(ctx, result[0].Interface(), invocation, runtime, false)
	}
	return nil
}
func (p *ReadModelPlan) modelArgument(typ reflect.Type, change readmodels.Change[json.RawMessage]) (reflect.Value, error) {
	collection := typ.Kind() == reflect.Slice
	itemType := typ
	if collection {
		itemType = typ.Elem()
	}
	if !change.HasValue {
		if collection {
			return reflect.MakeSlice(typ, 0, 0), nil
		}
		if typ.Kind() == reflect.Pointer {
			return reflect.Zero(typ), nil
		}
		return reflect.Value{}, invalid("absent model requires pointer or collection handler")
	}
	decoded, err := p.model.Unmarshal(change.Value)
	if err != nil {
		return reflect.Value{}, err
	}
	value := reflect.ValueOf(decoded)
	if itemType.Kind() != reflect.Pointer {
		value = value.Elem()
	}
	if collection {
		result := reflect.MakeSlice(typ, 1, 1)
		result.Index(0).Set(value)
		return result, nil
	}
	return value, nil
}
