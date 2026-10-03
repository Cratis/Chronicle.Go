// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package reactors declares convention-discovered and explicit event handlers.
// Declarations are compiled per client catalog; there is no global type discovery.
package reactors

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"strings"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/diagnostics"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/readmodels"
)

// ID is a stable observer identity. Use an explicit ID when renaming Go types.
type ID string

// Declaration is immutable reactor authoring metadata. Its zero value is invalid.
type Declaration struct {
	typ      reflect.Type
	factory  any
	config   configuration
	explicit bool
}
type configuration struct {
	id               ID
	sequence         events.SequenceID
	sequenceExplicit bool
	sourceStore      string
	perEvent         bool
	replayable       bool
	tags             []string
	filterTags       []string
	sourceType       events.SourceType
	streamType       events.StreamType
	streamID         events.StreamID
	replayMethods    []string
	onceMethods      []string
	sideEffects      []SideEffectHandler
	middlewares      []any
	handlers         []Handler
	key              func(context.Context, any, events.Context) (readmodels.Key, error)
	logger           *slog.Logger
	loggerSet        bool
	invalid          bool
}

// Option configures a reactor. Scalar options are last-wins; middleware and
// handler options append in registration order. Nil options are invalid.
type Option func(*configuration)

// WithID overrides the default full Go import path plus type name.
func WithID(id ID) Option { return func(c *configuration) { c.id = id } }

// WithEventSequence selects a sequence; the default is the event log.
func WithEventSequence(id events.SequenceID) Option {
	return func(c *configuration) { c.sequence = id; c.sequenceExplicit = true }
}

// WithSourceStore overrides event-origin inference, like C# [EventStore] on an
// observer. It selects inbox-<store> even in that store. Combining it with an
// explicit sequence is invalid. For ordinary origin metadata use events.WithSourceStore.
func WithSourceStore(store string) Option {
	return func(c *configuration) { c.sourceStore = store; c.invalid = c.invalid || strings.TrimSpace(store) == "" }
}

// PerEvent opts into a fresh scope, artifact and middleware chain for each event.
// The default is one scope per received batch, matching C#.
func PerEvent() Option { return func(c *configuration) { c.perEvent = true } }

// WithMiddleware adds a constructor returning a Middleware (with optional error).
// An optional leading context.Context and service parameters are resolved at activation.
// Instances constructed here are owned by the batch; service-resolved instances
// are owned by the scope/provider, never disposed twice by Chronicle.
func WithMiddleware(factory any) Option {
	return func(c *configuration) { c.middlewares = append(c.middlewares, factory) }
}

// WithReadModelKey selects the key for all injected read models. Otherwise the
// reactor's ReadModelKeyResolver is used, falling back to the event source ID.
func WithReadModelKey(resolve func(context.Context, any, events.Context) (readmodels.Key, error)) Option {
	return func(c *configuration) { c.key = resolve; c.invalid = c.invalid || resolve == nil }
}

// WithLogger selects borrowed diagnostics for this reactor, overriding the client
// fallback. Nil is invalid; handlers follow chronicle.WithLogger's concurrency,
// ownership and redaction contract. Standalone declarations capture slog.Default.
func WithLogger(logger *slog.Logger) Option {
	return func(c *configuration) { c.logger, c.loggerSet = logger, true; c.invalid = c.invalid || logger == nil }
}

// WithHandler adds an explicit typed callback. Duplicate event bindings are errors,
// including conflicts with discovered methods, rather than implicit overrides.
func WithHandler(handler Handler) Option {
	return func(c *configuration) { c.handlers = append(c.handlers, handler) }
}

// EventSourceIDProvider overrides the source for a bare returned event, matching
// C# ICanProvideEventSourceId. It does not change delivery identity or partitioning.
type EventSourceIDProvider interface{ GetEventSourceID() events.SourceID }

// ReadModelKeyResolver supplies a custom key on the activated reactor.
type ReadModelKeyResolver interface {
	ResolveReadModelKey(context.Context, any, events.Context) (readmodels.Key, error)
}

// Define admits R for later catalog-based validation, without invoking its factory.
// R must be a named struct or pointer to one. A constructor may take an optional
// context.Context then services (or Scope), and return R or (R,error). A nil factory
// requests service activation. Catalog-advertised services take precedence over constructors.
func Define[R any](factory any, options ...Option) (Declaration, error) {
	typ := reflect.TypeFor[R]()
	base := typ
	if base.Kind() == reflect.Pointer {
		base = base.Elem()
	}
	if base.Kind() != reflect.Struct || base.Name() == "" {
		return Declaration{}, invalid("named reactor struct or pointer required")
	}
	return define(typ, factory, false, ID(base.PkgPath()+"."+base.Name()), options)
}

// DefineHandler declares a zero-container typed callback with an explicit stable ID.
// It shares signature compilation and invocation with convention-based reactors.
func DefineHandler[E any](id ID, handler func(context.Context, E) error, options ...Option) (Declaration, error) {
	return DefineHandlers(id, []Handler{On(handler)}, options...)
}

// DefineHandlers declares typed callbacks, including a returned-event-only
// reactor. The input slice is copied; duplicates are rejected during Compile.
func DefineHandlers(id ID, handlers []Handler, options ...Option) (Declaration, error) {
	initial := make([]Option, 0, len(handlers)+len(options))
	for _, handler := range handlers {
		initial = append(initial, WithHandler(handler))
	}
	return define(nil, nil, true, id, append(initial, options...))
}
func define(typ reflect.Type, factory any, explicit bool, id ID, options []Option) (Declaration, error) {
	c := configuration{id: id, sequence: events.EventLog, replayable: true, streamType: "All", logger: slog.Default()}
	for _, option := range options {
		if option == nil {
			return Declaration{}, invalid("nil reactor option")
		}
		option(&c)
	}
	if strings.TrimSpace(string(c.id)) == "" || strings.TrimSpace(string(c.sequence)) == "" || strings.TrimSpace(string(c.streamType)) == "" || (c.sourceType != "" && strings.TrimSpace(string(c.sourceType)) == "") || c.invalid || (c.sourceStore != "" && c.sequenceExplicit) {
		return Declaration{}, invalid("invalid reactor options")
	}
	for _, value := range append(append([]string(nil), c.tags...), c.filterTags...) {
		if strings.TrimSpace(value) == "" {
			return Declaration{}, invalid("blank reactor tag or filter")
		}
	}
	return Declaration{typ: typ, factory: factory, config: c, explicit: explicit}, nil
}

// WithClientDiagnostics is a module-private binding seam. It returns a detached
// declaration with the client fallback, preserving an explicit WithLogger choice.
func (d Declaration) WithClientDiagnostics(config diagnostics.Configuration) Declaration {
	if !d.config.loggerSet {
		d.config.logger = config.Logger
	}
	return d
}

// Identifier returns the persisted observer identity.
func (d Declaration) Identifier() ID { return d.config.id }

// GoType returns the registered artifact type, or nil for explicit callbacks.
func (d Declaration) GoType() reflect.Type { return d.typ }

// Handler is an immutable typed callback declaration.
type Handler struct {
	function any
	event    reflect.Type
	replay   bool
	onceOnly bool
}

// On declares an error-returning callback. Capture explicitly owned collaborators
// in its closure; Chronicle does not dispose captured resources.
func On[E any](handler func(context.Context, E) error) Handler {
	return Handler{function: handler, event: reflect.TypeFor[E]()}
}

// Returning declares a callback whose non-nil result is processed as a side effect.
// E must be registered; F must be an event, supported collection/wrapper or a
// custom handler-claimed result. An error suppresses all returned effects.
func Returning[E, F any](handler func(context.Context, E) (F, error)) Handler {
	return Handler{function: handler, event: reflect.TypeFor[E]()}
}

func invalid(message string) error {
	return fmt.Errorf("%w: %s", faults.ErrInvalidConfiguration, message)
}

// DeclarationError identifies an invalid constructor, handler or parameter.
// Cause preserves the inspectable configuration or dependency error identity.
type DeclarationError struct {
	Reactor   ID
	Method    string
	Parameter reflect.Type
	Cause     error
}

func (e *DeclarationError) Error() string {
	return fmt.Sprintf("chronicle: reactor %s method %s parameter %v: %v", e.Reactor, e.Method, e.Parameter, e.Cause)
}

// Unwrap preserves the validation cause.
func (e *DeclarationError) Unwrap() error { return e.Cause }

// ActivationError identifies a failed artifact construction or cleanup.
type ActivationError struct {
	Reactor   ID
	Operation string
	Cause     error
}

func (e *ActivationError) Error() string {
	return fmt.Sprintf("chronicle: reactor %s %s: %v", e.Reactor, e.Operation, e.Cause)
}

// Unwrap preserves factory, scope and cleanup error identities.
func (e *ActivationError) Unwrap() error { return e.Cause }
