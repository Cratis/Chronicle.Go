// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package reducers declares sequential read-model folds. Use a reducer when a
// state transition cannot be expressed by a projection. Explicit callbacks and
// exported-method discovery compile to the same catalog-isolated fold plan.
package reducers

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/artifacts"
	"github.com/cratis/chronicle.go/internal/diagnostics"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/readmodels"
)

// ID is a stable observer identity. Explicit IDs survive Go type renames.
type ID string

// Scope is the same zero-container operation resolver used by reactors.
type Scope = artifacts.Scope

// ScopeFactory is borrowed; Chronicle never closes the provider itself.
type ScopeFactory = artifacts.ScopeFactory

// Declaration is immutable authoring metadata. Its zero value is invalid.
type Declaration struct {
	typ      reflect.Type
	model    readmodels.Descriptor
	factory  any
	config   configuration
	explicit bool
}
type configuration struct {
	id               ID
	sequence         events.SequenceID
	sequenceExplicit bool
	sourceStore      string
	version          string
	active           bool
	passive          bool
	tags, filterTags []string
	sourceType       events.SourceType
	streamType       events.StreamType
	handlers         []Handler
	replay           ReplayCallbacks
	logger           *slog.Logger
	loggerSet        bool
	invalid          bool
}

// Option configures a reducer. Scalars are last-wins; handlers accumulate and
// duplicate bindings fail at NewClient. Slices are copied. Nil options fail.
type Option func(*configuration)

// WithID overrides the full Go import path plus reducer type name.
func WithID(id ID) Option { return func(c *configuration) { c.id = id } }

// WithVersion supplies an implementation version included in the fingerprint.
// Bump it whenever fold/helper logic changes: Go cannot inspect method bodies.
func WithVersion(version string) Option {
	return func(c *configuration) { c.version = version; c.invalid = c.invalid || strings.TrimSpace(version) == "" }
}

// WithEventSequence selects the source sequence; the default is event-log.
func WithEventSequence(id events.SequenceID) Option {
	return func(c *configuration) { c.sequence = id; c.sequenceExplicit = true }
}

// WithEventLog explicitly selects the event log.
func WithEventLog() Option { return WithEventSequence(events.EventLog) }

// WithSourceStore overrides event-origin inference, like C# [EventStore]. It
// selects inbox-<store> even in that store and cannot be combined with an explicit
// sequence. For ordinary origin metadata use events.WithSourceStore.
func WithSourceStore(store string) Option {
	return func(c *configuration) { c.sourceStore = store; c.invalid = c.invalid || strings.TrimSpace(store) == "" }
}

// WithActive disables or enables kernel materialization (default true). It does
// not make reads local; use Passive for on-demand in-process folding.
func WithActive(active bool) Option { return func(c *configuration) { c.active = active } }

// Passive disables materialization and binds the model to local on-demand reads.
func Passive() Option { return func(c *configuration) { c.passive = true } }

// WithTags sets artifact labels, not event filters.
func WithTags(tags ...string) Option {
	copy := slices.Clone(tags)
	return func(c *configuration) { c.tags = copy }
}

// WithEventTagFilter admits events with any matching appended tag.
func WithEventTagFilter(tags ...events.Tag) Option {
	copy := make([]string, len(tags))
	for i, tag := range tags {
		copy[i] = string(tag)
	}
	return func(c *configuration) { c.filterTags = copy }
}

// WithEventSourceType filters on appended source type (empty means all).
func WithEventSourceType(typ events.SourceType) Option {
	return func(c *configuration) { c.sourceType = typ }
}

// WithEventStreamType filters on appended stream type (default All).
func WithEventStreamType(typ events.StreamType) Option {
	return func(c *configuration) { c.streamType = typ }
}

// WithLogger selects borrowed diagnostics for this reducer, overriding the client
// fallback. Nil is invalid; handlers follow chronicle.WithLogger's concurrency,
// ownership and redaction contract. Standalone declarations capture slog.Default.
func WithLogger(logger *slog.Logger) Option {
	return func(c *configuration) { c.logger, c.loggerSet = logger, true; c.invalid = c.invalid || logger == nil }
}

// WithHandler adds a typed callback. Conflicts with discovered methods fail.
func WithHandler(handler Handler) Option {
	return func(c *configuration) { c.handlers = append(c.handlers, handler) }
}

// Handler is an immutable explicit fold declaration.
type Handler struct {
	function     any
	event, model reflect.Type
}

// On declares a fold. Nil current means absent; nil result deletes. On error no
// partial state is published. Captured collaborators remain caller-owned. Calls
// are serial per operation, but separate stores/reads can invoke concurrently.
func On[E, M any](handler func(context.Context, E, *M, events.Context) (*M, error)) Handler {
	return Handler{handler, reflect.TypeFor[E](), reflect.TypeFor[M]()}
}

// Define admits an artifact without executing its constructor. R must be a named
// struct or pointer. The optional constructor accepts context then dependencies
// (or Scope) and returns R or (R,error). Nil requests service/default activation.
func Define[R, M any](model readmodels.Model[M], factory any, options ...Option) (Declaration, error) {
	typ := reflect.TypeFor[R]()
	base := typ
	if base.Kind() == reflect.Pointer {
		base = base.Elem()
	}
	if base.Kind() != reflect.Struct || base.Name() == "" {
		return Declaration{}, invalid("named reducer struct or pointer required")
	}
	return define(typ, model.Descriptor(), factory, false, ID(base.PkgPath()+"."+base.Name()), options)
}

// DefineHandlers declares zero-container callbacks with one associated model.
func DefineHandlers[M any](model readmodels.Model[M], id ID, handlers []Handler, options ...Option) (Declaration, error) {
	initial := make([]Option, 0, len(handlers)+len(options))
	for _, h := range handlers {
		initial = append(initial, WithHandler(h))
	}
	return define(nil, model.Descriptor(), nil, true, id, append(initial, options...))
}
func define(typ reflect.Type, model readmodels.Descriptor, factory any, explicit bool, id ID, options []Option) (Declaration, error) {
	c := configuration{id: id, sequence: events.EventLog, active: true, streamType: "All", logger: slog.Default()}
	for _, option := range options {
		if option == nil {
			return Declaration{}, invalid("nil reducer option")
		}
		option(&c)
	}
	if model.GoType() == nil || strings.TrimSpace(string(c.id)) == "" || strings.TrimSpace(string(c.sequence)) == "" || c.invalid || (c.sourceStore != "" && c.sequenceExplicit) {
		return Declaration{}, invalid("invalid reducer options or model")
	}
	for _, tag := range append(slices.Clone(c.tags), c.filterTags...) {
		if strings.TrimSpace(tag) == "" {
			return Declaration{}, invalid("blank tag")
		}
	}
	return Declaration{typ, model, factory, c, explicit}, nil
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

// GoType returns the artifact type, nil for callbacks.
func (d Declaration) GoType() reflect.Type { return d.typ }

// Model returns the associated declaration.
func (d Declaration) Model() readmodels.Descriptor { return d.model }

// DeclarationError describes a startup signature or model-association failure.
type DeclarationError struct {
	Reducer   ID
	Method    string
	Parameter reflect.Type
	Cause     error
}

func (e *DeclarationError) Error() string {
	return fmt.Sprintf("chronicle: reducer %s method %s parameter %v: %v", e.Reducer, e.Method, e.Parameter, e.Cause)
}

// Unwrap preserves configuration and dependency errors.
func (e *DeclarationError) Unwrap() error { return e.Cause }

// ActivationError identifies failed construction or cleanup, never acknowledged as success.
type ActivationError struct {
	Reducer   ID
	Operation string
	Cause     error
}

func (e *ActivationError) Error() string {
	return fmt.Sprintf("chronicle: reducer %s %s: %v", e.Reducer, e.Operation, e.Cause)
}

// Unwrap preserves the original factory/scope failure.
func (e *ActivationError) Unwrap() error { return e.Cause }
func invalid(message string) error {
	return fmt.Errorf("%w: %s", faults.ErrInvalidConfiguration, message)
}
