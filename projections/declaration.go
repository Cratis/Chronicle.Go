// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package projections compiles model-bound tags and fluent declarations into the
// same immutable kernel definition. The kernel, not this package, projects events.
package projections

import (
	"fmt"
	"maps"
	"reflect"
	"slices"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/readmodels"
)

// DeclarationError locates a malformed tag or typed declaration. Default text
// redacts literal contents; use errors.As to inspect provenance and the cause.
type DeclarationError = declarations.DeclarationError

// Declaration is immutable authoring metadata. NewClient resolves it against the
// store's frozen catalog, before any connection work. Its zero value is invalid.
type Declaration struct{ data *declaration }
type declaration struct {
	model                                      readmodels.Descriptor
	id                                         string
	sequence                                   events.SequenceID
	passive, notRewindable, noAuto, modelBound bool
	aliases                                    map[string]events.Descriptor
	subscriptions                              []subscription
	err                                        error
}
type subscription struct {
	event               events.Descriptor
	key, parent         expression
	keyType, parentType reflect.Type
	writes              []write
	err                 error
}
type write struct {
	path                   string
	targetType, sourceType reflect.Type
	expression             expression
	provenance             Provenance
}

// Option configures a declaration. Settings are last-wins; duplicate aliases and
// subscriptions are errors. Options are applied once, without invoking user code
// during reconnect. Nil options fail at Build or NewClient.
type Option func(*declaration)

// ModelBound declares a projection using field tags and typed type-level options.
// Mapping tags alone also trigger discovery on registered models at NewClient.
func ModelBound[M any](model readmodels.Model[M], options ...Option) Declaration {
	return ModelBoundDescriptor(model.Descriptor(), options...)
}

// ModelBoundDescriptor is the untyped registry/tooling equivalent of ModelBound.
func ModelBoundDescriptor(model readmodels.Descriptor, options ...Option) Declaration {
	d := newDeclaration(model, options)
	d.modelBound = true
	return Declaration{data: d}
}
func newDeclaration(model readmodels.Descriptor, options []Option) *declaration {
	d := &declaration{model: model, sequence: events.EventLog, aliases: map[string]events.Descriptor{}}
	if typ := model.GoType(); typ != nil {
		d.id = typ.PkgPath() + "." + typ.Name()
	}
	for _, option := range options {
		if option == nil {
			d.err = invalid("nil projection option")
			continue
		}
		option(d)
	}
	return d
}

// Identifier returns the projection identity, not the model's container name.
func (d Declaration) Identifier() string {
	if d.data == nil {
		return ""
	}
	return d.data.id
}

// Model returns the immutable target descriptor (zero for an empty declaration).
func (d Declaration) Model() readmodels.Descriptor {
	if d.data == nil {
		return readmodels.Descriptor{}
	}
	return d.data.model
}

// WithIdentifier selects a stable projection identity. The default is the full
// Go model type name, matching the C# model-bound/explicit registration convention.
func WithIdentifier(id string) Option { return func(d *declaration) { d.id = id } }

// WithEventSequence selects the source sequence (default event-log).
func WithEventSequence(sequence events.SequenceID) Option {
	return func(d *declaration) { d.sequence = sequence }
}

// WithEventLog explicitly selects the event log.
func WithEventLog() Option { return WithEventSequence(events.EventLog) }

// NotRewindable disables replay for this projection.
func NotRewindable() Option { return func(d *declaration) { d.notRewindable = true } }

// Passive disables active observation and binds the model to immediate reads.
func Passive() Option { return func(d *declaration) { d.passive = true } }

// NoAutoMap disables automatic payload mapping for this node.
func NoAutoMap() Option { return func(d *declaration) { d.noAuto = true } }

// AutoMap enables automatic payload mapping (the default).
func AutoMap() Option { return func(d *declaration) { d.noAuto = false } }

// BindEvent binds a refactoring-safe event handle to an @alias used in tags.
// An alias must be a simple identifier and can only be declared once.
func BindEvent[E any](alias string, event events.Type[E]) Option {
	return func(d *declaration) {
		if !simpleName(alias) {
			d.err = invalid("invalid event alias")
			return
		}
		if _, ok := d.aliases[alias]; ok {
			d.err = invalid("duplicate event alias")
			return
		}
		d.aliases[alias] = event.Descriptor()
	}
}

// FromEvent subscribes a model-bound projection even without property mappings.
func FromEvent[E any](event events.Type[E], options ...FromOption) Option {
	s := newSubscription(event.Descriptor(), options)
	return func(d *declaration) { d.subscriptions = append(d.subscriptions, s) }
}

// FromOption configures the event's correlation key, not the model's key metadata.
// Key settings are last-wins (constant key therefore overrides a preceding path).
type FromOption func(*subscription)

func newSubscription(event events.Descriptor, options []FromOption) subscription {
	s := subscription{event: event, key: expression{kind: sourceExpression}}
	for _, option := range options {
		if option == nil {
			s.err = invalid("nil event subscription option")
			continue
		}
		option(&s)
	}
	return s
}

// UsingKey correlates the event using an exact serialized scalar field.
func UsingKey[E, V any](field Field[E, V]) FromOption {
	return func(s *subscription) {
		s.key = expression{kind: pathExpression, text: field.path}
		s.keyType = reflect.TypeFor[V]()
		if field.owner != s.event.GoType() {
			s.key.kind = invalidExpression
		}
	}
}

// UsingParentKey supplies the event's parent correlation field.
func UsingParentKey[E, V any](field Field[E, V]) FromOption {
	return func(s *subscription) {
		s.parent = expression{kind: pathExpression, text: field.path}
		s.parentType = reflect.TypeFor[V]()
		if field.owner != s.event.GoType() {
			s.parent.kind = invalidExpression
		}
	}
}

// UsingConstantKey correlates every event of this subscription to one constant key.
func UsingConstantKey(value string) FromOption {
	return func(s *subscription) {
		s.key = expression{kind: literalExpression, text: value, literalKind: declarations.String}
		s.keyType = nil
	}
}

func cloneDeclaration(d *declaration) *declaration {
	copy := *d
	copy.aliases = maps.Clone(d.aliases)
	copy.subscriptions = slices.Clone(d.subscriptions)
	for i := range copy.subscriptions {
		copy.subscriptions[i].writes = slices.Clone(d.subscriptions[i].writes)
	}
	return &copy
}
func invalid(message string) error {
	return fmt.Errorf("%w: %s", faults.ErrInvalidConfiguration, message)
}
func simpleName(name string) bool {
	if !declarations.Path(name) {
		return false
	}
	for _, r := range name {
		if r == '.' {
			return false
		}
	}
	return true
}
