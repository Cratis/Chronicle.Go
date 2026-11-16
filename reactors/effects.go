// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reactors

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/reactoreffects"
)

// EventStreamIDProvider overrides WithEventStreamID for bare returned events.
type EventStreamIDProvider interface{ GetEventStreamID() events.StreamID }

// SubjectProvider overrides event-level subject resolution for bare returned
// events. Nil leaves the event descriptor's resolver/source fallback in control.
type SubjectProvider interface{ GetSubject() *events.Subject }

// EventAppender is the event-log append surface available to returned effects.
// Implementations must preserve atomicity, ordering, and domain rejection results
// and must not retry ambiguous writes. eventsequences.Sequence implements it.
type EventAppender interface {
	Append(context.Context, events.SourceID, any, ...eventsequences.AppendOption) (eventsequences.AppendResult, error)
	AppendMany(context.Context, events.SourceID, []any, ...eventsequences.AppendOption) (eventsequences.BatchResult, error)
	AppendBatch(context.Context, []eventsequences.Entry, ...eventsequences.BatchOption) (eventsequences.BatchResult, error)
}

// EventLogRuntime optionally extends Runtime for metadata-rich and atomic effects.
// Runtime.Append remains supported for a single bare event with default metadata.
// The store runtime always implements this extension; it never targets an inbox.
type EventLogRuntime interface{ EventLog() EventAppender }

// SideEffectContext supplies invocation-local dependencies to extensions. Scope,
// Reactor and Runtime are borrowed only for the call. Events is the immutable
// catalog for this store, never a global or first-resolved-store registry.
type SideEffectContext struct {
	Invocation
	Reactor any
	Runtime Runtime
	Events  *events.Catalog
	// Replay reports the delivery's replay observation flag (including combined flags).
	Replay bool
	// OnceOnly reports the selected method/callback policy. Such a handler never
	// produces a replay effect; ordinary failure recovery can still repeat it.
	OnceOnly bool
	// Replayable reports the reactor-wide kernel registration policy.
	Replayable bool
}

// SideEffectHandler extends reactor return values, for example Arc commands.
// CanHandleReturnType validates declared results at compilation. CanHandle must
// only classify, with no I/O or mutation: ALL matching handlers are selected
// before any Handle/append. All selected handlers run in registration order;
// returned errors are joined (partial effects can already exist), never retried.
// A handler may claim a whole collection. Otherwise collections of claimed items
// are fully preflighted and then executed in order, stopping after a failed item.
// Collections containing only events/wrappers remain a single atomic append.
// Instances are borrowed, never closed, and must be safe for concurrent reactors.
// Resolve scoped collaborators from SideEffectContext.Scope inside Handle.
type SideEffectHandler interface {
	CanHandleReturnType(reflect.Type) bool
	CanHandle(SideEffectContext, any) bool
	Handle(context.Context, SideEffectContext, any) error
}

// WithSideEffectHandlers appends borrowed custom handlers after registry handlers.
// Nil handlers fail compilation. The input slice is copied. Built-in event
// handling also runs if it matches; extensions do not replace it.
func WithSideEffectHandlers(handlers ...SideEffectHandler) Option {
	copy := slices.Clone(handlers)
	return func(c *configuration) { c.sideEffects = append(c.sideEffects, copy...) }
}

func supportsResult(typ reflect.Type, catalog *events.Catalog, handlers []SideEffectHandler) bool {
	if builtinResult(typ, catalog) {
		return true
	}
	if handlersSupportResult(typ, handlers) {
		return true
	}
	if !isEffectCollection(typ) {
		return false
	}
	elem := typ.Elem()
	return !isEffectCollection(elem) && supportsResult(elem, catalog, handlers)
}

func handlersSupportResult(typ reflect.Type, handlers []SideEffectHandler) bool {
	for _, handler := range handlers {
		if handler.CanHandleReturnType(typ) {
			return true
		}
	}
	return false
}

func isEffectCollection(typ reflect.Type) bool {
	return typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array
}

func builtinResult(typ reflect.Type, catalog *events.Catalog) bool {
	if len(matchingEvents(typ, catalog)) != 0 {
		return true
	}
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ == reflect.TypeFor[eventsequences.Entry]() || typ == reflect.TypeFor[eventsequences.EventsWithConcurrencyScopes]() {
		return true
	}
	return isEffectCollection(typ) && builtinCollectionResult(typ.Elem(), catalog)
}

func builtinCollectionResult(elem reflect.Type, catalog *events.Catalog) bool {
	if elem == reflect.TypeFor[any]() || len(matchingEvents(elem, catalog)) != 0 {
		return true
	}
	if elem.Kind() == reflect.Pointer {
		elem = elem.Elem()
	}
	return elem == reflect.TypeFor[eventsequences.Entry]()
}

type eventEffect struct {
	entries []eventsequences.Entry
	scopes  []eventsequences.LabeledScope
	bare    bool
	single  bool
	batch   bool
}

type effectAction struct {
	value    any
	events   eventEffect
	builtin  bool
	handlers []SideEffectHandler
	prepared func(context.Context) error
}

func (l *Lease) handleEffect(ctx context.Context, value any, invocation Invocation, runtime Runtime, onceOnly bool) error {
	if nilLike(value) {
		return nil
	}
	effectContext := SideEffectContext{Invocation: invocation, Reactor: l.instance, Runtime: runtime, Events: l.plan.catalog,
		Replay: invocation.Context.ObservationState&events.ObservationReplay != 0, OnceOnly: onceOnly, Replayable: l.plan.IsReplayable()}
	return l.handleEffectContext(ctx, value, effectContext)
}

func (l *Lease) handleEffectContext(ctx context.Context, value any, effectContext SideEffectContext) error {
	actions, err := l.classifyEffect(value, effectContext, true)
	if err != nil {
		return err
	}
	if recorder, ok := effectContext.Runtime.(reactoreffects.Recorder); ok {
		return recorder.RecordEffect(ctx, value)
	}
	if err := l.prepareEffectActions(ctx, effectContext.Runtime, actions); err != nil {
		return err
	}
	for _, action := range actions {
		if err := action.execute(ctx, effectContext); err != nil {
			return err
		}
	}
	return nil
}

func (l *Lease) prepareEffectActions(ctx context.Context, runtime Runtime, actions []effectAction) error {
	// SDK appenders retain immutable prepared actions. Borrowed custom appenders
	// keep their existing contract, which cannot guarantee private preparation.
	var preparer reactoreffects.Preparer[eventsequences.Entry, eventsequences.LabeledScope]
	if provider, ok := runtime.(EventLogRuntime); ok {
		preparer, _ = provider.EventLog().(reactoreffects.Preparer[eventsequences.Entry, eventsequences.LabeledScope])
	}
	for i := range actions {
		if err := l.prepareEffectAction(ctx, &actions[i], preparer); err != nil {
			return err
		}
	}
	return nil
}

func (l *Lease) prepareEffectAction(ctx context.Context, action *effectAction, preparer reactoreffects.Preparer[eventsequences.Entry, eventsequences.LabeledScope]) error {
	if !action.builtin {
		return nil
	}
	if preparer == nil {
		return l.marshalEffectEntries(ctx, action.events.entries)
	}
	var err error
	action.prepared, err = preparer.PrepareReturnedEvents(ctx, reactoreffects.Preparation[eventsequences.Entry, eventsequences.LabeledScope]{Entries: action.events.entries, Scopes: action.events.scopes, Bare: action.events.bare, Single: action.events.single, Batch: action.events.batch})
	return err
}

func (l *Lease) marshalEffectEntries(ctx context.Context, entries []eventsequences.Entry) error {
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		descriptor, ok := l.plan.catalog.Lookup(entry.Event)
		if !ok {
			return faults.ErrNotRegistered
		}
		if _, err := descriptor.Marshal(entry.Event); err != nil {
			return err
		}
	}
	return nil
}

func (a effectAction) execute(ctx context.Context, effectContext SideEffectContext) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	failure := a.executeBuiltin(ctx, effectContext.Runtime)
	for _, handler := range a.handlers {
		failure = errors.Join(failure, handler.Handle(ctx, effectContext, a.value))
	}
	return failure
}

func (a effectAction) executeBuiltin(ctx context.Context, runtime Runtime) error {
	if a.prepared != nil {
		return a.prepared(ctx)
	}
	if a.builtin {
		return appendEffect(ctx, runtime, a.events)
	}
	return nil
}

// classifyEffect preflights finite collections without performing effects.
// Nested/lazy collections need a handler claiming that shape; no recursive walk
// of arbitrary application object graphs (which may be cyclic) is performed.
func (l *Lease) classifyEffect(value any, ec SideEffectContext, collection bool) ([]effectAction, error) {
	if nilLike(value) {
		return nil, invalid("nil item in effect collection")
	}
	effect, builtin, err := l.classifyEvents(value, ec.Context)
	if err != nil {
		return nil, err
	}
	action := effectAction{value: value, events: effect, builtin: builtin}
	action.handlers = l.matchingEffectHandlers(ec, value)
	if builtin || len(action.handlers) != 0 {
		return []effectAction{action}, nil
	}
	v := reflect.ValueOf(value)
	if !collection || !isEffectCollection(v.Type()) {
		return nil, invalid("unhandled reactor return value")
	}
	return l.classifyEffectItems(v, ec)
}

func (l *Lease) matchingEffectHandlers(ec SideEffectContext, value any) []SideEffectHandler {
	var handlers []SideEffectHandler
	for _, handler := range l.plan.sideEffects {
		if handler.CanHandle(ec, value) {
			handlers = append(handlers, handler)
		}
	}
	return handlers
}

func (l *Lease) classifyEffectItems(v reflect.Value, ec SideEffectContext) ([]effectAction, error) {
	var actions []effectAction
	for i := 0; i < v.Len(); i++ {
		item, err := l.classifyEffect(v.Index(i).Interface(), ec, false)
		if err != nil {
			return nil, err
		}
		actions = append(actions, item...)
	}
	return actions, nil
}

func (l *Lease) classifyEvents(value any, ec events.Context) (eventEffect, bool, error) {
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if batch, ok := v.Interface().(eventsequences.EventsWithConcurrencyScopes); ok {
		effect := eventEffect{entries: slices.Clone(batch.Events), scopes: slices.Clone(batch.Scopes), batch: true}
		return effect, true, l.validateEffectEntries(effect.entries)
	}
	values, single := effectValues(value, v)
	effect, matched := l.collectEventEntries(values)
	if !matched {
		return eventEffect{}, false, nil
	}
	effect.single = single
	l.applyEffectDefaults(&effect, values, ec)
	return effect, true, l.validateEffectEntries(effect.entries)
}

func effectValues(value any, v reflect.Value) ([]any, bool) {
	if !isEffectCollection(v.Type()) {
		return []any{value}, true
	}
	values := make([]any, v.Len())
	for i := range values {
		values[i] = v.Index(i).Interface()
	}
	return values, false
}

func targetedEffectEntry(item any) (eventsequences.Entry, bool) {
	v := reflect.ValueOf(item)
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	entry, ok := v.Interface().(eventsequences.Entry)
	return entry, ok
}

func (l *Lease) collectEventEntries(values []any) (eventEffect, bool) {
	effect := eventEffect{bare: true}
	for _, item := range values {
		if nilLike(item) {
			return eventEffect{}, false
		}
		if entry, ok := targetedEffectEntry(item); ok {
			effect.bare = false
			effect.entries = append(effect.entries, entry)
			continue
		}
		if _, ok := l.plan.catalog.Lookup(item); !ok {
			return eventEffect{}, false
		}
		effect.entries = append(effect.entries, eventsequences.Entry{Event: item})
	}
	return effect, true
}

func (l *Lease) applyEffectDefaults(effect *eventEffect, values []any, ec events.Context) {
	// Resolve providers once, and only for bare items. Targeted entries retain
	// their own metadata, including zero/default fields.
	var defaults eventsequences.Entry
	resolved := false
	for i, item := range values {
		if _, targeted := targetedEffectEntry(item); targeted {
			continue
		}
		if !resolved {
			defaults = l.effectDefaults(ec)
			resolved = true
		}
		entry := defaults
		entry.Event = effect.entries[i].Event
		effect.entries[i] = entry
	}
}

func (l *Lease) validateEffectEntries(entries []eventsequences.Entry) error {
	for _, entry := range entries {
		if nilLike(entry.Event) {
			return invalid("nil event in effect entry")
		}
		if strings.TrimSpace(string(entry.Source)) == "" {
			return invalid("source is required for effect entry")
		}
		if _, ok := l.plan.catalog.Lookup(entry.Event); !ok {
			return faults.ErrNotRegistered
		}
	}
	return nil
}

func (l *Lease) effectDefaults(ec events.Context) eventsequences.Entry {
	c := l.plan.declaration.config
	entry := eventsequences.Entry{Source: ec.SourceID, Route: eventsequences.Route{SourceType: c.sourceType, StreamType: c.streamType, StreamID: c.streamID}}
	if provider, ok := l.instance.(EventSourceIDProvider); ok {
		entry.Source = provider.GetEventSourceID()
	}
	if provider, ok := l.instance.(EventStreamIDProvider); ok {
		entry.Route.StreamID = provider.GetEventStreamID()
	}
	if provider, ok := l.instance.(SubjectProvider); ok {
		entry.Subject = provider.GetSubject()
	}
	return entry
}
func appendEffect(ctx context.Context, runtime Runtime, effect eventEffect) error {
	if len(effect.entries) == 0 && !effect.batch {
		return nil
	}
	provider, ok := runtime.(EventLogRuntime)
	if !ok {
		return appendLegacyEffect(ctx, runtime, effect)
	}
	log := provider.EventLog()
	if nilLike(log) {
		return invalid("nil event-log appender")
	}
	if effect.bare {
		return appendBareEffect(ctx, log, effect)
	}
	result, err := log.AppendBatch(ctx, effect.entries, eventsequences.WithScopes(effect.scopes...))
	if err != nil {
		return err
	}
	return result.Err()
}

func appendLegacyEffect(ctx context.Context, runtime Runtime, effect eventEffect) error {
	if effect.bare && effect.single {
		entry := effect.entries[0]
		if hasDefaultEffectMetadata(entry) {
			return runtime.Append(ctx, entry.Source, entry.Event)
		}
	}
	return invalid("runtime does not support event-log effects")
}

func hasDefaultEffectMetadata(entry eventsequences.Entry) bool {
	return entry.Subject == nil && entry.Route.SourceType == "" && entry.Route.StreamID == "" && entry.Route.StreamType == events.AllStreamTypes
}

func appendBareEffect(ctx context.Context, log EventAppender, effect eventEffect) error {
	first := effect.entries[0]
	options := []eventsequences.AppendOption{eventsequences.WithRoute(first.Route)}
	if first.Subject != nil {
		options = append(options, eventsequences.WithSubject(*first.Subject))
	}
	if effect.single {
		result, err := log.Append(ctx, first.Source, first.Event, options...)
		if err != nil {
			return err
		}
		return result.Err()
	}
	result, err := log.AppendMany(ctx, first.Source, effectEventValues(effect.entries), options...)
	if err != nil {
		return err
	}
	return result.Err()
}

func effectEventValues(entries []eventsequences.Entry) []any {
	values := make([]any, len(entries))
	for i, entry := range entries {
		values[i] = entry.Event
	}
	return values
}
