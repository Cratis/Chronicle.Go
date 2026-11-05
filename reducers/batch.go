// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reducers

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/diagnostics"
	"github.com/cratis/chronicle.go/internal/discovery"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/observation"
)

var eventBatchType = reflect.TypeFor[observation.EventBatch]()
var eventBatchPointerType = reflect.PointerTo(eventBatchType)
var batchContextType = reflect.TypeFor[observation.BatchContext]()

// WithBatchMethod declares the event types an exported batch method observes; it
// is Go's form of C# [Observes<TEvent>] on that method, and the listed types are
// the reducer's whole set of event types. A reducer batch method takes an
// optional context.Context, then *observation.EventBatch, the current model (M or
// *M; nil or zero when absent) and optionally observation.BatchContext, and
// returns M or *M, optionally followed by error. A nil *M result deletes the
// model. It is called once per event source (partition) with that source's
// events in order. A reducer has either one batch method or per-event folds,
// never both. Repeat the option or list several types.
func WithBatchMethod(method string, observes ...observation.Observed) Option {
	copied := slices.Clone(observes)
	return func(c *configuration) {
		c.invalid = c.invalid || strings.TrimSpace(method) == "" || len(copied) == 0
		if c.batchMethods == nil {
			c.batchMethods = map[string][]observation.Observed{}
		}
		c.batchMethods[method] = append(c.batchMethods[method], copied...)
	}
}

// OnBatch declares a batch fold callback for DefineHandlers; see WithBatchMethod.
func OnBatch[M any](handler func(context.Context, *observation.EventBatch, *M, observation.BatchContext) (*M, error), observes ...observation.Observed) Handler {
	return Handler{function: handler, model: reflect.TypeFor[M](), batch: true, observes: slices.Clone(observes)}
}

// WithMaxConcurrency bounds how many partitions (event sources) of one delivered
// page fold at the same time. The default is the number of CPUs; 1 folds them one
// after another. Each partition gets its own scope and artifact. Values below 1
// are invalid.
func WithMaxConcurrency(partitions int) Option {
	return func(c *configuration) { c.maxConcurrency = partitions; c.invalid = c.invalid || partitions < 1 }
}

// MaxConcurrency returns how many partitions of one page fold at the same time.
func (p *Plan) MaxConcurrency() int {
	if p.declaration.config.maxConcurrency > 0 {
		return p.declaration.config.maxConcurrency
	}
	return runtime.NumCPU()
}

// HasBatchMethod reports whether the reducer folds with a batch method.
func (p *Plan) HasBatchMethod() bool { return p.batch != nil }

type batchFold struct {
	name                                                        string
	function                                                    reflect.Value
	receiver, context, batchContext, valueCurrent, returnsError bool
}

// takesBatch reports the index of the first parameter after the receiver and an
// optional context, and whether it is an EventBatch.
func takesBatch(t reflect.Type, receiver bool) (int, bool) {
	first := 0
	if receiver {
		first++
	}
	if t.NumIn() > first && t.In(first) == contextType {
		first++
	}
	return first, t.NumIn() > first && (t.In(first) == eventBatchPointerType || t.In(first) == eventBatchType)
}

func compileBatchFold(name string, function reflect.Value, receiver bool, model reflect.Type) (batchFold, error) {
	f := batchFold{name: name, function: function, receiver: receiver}
	t := function.Type()
	first := 0
	if receiver {
		first++
	}
	if t.NumIn() > first && t.In(first) == contextType {
		f.context = true
		first++
	}
	if t.IsVariadic() || t.NumIn() <= first || t.In(first) != eventBatchPointerType {
		return f, signature("take *observation.EventBatch, not a value or variadic parameters")
	}
	n := t.NumIn() - first
	if n < 2 || n > 3 {
		return f, signature("take the batch, the current model and optionally observation.BatchContext; services belong in constructors")
	}
	current := t.In(first + 1)
	if current != model && current != reflect.PointerTo(model) {
		return f, signature(fmt.Sprintf("take the current read model %s or *%s as the second parameter", model.Name(), model.Name()))
	}
	f.valueCurrent = current == model
	if n == 3 {
		if t.In(first+2) != batchContextType {
			return f, signature("the third parameter must be observation.BatchContext")
		}
		f.batchContext = true
	}
	if t.NumOut() < 1 || t.NumOut() > 2 || (t.Out(0) != model && t.Out(0) != reflect.PointerTo(model)) || (t.NumOut() == 2 && t.Out(1) != errorType) {
		return f, signature(fmt.Sprintf("return %s or *%s, optionally followed by error; nil deletes the read model", model.Name(), model.Name()))
	}
	f.returnsError = t.NumOut() == 2
	return f, nil
}

func (p *Plan) bindBatch(f batchFold, observes []observation.Observed, catalog *events.Catalog) error {
	if p.batch != nil {
		return observation.ErrReducerHasMoreThanOneBatchMethod
	}
	if len(observes) == 0 {
		return observation.ErrBatchMethodObservesNothing
	}
	for _, observed := range observes {
		if observed.Type() == nil {
			return observation.ErrBatchMethodObservesUnknownEventType
		}
		matching := discovery.Events(observed.Type(), catalog)
		if len(matching) == 0 {
			return fmt.Errorf("%w: %v", observation.ErrBatchMethodObservesUnknownEventType, observed.Type())
		}
		for _, descriptor := range matching {
			id := descriptor.Ref().ID
			if selected, exists := p.descriptors[id]; exists && selected.Ref() != descriptor.Ref() {
				return invalid("a reducer must select one generation per event ID; use separate reducers for different generations")
			}
			p.descriptors[id] = descriptor
		}
	}
	p.batch = &f
	return nil
}

func signature(message string) error {
	return fmt.Errorf("%w: %s", observation.ErrInvalidBatchMethodSignature, message)
}

// reduceBatch calls the batch method once with every event of the partition.
// Any Fail or Defer fails or defers the whole partition and writes no state,
// matching C#: a reducer is one fold, so there is no partial state to keep.
func (l *Lease) reduceBatch(ctx context.Context, batch []Event, initial any, supplied *observation.BatchContext, result Result) Result {
	if len(batch) == 0 {
		return result
	}
	items := make([]observation.EventAndContext, len(batch))
	var previous events.SequenceNumber
	for i, event := range batch {
		if event.Context.SequenceNumber >= events.Unavailable-2 || (i > 0 && event.Context.SequenceNumber <= previous) {
			result.Err = faults.ErrProtocol
			return result
		}
		previous = event.Context.SequenceNumber
		items[i] = observation.EventAndContext{Event: eventValue(event.Content), Context: event.Context}
	}
	batchContext := l.plan.localBatchContext(ctx, batch)
	if supplied != nil {
		batchContext = *supplied
	}
	eventBatch := observation.NewEventBatch(items, batchContext.CanDefer)
	state, err := l.invokeBatch(ctx, eventBatch, initial, batchContext)
	if err != nil {
		result.State, result.Err = nil, err
		return result
	}
	if outcomes := eventBatch.Outcomes(); len(outcomes) > 0 {
		earliest := slices.MinFunc(outcomes, func(a, b observation.BatchOutcome) int { return cmpSequence(a.At, b.At) })
		result.State, result.Err = nil, earliest.Err
		if earliest.Kind == observation.OutcomeDeferred {
			result.Err = &observation.DeferredError{Partition: earliest.Partition, RetryAfter: earliest.RetryAfter}
		}
		return result
	}
	result.State, result.LastSuccessful = state, previous
	return result
}

func (l *Lease) invokeBatch(ctx context.Context, batch *observation.EventBatch, current any, batchContext observation.BatchContext) (state any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			state, err = nil, &diagnostics.PanicError{}
		}
	}()
	if l.resources.Closed() {
		return nil, invalid("artifact lease closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f := l.plan.batch
	value, err := currentValue(l.plan.model.GoType(), current, f.valueCurrent)
	if err != nil {
		return nil, err
	}
	args := []reflect.Value{}
	if f.receiver {
		args = append(args, reflect.ValueOf(l.instance))
	}
	if f.context {
		if !l.preserveIdentity {
			ctx = metadata.WithIdentity(ctx, identities.System())
		}
		args = append(args, reflect.ValueOf(ctx))
	}
	args = append(args, reflect.ValueOf(batch), value)
	if f.batchContext {
		args = append(args, reflect.ValueOf(batchContext))
	}
	results := f.function.Call(args)
	if f.returnsError && !results[1].IsNil() {
		return nil, results[1].Interface().(error)
	}
	return nextState(l.plan.model.GoType(), results[0]), nil
}

// localBatchContext describes a fold the runtime did not deliver as a page.
func (p *Plan) localBatchContext(ctx context.Context, batch []Event) observation.BatchContext {
	c := observation.BatchContext{Observer: string(p.Identifier()), Sequence: p.EventSequence(), EventCount: len(batch), FirstSequenceNumber: batch[0].Context.SequenceNumber, LastSequenceNumber: batch[len(batch)-1].Context.SequenceNumber}
	if coordinates, ok := BatchFromContext(ctx); ok {
		c.Store, c.Namespace = coordinates.Store, coordinates.Namespace
	}
	for _, event := range batch {
		if !slices.Contains(c.Partitions, event.Context.SourceID) {
			c.Partitions = append(c.Partitions, event.Context.SourceID)
		}
	}
	return c
}

func eventValue(content any) any {
	value := reflect.ValueOf(content)
	if value.Kind() == reflect.Pointer && !value.IsNil() {
		return value.Elem().Interface()
	}
	return content
}

func cmpSequence(a, b events.SequenceNumber) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
