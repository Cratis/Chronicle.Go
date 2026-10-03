// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reducers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/diagnostics"
	"github.com/cratis/chronicle.go/internal/discovery"
	"github.com/cratis/chronicle.go/internal/faults"
)

// ReplayNotifier is the Go counterpart of ICanBeNotifiedWhenReplay. Each
// notification uses a separate artifact and scope, never the folding instance.
// Callbacks must honor cancellation; delivery is not exactly once.
type ReplayNotifier interface {
	BeginReplay(context.Context) error
	EndReplay(context.Context) error
}

// PartitionReplayNotifier corresponds to ICanBeNotifiedWhenPartitionReplayed.
// The source ID carries the opaque kernel partition key without normalization.
type PartitionReplayNotifier interface {
	BeginReplayPartition(context.Context, events.SourceID) error
	EndReplayPartition(context.Context, events.SourceID) error
}

// ReplayCallbacks supplies explicit alternatives to the notification interfaces.
// Nil fields mean no callback. Captures remain caller-owned; different stores and
// retired generations can invoke them concurrently. Each delivery gets a scope.
type ReplayCallbacks struct {
	// BeginReplay receives reducer-wide replay start.
	BeginReplay func(context.Context) error
	// EndReplay receives reducer-wide replay completion.
	EndReplay func(context.Context) error
	// BeginReplayPartition receives partition replay start.
	BeginReplayPartition func(context.Context, events.SourceID) error
	// EndReplayPartition receives partition replay completion.
	EndReplayPartition func(context.Context, events.SourceID) error
}

// WithReplayCallbacks supplies lifecycle callbacks for either authoring path.
// Scalars are last-wins. A callback conflicting with an artifact method fails
// at compilation; explicit callbacks do not reuse the folding artifact.
func WithReplayCallbacks(callbacks ReplayCallbacks) Option {
	return func(c *configuration) { c.replay = callbacks }
}

// ReplayState identifies a lifecycle notification, not an event observation flag.
type ReplayState int32

const (
	// BeginReplay starts reducer-wide replay.
	BeginReplay ReplayState = 1
	// EndReplay ends reducer-wide replay.
	EndReplay ReplayState = 2
	// BeginReplayPartition starts replay of one partition.
	BeginReplayPartition ReplayState = 3
	// EndReplayPartition ends replay of one partition.
	EndReplayPartition ReplayState = 4
)

var replayNames = []string{"BeginReplay", "EndReplay", "BeginReplayPartition", "EndReplayPartition"}

func replaySignature(state ReplayState) reflect.Type {
	if state == BeginReplayPartition || state == EndReplayPartition {
		return reflect.TypeFor[func(context.Context, events.SourceID) error]()
	}
	return reflect.TypeFor[func(context.Context) error]()
}

func (p *Plan) compileReplay(catalog *events.Catalog) error {
	fail := func(name string, typ reflect.Type, message string) error {
		return &DeclarationError{p.Identifier(), name, typ, invalid(message)}
	}
	p.replay = make(map[ReplayState]bool)
	if !p.declaration.explicit {
		for i, name := range replayNames {
			state := ReplayState(i + 1)
			method, ok := p.declaration.typ.MethodByName(name)
			if !ok {
				continue
			}
			t := method.Type
			first := 1
			if t.NumIn() > first && t.In(first) == contextType {
				first++
			}
			if t.NumIn() > first && len(discovery.Events(t.In(first), catalog)) != 0 {
				if _, err := compileFold(name, method.Func, true, p.model.GoType()); err == nil {
					// C# fold dispatch is signature-based, even for a method whose
					// name happens to match a lifecycle method. Do not reserve names.
					continue
				}
			}
			args := make([]reflect.Type, t.NumIn()-1)
			for j := range args {
				args[j] = t.In(j + 1)
			}
			outputs := make([]reflect.Type, t.NumOut())
			for j := range outputs {
				outputs[j] = t.Out(j)
			}
			if reflect.FuncOf(args, outputs, t.IsVariadic()) != replaySignature(state) {
				return fail(name, t, "notification must match ReplayNotifier or PartitionReplayNotifier; services belong in constructors")
			}
			p.replay[state] = true
		}
		for _, pair := range [][2]ReplayState{{BeginReplay, EndReplay}, {BeginReplayPartition, EndReplayPartition}} {
			if p.replay[pair[0]] != p.replay[pair[1]] {
				return fail(replayNames[pair[0]-1], p.declaration.typ, "notification interface requires both begin and end methods")
			}
		}
	}
	for i, callback := range p.declaration.config.replay.values() {
		if reflect.ValueOf(callback).IsNil() {
			continue
		}
		state := ReplayState(i + 1)
		if p.replay[state] {
			return fail(replayNames[i], reflect.TypeOf(callback), "duplicate lifecycle callback and artifact method")
		}
	}
	return nil
}

func (c ReplayCallbacks) values() []any {
	return []any{c.BeginReplay, c.EndReplay, c.BeginReplayPartition, c.EndReplayPartition}
}

// NotifyReplay dispatches a known notification in a fresh scope and releases it
// before returning. Like C# Reducers.HandleReplayNotification, it preserves the
// incoming identity and does not fabricate event context/correlation/causation.
// Activation, handler, cancellation and cleanup failures terminate the stream;
// activation failure is deliberately stricter than C#'s log-and-ignore behavior.
// Notifications have no result acknowledgement. Unknown states fail before I/O.
func (p *Plan) NotifyReplay(ctx context.Context, state ReplayState, partition events.SourceID) (err error) {
	defer func() {
		if err != nil {
			diagnostics.Log(ctx, p.Logger(), slog.LevelError, "reducer replay notification failed", "reducer", "replay", err)
		}
	}()
	if state < BeginReplay || state > EndReplayPartition {
		return fmt.Errorf("%w: unknown reducer replay state %d", faults.ErrProtocol, state)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx, cancelOperation := context.WithCancel(ctx)
	defer cancelOperation()
	lease, err := p.activate(ctx, true)
	if err != nil {
		return err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &diagnostics.PanicError{}
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		if failure := lease.Close(cleanup); failure != nil {
			err = errors.Join(err, &ActivationError{p.Identifier(), "close replay notification", failure})
		}
		cancel()
		err = errors.Join(err, ctx.Err())
	}()
	if p.replay[state] {
		switch state {
		case BeginReplay:
			return lease.instance.(ReplayNotifier).BeginReplay(ctx)
		case EndReplay:
			return lease.instance.(ReplayNotifier).EndReplay(ctx)
		case BeginReplayPartition:
			return lease.instance.(PartitionReplayNotifier).BeginReplayPartition(ctx, partition)
		case EndReplayPartition:
			return lease.instance.(PartitionReplayNotifier).EndReplayPartition(ctx, partition)
		}
	}
	callbacks := p.declaration.config.replay
	switch state {
	case BeginReplay:
		if callbacks.BeginReplay != nil {
			return callbacks.BeginReplay(ctx)
		}
	case EndReplay:
		if callbacks.EndReplay != nil {
			return callbacks.EndReplay(ctx)
		}
	case BeginReplayPartition:
		if callbacks.BeginReplayPartition != nil {
			return callbacks.BeginReplayPartition(ctx, partition)
		}
	case EndReplayPartition:
		if callbacks.EndReplayPartition != nil {
			return callbacks.EndReplayPartition(ctx, partition)
		}
	}
	return nil
}
