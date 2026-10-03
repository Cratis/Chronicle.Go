// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reactors

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"time"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/artifacts"
	"github.com/cratis/chronicle.go/internal/diagnostics"
)

// Replay selects exported methods that replace ordinary handlers during replay.
// Every name must identify a discovered event handler; empty/unknown selectors
// fail compilation. Repeated selectors are harmless. Names have no implicit policy.
func Replay(methods ...string) Option {
	names := slices.Clone(methods)
	return func(c *configuration) {
		c.invalid = c.invalid || len(names) == 0
		c.replayMethods = append(c.replayMethods, names...)
	}
}

// OnceOnly with no names prevents the kernel from replaying the entire reactor.
// With names, it skips only those validated handler methods during replay,
// including a selected replacement. Recovery is not replay: no deduplication is
// implied. Repeated selectors are harmless.
func OnceOnly(methods ...string) Option {
	names := slices.Clone(methods)
	return func(c *configuration) {
		if len(names) == 0 {
			c.replayable = false
		}
		c.onceMethods = append(c.onceMethods, names...)
	}
}

// DuringReplay returns a callback declaration used only during replay. It replaces
// a live handler for the same event, or contributes a replay-only subscription.
func (h Handler) DuringReplay() Handler { h.replay = true; return h }

// OnceOnly returns a callback declaration skipped during replay, not recovery.
func (h Handler) OnceOnly() Handler { h.onceOnly = true; return h }

// ReplayNotifier receives replay-wide notifications on separately activated
// instances. No event middleware runs. Methods must honor cancellation.
type ReplayNotifier interface {
	BeginReplay(context.Context) error
	EndReplay(context.Context) error
}

// PartitionReplayNotifier receives partition notifications on separately
// activated instances, independent of batch scopes and replay-wide notifications.
type PartitionReplayNotifier interface {
	BeginReplayPartition(context.Context, events.SourceID) error
	EndReplayPartition(context.Context, events.SourceID) error
}

// ReplayState identifies a kernel notification, not an event observation flag.
type ReplayState int32

const (
	// BeginReplay starts an observer-wide replay.
	BeginReplay ReplayState = 1
	// EndReplay ends an observer-wide replay.
	EndReplay ReplayState = 2
	// BeginReplayPartition starts replay of one partition.
	BeginReplayPartition ReplayState = 3
	// EndReplayPartition ends replay of one partition.
	EndReplayPartition ReplayState = 4
)

// NotifyReplay activates only the artifact in a fresh scope and releases it before
// returning. Activation failures are logged and ignored like C#; notification or
// cleanup failures terminate the stream (there is no notification acknowledgement).
// Callback-only declarations have no notification instance and are ignored.
func (p *Plan) NotifyReplay(ctx context.Context, state ReplayState, partition events.SourceID) (err error) {
	if p.declaration.explicit {
		return nil
	}
	var resources *artifacts.Lease
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &diagnostics.PanicError{}
		}
		if resources != nil {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			err = errors.Join(err, resources.Close(cleanup))
			cancel()
		}
		if err != nil {
			diagnostics.Log(ctx, p.Logger(), slog.LevelError, "reactor replay notification failed", "reactor", "replay", err)
		}
	}()
	resources, err = artifacts.Open(ctx, p.services)
	var instance any
	if err == nil {
		instance, err = resources.Construct(ctx, p.factory)
	}
	if err != nil {
		diagnostics.Log(ctx, p.Logger(), slog.LevelError, "reactor replay notification activation failed", "reactor", "replay_activate", err)
		return nil
	}
	if notifier, ok := instance.(ReplayNotifier); ok {
		switch state {
		case BeginReplay:
			return notifier.BeginReplay(ctx)
		case EndReplay:
			return notifier.EndReplay(ctx)
		}
	}
	if notifier, ok := instance.(PartitionReplayNotifier); ok {
		switch state {
		case BeginReplayPartition:
			return notifier.BeginReplayPartition(ctx, partition)
		case EndReplayPartition:
			return notifier.EndReplayPartition(ctx, partition)
		}
	}
	return nil
}
