// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"

	"github.com/cratis/chronicle.go/internal/faults"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type definitionTransport struct {
	*generationTransport
	store       *EventStore
	root        *definitionRoot
	destructive bool
}

func (s *EventStore) definitionTransport(g *generation, root *definitionRoot, destructive bool) *definitionTransport {
	return &definitionTransport{generationTransport: g.transport, store: s, root: root, destructive: destructive}
}

func (t *definitionTransport) Invoke(ctx context.Context, method string, args, reply any, options ...grpc.CallOption) error {
	ctx, err := authorize(ctx, t.generation.tokens)
	if err != nil {
		return &faults.BeforeDispatch{Cause: err}
	}
	c, d, g := t.store.client, t.store.definitions, t.generation
	for {
		c.mu.Lock()
		switch {
		case c.closed:
			err = ErrClosed
		case ctx.Err() != nil:
			err = ctx.Err()
		case g.ctx.Err() != nil:
			err = g.ctx.Err()
		case d.root != t.root:
			err = errDefinitionSuperseded
		}
		if err != nil {
			c.mu.Unlock()
			return &faults.BeforeDispatch{Cause: err}
		}
		if t.destructive && d.flight {
			changed := d.changed
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return &faults.BeforeDispatch{Cause: ctx.Err()}
			case <-g.ctx.Done():
				return &faults.BeforeDispatch{Cause: g.ctx.Err()}
			case <-changed:
				continue
			}
		}
		if t.destructive {
			d.flight = true
		}
		// Close races admission, not the physical invocation after admission.
		g.work.Add(1)
		c.work.Add(1)
		c.mu.Unlock()
		break
	}
	err = func() error {
		defer g.work.Done()
		defer c.work.Done()
		known := false
		if t.destructive {
			defer func() {
				// A panic has no acknowledged disposition. Latch uncertainty and
				// release the flight atomically before propagating it unchanged.
				c.mu.Lock()
				if !known {
					d.destructiveUnknown = true
				}
				d.flight = false
				d.notifyLocked()
				c.mu.Unlock()
			}()
		}
		rawErr := g.raw.Invoke(ctx, method, args, reply, t.options(options)...)
		// Validate the raw acknowledgement while the destructive flight is held,
		// before context joining or token invalidation can obscure its disposition.
		if rawErr == nil {
			ack, ok := reply.(*emptypb.Empty)
			if !ok || ack == nil || len(ack.ProtoReflect().GetUnknown()) != 0 {
				rawErr = ErrProtocol
			}
		}
		if t.destructive {
			var before *faults.BeforeDispatch
			known = rawErr == nil || status.Code(rawErr) == codes.InvalidArgument || errors.As(rawErr, &before)
		}
		return rawErr
	}()
	invalidateRejectedToken(g.tokens, err)
	if err != nil && ctx.Err() != nil {
		return errors.Join(err, ctx.Err())
	}
	return err
}
