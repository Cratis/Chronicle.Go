// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"log/slog"
	"sync"

	"github.com/cratis/chronicle.go/internal/diagnostics"
)

type connectionNotification struct {
	ctx       context.Context
	event     ConnectionEvent
	connected bool
}

type connectionHooks struct {
	mu                      sync.Mutex
	once                    sync.Once
	connected, disconnected []ConnectionHook
	queue                   []connectionNotification
	wake                    chan struct{}
	finished                bool
}

func (h *connectionHooks) start(c *Client) {
	h.once.Do(func() {
		// The supervisor retains a work lease while admitting this worker.
		c.work.Add(1)
		go func() { defer c.work.Done(); h.dispatch(c) }()
	})
}

func (h *connectionHooks) signal() {
	select {
	case h.wake <- struct{}{}:
	default:
	}
}

func (h *connectionHooks) publishConnected(g *generation) {
	h.mu.Lock()
	h.queue = append(h.queue, connectionNotification{ctx: g.ctx, connected: true,
		event: ConnectionEvent{Generation: g.number, ConnectionID: g.id, Address: g.address}})
	h.signal()
	h.mu.Unlock()
}

func (h *connectionHooks) publishDisconnected(g *generation, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	last := len(h.queue) - 1
	if last >= 0 && h.queue[last].connected && h.queue[last].event.Generation == g.number {
		// The dispatcher has not started this generation: coalesce both events.
		h.queue[last] = connectionNotification{}
		h.queue = h.queue[:last]
		return
	}
	h.queue = append(h.queue, connectionNotification{ctx: g.client.life,
		event: ConnectionEvent{Generation: g.number, ConnectionID: g.id, Address: g.address, Err: err}})
	h.signal()
}

// finish is called only after the supervisor has joined: no publishers remain.
func (h *connectionHooks) finish() {
	h.mu.Lock()
	h.finished = true
	h.signal()
	h.mu.Unlock()
}

func (h *connectionHooks) dispatch(c *Client) {
	for {
		h.mu.Lock()
		if len(h.queue) == 0 {
			finished := h.finished
			h.mu.Unlock()
			if finished {
				return
			}
			<-h.wake
			continue
		}
		notification := h.queue[0]
		h.queue[0] = connectionNotification{}
		h.queue = h.queue[1:]
		h.mu.Unlock()
		hooks, stage := h.disconnected, "disconnected"
		if notification.connected {
			hooks, stage = h.connected, "connected"
		}
		var work sync.WaitGroup
		for _, hook := range hooks {
			work.Add(1)
			go func() {
				defer work.Done()
				defer func() {
					if recover() != nil {
						diagnostics.Log(notification.ctx, c.config.logger, slog.LevelError, "connection hook panicked", "client", stage, &diagnostics.PanicError{})
					}
				}()
				hook(notification.ctx, notification.event)
			}()
		}
		work.Wait()
	}
}
