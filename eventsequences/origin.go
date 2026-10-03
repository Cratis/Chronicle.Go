// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"context"
	"sync/atomic"
)

// Origin is an opaque, comparable identity for local append attribution. The zero
// value means unattributed. Copy and compare values directly; an origin is not a
// correlation ID, durable identity, wire value or authorization credential.
// Origins remain valid for the lifetime of the process, not across restarts.
type Origin struct{ id uint64 }

var originCounter atomic.Uint64

// NewOrigin returns a nonzero identity never reused within this process, even
// after callers release it. It is safe for concurrent use. It panics if all
// uint64 identities have been allocated rather than reusing an identity.
func NewOrigin() Origin {
	for {
		prior := originCounter.Load()
		if prior == ^uint64(0) {
			panic("chronicle: append origin identities exhausted")
		}
		if originCounter.CompareAndSwap(prior, prior+1) {
			return Origin{id: prior + 1}
		}
	}
}

type originKey struct{}

// WithOrigin returns a context carrying origin for immediate append notifications.
// A zero origin masks an inherited origin. It does not change correlation or wire
// metadata. Unit-of-work commits use the unit's own origin instead.
func WithOrigin(ctx context.Context, origin Origin) context.Context {
	return context.WithValue(ctx, originKey{}, origin)
}

// OriginFrom returns the installed origin, or zero when none is installed.
func OriginFrom(ctx context.Context) Origin {
	origin, _ := ctx.Value(originKey{}).(Origin)
	return origin
}
