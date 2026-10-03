// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import "context"

// AppendOriginResolver selects local attribution metadata from the actual append
// context. It cannot replace that context. Handled true selects the returned
// origin, including zero; false falls back to OriginFrom(ctx). Errors and panics
// fail before any RPC or append notification, without fallback. SDK-owned unit
// commits bypass the resolver and always use the unit's origin.
//
// The callback is borrowed, synchronous and invoked outside SDK locks. It must
// support concurrent calls, honor cancellation and return promptly. It owns no
// transport or request lifetime. The SDK neither retains ctx nor starts a worker
// to enforce cancellation. Nil preserves the OriginFrom behavior.
type AppendOriginResolver func(context.Context) (origin Origin, handled bool, err error)

// AppendOriginResolverProvider optionally supplies an immutable callback on a
// caller-owned connection passed to New. New reads it once; Chronicle clients
// supply their configured callback automatically. The provider must not perform
// I/O or invoke the callback while supplying it.
type AppendOriginResolverProvider interface {
	AppendOriginResolver() AppendOriginResolver
}

// AppendOriginResolutionError means local attribution failed before dispatch.
// The result is Rejected, no RPC was made and no append notification was emitted.
// It deliberately retains neither the callback error nor panic payload, and does
// not unwrap them: callback failures can contain sensitive context values.
type AppendOriginResolutionError struct {
	// Panicked distinguishes a recovered callback panic from a returned error.
	Panicked bool
}

// Error returns a fixed, payload-free diagnostic.
func (e *AppendOriginResolutionError) Error() string {
	if e.Panicked {
		return "chronicle: append origin resolver panicked before dispatch"
	}
	return "chronicle: append origin resolver failed before dispatch"
}

func (s *Sequence) resolveAppendOrigin(ctx context.Context) (origin Origin, err error) {
	if s.appendOriginResolver == nil {
		return OriginFrom(ctx), nil
	}
	defer func() {
		if recover() != nil {
			origin, err = Origin{}, &AppendOriginResolutionError{Panicked: true}
		}
	}()
	origin, handled, failure := s.appendOriginResolver(ctx)
	if failure != nil {
		return Origin{}, &AppendOriginResolutionError{}
	}
	if !handled {
		origin = OriginFrom(ctx)
	}
	return origin, nil
}
