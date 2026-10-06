// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package kernelcapability records which kernel defect fixes a connected server
// is known to contain. Wire compatibility alone is not evidence: Chronicle
// 19.32.0 through 19.32.3 share contracts but not these fixes.
package kernelcapability

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/cratis/chronicle.go/internal/faults"
)

// Capabilities are positive evidence only; the zero value admits nothing.
type Capabilities struct {
	// MixedAllReplay means replay and history fold every event a projection
	// that subscribes to all events handles live (Chronicle#4562, 19.32.1).
	MixedAllReplay bool
	// ProtectedRelease means revisions are protected with the original subject
	// (Chronicle#4525), projection replay releases each value with its original
	// subject (Chronicle#4561), and metadata beneath maps and on collection-valued
	// array elements is applied (Chronicle#4551, #4552); all fixed in 19.32.2.
	ProtectedRelease bool
}

// Minimum kernel releases for each capability.
const (
	MixedAllReplayVersion   = "19.32.1"
	ProtectedReleaseVersion = "19.32.2"
)

// FromVersion derives capabilities from a reported kernel version. Unknown or
// unparseable versions have none. A "-development" image counts as its release;
// any other pre-release only counts when it is newer than the minimum release.
func FromVersion(version string) Capabilities {
	return Capabilities{
		MixedAllReplay:   atLeast(version, MixedAllReplayVersion),
		ProtectedRelease: atLeast(version, ProtectedReleaseVersion),
	}
}

func atLeast(version, minimum string) bool {
	core, suffix, prerelease := strings.Cut(version, "-")
	if prerelease && suffix == "development" {
		prerelease = false
	}
	have, ok := parse(core)
	if !ok {
		return false
	}
	want, _ := parse(minimum)
	for i := range have {
		if have[i] != want[i] {
			return have[i] > want[i]
		}
	}
	return !prerelease
}

func parse(version string) ([3]int, bool) {
	var result [3]int
	parts := strings.Split(version, ".")
	if len(parts) != 3 {
		return result, false
	}
	for i, part := range parts {
		n, err := strconv.Atoi(part)
		if err != nil || n < 0 || strconv.Itoa(n) != part {
			return result, false
		}
		result[i] = n
	}
	return result, true
}

// Provider reports the capabilities of the connection an operation will most
// likely use. The report is only a fast pre-check: a reconnect can change the
// kernel before dispatch. Transports that report capabilities must enforce the
// requirements tracked in the call context against the generation that actually
// dispatches (see Check).
type Provider interface {
	KernelCapabilities(context.Context) (Capabilities, error)
}

// Of returns the capabilities reported by conn, or none.
func Of(ctx context.Context, conn any) (Capabilities, error) {
	provider, ok := conn.(Provider)
	if !ok {
		return Capabilities{}, nil
	}
	return provider.KernelCapabilities(ctx)
}

// Need names one capability an operation depends on.
type Need struct {
	feature, minimum string
	has              func(Capabilities) bool
}

// The capabilities gated operations depend on, with payload-free feature names.
var (
	NeedMixedAllReplay             = Need{"mixed all-event projection replay", MixedAllReplayVersion, func(c Capabilities) bool { return c.MixedAllReplay }}
	NeedProtectedRevision          = Need{"protected revision", ProtectedReleaseVersion, func(c Capabilities) bool { return c.ProtectedRelease }}
	NeedProtectedProjectionRead    = Need{"protected projection replay release", ProtectedReleaseVersion, func(c Capabilities) bool { return c.ProtectedRelease }}
	NeedNestedCollectionProtection = Need{"protection beneath maps or on collection-valued array elements", ProtectedReleaseVersion, func(c Capabilities) bool { return c.ProtectedRelease }}
)

// Refusal returns the payload-free ErrUnsupported naming the minimum release.
func (n Need) Refusal() error {
	return fmt.Errorf("%w: %s requires Chronicle %s or later", faults.ErrUnsupported, n.feature, n.minimum)
}

// Satisfied reports whether have contains the capability.
func (n Need) Satisfied(have Capabilities) bool { return n.has != nil && n.has(have) }

// Precheck refuses early when conn does not report the capability. It is not
// authoritative; Track the operation and Add the need for the dispatch check.
func Precheck(ctx context.Context, conn any, n Need) error {
	have, err := Of(ctx, conn)
	if err != nil {
		return err
	}
	if !n.Satisfied(have) {
		return n.Refusal()
	}
	return nil
}

type requirementsKey struct{}

// requirements accumulate the needs of one operation. Adds and checks may run
// on different goroutines; the mutex guards only the slice, never callbacks.
type requirements struct {
	mu    sync.Mutex
	needs []Need
}

func (r *requirements) snapshot() []Need {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Need(nil), r.needs...)
}

// Track returns a context with a fresh requirement set that inherits the needs
// already tracked by ctx. Adding to it never changes the parent's set.
func Track(ctx context.Context) context.Context {
	tracked := &requirements{}
	if parent, ok := ctx.Value(requirementsKey{}).(*requirements); ok && parent != nil {
		tracked.needs = parent.snapshot()
	}
	return context.WithValue(ctx, requirementsKey{}, tracked)
}

// Without returns ctx with no tracked requirements, keeping its other values
// and cancellation. Shared work that a gated operation merely triggers, such as
// connecting or registering a generation, must not inherit that operation's
// needs: it serves every caller and needs none of their kernel fixes.
func Without(ctx context.Context) context.Context {
	if tracked, _ := ctx.Value(requirementsKey{}).(*requirements); tracked == nil {
		return ctx
	}
	return context.WithValue(ctx, requirementsKey{}, (*requirements)(nil))
}

// Add records n for every later dispatch under ctx. It reports false when ctx
// is not tracked; callers must then refuse rather than dispatch unchecked.
func Add(ctx context.Context, n Need) bool {
	tracked, ok := ctx.Value(requirementsKey{}).(*requirements)
	if !ok || tracked == nil {
		return false
	}
	tracked.mu.Lock()
	tracked.needs = append(tracked.needs, n)
	tracked.mu.Unlock()
	return true
}

// With tracks ctx and records n.
func With(ctx context.Context, n Need) context.Context {
	ctx = Track(ctx)
	Add(ctx, n)
	return ctx
}

// Check is the authoritative dispatch-time check. Transports call it with the
// capabilities of the generation that will send the RPC, after pinning it and
// before dispatching; a failure must be reported as not dispatched.
func Check(ctx context.Context, have Capabilities) error {
	tracked, ok := ctx.Value(requirementsKey{}).(*requirements)
	if !ok || tracked == nil {
		return nil
	}
	for _, n := range tracked.snapshot() {
		if !n.Satisfied(have) {
			return n.Refusal()
		}
	}
	return nil
}
