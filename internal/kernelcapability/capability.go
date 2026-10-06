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

// Provider reports the capabilities of the connection an operation will use.
// Connections that do not implement it are treated as having no capabilities.
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

// Require returns ErrUnsupported naming the minimum kernel release when
// supported is false.
func Require(supported bool, feature, minimum string) error {
	if supported {
		return nil
	}
	return fmt.Errorf("%w: %s requires Chronicle %s or later", faults.ErrUnsupported, feature, minimum)
}
