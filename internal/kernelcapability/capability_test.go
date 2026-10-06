// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package kernelcapability

import (
	"context"
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
)

func TestCapabilitiesFollowFixReleases(t *testing.T) {
	for _, tc := range []struct {
		version             string
		mixedAll, protected bool
	}{
		{"", false, false},
		{"test-kernel", false, false},
		{"19.32", false, false},
		{"19.32.x", false, false},
		{"019.32.3", false, false},
		{"19.31.9", false, false},
		{"19.32.0", false, false},
		{"19.32.1-rc.1", false, false},
		{"19.32.1", true, false},
		{"19.32.1-development", true, false},
		{"19.32.2-rc.1", true, false},
		{"19.32.2", true, true},
		{"19.32.2-development", true, true},
		{"19.32.3-development", true, true},
		{"19.33.0-rc.1", true, true},
		{"20.0.0", true, true},
	} {
		got := FromVersion(tc.version)
		if got.MixedAllReplay != tc.mixedAll || got.ProtectedRelease != tc.protected {
			t.Errorf("FromVersion(%q) = %+v", tc.version, got)
		}
	}
}

type provider Capabilities

func (p provider) KernelCapabilities(context.Context) (Capabilities, error) {
	return Capabilities(p), nil
}

func TestUnreportedCapabilitiesAreAbsent(t *testing.T) {
	if got, err := Of(t.Context(), struct{}{}); err != nil || got != (Capabilities{}) {
		t.Fatalf("connection without a provider: %+v %v", got, err)
	}
	if got, err := Of(t.Context(), provider{ProtectedRelease: true}); err != nil || !got.ProtectedRelease {
		t.Fatalf("provider: %+v %v", got, err)
	}
	err := Precheck(t.Context(), struct{}{}, NeedProtectedRevision)
	if !errors.Is(err, faults.ErrUnsupported) || err.Error() != "chronicle: unsupported capability: protected revision requires Chronicle 19.32.2 or later" {
		t.Fatalf("refusal = %v", err)
	}
	if err := Precheck(t.Context(), provider{ProtectedRelease: true}, NeedProtectedRevision); err != nil {
		t.Fatal(err)
	}
	if err := Precheck(t.Context(), provider{ProtectedRelease: true}, NeedMixedAllReplay); err == nil || err.Error() != "chronicle: unsupported capability: mixed all-event projection replay requires Chronicle 19.32.1 or later" {
		t.Fatalf("mixed refusal = %v", err)
	}
}

func TestTrackedNeedsAreCheckedAgainstTheDispatchingKernel(t *testing.T) {
	if Add(t.Context(), NeedMixedAllReplay) {
		t.Fatal("untracked context accepted a need")
	}
	if Check(t.Context(), Capabilities{}) != nil {
		t.Fatal("untracked context refused")
	}
	parent := With(t.Context(), NeedMixedAllReplay)
	child := Track(parent)
	if !Add(child, NeedProtectedProjectionRead) {
		t.Fatal("tracked context refused a need")
	}
	mixedOnly := Capabilities{MixedAllReplay: true}
	if err := Check(parent, mixedOnly); err != nil {
		t.Fatal("child need leaked into parent", err)
	}
	if err := Check(child, mixedOnly); !errors.Is(err, faults.ErrUnsupported) {
		t.Fatal("child need not checked", err)
	}
	if err := Check(child, Capabilities{}); err == nil || err.Error() != NeedMixedAllReplay.Refusal().Error() {
		t.Fatal("inherited need not checked first", err)
	}
	if err := Check(child, Capabilities{MixedAllReplay: true, ProtectedRelease: true}); err != nil {
		t.Fatal(err)
	}
}
