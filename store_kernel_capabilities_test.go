// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	readmodelcontracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/internal/kernelcapability"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestKernelCapabilitiesAreRecordedFromVerifiedVersion(t *testing.T) {
	for _, tc := range []struct {
		name, version string
		skip          bool
		want          kernelcapability.Capabilities
	}{
		{"19.32.0", "19.32.0", false, kernelcapability.Capabilities{}},
		{"19.32.1", "19.32.1", false, kernelcapability.Capabilities{MixedAllReplay: true}},
		{"19.32.2", "19.32.2", false, kernelcapability.Capabilities{MixedAllReplay: true, ProtectedRelease: true}},
		{"development", "19.32.3-development", false, kernelcapability.Capabilities{MixedAllReplay: true, ProtectedRelease: true}},
		{"unknown", "test-kernel", false, kernelcapability.Capabilities{}},
		{"skipped verification", "19.32.3", true, kernelcapability.Capabilities{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var options []ClientOption
			if tc.skip {
				options = append(options, WithSkipCompatibilityCheck())
			}
			client, ctx := supervisionClient(t, &supervisedKernel{}, options...)
			client.config.borrowed = &decisionProfileConn{ClientConnInterface: client.config.borrowed, version: tc.version, protocol: "19.32.3"}
			store, err := client.EventStore(ctx, "store")
			if err != nil {
				t.Fatal(err)
			}
			got, err := (&clientTransport{client: client, store: store}).KernelCapabilities(ctx)
			if err != nil || got != tc.want {
				t.Fatalf("capabilities = %+v, %v; want %+v", got, err, tc.want)
			}
		})
	}
}

type nestedProtectionContact struct {
	Email string `chronicle:"pii"`
}
type nestedProtectionEvent struct {
	Contacts map[string]nestedProtectionContact
}
type nestedProtectionNames []string
type nestedProtectionModel struct {
	ID   string
	Rows []nestedProtectionNames
}

type countingReadModels struct {
	readmodelcontracts.UnimplementedReadModelsServer
	registrations *atomic.Int32
}

func (k countingReadModels) RegisterMany(context.Context, *readmodelcontracts.RegisterManyRequest) (*emptypb.Empty, error) {
	k.registrations.Add(1)
	return &emptypb.Empty{}, nil
}

// Kernels before 19.32.2 skip metadata beneath maps and on collection-valued
// array elements (Chronicle#4551, #4552); registration refuses before any RPC.
func TestNestedProtectionRegistrationRequiresProtectedReleaseKernel(t *testing.T) {
	for _, tc := range []struct {
		name, version string
		model         bool
		admitted      bool
	}{
		{"event on 19.32.1", "19.32.1", false, false},
		{"model on 19.32.1", "19.32.1", true, false},
		{"event on unknown", "test-kernel", false, false},
		{"event on 19.32.2", "19.32.2", false, true},
		{"model on 19.32.2", "19.32.2", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := NewRegistry()
			if tc.model {
				if _, err := RegisterReadModel[nestedProtectionModel](registry, readmodels.WithProtection(compliance.For[nestedProtectionNames](compliance.Classification{PII: true}))); err != nil {
					t.Fatal(err)
				}
			} else if _, err := RegisterEvent[nestedProtectionEvent](registry); err != nil {
				t.Fatal(err)
			}
			var eventTypeRPCs, modelRPCs atomic.Int32
			kernel := &supervisedKernel{registerEventTypes: func(context.Context, *eventtypes.RegisterEventTypesRequest) error {
				eventTypeRPCs.Add(1)
				return nil
			}, readModels: countingReadModels{registrations: &modelRPCs}}
			client, ctx := supervisionClient(t, kernel, WithRegistry(registry))
			client.config.borrowed = &decisionProfileConn{ClientConnInterface: client.config.borrowed, version: tc.version, protocol: "19.32.3"}
			_, err := client.EventStore(ctx, "store")
			if tc.admitted {
				if err != nil || (tc.model && modelRPCs.Load() == 0) {
					t.Fatal(err, modelRPCs.Load())
				}
				return
			}
			if !errors.Is(err, ErrUnsupported) || !strings.Contains(err.Error(), "requires Chronicle 19.32.2") {
				t.Fatalf("nested protection registered on %s: %v", tc.version, err)
			}
			if (!tc.model && eventTypeRPCs.Load() != 0) || modelRPCs.Load() != 0 {
				t.Fatal("refused definitions reached the kernel")
			}
		})
	}
}
