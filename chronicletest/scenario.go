// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package chronicletest provides isolated scenarios using production Chronicle
// client plans. The substitute is not a Go implementation of the Chronicle kernel.
// Scenarios are serial fixtures: do not call their methods concurrently. Create
// one per test and close it, or use New* helpers for testing.T-owned cleanup.
package chronicletest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/serialization"
	"github.com/google/uuid"
)

// Engine selects the event storage/execution boundary.
type Engine uint8

const (
	// Substitute uses a private bufconn server; only append/read storage is substituted.
	Substitute Engine = iota
	// Kernel uses the real server at Config.ConnectionString. Recommended for fidelity.
	Kernel
)

// Config selects isolated scenario resources. Empty Store creates a unique store;
// Namespace defaults to "Default". Explicit stores are borrowed: use an isolated
// test store. Close never deletes remote data. Registry and Services are borrowed;
// registry declarations are frozen at construction. No global registration exists.
type Config struct {
	// Registry supplies the same declarations as the production client.
	Registry *chronicle.Registry
	// Engine selects event storage; reducers and reactors always invoke locally.
	Engine Engine
	// ConnectionString is required for Kernel; there is no implicit endpoint.
	ConnectionString string
	// Store defaults to a unique test-owned name.
	Store chronicle.StoreName
	// Namespace defaults to the production Default namespace.
	Namespace chronicle.Namespace
	// Naming defaults to production preserved property spelling.
	Naming serialization.NamingPolicy
	// Services optionally supplies borrowed scopes; nil needs no container.
	Services reactors.ScopeFactory
	// Development explicitly permits the kernel's self-signed development certificate.
	Development bool
}

// CSharpScenarioNaming is the explicit C# scenario-compatible naming option.
// Production Go defaults preserve field spelling; explicit JSON tags always win.
const CSharpScenarioNaming = serialization.CamelCase

var (
	// ErrFidelityUnavailable means the requested layer is substituted or unproven.
	ErrFidelityUnavailable = errors.New("scenario cannot prove requested fidelity")
	// ErrKernelUnavailable means no kernel connection string was supplied.
	ErrKernelUnavailable = errors.New("kernel scenario requires a connection string")
	// ErrAmbiguousInstance requires selection by key when multiple instances exist.
	ErrAmbiguousInstance = errors.New("multiple read model instances; select a key")
)

// Layer identifies a fidelity boundary, not a claim that every behavior is tested.
type Layer string

const (
	// Constraints is append-time invariant enforcement.
	Constraints Layer = "constraint enforcement"
	// Concurrency is expected-position enforcement.
	Concurrency Layer = "concurrency enforcement"
	// Encryption includes protected data and key lifecycle.
	Encryption Layer = "encryption and protected data"
	// ProjectionExecution is kernel projection evaluation.
	ProjectionExecution Layer = "projection execution"
	// ObserverLifecycle includes live delivery, retries and quarantine.
	ObserverLifecycle Layer = "observer lifecycle, retries and quarantine"
	// DurableStorage is persistence beyond the scenario's process.
	DurableStorage Layer = "durable storage"
	// DeliveryMetadata distinguishes actual deliveries from synthetic contexts.
	DeliveryMetadata Layer = "kernel delivery metadata"
	// ReadModelStorage is the actual model sink, not seeded dependency JSON.
	ReadModelStorage Layer = "read model storage"
	// EffectAcceptance is execution of returned effects, not recording.
	EffectAcceptance Layer = "side-effect transport acceptance"
	// MultiNode is distributed behavior, unproven by these fixtures.
	MultiNode Layer = "multi-node behavior"
	// SchemaValidation is kernel generation/schema validation and migration.
	SchemaValidation Layer = "kernel schema validation and migration"
	// EventHashing is the kernel's hash calculation, absent in the substitute.
	EventHashing Layer = "event hashing"
	// Authorization is kernel authorization rather than permissive fake envelopes.
	Authorization Layer = "kernel authorization"
)

// Fidelity reports substituted/unproven layers. Use Require before assertions
// whose correctness depends on these layers. A successful Require is not itself
// evidence that a behavior occurred; assert an observable result as well.
type Fidelity struct {
	substituted []Layer
	initialized bool
}

func knownLayers() []Layer {
	return []Layer{Constraints, Concurrency, Encryption, ProjectionExecution, ObserverLifecycle, DurableStorage, DeliveryMetadata, ReadModelStorage, EffectAcceptance, MultiNode, SchemaValidation, EventHashing, Authorization}
}

// Substitutions returns a detached list. A zero Fidelity substitutes every layer.
// Unknown layers always fail Require.
func (f Fidelity) Substitutions() []Layer {
	if !f.initialized {
		return knownLayers()
	}
	return append([]Layer(nil), f.substituted...)
}

// Require refuses any substituted or unknown layer. No arguments requires every
// known layer, intentionally refusing blanket full-fidelity claims.
func (f Fidelity) Require(layers ...Layer) error {
	if !f.initialized {
		return fmt.Errorf("%w: uninitialized fidelity", ErrFidelityUnavailable)
	}
	known := knownLayers()
	if len(layers) == 0 {
		layers = known
	}
	for _, layer := range layers {
		recognized := false
		for _, candidate := range known {
			if candidate == layer {
				recognized = true
			}
		}
		if !recognized {
			return fmt.Errorf("%w: unknown layer %q", ErrFidelityUnavailable, layer)
		}
		for _, substitute := range f.substituted {
			if substitute == layer {
				return fmt.Errorf("%w: %s", ErrFidelityUnavailable, layer)
			}
		}
	}
	return nil
}

func localFidelity() Fidelity {
	return Fidelity{substituted: knownLayers(), initialized: true}
}
func kernelFidelity() Fidelity {
	return Fidelity{substituted: []Layer{Encryption, MultiNode, SchemaValidation}, initialized: true}
}

func configuration(config Config) (Config, []chronicle.ClientOption, error) {
	if config.Engine != Substitute && config.Engine != Kernel {
		return config, nil, chronicle.ErrInvalidConfiguration
	}
	if config.Store == "" {
		config.Store = chronicle.StoreName("scenario-" + uuid.NewString())
	}
	if config.Namespace == "" {
		config.Namespace = chronicle.DefaultNamespace
	}
	options := []chronicle.ClientOption{chronicle.WithRegistry(config.Registry), chronicle.WithNamingPolicy(config.Naming)}
	if config.Services != nil {
		options = append(options, chronicle.WithServices(config.Services))
	}
	if config.ConnectionString != "" {
		options = append(options, chronicle.WithConnectionString(config.ConnectionString))
	}
	if config.Development {
		options = append(options, chronicle.WithDevelopmentDefaults())
	}
	return config, options, nil
}

func cleanup(t *testing.T, close func() error) {
	t.Helper()
	t.Cleanup(func() {
		if err := close(); err != nil {
			t.Errorf("close scenario: %v", err)
		}
	})
}
func constructionFailure(t *testing.T, err error) {
	t.Helper()
	if errors.Is(err, ErrKernelUnavailable) {
		t.Skipf("kernel scenario skipped: %v (set CHRONICLE_INTEGRATION_CONNECTION_STRING)", err)
	}
	t.Fatalf("create scenario: %v", err)
}

func synthetic(config Config, sequence events.SequenceID, descriptor events.Descriptor, source events.SourceID, number events.SequenceNumber) events.Context {
	return events.Context{Store: config.Store, Namespace: config.Namespace, Sequence: sequence, EventType: descriptor.Ref(), SourceID: source,
		SequenceNumber: number, SourceType: events.DefaultSourceType, StreamType: events.AllStreamTypes, StreamID: events.DefaultStreamID, Subject: events.Subject(source), Occurred: time.Unix(0, int64(number)).UTC(), Tags: descriptor.Tags(),
		CorrelationID: metadata.CorrelationID(uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("%s/%s/%d", config.Store, sequence, number))))}
}

func closeLease(ctx context.Context, close func(context.Context) error) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return close(cleanup)
}
