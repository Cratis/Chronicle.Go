// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"time"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/metadata"
)

// WithEventEnrichers appends borrowed callbacks in order, including duplicates.
// The slice is copied at option creation. Any nil entry fails configuration.
// Providers are synchronous and concurrent; they run without SDK locks or work
// leases. They are never closed. Join calls before disposing their dependencies.
func WithEventEnrichers(providers ...events.EventEnricher) ClientOption {
	providers = slices.Clone(providers)
	return func(c *clientConfig) { c.outgoing.Enrichers = append(c.outgoing.Enrichers, providers...) }
}

// WithIdentityProvider selects the borrowed actor provider. Last wins; nil
// disables it. Handled values override context identity, including NotSet.
func WithIdentityProvider(provider metadata.IdentityProvider) ClientOption {
	return func(c *clientConfig) { c.outgoing.Identity = provider }
}

// WithCorrelationProvider selects the borrowed correlation provider. Last wins;
// nil disables it. Explicit append correlation options always bypass it.
func WithCorrelationProvider(provider metadata.CorrelationProvider) ClientOption {
	return func(c *clientConfig) { c.outgoing.Correlation = provider }
}

// WithCausationProvider selects the borrowed complete-chain provider. Last wins;
// nil disables it. A handled empty chain masks context causation.
func WithCausationProvider(provider metadata.CausationProvider) ClientOption {
	return func(c *clientConfig) { c.outgoing.Causation = provider }
}

// WithRootCausation opts into one Root snapshot per CaptureClient, not per option
// creation, append or reconnect. Last wins. Existing upstream Root links win.
// No option means no implicit Root. Host/process information is separately opt-in.
func WithRootCausation(config metadata.RootCausation) ClientOption {
	return func(c *clientConfig) { copy := config; c.rootCausation = &copy }
}

func captureOutgoing(config *clientConfig) error {
	config.outgoing.Enrichers = slices.Clone(config.outgoing.Enrichers)
	for _, provider := range config.outgoing.Enrichers {
		if provider == nil {
			return fmt.Errorf("%w: nil event enricher", ErrInvalidConfiguration)
		}
	}
	if config.rootCausation == nil {
		return nil
	}
	root := config.rootCausation
	facts := map[string]string{
		"softwareVersion": auditFact(root.SoftwareVersion), "softwareCommit": auditFact(root.SoftwareCommit),
		"programIdentifier": auditFact(root.ProgramIdentifier), "goClientVersion": "N/A", "goClientCommit": "N/A",
		"osArchitecture": runtime.GOARCH, "osPlatform": runtime.GOOS,
	}
	// Dependency build info describes the SDK. The main application's VCS
	// settings and local replacement paths are not SDK version/commit facts.
	if build, ok := debug.ReadBuildInfo(); ok {
		modules := append([]*debug.Module{&build.Main}, build.Deps...)
		for _, module := range modules {
			if module.Path == "github.com/cratis/chronicle.go" && module.Replace == nil {
				facts["goClientVersion"] = auditFact(module.Version)
			}
		}
	}
	if root.IncludeMachineName {
		name, err := os.Hostname()
		if err != nil || name == "" {
			return fmt.Errorf("%w: requested machine name unavailable", ErrInvalidConfiguration)
		}
		facts["machineName"] = name
	}
	if root.IncludeProcessID {
		facts["processId"] = strconv.Itoa(os.Getpid())
	}
	config.outgoing.Root = &metadata.Causation{Occurred: time.Now().UTC(), Type: "Root", Properties: facts}
	return nil
}
func auditFact(value string) string {
	if value == "" {
		return "N/A"
	}
	return value
}
