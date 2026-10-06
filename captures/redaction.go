// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package captures

import (
	"fmt"
	"log/slog"

	"github.com/cratis/chronicle.go/internal/faults"
)

// Format redacts all configuration for every fmt verb.
func (a SourceAuthorization) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("captures.SourceAuthorization{redacted}"))
}

// LogValue redacts authorization in structured logs.
func (a SourceAuthorization) LogValue() slog.Value {
	return slog.StringValue("captures.SourceAuthorization{redacted}")
}

// MarshalJSON refuses credential export with ErrUnsupported.
func (a SourceAuthorization) MarshalJSON() ([]byte, error) { return nil, unsupportedJSON() }

// UnmarshalJSON refuses all input with ErrUnsupported, leaving the value intact.
// Unknown or missing discriminators must never downgrade credentials to None.
func (a *SourceAuthorization) UnmarshalJSON([]byte) error { return unsupportedJSON() }

// Format redacts source configuration for every fmt verb.
func (s Source) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("captures.Source{redacted}"))
}

// LogValue redacts source configuration in structured logs.
func (s Source) LogValue() slog.Value { return slog.StringValue("captures.Source{redacted}") }

// MarshalJSON refuses source export with ErrUnsupported, never silently losing authorization.
func (s Source) MarshalJSON() ([]byte, error) { return nil, unsupportedJSON() }

// UnmarshalJSON refuses source import with ErrUnsupported, leaving the value intact.
func (s *Source) UnmarshalJSON([]byte) error { return unsupportedJSON() }

// Format redacts configuration, including mapping literals, for every fmt verb.
func (d Definition) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("captures.Definition{redacted}"))
}

// LogValue redacts the declaration and authorization in structured logs.
func (d Definition) LogValue() slog.Value { return slog.StringValue("captures.Definition{redacted}") }

// MarshalJSON refuses definition export with ErrUnsupported. Declaration explicitly exports CDL only.
func (d Definition) MarshalJSON() ([]byte, error) { return nil, unsupportedJSON() }

// UnmarshalJSON refuses definition import with ErrUnsupported, leaving the value intact.
func (d *Definition) UnmarshalJSON([]byte) error { return unsupportedJSON() }

// Format redacts builder configuration for every fmt verb on both values and pointers.
func (b Builder) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("captures.Builder{redacted}"))
}

// LogValue redacts builder configuration in structured logs.
func (b Builder) LogValue() slog.Value { return slog.StringValue("captures.Builder{redacted}") }

// MarshalJSON refuses builder export with ErrUnsupported.
func (b Builder) MarshalJSON() ([]byte, error) { return nil, unsupportedJSON() }

// UnmarshalJSON refuses builder import with ErrUnsupported, leaving the value intact.
func (b *Builder) UnmarshalJSON([]byte) error { return unsupportedJSON() }

func unsupportedJSON() error {
	return fmt.Errorf("%w: capture configuration JSON import and export are not supported", faults.ErrUnsupported)
}
