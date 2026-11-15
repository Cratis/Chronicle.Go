// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/readmodels"
)

type validationModel struct{ Value string }

func TestModelConfigurationValidationOrder(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options []readmodels.ModelOption
		message string
	}{
		{"nil first", []readmodels.ModelOption{readmodels.WithIdentifier(""), nil}, "nil model option"},
		{"identity before observer", []readmodels.ModelOption{readmodels.WithIdentifier(""), readmodels.WithObserver(0, "")}, "nonblank model identity, container, display, sequence and positive generation required"},
		{"display", []readmodels.ModelOption{readmodels.WithDisplayName(" ")}, "nonblank model identity, container, display, sequence and positive generation required"},
		{"sequence", []readmodels.ModelOption{readmodels.WithEventSequence(" ")}, "nonblank model identity, container, display, sequence and positive generation required"},
		{"observer before sink", []readmodels.ModelOption{readmodels.WithObserver(0, ""), readmodels.WithSink(readmodels.Sink{Type: "unknown"})}, "unknown observer type"},
		{"uppercase UUID", []readmodels.ModelOption{readmodels.WithSink(readmodels.Sink{Type: readmodels.MongoDB, ConfigurationID: "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA"})}, "sink configuration requires a canonical UUID"},
		{"compact UUID", []readmodels.ModelOption{readmodels.WithSink(readmodels.Sink{Type: readmodels.MongoDB, ConfigurationID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})}, "sink configuration requires a canonical UUID"},
		{"UUID before observer ID", []readmodels.ModelOption{readmodels.WithSink(readmodels.Sink{Type: readmodels.NoSink, ConfigurationID: "invalid"})}, "sink configuration requires a canonical UUID"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := readmodels.Define[validationModel](tc.options...)
			if !errors.Is(err, faults.ErrInvalidConfiguration) {
				t.Fatalf("error = %v", err)
			}
			if want := faults.ErrInvalidConfiguration.Error() + ": " + tc.message; err.Error() != want {
				t.Fatalf("error = %q, want %q", err, want)
			}
		})
	}
}

func TestModelSinkCanonicalConfigurationIsPreserved(t *testing.T) {
	const configurationID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	for _, kind := range []readmodels.SinkType{readmodels.MongoDB, readmodels.SQL, readmodels.InMemory, readmodels.NoSink} {
		t.Run(string(kind), func(t *testing.T) {
			want := readmodels.Sink{Type: kind, ConfigurationID: configurationID}
			model, err := readmodels.Define[validationModel](readmodels.WithSink(want), readmodels.WithObserver(readmodels.Projection, "producer"))
			if err != nil {
				t.Fatal(err)
			}
			if got := model.Descriptor().Sink(); got != want {
				t.Fatalf("sink = %+v, want %+v", got, want)
			}
		})
	}
}
