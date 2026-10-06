// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events_test

import (
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
)

type binaryDeclarationEvent struct {
	Payload  []byte
	Optional *[]byte
	Nested   struct{ Payload []byte }
}

type binaryDeclarationModel struct {
	Payload  []byte
	Optional *[]byte
}

type binaryUniqueTag struct {
	Payload []byte `chronicle:"unique"`
}
type binaryUniqueObjectTag struct {
	Nested struct{ Payload []byte } `chronicle:"unique"`
}
type binarySubjectTag struct {
	Payload []byte `chronicle:"subject"`
}
type binaryNullableSubjectTag struct {
	Payload *[]byte `chronicle:"subject"`
}
type binaryIndexTag struct {
	Payload []byte `chronicle:"index"`
}
type binaryIndexObjectTag struct {
	Nested struct{ Payload []byte } `chronicle:"index"`
}

func TestBinaryUniqueConstraintsRefuseBeforeRegistration(t *testing.T) {
	for name, define := range map[string]func() error{
		"tag": func() error {
			_, err := events.Define[binaryUniqueTag]()
			return err
		},
		"object tag": func() error {
			_, err := events.Define[binaryUniqueObjectTag]()
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := define(); !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatalf("binary unique tag admitted: %v", err)
			}
		})
	}
	event, err := events.Define[binaryDeclarationEvent]()
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"Payload", "Optional", "Nested", "Nested.Payload"} {
		t.Run(path, func(t *testing.T) {
			_, err := constraints.UniqueValues("binary").On(event.Descriptor(), path).Build()
			if !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatalf("binary unique property admitted: %v", err)
			}
		})
	}
}

func TestBinarySubjectsRefuseBeforeRegistration(t *testing.T) {
	for name, define := range map[string]func() error{
		"event tag": func() error {
			_, err := events.Define[binarySubjectTag]()
			return err
		},
		"nullable event tag": func() error {
			_, err := events.Define[binaryNullableSubjectTag]()
			return err
		},
		"model tag": func() error {
			_, err := readmodels.Define[binarySubjectTag]()
			return err
		},
		"nullable model tag": func() error {
			_, err := readmodels.Define[binaryNullableSubjectTag]()
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := define(); !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatalf("binary subject tag admitted: %v", err)
			}
		})
	}
	for _, path := range []string{"Payload", "Optional"} {
		t.Run("explicit "+path, func(t *testing.T) {
			_, err := readmodels.Define[binaryDeclarationModel](readmodels.WithSubjectProperty(path))
			if !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatalf("binary subject property admitted: %v", err)
			}
		})
	}
}

func TestBinaryIndexesRefuseBeforeRegistration(t *testing.T) {
	for _, path := range []string{"Payload", "Optional", "Nested", "Nested.Payload"} {
		t.Run(path, func(t *testing.T) {
			var err error
			if path == "Nested" || path == "Nested.Payload" {
				_, err = readmodels.Define[binaryDeclarationEvent](readmodels.WithIndexes(path))
			} else {
				_, err = readmodels.Define[binaryDeclarationModel](readmodels.WithIndexes(path))
			}
			if !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatalf("binary index admitted: %v", err)
			}
		})
	}
	for name, define := range map[string]func() error{
		"tag": func() error {
			_, err := readmodels.Define[binaryIndexTag]()
			return err
		},
		"object tag": func() error {
			_, err := readmodels.Define[binaryIndexObjectTag]()
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := define(); !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatalf("binary index tag admitted: %v", err)
			}
		})
	}
}
