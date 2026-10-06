// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/readmodels"
)

type binaryFallbackIdentity struct {
	ID   []byte
	Name string `chronicle:"pii"`
}
type binarySerializedIdentity struct {
	Payload []byte `json:"id"`
}
type binaryPascalIdentity struct {
	Payload []byte `json:"Id"`
}

type binaryUpperIdentity struct {
	Payload []byte `json:"ID"`
}
type binaryMixedIdentity struct {
	Payload []byte `json:"iD"`
}
type binaryMongoIdentity struct {
	Payload []byte `json:"_id"`
}

func TestBinaryReadModelIdentitiesRefuseBeforeRegistration(t *testing.T) {
	for name, define := range map[string]func() error{
		"subject fallback": func() error { _, err := readmodels.Define[binaryFallbackIdentity](); return err },
		"serialized id":    func() error { _, err := readmodels.Define[binarySerializedIdentity](); return err },
		"serialized Id":    func() error { _, err := readmodels.Define[binaryPascalIdentity](); return err },
		"serialized ID":    func() error { _, err := readmodels.Define[binaryUpperIdentity](); return err },
		"serialized iD":    func() error { _, err := readmodels.Define[binaryMixedIdentity](); return err },
		"serialized _id":   func() error { _, err := readmodels.Define[binaryMongoIdentity](); return err },
	} {
		t.Run(name, func(t *testing.T) {
			if err := define(); !errors.Is(err, chronicle.ErrInvalidConfiguration) && !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatalf("binary model identity admitted: %v", err)
			}
		})
	}
}
