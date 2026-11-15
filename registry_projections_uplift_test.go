// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/projections"
)

func TestRegistryRejectsGlobalHandlerRegisteredAsReadModel(t *testing.T) {
	registry, _ := projectionRegistry(t)
	global, err := projections.Global[ProjectionModel](projections.GlobalFor[ProjectionModel]())
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.AddProjection(global); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(WithRegistry(registry))
	if client != nil {
		_ = client.Close()
		t.Fatal("invalid global published a client")
	}
	if !errors.Is(err, ErrInvalidConfiguration) || err.Error() != "chronicle: invalid configuration: global handler must not be registered as a read model" {
		t.Fatalf("global model conflict = %v", err)
	}
}
