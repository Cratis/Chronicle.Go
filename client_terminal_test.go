// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/clients"
)

func TestTerminalCompatibilityRequiresExplicitConnect(t *testing.T) {
	var preflights atomic.Int32
	kernel := &fakeKernel{incompatible: true, compatibility: func(context.Context, *clients.CompatibilityRequest) { preflights.Add(1) }}
	client, _ := testClient(t, kernel)
	ctx := testContext(t)
	for range 2 {
		_, err := client.EventStore(ctx, "store")
		var mismatch *chronicle.CompatibilityError
		if !errors.As(err, &mismatch) {
			t.Fatal(err)
		}
	}
	if _, err := client.EventStores(ctx); err == nil {
		t.Fatal("terminal failure lost")
	}
	if err := client.Ready(ctx); err == nil {
		t.Fatal("incompatible client became ready")
	}
	if preflights.Load() != 1 {
		t.Fatal("ordinary API calls restarted terminal supervision")
	}
	if err := client.Connect(ctx); err == nil {
		t.Fatal("explicit retry lost compatibility refusal")
	}
	if preflights.Load() != 2 {
		t.Fatal("explicit Connect did not retry")
	}
}
