//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
)

// Capture the mature kernel reply before SDK release/typed decoding can reject
// it. One event per source makes its last-handled position an exact readiness
// condition; malformed content is retained, never repaired or accepted here.
func captureBinaryModel(t *testing.T, f *kernelFixture, model readmodels.Identifier, source events.SourceID, position events.SequenceNumber, directory string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		response, err := contracts.NewReadModelsClient(f.conn).GetInstanceByKey(ctx, &contracts.GetInstanceByKeyRequest{
			EventStore: string(f.storeName), Namespace: string(chronicle.DefaultNamespace),
			ReadModelIdentifier: string(model), EventSequenceId: "event-log", ReadModelKey: string(source),
		})
		if err != nil {
			t.Fatal(err)
		}
		if response == nil {
			t.Fatal("missing raw binary model reply")
		}
		if response.LastHandledEventSequenceNumber == uint64(position) && response.ReadModel != "null" {
			if err := os.WriteFile(filepath.Join(directory, string(source)+".model.raw.kernel.json"), []byte(response.ReadModel), 0600); err != nil {
				t.Fatal(err)
			}
			data, err := json.MarshalIndent(response, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(directory, string(source)+".model.response.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("raw binary projection did not reach its event position", ctx.Err())
		case <-ticker.C:
		}
	}
}
