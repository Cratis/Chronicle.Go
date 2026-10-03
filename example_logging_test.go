// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	chronicle "github.com/cratis/chronicle.go"
)

type workerEvent struct{ ID string }
type loggingWorker struct{ logger *slog.Logger }

func (w *loggingWorker) Handle(ctx context.Context, _ workerEvent) error {
	w.logger.InfoContext(ctx, "worker handled event") // Application-owned diagnostics.
	return nil
}

func ExampleWithLogger() {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[workerEvent](registry); err != nil {
		panic(err)
	}
	// No container: pass application dependencies in the constructor closure.
	if err := chronicle.RegisterReactor[*loggingWorker](registry, func() *loggingWorker {
		return &loggingWorker{logger: logger}
	}); err != nil {
		panic(err)
	}
	client, err := chronicle.NewClientContext(context.Background(),
		chronicle.WithRegistry(registry), chronicle.WithLogger(logger))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			panic(err)
		}
	}()
	catalog, _, err := client.Catalogs("worker")
	if err != nil {
		panic(err)
	}
	fmt.Println(len(catalog.Descriptors()))
	// In a running worker: client.Ready(ctx), then client.EventStore(ctx, "worker").
	// Chronicle borrows the logger/handler and never closes them.
	// Output: 1
}
