// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Command otelgrpc demonstrates application-owned tracing of Chronicle connections.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/recipes/otelgrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
)

func main() {
	address := flag.String("address", "chronicle://localhost:35000", "Chronicle address (TLS verification remains enabled)")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	exporter, err := stdouttrace.New()
	if err == nil {
		// This recipe targets a local development kernel without authentication.
		err = otelgrpc.Run(ctx, exporter, chronicle.WithConnectionString(*address), chronicle.WithNoAuthentication())
	}
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
