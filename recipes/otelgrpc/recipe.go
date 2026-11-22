// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package otelgrpc demonstrates application-owned tracing of Chronicle connections.
package otelgrpc

import (
	"context"
	"errors"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func newTracerProvider(exporter sdktrace.SpanExporter) *sdktrace.TracerProvider {
	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
}

func newClient(provider *sdktrace.TracerProvider, options ...chronicle.ClientOption) (*chronicle.Client, error) {
	handler := otelgrpc.NewClientHandler(
		otelgrpc.WithTracerProvider(provider),
		otelgrpc.WithPropagators(propagation.TraceContext{}),
	)
	return chronicle.NewClient(append(options, chronicle.WithGRPCStatsHandler(handler))...)
}

// Run owns exporter and a tracer provider, instruments an SDK-owned client, and
// waits for cancellation. It closes the client before flushing owned telemetry.
func Run(ctx context.Context, exporter sdktrace.SpanExporter, options ...chronicle.ClientOption) (err error) {
	provider := newTracerProvider(exporter)
	client, err := newClient(provider, options...)
	defer func() { err = errors.Join(err, shutdownTelemetry(ctx, client, provider)) }()
	if err != nil {
		return err
	}
	if err := client.Connect(ctx); err != nil {
		return err
	}
	// Application operations use their caller's context. The client supervisor
	// keeps instrumenting replacement connections until the application stops.
	<-ctx.Done()
	return nil
}

func shutdownTelemetry(ctx context.Context, client *chronicle.Client, provider *sdktrace.TracerProvider) error {
	var closeErr error
	if client != nil {
		closeErr = client.Close() // End RPCs (including the session stream) first.
	}
	// Cleanup must still flush after the application's signal context is canceled.
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return errors.Join(closeErr, provider.Shutdown(cleanup)) // Also shuts down the exporter.
}
