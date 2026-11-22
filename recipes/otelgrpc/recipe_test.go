// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package otelgrpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type recordedEvent struct{ Value string }

type ownedExporter struct {
	*tracetest.InMemoryExporter
	closed atomic.Bool
	// Set before shutdown; called only by the processor's shutdown worker.
	onShutdown func(context.Context) error
	final      tracetest.SpanStubs
}

func (e *ownedExporter) Shutdown(ctx context.Context) error {
	e.closed.Store(true)
	e.final = e.GetSpans() // tracetest clears its storage at Shutdown.
	var err error
	if e.onShutdown != nil {
		err = e.onShutdown(ctx)
	}
	return errors.Join(err, e.InMemoryExporter.Shutdown(ctx))
}

func TestSpansSurviveOwnedReplacementWithoutRetryingWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	kernel, address, policy := startFixture(t)
	exporter := &ownedExporter{InMemoryExporter: tracetest.NewInMemoryExporter()}
	provider := newTracerProvider(exporter)
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[recordedEvent](registry, events.WithID("recipe-recorded")); err != nil {
		t.Fatal(err)
	}
	connected := make(chan chronicle.ConnectionEvent, 8)
	client, err := newClient(provider, chronicle.WithConnectionString(address), chronicle.WithTLS(policy),
		chronicle.WithNoAuthentication(), chronicle.WithSkipCompatibilityCheck(), chronicle.WithKeepAliveTimeout(time.Minute),
		chronicle.WithRegistry(registry), chronicle.WithOnConnected(func(ctx context.Context, event chronicle.ConnectionEvent) {
			select {
			case connected <- event:
			case <-ctx.Done():
			}
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := shutdownTelemetry(ctx, client, provider); err != nil {
			t.Error(err)
		}
	})
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	first := await(t, ctx, connected)
	firstSession := await(t, ctx, kernel.sessions)
	store, err := client.EventStore(ctx, "recipe")
	if err != nil {
		t.Fatal(err)
	}
	assertRPCSpans(t, ctx, provider, exporter, "/EnsureNamespace", 1)

	appendCtx, parent := provider.Tracer("recipe").Start(ctx, "application append")
	result, appendErr := store.EventLog().Append(appendCtx, "source", recordedEvent{Value: "example"},
		eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
	parent.End()
	var unknown *eventsequences.OutcomeUnknownError
	if !errors.As(errors.Join(appendErr, result.Err()), &unknown) {
		t.Fatalf("interrupted write = %v / %v, want OutcomeUnknownError", appendErr, result.Err())
	}
	second := await(t, ctx, connected)
	secondSession := await(t, ctx, kernel.sessions)
	if second.Generation <= first.Generation || secondSession.id == firstSession.id || secondSession.address == firstSession.address {
		t.Fatalf("owned connection not replaced: %+v/%+v -> %+v/%+v", first, firstSession, second, secondSession)
	}
	if _, err := store.WaitForRegistration(ctx); err != nil {
		t.Fatal(err)
	}
	// The cached handle registers through the new owned transport, not the old one.
	assertRPCSpans(t, ctx, provider, exporter, "/EnsureNamespace", 2)
	if got := kernel.appends.Load(); got != 1 {
		t.Fatalf("server append calls = %d, want 1 (no automatic retry)", got)
	}
	if exporter.closed.Load() {
		t.Fatal("SDK closed the application-owned exporter")
	}
	// Closing only the client must end streams without shutting down telemetry.
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if exporter.closed.Load() {
		t.Fatal("client.Close shut down the exporter")
	}
	exporter.onShutdown = func(ctx context.Context) error {
		if !errors.Is(client.Ready(ctx), chronicle.ErrClosed) {
			return errors.New("provider shut down before client")
		}
		return ctx.Err()
	}
	if err := shutdownTelemetry(ctx, client, provider); err != nil {
		t.Fatal(err)
	}
	if !exporter.closed.Load() {
		t.Fatal("application did not shut down exporter")
	}
	assertFinalSpans(t, exporter.final, parent.SpanContext())
}

func assertRPCSpans(t *testing.T, ctx context.Context, provider *sdktrace.TracerProvider, exporter *ownedExporter, suffix string, want int) {
	t.Helper()
	if err := provider.ForceFlush(ctx); err != nil {
		t.Fatal(err)
	}
	var got int
	for _, span := range exporter.GetSpans() {
		if strings.HasSuffix(span.Name, suffix) && span.SpanKind == trace.SpanKindClient {
			got++
		}
	}
	if got != want {
		t.Fatalf("client RPC spans ending %s = %d, want %d: %+v", suffix, got, want, exporter.GetSpans())
	}
}

func assertFinalSpans(t *testing.T, spans tracetest.SpanStubs, parent trace.SpanContext) {
	t.Helper()
	var appends, streams int
	for _, span := range spans {
		if strings.HasSuffix(span.Name, "/Connect") {
			streams++
		}
		if strings.HasSuffix(span.Name, "/Append") {
			appends++
			if span.Parent.SpanID() != parent.SpanID() || span.SpanContext.TraceID() != parent.TraceID() || span.Status.Code != codes.Error {
				t.Errorf("append span lost caller context or failure: %+v", span)
			}
		}
	}
	if appends != 1 || streams != 2 {
		t.Fatalf("final spans: appends=%d, session streams=%d, want 1 and 2", appends, streams)
	}
}

func TestRunOwnsShutdownOnCancellationAndInvalidOptions(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(map[bool]string{false: "canceled application", true: "invalid configuration"}[invalid], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			kernel, address, policy := startFixture(t)
			exporter := &ownedExporter{InMemoryExporter: tracetest.NewInMemoryExporter()}
			options := []chronicle.ClientOption{chronicle.WithConnectionString(address), chronicle.WithTLS(policy),
				chronicle.WithNoAuthentication(), chronicle.WithSkipCompatibilityCheck()}
			if invalid {
				options = append(options, chronicle.WithKeepAliveTimeout(0))
			}
			done := make(chan error, 1)
			go func() { done <- Run(ctx, exporter, options...) }()
			if !invalid {
				await(t, ctx, kernel.sessions)
				cancel()
			}
			// Join with a separate bounded lifetime after canceling the application.
			joinCtx, stopJoin := context.WithTimeout(t.Context(), 15*time.Second)
			defer stopJoin()
			err := await(t, joinCtx, done)
			if invalid && !errors.Is(err, chronicle.ErrInvalidConfiguration) || !invalid && err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("run error = %v", err)
			}
			if !exporter.closed.Load() {
				t.Fatal("run left exporter open")
			}
		})
	}
}

func TestUntrustedTLSCertificateIsRejected(t *testing.T) {
	kernel, address, _ := startFixture(t)
	exporter := &ownedExporter{InMemoryExporter: tracetest.NewInMemoryExporter()}
	provider := newTracerProvider(exporter)
	client, err := newClient(provider, chronicle.WithConnectionString(address), chronicle.WithNoAuthentication(),
		chronicle.WithSkipCompatibilityCheck(), chronicle.WithTLS(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: x509.NewCertPool()}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := shutdownTelemetry(t.Context(), client, provider); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := client.Connect(ctx); err == nil {
		t.Fatal("connection accepted an untrusted certificate")
	}
	select {
	case got := <-kernel.sessions:
		t.Fatalf("untrusted client opened session: %+v", got)
	default:
	}
}
