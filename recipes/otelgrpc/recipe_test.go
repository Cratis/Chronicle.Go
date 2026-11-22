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

func (e *ownedExporter) assertOpen(t *testing.T, message string) {
	t.Helper()
	if e.closed.Load() {
		t.Fatal(message)
	}
}

func (e *ownedExporter) assertClosed(t *testing.T, message string) {
	t.Helper()
	if !e.closed.Load() {
		t.Fatal(message)
	}
}

type tracedClient struct {
	ctx       context.Context
	client    *chronicle.Client
	provider  *sdktrace.TracerProvider
	exporter  *ownedExporter
	kernel    *kernelFixture
	connected <-chan chronicle.ConnectionEvent
}

type connectionSnapshot struct {
	event   chronicle.ConnectionEvent
	session session
}

func newTracedClient(t *testing.T, ctx context.Context) *tracedClient {
	t.Helper()
	kernel, address, policy := startFixture(t)
	exporter := &ownedExporter{InMemoryExporter: tracetest.NewInMemoryExporter()}
	provider := newTracerProvider(exporter)
	connected := make(chan chronicle.ConnectionEvent, 8)
	client, err := newClient(provider, chronicle.WithConnectionString(address), chronicle.WithTLS(policy),
		chronicle.WithNoAuthentication(), chronicle.WithSkipCompatibilityCheck(), chronicle.WithKeepAliveTimeout(time.Minute),
		chronicle.WithRegistry(recordedEventRegistry(t)), chronicle.WithOnConnected(func(ctx context.Context, event chronicle.ConnectionEvent) {
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
	return &tracedClient{ctx: ctx, client: client, provider: provider, exporter: exporter, kernel: kernel, connected: connected}
}

func recordedEventRegistry(t *testing.T) *chronicle.Registry {
	t.Helper()
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[recordedEvent](registry, events.WithID("recipe-recorded")); err != nil {
		t.Fatal(err)
	}
	return registry
}

func (f *tracedClient) awaitConnection(t *testing.T) connectionSnapshot {
	t.Helper()
	return connectionSnapshot{event: await(t, f.ctx, f.connected), session: await(t, f.ctx, f.kernel.sessions)}
}

func (f *tracedClient) openStore(t *testing.T) *chronicle.EventStore {
	t.Helper()
	store, err := f.client.EventStore(f.ctx, "recipe")
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func (f *tracedClient) interruptAppend(t *testing.T, store *chronicle.EventStore) trace.SpanContext {
	t.Helper()
	appendCtx, parent := f.provider.Tracer("recipe").Start(f.ctx, "application append")
	result, appendErr := store.EventLog().Append(appendCtx, "source", recordedEvent{Value: "example"},
		eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
	parent.End()
	var unknown *eventsequences.OutcomeUnknownError
	if !errors.As(errors.Join(appendErr, result.Err()), &unknown) {
		t.Fatalf("interrupted write = %v / %v, want OutcomeUnknownError", appendErr, result.Err())
	}
	return parent.SpanContext()
}

func (f *tracedClient) assertReplaced(t *testing.T, first connectionSnapshot) {
	t.Helper()
	second := f.awaitConnection(t)
	if second.event.Generation <= first.event.Generation {
		t.Fatalf("owned generation not replaced: %+v -> %+v", first, second)
	}
	if second.session.id == first.session.id {
		t.Fatalf("owned connection ID not replaced: %+v -> %+v", first, second)
	}
	if second.session.address == first.session.address {
		t.Fatalf("owned TCP connection not replaced: %+v -> %+v", first, second)
	}
}

func (f *tracedClient) assertRecoveryWithoutRetry(t *testing.T, store *chronicle.EventStore) {
	t.Helper()
	if _, err := store.WaitForRegistration(f.ctx); err != nil {
		t.Fatal(err)
	}
	// The cached handle registers through the new owned transport, not the old one.
	f.assertRPCSpans(t, "/EnsureNamespace", 2)
	if got := f.kernel.appends.Load(); got != 1 {
		t.Fatalf("server append calls = %d, want 1 (no automatic retry)", got)
	}
}

func (f *tracedClient) assertOrderedShutdown(t *testing.T) {
	t.Helper()
	f.exporter.assertOpen(t, "SDK closed the application-owned exporter")
	// Closing only the client must end streams without shutting down telemetry.
	if err := f.client.Close(); err != nil {
		t.Fatal(err)
	}
	f.exporter.assertOpen(t, "client.Close shut down the exporter")
	f.exporter.onShutdown = func(ctx context.Context) error {
		if !errors.Is(f.client.Ready(ctx), chronicle.ErrClosed) {
			return errors.New("provider shut down before client")
		}
		return ctx.Err()
	}
	if err := shutdownTelemetry(f.ctx, f.client, f.provider); err != nil {
		t.Fatal(err)
	}
	f.exporter.assertClosed(t, "application did not shut down exporter")
}

func TestSpansSurviveOwnedReplacementWithoutRetryingWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	fixture := newTracedClient(t, ctx)
	if err := fixture.client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	first := fixture.awaitConnection(t)
	store := fixture.openStore(t)
	fixture.assertRPCSpans(t, "/EnsureNamespace", 1)

	parent := fixture.interruptAppend(t, store)
	fixture.assertReplaced(t, first)
	fixture.assertRecoveryWithoutRetry(t, store)
	fixture.assertOrderedShutdown(t)
	assertFinalSpans(t, fixture.exporter.final, parent)
}

func (f *tracedClient) assertRPCSpans(t *testing.T, suffix string, want int) {
	t.Helper()
	if err := f.provider.ForceFlush(f.ctx); err != nil {
		t.Fatal(err)
	}
	var got int
	for _, span := range spansWithSuffix(f.exporter.GetSpans(), suffix) {
		if span.SpanKind == trace.SpanKindClient {
			got++
		}
	}
	if got != want {
		t.Fatalf("client RPC spans ending %s = %d, want %d: %+v", suffix, got, want, f.exporter.GetSpans())
	}
}

func spansWithSuffix(spans tracetest.SpanStubs, suffix string) tracetest.SpanStubs {
	var matches tracetest.SpanStubs
	for _, span := range spans {
		if strings.HasSuffix(span.Name, suffix) {
			matches = append(matches, span)
		}
	}
	return matches
}

func assertFinalSpans(t *testing.T, spans tracetest.SpanStubs, parent trace.SpanContext) {
	t.Helper()
	appends := spansWithSuffix(spans, "/Append")
	streams := spansWithSuffix(spans, "/Connect")
	for _, span := range appends {
		assertAppendSpan(t, span, parent)
	}
	if len(appends) != 1 || len(streams) != 2 {
		t.Fatalf("final spans: appends=%d, session streams=%d, want 1 and 2", len(appends), len(streams))
	}
}

func assertAppendSpan(t *testing.T, span tracetest.SpanStub, parent trace.SpanContext) {
	t.Helper()
	if span.Parent.SpanID() != parent.SpanID() {
		t.Errorf("append span lost caller context or failure: %+v", span)
	}
	if span.SpanContext.TraceID() != parent.TraceID() {
		t.Errorf("append span lost caller context or failure: %+v", span)
	}
	if span.Status.Code != codes.Error {
		t.Errorf("append span lost caller context or failure: %+v", span)
	}
}

type runFixture struct {
	ctx      context.Context
	cancel   context.CancelFunc
	kernel   *kernelFixture
	exporter *ownedExporter
	options  []chronicle.ClientOption
}

func newRunFixture(t *testing.T) *runFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	kernel, address, policy := startFixture(t)
	t.Cleanup(cancel)
	return &runFixture{
		ctx: ctx, cancel: cancel, kernel: kernel,
		exporter: &ownedExporter{InMemoryExporter: tracetest.NewInMemoryExporter()},
		options: []chronicle.ClientOption{chronicle.WithConnectionString(address), chronicle.WithTLS(policy),
			chronicle.WithNoAuthentication(), chronicle.WithSkipCompatibilityCheck()},
	}
}

func (f *runFixture) start() <-chan error {
	done := make(chan error, 1)
	go func() { done <- Run(f.ctx, f.exporter, f.options...) }()
	return done
}

func awaitRun(t *testing.T, done <-chan error) error {
	t.Helper()
	// Join with a separate bounded lifetime after canceling the application.
	joinCtx, stopJoin := context.WithTimeout(t.Context(), 15*time.Second)
	defer stopJoin()
	return await(t, joinCtx, done)
}

func TestRunOwnsShutdownOnCancellation(t *testing.T) {
	fixture := newRunFixture(t)
	done := fixture.start()
	await(t, fixture.ctx, fixture.kernel.sessions)
	fixture.cancel()
	err := awaitRun(t, done)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v", err)
	}
	fixture.exporter.assertClosed(t, "run left exporter open")
}

func TestRunOwnsShutdownOnInvalidOptions(t *testing.T) {
	fixture := newRunFixture(t)
	fixture.options = append(fixture.options, chronicle.WithKeepAliveTimeout(0))
	err := awaitRun(t, fixture.start())
	if !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatalf("run error = %v", err)
	}
	fixture.exporter.assertClosed(t, "run left exporter open")
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
