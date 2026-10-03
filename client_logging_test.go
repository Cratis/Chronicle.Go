// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
	"github.com/cratis/chronicle.go/seeding"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
	closed  atomic.Int32
	signal  chan struct{}
}

func (*recordingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordingHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, record.Clone())
	h.mu.Unlock()
	if h.signal != nil {
		select {
		case h.signal <- struct{}{}:
		default:
		}
	}
	return nil
}
func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }
func (h *recordingHandler) Close() error                       { h.closed.Add(1); return nil }
func (h *recordingHandler) snapshot() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]slog.Record(nil), h.records...)
}
func (h *recordingHandler) hasStage(stage string) bool {
	for _, record := range h.snapshot() {
		found := false
		record.Attrs(func(a slog.Attr) bool { found = found || (a.Key == "stage" && a.Value.String() == stage); return true })
		if found {
			return true
		}
	}
	return false
}
func assertBoundedRecords(t *testing.T, h *recordingHandler) {
	t.Helper()
	if len(h.snapshot()) == 0 {
		t.Fatal("no diagnostics recorded")
	}
	for _, record := range h.snapshot() {
		if strings.Contains(record.Message, "secret") {
			t.Fatal("secret in message")
		}
		if record.NumAttrs() != 3 {
			t.Fatalf("attribute count = %d", record.NumAttrs())
		}
		record.Attrs(func(a slog.Attr) bool {
			if a.Key != "operation" && a.Key != "stage" && a.Key != "category" {
				t.Errorf("unexpected field %s", a.Key)
			}
			if a.Value.Kind() != slog.KindString || strings.Contains(a.Value.String(), "secret") {
				t.Error("unsafe attribute")
			}
			return true
		})
	}
}

type loggingEvent struct{ Secret string }
type loggingModel struct{ ID string }
type loggingSecretError struct{ calls *atomic.Int32 }

func (e *loggingSecretError) Error() string { e.calls.Add(1); return "secret-error" }
func (e *loggingSecretError) Format(s fmt.State, _ rune) {
	e.calls.Add(1)
	_, _ = io.WriteString(s, "secret-format")
}
func (e *loggingSecretError) Unwrap() error { e.calls.Add(1); return nil }
func (e *loggingSecretError) Is(error) bool { e.calls.Add(1); return false }
func (e *loggingSecretError) As(any) bool   { e.calls.Add(1); return false }
func (e *loggingSecretError) GRPCStatus() *status.Status {
	e.calls.Add(1)
	return status.New(codes.Internal, "secret-status")
}

type loggingMiddleware struct {
	failure    error
	panicValue any
}

func (loggingMiddleware) Before(context.Context, reactors.Invocation) error { return nil }
func (m loggingMiddleware) After(context.Context, reactors.Invocation) error {
	if m.panicValue != nil {
		panic(m.panicValue)
	}
	return m.failure
}

func loggingRegistry(t *testing.T, failure error, override *slog.Logger) *Registry {
	t.Helper()
	r := NewRegistry()
	if _, err := RegisterEvent[loggingEvent](r); err != nil {
		t.Fatal(err)
	}
	model, err := RegisterReadModel[loggingModel](r)
	if err != nil {
		t.Fatal(err)
	}
	var ro []reactors.Option
	var fo []reducers.Option
	var mo []reactors.ReadModelOption
	if override != nil {
		ro = append(ro, reactors.WithLogger(override))
		fo = append(fo, reducers.WithLogger(override))
		mo = append(mo, reactors.WithReadModelLogger(override))
	}
	ro = append(ro, reactors.WithMiddleware(func() reactors.Middleware { return loggingMiddleware{failure: failure} }))
	if err := RegisterReactorHandler(r, "secret-reactor-id", func(context.Context, loggingEvent) error { return nil }, ro...); err != nil {
		t.Fatal(err)
	}
	if err := RegisterReducerHandlers(r, model, "secret-reducer-id", []reducers.Handler{reducers.On(func(context.Context, loggingEvent, *loggingModel, events.Context) (*loggingModel, error) {
		return nil, failure
	})}, fo...); err != nil {
		t.Fatal(err)
	}
	mo = append(mo, reactors.Materialized(nil))
	if err := RegisterReadModelReactorHandlers(r, "secret-model-reactor-id", model, []reactors.ReadModelHandler{reactors.ReadModelOn(readmodels.Added, func(context.Context, loggingModel) error { return failure })}, mo...); err != nil {
		t.Fatal(err)
	}
	return r
}
func invokeLoggingPlans(t *testing.T, c *Client) {
	t.Helper()
	ctx := t.Context()
	plan := c.reactors.defaults[0]
	lease, err := plan.Activate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := c.catalog.Descriptors()[0]
	ec := events.Context{EventType: descriptor.Ref(), SourceID: "secret-source", SequenceNumber: 1}
	if err := lease.Invoke(ctx, &loggingEvent{Secret: "secret-payload"}, ec, nil); err != nil {
		t.Fatal("after-hook changed outcome")
	}
	if err := lease.Close(ctx); err != nil {
		t.Fatal(err)
	}
	result := c.reducers.defaults[0].Reduce(ctx, []reducers.Event{{Content: &loggingEvent{}, Context: ec}}, nil)
	if result.Err == nil || result.State != nil {
		t.Fatal("reducer failure suppressed")
	}
	if err := c.readModelReactors.defaults[0].Dispatch(ctx, readmodels.Change[json.RawMessage]{Type: readmodels.Added, HasValue: true, Value: json.RawMessage(`{"ID":"secret-model"}`)}, nil); err == nil {
		t.Fatal("model callback failure suppressed")
	}
}

// The pristine standard-log bridge cannot be restored with slog.SetDefault after
// installing a custom handler. Use a fresh test process, never parent globals.
func TestClientLoggerPristineDefaultBridge(t *testing.T) {
	const guard = "CHRONICLE_TEST_PRISTINE_LOGGER"
	if os.Getenv(guard) != "1" {
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		command := exec.CommandContext(t.Context(), executable, "-test.run=^TestClientLoggerPristineDefaultBridge$", "-test.count=1", "-test.timeout=30s")
		command.Env = append(os.Environ(), guard+"=1")
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("isolated default bridge: %v\n%s", err, output)
		}
		return
	}

	pristine := slog.Default()
	var originalWriter, currentWriter strings.Builder
	log.SetOutput(&originalWriter)
	var calls atomic.Int32
	registry := NewRegistry()
	if err := RegisterSeederFunc(registry, func(*seeding.Builder) error { return &loggingSecretError{&calls} }); err != nil {
		t.Fatal(err)
	}
	beforeBridge := captureForTest(t, WithRegistry(registry))
	afterBridge := captureForTest(t, WithRegistry(registry))
	for _, p := range []*ClientPreparation{beforeBridge, afterBridge} {
		if p.Client().config.logger != pristine || p.Client().config.logger.Handler() != pristine.Handler() {
			t.Fatal("pristine logger/handler identity replaced at capture")
		}
	}
	// Capturing identity does not snapshot log.Default's writer.
	log.SetOutput(&currentWriter)
	if c, err := beforeBridge.Prepare(t.Context(), nil); c != nil || err == nil {
		t.Fatal("preparation failure suppressed")
	}
	if originalWriter.Len() != 0 || !strings.Contains(currentWriter.String(), "client preparation failed") {
		t.Fatal("captured pristine handler did not use current standard-log writer")
	}

	custom := &recordingHandler{}
	slog.SetDefault(slog.New(custom))
	if c, err := afterBridge.Prepare(t.Context(), nil); c != nil || err == nil {
		t.Fatal("preparation failure suppressed after bridge installation")
	}
	if afterBridge.Client().config.logger != pristine || afterBridge.Client().config.logger.Handler() != pristine.Handler() {
		t.Fatal("SDK replaced captured identity after SetDefault")
	}
	records := custom.snapshot()
	if len(records) != 1 || !strings.Contains(records[0].Message, "client preparation failed") ||
		!strings.Contains(records[0].Message, "stage=prepare") || strings.Contains(records[0].Message, "secret") {
		t.Fatal("actual SDK preparation diagnostic did not reach custom handler through standard-log bridge")
	}
	if calls.Load() != 0 {
		t.Fatal("SDK diagnostic inspected application error")
	}
}

func TestClientLoggerCapturePrecedenceAndArtifactOverride(t *testing.T) {
	old := slog.Default()
	t.Cleanup(func() { slog.SetDefault(old) })
	defaultHandler, laterHandler, clientHandler := &recordingHandler{}, &recordingHandler{}, &recordingHandler{}
	defaultLogger, clientLogger := slog.New(defaultHandler), slog.New(clientHandler)
	slog.SetDefault(defaultLogger)
	var calls atomic.Int32
	failure := &loggingSecretError{&calls}
	r := loggingRegistry(t, failure, nil)
	explicit := loggingRegistry(t, failure, defaultLogger) // Pointer equality must not imply fallback.
	p := captureForTest(t, WithRegistry(r))
	q := captureForTest(t, WithRegistry(explicit), WithLogger(slog.New(laterHandler)), WithLogger(clientLogger))
	slog.SetDefault(slog.New(laterHandler))
	for _, preparation := range []*ClientPreparation{p, q} {
		c, err := preparation.Prepare(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, got := range []*slog.Logger{c.reactors.defaults[0].Logger(), c.reducers.defaults[0].Logger(), c.readModelReactors.defaults[0].Logger()} {
			if got != defaultLogger {
				t.Fatal("captured or explicit logger replaced")
			}
		}
		invokeLoggingPlans(t, c)
	}
	if q.Client().config.logger != clientLogger {
		t.Fatal("client last-wins violated")
	}
	if len(clientHandler.snapshot()) != 0 || len(laterHandler.snapshot()) != 0 {
		t.Fatal("diagnostics reached wrong logger")
	}
	assertBoundedRecords(t, defaultHandler)
	if calls.Load() != 0 {
		t.Fatalf("application error hooks called %d times", calls.Load())
	}
}

func TestClientLoggerDefaultIsCapturedAfterDeclarationAndBeforePreparation(t *testing.T) {
	old := slog.Default()
	t.Cleanup(func() { slog.SetDefault(old) })
	declarationHandler, capturedHandler, laterHandler := &recordingHandler{}, &recordingHandler{}, &recordingHandler{}
	slog.SetDefault(slog.New(declarationHandler))
	r := loggingRegistry(t, errors.New("secret-callback-error"), nil)
	capturedLogger := slog.New(capturedHandler)
	slog.SetDefault(capturedLogger)
	p := captureForTest(t, WithRegistry(r))
	slog.SetDefault(slog.New(laterHandler))
	c, err := p.Prepare(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	invokeLoggingPlans(t, c)
	if len(declarationHandler.snapshot()) != 0 || len(laterHandler.snapshot()) != 0 {
		t.Fatal("mutable global default used")
	}
	assertBoundedRecords(t, capturedHandler)
	immediate, err := NewClientContext(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if immediate.config.logger != slog.Default() {
		t.Fatal("NewClientContext did not capture immediately")
	}
	if err := immediate.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClientLoggerValidationAndImmediateCapture(t *testing.T) {
	var typedNil *slog.Logger
	for _, options := range [][]ClientOption{{WithLogger(nil)}, {WithLogger(typedNil)}} {
		if p, err := CaptureClient(options...); p != nil || !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatal("nil logger accepted")
		}
	}
	h := &recordingHandler{}
	logger := slog.New(h)
	c, err := NewClientContext(t.Context(), WithLogger(nil), WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	if c.config.logger != logger {
		t.Fatal("final nonnil logger not selected")
	}
	if err := c.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if h.closed.Load() != 0 {
		t.Fatal("borrowed handler closed")
	}
	assertBoundedRecords(t, h)
}

func TestClientLoggersIsolateSharedRegistryConcurrentFailures(t *testing.T) {
	var calls atomic.Int32
	r := loggingRegistry(t, &loggingSecretError{&calls}, nil)
	handlers := []*recordingHandler{{}, {}}
	clients := make([]*Client, 2)
	for i := range clients {
		p := captureForTest(t, WithRegistry(r), WithRegistryForStore("shared", r), WithLogger(slog.New(handlers[i])))
		var err error
		clients[i], err = p.Prepare(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if clients[i].reactors.defaults[0] != clients[i].reactors.stores["shared"][0] {
			t.Fatal("registry compiled more than once")
		}
	}
	var work sync.WaitGroup
	for _, c := range clients {
		work.Add(1)
		go func() { defer work.Done(); invokeLoggingPlans(t, c) }()
	}
	work.Wait()
	for _, h := range handlers {
		assertBoundedRecords(t, h)
		if len(h.snapshot()) != 3 {
			t.Fatalf("records = %d, want three isolated failures", len(h.snapshot()))
		}
	}
	if calls.Load() != 0 {
		t.Fatal("application error hooks invoked")
	}
}

func TestClientPreparationLoggingDoesNotInspectCauseOrPanic(t *testing.T) {
	for _, panicked := range []bool{false, true} {
		t.Run(fmt.Sprint(panicked), func(t *testing.T) {
			var calls atomic.Int32
			failure := &loggingSecretError{&calls}
			r := NewRegistry()
			if err := RegisterSeederFunc(r, func(*seeding.Builder) error {
				if panicked {
					panic(failure)
				}
				return failure
			}); err != nil {
				t.Fatal(err)
			}
			h := &recordingHandler{}
			p := captureForTest(t, WithRegistry(r), WithLogger(slog.New(h)))
			if c, err := p.Prepare(t.Context(), nil); c != nil || err == nil {
				t.Fatal("preparation failure suppressed")
			}
			assertBoundedRecords(t, h)
			if calls.Load() != 0 {
				t.Fatal("preparation logging inspected failure")
			}
		})
	}
}

func TestArtifactLoggingContainsPanicValuesAndReporterPanics(t *testing.T) {
	var calls atomic.Int32
	payload := &loggingSecretError{&calls}
	h := &recordingHandler{}
	r := NewRegistry()
	if _, err := RegisterEvent[loggingEvent](r); err != nil {
		t.Fatal(err)
	}
	model, err := RegisterReadModel[loggingModel](r)
	if err != nil {
		t.Fatal(err)
	}
	if err := RegisterReactorHandler(r, "panic-reactor", func(context.Context, loggingEvent) error { return nil }, reactors.WithMiddleware(func() reactors.Middleware { return loggingMiddleware{panicValue: payload} })); err != nil {
		t.Fatal(err)
	}
	if err := RegisterReducerHandlers(r, model, "panic-reducer", []reducers.Handler{reducers.On(func(context.Context, loggingEvent, *loggingModel, events.Context) (*loggingModel, error) {
		panic(payload)
	})}); err != nil {
		t.Fatal(err)
	}
	if err := RegisterReadModelReactorHandlers(r, "panic-model", model, []reactors.ReadModelHandler{reactors.ReadModelOn(readmodels.Added, func(loggingModel) { panic(payload) })}, reactors.Materialized(nil), reactors.WithReadModelErrorHandler(func(context.Context, error) { panic(payload) })); err != nil {
		t.Fatal(err)
	}
	p := captureForTest(t, WithRegistry(r), WithLogger(slog.New(h)))
	c, err := p.Prepare(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	invokeLoggingPlans(t, c)
	assertBoundedRecords(t, h)
	if calls.Load() != 0 {
		t.Fatal("panic payload formatted")
	}
}

type loggingFactoryReactor struct{}

func (*loggingFactoryReactor) Handle(loggingEvent) {}
func TestLoggerOptionIsNotAConstructorService(t *testing.T) {
	r := NewRegistry()
	if _, err := RegisterEvent[loggingEvent](r); err != nil {
		t.Fatal(err)
	}
	if err := RegisterReactor[*loggingFactoryReactor](r, func(*slog.Logger) *loggingFactoryReactor {
		t.Error("logger auto-wired")
		return &loggingFactoryReactor{}
	}); err != nil {
		t.Fatal(err)
	}
	if c, err := NewClient(WithRegistry(r), WithLogger(slog.New(&recordingHandler{}))); c != nil || err == nil {
		t.Fatal("missing logger service accepted")
	}
	if reactors.DefaultScopeFactory().(interface{ Contains(reflect.Type) bool }).Contains(reflect.TypeFor[*slog.Logger]()) {
		t.Fatal("zero logger service fabricated")
	}
}

func TestClientLoggerReconnectRetainsCaptureAndRedactsStatus(t *testing.T) {
	h := &recordingHandler{signal: make(chan struct{}, 20)}
	k := &supervisedKernel{}
	c, ctx := supervisionClient(t, k, WithLogger(slog.New(h)))
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	k.endStream <- status.Error(codes.Unavailable, "secret-grpc-status")
	for !h.hasStage("reconnect") {
		select {
		case <-h.signal:
		case <-ctx.Done():
			t.Fatal("no reconnect diagnostic")
		}
	}
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	assertBoundedRecords(t, h)
}

type loggingEndedStream struct{ failure error }

func (s loggingEndedStream) Run(context.Context) error { return s.failure }
func TestObserverResubscriptionUsesFrozenArtifactRoute(t *testing.T) {
	h := &recordingHandler{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	g := &generation{ctx: ctx, number: 1}
	s := &storeObservers{}
	var opened atomic.Int32
	plan := observerPlan{id: "secret-observer", logger: slog.New(h), operation: "reactor", open: func(context.Context, *generation) (observerStream, error) {
		opened.Add(1)
		return loggingEndedStream{status.Error(codes.Unavailable, "secret-stream-status")}, nil
	}}
	if err := s.start(ctx, g, true, []observerPlan{plan}, func(context.Context, time.Duration) error {
		if opened.Load() == 1 {
			return nil
		}
		cancel()
		return context.Canceled
	}); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	g.observers.Wait()
	if opened.Load() != 2 || !h.hasStage("resubscribe") {
		t.Fatal("missing routed resubscription")
	}
	assertBoundedRecords(t, h)
}
