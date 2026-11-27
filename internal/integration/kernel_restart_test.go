//go:build integration && kernelrestart

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/observation"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/connection"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	restartAddress   = "localhost:35000"
	restartEndpoint  = "chronicle://" + restartAddress
	restartHealthURL = "https://" + restartAddress + "/health"
	restartMarker    = "disposable-service-v1"
	restartLabel     = "org.cratis.go-kernel-restart"
	restartReactor   = "kernel-restart-reactor"
	restartSequence  = "event-log"
)

var (
	restartContainerID = regexp.MustCompile(`^[0-9a-f]{64}$`)
	restartRunNumber   = regexp.MustCompile(`^[0-9]+$`)
)

type restartPermit struct{ container, label string }

func restartFixture(lookup func(string) (string, bool)) (restartPermit, error) {
	get := func(key string) string { value, _ := lookup(key); return value }
	if get("GITHUB_ACTIONS") != "true" || get("CHRONICLE_RESTART_FIXTURE") != restartMarker {
		return restartPermit{}, errors.New("restart requires the disposable GitHub Actions fixture")
	}
	if get("RUNNER_ENVIRONMENT") != "github-hosted" {
		return restartPermit{}, errors.New("restart requires a GitHub-hosted runner")
	}
	if get("CHRONICLE_INTEGRATION_CONNECTION_STRING") != restartEndpoint {
		return restartPermit{}, errors.New("restart requires the exact local kernel endpoint")
	}
	id := get("CHRONICLE_RESTART_SERVICE_ID")
	if !restartContainerID.MatchString(id) {
		return restartPermit{}, errors.New("restart requires a full lowercase Docker container ID")
	}
	for _, key := range []string{"DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG"} {
		if _, set := lookup(key); set {
			return restartPermit{}, fmt.Errorf("restart refuses %s", key)
		}
	}
	run, attempt := get("GITHUB_RUN_ID"), get("GITHUB_RUN_ATTEMPT")
	if !restartRunNumber.MatchString(run) || !restartRunNumber.MatchString(attempt) {
		return restartPermit{}, errors.New("restart requires decimal GitHub run ID and attempt")
	}
	return restartPermit{container: id, label: run + "-" + attempt}, nil
}

func (p restartPermit) checkLabel(label string) error {
	if label != p.label {
		return errors.New("restart service label is missing or does not match this run attempt")
	}
	return nil
}

func TestKernelRestartFixtureGuard(t *testing.T) {
	id := strings.Repeat("0123456789abcdef", 4)
	values := map[string]string{
		"GITHUB_ACTIONS": "true", "GITHUB_RUN_ID": "42", "GITHUB_RUN_ATTEMPT": "2", "RUNNER_ENVIRONMENT": "github-hosted",
		"CHRONICLE_RESTART_FIXTURE": restartMarker, "CHRONICLE_INTEGRATION_CONNECTION_STRING": restartEndpoint,
		"CHRONICLE_RESTART_SERVICE_ID": id,
	}
	lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
	permit, err := restartFixture(lookup)
	if err != nil || permit.container != id {
		t.Fatal("valid fixture was not accepted", err)
	}
	if err := permit.checkLabel("42-2"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, key, value string
	}{
		{name: "not Actions", key: "GITHUB_ACTIONS"},
		{name: "missing runner environment", key: "RUNNER_ENVIRONMENT"},
		{name: "self-hosted runner", key: "RUNNER_ENVIRONMENT", value: "self-hosted"},
		{name: "missing marker", key: "CHRONICLE_RESTART_FIXTURE"},
		{name: "missing endpoint", key: "CHRONICLE_INTEGRATION_CONNECTION_STRING"},
		{name: "endpoint alias", key: "CHRONICLE_INTEGRATION_CONNECTION_STRING", value: "chronicle://127.0.0.1:35000"},
		{name: "endpoint options", key: "CHRONICLE_INTEGRATION_CONNECTION_STRING", value: restartEndpoint + "?disableTls=true"},
		{name: "missing container", key: "CHRONICLE_RESTART_SERVICE_ID"},
		{name: "short container", key: "CHRONICLE_RESTART_SERVICE_ID", value: id[:12]},
		{name: "uppercase container", key: "CHRONICLE_RESTART_SERVICE_ID", value: strings.ToUpper(id)},
		{name: "option instead of container", key: "CHRONICLE_RESTART_SERVICE_ID", value: "--latest"},
		{name: "host override", key: "DOCKER_HOST", value: "tcp://localhost:2375"},
		{name: "context override", key: "DOCKER_CONTEXT", value: "other"},
		{name: "config override", key: "DOCKER_CONFIG", value: "/other"},
		{name: "missing run ID", key: "GITHUB_RUN_ID"},
		{name: "malformed attempt", key: "GITHUB_RUN_ATTEMPT", value: "1-2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			invalid, err := restartFixture(func(key string) (string, bool) {
				if key == tc.key {
					return tc.value, tc.value != ""
				}
				return lookup(key)
			})
			if err == nil || invalid != (restartPermit{}) {
				t.Fatal("invalid fixture did not fail closed")
			}
		})
	}
	for _, label := range []string{"", "42-1", "43-2"} {
		if permit.checkLabel(label) == nil {
			t.Fatalf("accepted label %q from outside this run attempt", label)
		}
	}
	t.Run("paired losses during stop grace", testRestartLossDuringStopGrace)
}

func testRestartLossDuringStopGrace(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	hooks := &restartHooks{wake: make(chan struct{}, 1)}
	connected, disconnected := hooks.record(true), hooks.record(false)
	initial := chronicle.ConnectionEvent{Generation: 1, ConnectionID: "initial-session"}
	connected(ctx, initial)
	hooks.requireLive(t, "before stop", initial)

	loss := status.Error(codes.Unavailable, "transport ended")
	disconnected(ctx, chronicle.ConnectionEvent{Generation: initial.Generation, ConnectionID: initial.ConnectionID, Err: loss})
	connected(ctx, chronicle.ConnectionEvent{Generation: 2, ConnectionID: "intermediate-session"})
	latestLoss := chronicle.ConnectionEvent{Generation: 2, ConnectionID: "intermediate-session", Err: loss}
	disconnected(ctx, latestLoss)
	awaitStoppedRestartLoss(t, ctx, hooks)
	if hooks.live() || hooks.last != latestLoss {
		t.Fatalf("expected the latest paired loss with no live generation; %s", hooks.summary())
	}
}

func (p restartPermit) requireLabel(t *testing.T, ctx context.Context) {
	t.Helper()
	probe, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	format := `{{ index .Config.Labels "` + restartLabel + `" }}`
	label, err := exec.CommandContext(probe, "docker", "--context", "default", "inspect", "--type", "container", "--format", format, p.container).Output()
	if err != nil {
		t.Fatal("restart service label inspection failed", restartCause(err))
	}
	if err := p.checkLabel(strings.TrimSuffix(string(label), "\n")); err != nil {
		t.Fatal(err)
	}
}

func (p restartPermit) changeService(t *testing.T, ctx context.Context, action string) {
	t.Helper()
	args := []string{"--context", "default"}
	switch action {
	case "stop":
		args = append(args, "stop", "-t", "10", p.container)
	case "start":
		args = append(args, "start", p.container)
	default:
		t.Fatal("invalid restart service action")
	}
	p.requireLabel(t, ctx)
	operation, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	if err := exec.CommandContext(operation, "docker", args...).Run(); err != nil {
		t.Fatalf("docker %s failed: %v", action, err)
	}
}

type restartHookRecord struct {
	event     chronicle.ConnectionEvent
	connected bool
}

// The SDK orders delivered hooks. Only the test goroutine consumes records and
// owns the cursor; hook callbacks never wait for a receiver, including on cleanup.
type restartHooks struct {
	mu      sync.Mutex
	pending []restartHookRecord
	wake    chan struct{}

	last, loss chronicle.ConnectionEvent
}

func (h *restartHooks) record(connected bool) chronicle.ConnectionHook {
	return func(_ context.Context, event chronicle.ConnectionEvent) {
		h.mu.Lock()
		h.pending = append(h.pending, restartHookRecord{event: event, connected: connected})
		h.mu.Unlock()
		select {
		case h.wake <- struct{}{}:
		default:
		}
	}
}

func (h *restartHooks) drain(t *testing.T, stage string) {
	t.Helper()
	h.mu.Lock()
	records := h.pending
	h.pending = nil
	h.mu.Unlock()
	for _, record := range records {
		if record.connected {
			h.acceptConnection(t, stage, record.event)
		} else {
			h.acceptLoss(t, stage, record.event)
		}
	}
}

func (h *restartHooks) acceptConnection(t *testing.T, stage string, event chronicle.ConnectionEvent) {
	t.Helper()
	if event.Err != nil || event.ConnectionID == "" {
		t.Fatalf("%s: invalid Connected generation %d (%s); %s", stage, event.Generation, restartCause(event.Err), h.summary())
	}
	if h.live() || event.Generation <= h.last.Generation {
		t.Fatalf("%s: out-of-order Connected generation %d; %s", stage, event.Generation, h.summary())
	}
	h.last = event
}

func (h *restartHooks) acceptLoss(t *testing.T, stage string, event chronicle.ConnectionEvent) {
	t.Helper()
	paired := h.last.Generation == event.Generation && h.last.ConnectionID == event.ConnectionID
	if !h.live() || !paired {
		t.Fatalf("%s: unpaired Disconnected generation %d; %s", stage, event.Generation, h.summary())
	}
	if event.Err == nil || errors.Is(event.Err, chronicle.ErrClosed) {
		t.Fatalf("%s: invalid Disconnected cause (%s); %s", stage, restartCause(event.Err), h.summary())
	}
	h.last, h.loss = event, event
	if restartAuthFailure(event.Err) {
		t.Fatalf("%s: SDK terminal authentication or permission failure; %s", stage, h.summary())
	}
}

func restartAuthFailure(err error) bool {
	var auth *connection.AuthenticationError
	if errors.As(err, &auth) {
		return true
	}
	code := status.Code(err)
	return code == codes.Unauthenticated || code == codes.PermissionDenied
}

func (h *restartHooks) live() bool { return h.last.Generation != 0 && h.last.Err == nil }

func (h *restartHooks) summary() string {
	return fmt.Sprintf("last delivered generation %d (%s), last disconnect %d (%s)",
		h.last.Generation, restartCause(h.last.Err), h.loss.Generation, restartCause(h.loss.Err))
}

func (h *restartHooks) await(t *testing.T, ctx context.Context, stage string, live bool) chronicle.ConnectionEvent {
	t.Helper()
	for {
		h.drain(t, stage)
		if h.last.Generation != 0 && h.live() == live && ctx.Err() == nil {
			return h.last
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s: %v; %s", stage, ctx.Err(), h.summary())
		case <-h.wake:
		}
	}
}

func (h *restartHooks) requireLive(t *testing.T, stage string, selected chronicle.ConnectionEvent) {
	t.Helper()
	h.drain(t, stage)
	if !h.live() || h.last.Generation != selected.Generation {
		t.Fatalf("%s: selected generation %d dropped; %s", stage, selected.Generation, h.summary())
	}
}

func restartCause(err error) string {
	var auth *connection.AuthenticationError
	switch {
	case err == nil:
		return "none"
	case errors.As(err, &auth):
		return fmt.Sprintf("authentication HTTP %d", auth.StatusCode)
	case errors.Is(err, chronicle.ErrClosed):
		return "closed"
	case errors.Is(err, chronicle.ErrSessionLost):
		return "session lost"
	case errors.Is(err, chronicle.ErrProtocol):
		return "protocol"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline exceeded"
	case errors.Is(err, io.EOF):
		return "stream ended"
	default:
		return status.Code(err).String()
	}
}

type restartEvent struct{ Identity string }

type restartObserverScope struct{ store, namespace string }

type restartAttachment struct {
	scope    restartObserverScope
	hooks    *restartHooks
	selected chronicle.ConnectionEvent
}

type restartDelivery struct {
	store         *chronicle.EventStore
	source        events.SourceID
	handled       <-chan restartEvent
	before, after restartEvent
}

func TestKernelRestartAutomaticallyReattachesReactor(t *testing.T) {
	permit, err := restartFixture(os.LookupEnv)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	permit.requireLabel(t, ctx)

	f := newKernelFixture(t)
	registry := integrationRegistry[restartEvent](t, events.WithID("go-kernel-restart-event"))
	handled := make(chan restartEvent, 16)
	if err := chronicle.RegisterReactorHandler(registry, restartReactor, func(ctx context.Context, value restartEvent) error {
		select {
		case handled <- value:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}); err != nil {
		t.Fatal(err)
	}
	hooks := &restartHooks{wake: make(chan struct{}, 1)}
	client := f.client(registry, chronicle.WithOnConnected(hooks.record(true)), chronicle.WithOnDisconnected(hooks.record(false)))
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal("initial store", restartCause(err))
	}
	scope := restartObserverScope{store: string(store.Name()), namespace: string(store.Namespace())}
	initial := awaitInitialRestartRegistration(t, f.ctx, hooks, store)
	attachment := restartAttachment{scope: scope, hooks: hooks, selected: initial}
	attachment.awaitActive(t, f.ctx, "initial attachment", f.conn)
	delivery := restartDelivery{
		store: store, source: events.SourceID(uuid.NewString()), handled: handled,
		before: restartEvent{Identity: uuid.NewString()}, after: restartEvent{Identity: uuid.NewString()},
	}
	delivery.append(t, f.ctx, delivery.before)
	delivery.await(t, f.ctx, delivery.before)
	hooks.requireLive(t, "before stop", initial)

	// No application SDK calls from here until the passive attachment proof succeeds.
	permit.changeService(t, ctx, "stop")
	awaitStoppedRestartLoss(t, ctx, hooks)
	permit.changeService(t, ctx, "start")
	awaitRestartHealth(t, ctx, hooks)
	successor := awaitRestartSuccessor(t, ctx, hooks, initial)
	attachment.selected = successor
	admin, auth := attachment.awaitPostStart(t, ctx)

	operation, finish := context.WithTimeout(ctx, 30*time.Second)
	defer finish()
	delivery.append(t, operation, delivery.after)
	delivery.await(t, operation, delivery.after)
	delivery.assertPersisted(t, metadata.NewOutgoingContext(operation, auth), scope, admin)
	t.Logf("same service restarted; reactor moved from generation %d to %d before any application call; handler delivered and both events persisted", initial.Generation, successor.Generation)
}

func awaitInitialRestartRegistration(t *testing.T, ctx context.Context, hooks *restartHooks, store *chronicle.EventStore) chronicle.ConnectionEvent {
	t.Helper()
	initial := hooks.await(t, ctx, "initial connection", true)
	outcome, err := store.WaitForRegistration(ctx)
	if err != nil || !outcome.IsSuccess() || outcome.Generation != initial.Generation {
		t.Fatalf("initial registration: generation %d, want successful generation %d (%s)", outcome.Generation, initial.Generation, restartCause(err))
	}
	return initial
}

func awaitStoppedRestartLoss(t *testing.T, ctx context.Context, hooks *restartHooks) {
	t.Helper()
	loss, cancel := context.WithTimeout(ctx, 35*time.Second)
	defer cancel()
	// The caller required the initial generation to be live before stop.
	// Ordered validation cannot leave it without its exact paired loss.
	// The newest loss may be from a later generation during the stop grace.
	hooks.await(t, loss, "disconnect while stopped", false)
}

func awaitRestartSuccessor(t *testing.T, ctx context.Context, hooks *restartHooks, initial chronicle.ConnectionEvent) chronicle.ConnectionEvent {
	t.Helper()
	ready, cancel := context.WithTimeout(ctx, 70*time.Second)
	defer cancel()
	const stage = "kernel healthy / SDK produced no live successor"
	successor := hooks.await(t, ready, stage, true)
	if successor.Generation <= initial.Generation || successor.ConnectionID == initial.ConnectionID {
		t.Fatalf("kernel healthy / successor is not a new session; %s", hooks.summary())
	}
	return successor
}

func restartTLS() *tls.Config {
	// The only target is the isolated localhost development fixture.
	return &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true}
}

func awaitRestartHealth(t *testing.T, ctx context.Context, hooks *restartHooks) {
	t.Helper()
	const stage = "restarted service health"
	health, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	transport := &http.Transport{TLSClientConfig: restartTLS()}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		hooks.drain(t, stage)
		if restartKernelHealthy(health, client) {
			return
		}
		select {
		case <-health.Done():
			t.Fatalf("%s: %v; %s", stage, health.Err(), hooks.summary())
		case <-ticker.C:
		}
	}
}

func restartKernelHealthy(ctx context.Context, client *http.Client) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, restartHealthURL, nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64))
	return err == nil && resp.StatusCode == http.StatusOK && strings.TrimSpace(string(body)) == "Healthy"
}

func (a restartAttachment) awaitPostStart(t *testing.T, ctx context.Context) (*grpc.ClientConn, metadata.MD) {
	t.Helper()
	attachment, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	auth := a.adminCredentials(t, attachment)
	admin, err := grpc.NewClient(restartAddress, grpc.WithTransportCredentials(credentials.NewTLS(restartTLS())), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal("post-start admin channel", restartCause(err))
	}
	t.Cleanup(func() {
		if err := admin.Close(); err != nil {
			t.Error("post-start admin close", restartCause(err))
		}
	})
	a.awaitActive(t, metadata.NewOutgoingContext(attachment, auth), "automatic attachment", admin)
	return admin, auth
}

func (a restartAttachment) adminCredentials(t *testing.T, ctx context.Context) metadata.MD {
	t.Helper()
	const stage = "post-start admin token"
	// Match the independent development admin setup in newKernelFixture.
	oauth := connection.NewOAuth(restartAddress, "chronicle-dev-client", "chronicle-dev-secret", restartTLS())
	defer oauth.Close()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		a.hooks.requireLive(t, stage, a.selected)
		probe, cancel := context.WithTimeout(ctx, 3*time.Second)
		token, err := oauth.Token(probe)
		cancel()
		if err == nil {
			return metadata.Pairs("authorization", "Bearer "+token.AccessToken)
		}
		// The HTTP exchange has no gRPC status. Only known startup transport
		// failures may retry; rejected credentials and malformed tokens never do.
		if !transientRestartTokenFailure(err) {
			t.Fatalf("%s: %s; %s", stage, restartCause(err), a.hooks.summary())
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s: %v; %s", stage, ctx.Err(), a.hooks.summary())
		case <-ticker.C:
		}
	}
}

func transientRestartTokenFailure(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET)
}

func (a restartAttachment) awaitActive(t *testing.T, ctx context.Context, stage string, conn *grpc.ClientConn) {
	t.Helper()
	observers := observation.NewObserversClient(conn)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		a.hooks.requireLive(t, stage, a.selected)
		attached, err := a.scope.probe(ctx, observers, a.selected.ConnectionID)
		a.hooks.requireLive(t, stage, a.selected)
		switch status.Code(err) {
		case codes.OK, codes.Unavailable, codes.DeadlineExceeded:
		default:
			t.Fatalf("%s: admin observer query: %s; %s", stage, restartCause(err), a.hooks.summary())
		}
		if attached && ctx.Err() == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s: admin did not observe exact membership and Active for generation %d (%s); %s", stage, a.selected.Generation, restartCause(err), a.hooks.summary())
		case <-ticker.C:
		}
	}
}

func (s restartObserverScope) probe(ctx context.Context, client observation.ObserversClient, id string) (bool, error) {
	membership, done := context.WithTimeout(ctx, 2*time.Second)
	members, err := client.GetConnectedClientsForObserver(membership, &observation.GetConnectedClientsForObserverRequest{
		EventStore: s.store, Namespace: s.namespace, EventSequenceId: restartSequence, ObserverId: restartReactor,
	})
	done()
	if err != nil {
		return false, err
	}
	if items := members.GetItems(); len(items) != 1 || items[0].GetConnectionId() != id {
		return false, nil
	}
	readiness, done := context.WithTimeout(ctx, 2*time.Second)
	defer done()
	info, err := client.GetObserverInformation(readiness, &observation.GetObserverInformationRequest{
		EventStore: s.store, Namespace: s.namespace, EventSequenceId: restartSequence, ObserverId: restartReactor,
	})
	if err != nil {
		return false, err
	}
	return info.GetEventSequenceId() == restartSequence && info.GetIsSubscribed() && info.GetRunningState() == observation.ObserverRunningState_Active, nil
}

func (d restartDelivery) append(t *testing.T, ctx context.Context, value restartEvent) {
	t.Helper()
	result, err := d.store.EventLog().Append(ctx, d.source, value)
	if err != nil || result.Err() != nil || result.Disposition != eventsequences.Committed {
		t.Fatalf("append %s was not committed (disposition %v, RPC %s, result %s)", value.Identity, result.Disposition, restartCause(err), restartCause(result.Err()))
	}
}

func (d restartDelivery) await(t *testing.T, ctx context.Context, want restartEvent) {
	t.Helper()
	for {
		select {
		case got := <-d.handled:
			if got == want {
				return
			}
			// After restart, tolerate at-least-once delivery of the known before event.
			if want != d.after || got != d.before {
				t.Fatal("reactor delivered an unexpected event identity")
			}
		case <-ctx.Done():
			t.Fatalf("reactor did not deliver event %s: %v", want.Identity, ctx.Err())
		}
	}
}

func (d restartDelivery) assertPersisted(t *testing.T, ctx context.Context, scope restartObserverScope, conn *grpc.ClientConn) {
	t.Helper()
	read, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	response, err := sequences.NewEventSequencesClient(conn).ForEventSourceIdAndEventTypes(read, &sequences.ForEventSourceIdAndEventTypesRequest{
		EventStore: scope.store, Namespace: scope.namespace, EventSequenceId: restartSequence, EventSourceId: string(d.source),
	})
	if err != nil {
		t.Fatal("admin read-back", restartCause(err))
	}
	if err := wire.CheckEnvelope(response); err != nil {
		t.Fatal("admin read-back returned an unsuccessful envelope")
	}
	persisted := response.GetData()
	if len(persisted) != 2 {
		t.Fatalf("persisted events = %d, want 2", len(persisted))
	}
	for i, want := range []restartEvent{d.before, d.after} {
		var got restartEvent
		if err := json.Unmarshal([]byte(persisted[i].GetContent()), &got); err != nil || got != want {
			t.Fatalf("persisted event %d did not match the appended identity", i)
		}
	}
}
