//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"crypto/tls"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/connection"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestKernelConfiguredFirstAppendRejectsConcurrentWriter(t *testing.T) {
	endpoint := os.Getenv("CHRONICLE_INTEGRATION_CONNECTION_STRING")
	if endpoint == "" {
		t.Fatal("set CHRONICLE_INTEGRATION_CONNECTION_STRING; integration never silently skips")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	uri, err := chronicle.ParseConnectionString(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true} // Own development kernel only.
	tokens := connection.NewOAuth(uri.Addresses()[0].String(), "chronicle-dev-client", "chronicle-dev-secret", tlsConfig)
	defer tokens.Close()
	var tailReads atomic.Int32
	release := make(chan struct{})
	// Hold both real empty-tail responses before either default-policy append can
	// dispatch. This deterministically reproduces concurrent first appends without
	// sleeps or replacing the SDK's automatic scope selection.
	conn, err := grpc.NewClient(uri.Addresses()[0].String(), grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), grpc.WithDisableRetry(), grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
			return err
		}
		if strings.HasSuffix(method, "/TailSequenceNumber") {
			if tailReads.Add(1) == 2 {
				close(release)
			}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	}()
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[BatchOpened](registry, events.WithID("slice2b-opened")); err != nil {
		t.Fatal(err)
	}
	client, err := chronicle.Dial(ctx, chronicle.WithGRPCConnection(conn), chronicle.WithTokenSource(tokens), chronicle.WithRegistry(registry), chronicle.WithCheckFirstAppendIntoAScope(true))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	}()
	store, err := client.EventStore(ctx, chronicle.StoreName("go-first-append-"+uuid.NewString()))
	if err != nil {
		t.Fatal(err)
	}
	// A non-log handle must inherit the same frozen client policy.
	sequence, err := store.EventSequence("slice2b-first-appends")
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		result eventsequences.AppendResult
		err    error
	}
	outcomes := make(chan outcome, 2)
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			result, err := sequence.Append(ctx, "same-source", BatchOpened{Value: "competing"})
			outcomes <- outcome{result: result, err: err}
		}()
	}
	committed, rejected := 0, 0
	for range 2 {
		outcome := <-outcomes
		if outcome.err != nil {
			t.Fatal(outcome.err)
		}
		switch outcome.result.Disposition {
		case eventsequences.Committed:
			committed++
			if !outcome.result.ConcurrencyCheckPerformed {
				t.Fatal("first append was not protected")
			}
		case eventsequences.Rejected:
			rejected++
			if len(outcome.result.ConcurrencyViolations) != 1 {
				t.Fatalf("rejection = %+v", outcome.result)
			}
		default:
			t.Fatalf("unknown disposition: %+v", outcome.result)
		}
	}
	if committed != 1 || rejected != 1 || tailReads.Load() != 2 {
		t.Fatalf("committed=%d rejected=%d tailReads=%d", committed, rejected, tailReads.Load())
	}
	persisted, err := sequence.ReadSource(ctx, "same-source", eventsequences.SourceFilter{})
	if err != nil || len(persisted) != 1 {
		t.Fatalf("persisted=%d error=%v", len(persisted), err)
	}
}
