// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"errors"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func TestFixtureServeStartsAfterStop(t *testing.T) {
	listener := bufconn.Listen(1024)
	server := grpc.NewServer()
	start := make(chan struct{})
	done := make(chan error)
	go func() {
		<-start
		done <- server.Serve(listener)
		close(done)
	}()

	// Force the exact ordering from a lazy-client, zero-RPC fixture cleanup,
	// without sleeps, dispatching a dummy RPC or relying on scheduler luck.
	server.Stop()
	close(start)
	err := <-done
	// Receiving the closed channel joins the worker, including its final send.
	if _, open := <-done; open {
		t.Fatal("fixture server worker did not finish")
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(err, grpc.ErrServerStopped) {
		t.Fatalf("Serve after Stop = %v, want grpc.ErrServerStopped", err)
	}
	if err := fixtureServeError(err); err != nil {
		t.Fatalf("expected fixture shutdown reported as failure: %v", err)
	}
}

type failingFixtureListener struct {
	net.Listener
	failure error
}

func (l failingFixtureListener) Accept() (net.Conn, error) { return nil, l.failure }

func TestFixtureServePreservesUnexpectedListenerFailure(t *testing.T) {
	failure := errors.New("unexpected fixture listener failure")
	listener := failingFixtureListener{Listener: bufconn.Listen(1024), failure: failure}
	server := grpc.NewServer()
	defer server.Stop()
	// Serve returns this fatal Accept error directly in the pinned grpc-go API.
	// Unlike ErrServerStopped, it must remain a test failure, unchanged.
	err := fixtureServeError(server.Serve(listener))
	if err != failure {
		t.Fatalf("unexpected listener error = %v, want original cause %v", err, failure)
	}
}
