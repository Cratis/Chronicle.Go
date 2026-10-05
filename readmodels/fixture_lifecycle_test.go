// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func TestServiceFixtureServeAfterStop(t *testing.T) {
	server := grpc.NewServer()
	server.Stop()
	listener := bufconn.Listen(1024)
	t.Cleanup(func() { _ = listener.Close() })
	if err := server.Serve(listener); !errors.Is(err, grpc.ErrServerStopped) {
		t.Fatalf("Serve after Stop = %v, want ErrServerStopped", err)
	}
	served := make(chan struct{})
	var reported []any
	go serveServiceFixture(server, listener, served, func(args ...any) { reported = args })
	select {
	case <-served:
	case <-time.After(5 * time.Second):
		t.Fatal("fixture Serve did not join after Stop")
	}
	if len(reported) != 0 {
		t.Fatalf("fixture reported expected shutdown: %v", reported)
	}
}

func TestServiceFixtureServeReportsUnexpectedError(t *testing.T) {
	server := grpc.NewServer()
	t.Cleanup(server.Stop)
	listener := bufconn.Listen(1024)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	_, want := listener.Accept()
	if want == nil || errors.Is(want, grpc.ErrServerStopped) {
		t.Fatalf("closed listener error = %v, want unexpected Serve error", want)
	}
	served := make(chan struct{})
	var reported []any
	go serveServiceFixture(server, listener, served, func(args ...any) { reported = args })
	select {
	case <-served:
	case <-time.After(5 * time.Second):
		t.Fatal("fixture Serve did not join after listener failure")
	}
	if len(reported) != 1 {
		t.Fatalf("reported errors = %v, want one listener failure", reported)
	}
	if err, ok := reported[0].(error); !ok || !errors.Is(err, want) {
		t.Fatalf("reported error = %v, want %v", reported[0], want)
	}
}
