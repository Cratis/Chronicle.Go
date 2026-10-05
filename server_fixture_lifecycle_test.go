// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func TestKernelFixtureServeAfterStopJoinsWithoutReportingShutdown(t *testing.T) {
	server := grpc.NewServer()
	server.Stop() // Deterministically make cleanup win the race to Serve.
	listener := bufconn.Listen(1024)
	t.Cleanup(func() { _ = listener.Close() })
	served := make(chan struct{})
	var reported []any
	go serveKernelFixture(server, listener, served, func(args ...any) { reported = args })
	<-served
	if len(reported) != 0 {
		t.Fatalf("fixture reported expected shutdown: %v", reported)
	}
}

func TestKernelFixtureReportsListenerFailureBeforeJoining(t *testing.T) {
	server := grpc.NewServer()
	t.Cleanup(server.Stop)
	listener := bufconn.Listen(1024)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	_, want := listener.Accept()
	if want == nil || want == grpc.ErrServerStopped {
		t.Fatalf("closed listener error = %v, want unexpected Serve error", want)
	}
	served := make(chan struct{})
	reporting := make(chan struct{})
	release := make(chan struct{})
	var reported []any
	go serveKernelFixture(server, listener, served, func(args ...any) {
		close(reporting)
		<-release
		reported = args
	})
	t.Cleanup(func() {
		close(release)
		<-served
		if len(reported) != 1 || reported[0] != want {
			t.Errorf("reported errors = %v, want exactly [%v]", reported, want)
		}
	})
	select {
	case <-reporting:
	case <-served:
		t.Fatal("fixture joined without reporting the listener failure")
	}
	select {
	case <-served:
		t.Error("fixture joined before error reporting completed")
	default:
	}
}
