// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/contracts/observation/reducers"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReducerOpenFailurePreservesRegistrationProgress(t *testing.T) {
	failure := status.Error(codes.InvalidArgument, "reducer registration rejected")
	kernel := &supervisedKernel{readModels: &seedModels{called: &atomic.Bool{}}, reducers: &seedReducers{}}
	kernel.streamInterceptor = func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if method == reducers.Reducers_Observe_FullMethodName {
			return nil, failure
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
	waitForCancellation := func(c *clientConfig) {
		c.observerRetryWait = func(ctx context.Context, _ time.Duration) error {
			<-ctx.Done()
			return ctx.Err()
		}
	}
	client, ctx := supervisionClient(t, kernel, WithRegistry(foldRegistry(t, sumFold)), waitForCancellation)
	_, err := client.EventStore(ctx, "store")
	assertReducerRegistrationFailure(t, err, failure)
	assertReducerRegistrationFailure(t, client.Ready(ctx), failure)
}

func assertReducerRegistrationFailure(t *testing.T, err, failure error) {
	t.Helper()
	var registration *RegistrationError
	if !errors.As(err, &registration) {
		t.Fatalf("registration error = %v", err)
	}
	if !errors.Is(err, failure) {
		t.Fatalf("lost reducer failure: %v", err)
	}
	assertReducerRegistrationOutcome(t, registration.Outcome, failure)
}

func assertReducerRegistrationOutcome(t *testing.T, outcome RegistrationOutcome, failure error) {
	t.Helper()
	if !outcome.HasRun || !outcome.RetryPending || outcome.IsSuccess() {
		t.Fatalf("lost registration state: %+v", outcome)
	}
	if len(outcome.Artifacts) == 0 {
		t.Fatal("lost acknowledged registration progress")
	}
	last := outcome.Artifacts[len(outcome.Artifacts)-1]
	if last.Name != "reducers" || !errors.Is(last.Failure, failure) {
		t.Fatalf("failed artifact = %+v", last)
	}
	if outcome.Artifacts[0].Failure != nil {
		t.Fatalf("lost first-stage acknowledgement: %+v", outcome.Artifacts[0])
	}
}
