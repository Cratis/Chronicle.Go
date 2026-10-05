//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHistoryProjectionPollingSuccess(t *testing.T) {
	want := readmodels.Instance[ProjectionAccount]{Exists: true, Value: ProjectionAccount{Name: "before"}}
	got, err := pollHistoryProjection(t.Context(), 15*time.Second, func(context.Context) (readmodels.Instance[ProjectionAccount], error) {
		return want, nil
	})
	if err != nil || !got.Exists || got.Value.Name != want.Value.Name {
		t.Fatalf("poll = %+v, %v; want %+v, nil", got, err, want)
	}
}

func TestHistoryProjectionPollingExhaustionRequiresSuccessfulReads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		want := readmodels.Instance[ProjectionAccount]{Exists: true, Value: ProjectionAccount{Name: "original"}}
		reads := 0
		got, err := pollHistoryProjection(t.Context(), 15*time.Second, func(context.Context) (readmodels.Instance[ProjectionAccount], error) {
			reads++
			return want, nil
		})
		if err != errInitialHistoryProjectionPollExhausted || reads < 2 || !got.Exists || got.Value.Name != want.Value.Name {
			t.Fatalf("poll = %+v, %v; successful reads = %d", got, err, reads)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("local polling exhaustion must have its own identity")
		}
	})
}

func TestHistoryProjectionPollingPreservesRemoteDeadlineBeforeBudget(t *testing.T) {
	want := wire.RPCError(status.Error(codes.DeadlineExceeded, "remote read deadline"))
	if !errors.Is(want, context.DeadlineExceeded) {
		t.Fatal("fixture must reproduce the wire deadline identity")
	}
	_, err := pollHistoryProjection(t.Context(), 15*time.Second, func(ctx context.Context) (readmodels.Instance[ProjectionAccount], error) {
		if ctx.Err() != nil {
			t.Fatal("local budget expired before remote failure")
		}
		return readmodels.Instance[ProjectionAccount]{}, want
	})
	if err != want || err == errInitialHistoryProjectionPollExhausted {
		t.Fatalf("read error replaced by %v; want %v", err, want)
	}
}

func TestHistoryProjectionPollingPreservesReadErrorAtLocalDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		want := wire.RPCError(status.Error(codes.DeadlineExceeded, "read failed as local budget elapsed"))
		_, err := pollHistoryProjection(t.Context(), 15*time.Second, func(ctx context.Context) (readmodels.Instance[ProjectionAccount], error) {
			<-ctx.Done()
			return readmodels.Instance[ProjectionAccount]{}, want
		})
		if err != want || err == errInitialHistoryProjectionPollExhausted {
			t.Fatalf("coincident read error replaced by %v; want %v", err, want)
		}
	})
}

func TestHistoryProjectionPollingPreservesReadErrorAtParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	want := errors.New("read failed concurrently with cancellation")
	_, err := pollHistoryProjection(ctx, 15*time.Second, func(context.Context) (readmodels.Instance[ProjectionAccount], error) {
		cancel()
		return readmodels.Instance[ProjectionAccount]{}, want
	})
	if err != want || err == errInitialHistoryProjectionPollExhausted {
		t.Fatalf("coincident read error replaced by %v; want %v", err, want)
	}
}

func TestHistoryProjectionPollingParentCancellationIsNotLocalExhaustion(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, err := pollHistoryProjection(ctx, 15*time.Second, func(context.Context) (readmodels.Instance[ProjectionAccount], error) {
		cancel()
		return readmodels.Instance[ProjectionAccount]{}, nil
	})
	if err != context.Canceled {
		t.Fatalf("poll cancellation = %v; want context.Canceled", err)
	}
}

func TestHistoryProjectionPollingPreservesReadFailureAndLastSuccessfulSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		want := errors.New("reader failed")
		reads := 0
		got, err := pollHistoryProjection(t.Context(), 15*time.Second, func(context.Context) (readmodels.Instance[ProjectionAccount], error) {
			reads++
			if reads == 1 {
				return readmodels.Instance[ProjectionAccount]{Exists: true, Value: ProjectionAccount{Name: "original"}}, nil
			}
			return readmodels.Instance[ProjectionAccount]{}, want
		})
		if err != want || !got.Exists || got.Value.Name != "original" || reads != 2 {
			t.Fatalf("poll = %+v, %v; reads = %d", got, err, reads)
		}
	})
}
