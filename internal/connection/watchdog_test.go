// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package connection

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestWatchdogResetsAndDetectsSilentStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		beats := make(chan struct{}, 1)
		result := make(chan error, 1)
		done := make(chan error, 1)
		go func() { done <- Watch(t.Context(), 5*time.Second, beats, result) }()
		synctest.Wait()
		time.Sleep(4 * time.Second)
		beats <- struct{}{}
		synctest.Wait()
		time.Sleep(4 * time.Second)
		select {
		case err := <-done:
			t.Fatalf("premature staleness: %v", err)
		default:
		}
		time.Sleep(time.Second)
		if err := <-done; !errors.Is(err, ErrStale) {
			t.Fatal(err)
		}
	})
}

func TestWatchdogCancellationAndReceiverFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		done := make(chan error, 1)
		go func() { done <- Watch(ctx, time.Hour, nil, result) }()
		synctest.Wait()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		expected := errors.New("receiver failed")
		result <- expected
		if err := Watch(t.Context(), time.Hour, nil, result); !errors.Is(err, expected) {
			t.Fatal(err)
		}
	})
}

func TestBackoffIsBoundedAndWaitCancels(t *testing.T) {
	for _, attempt := range []int{1, 2, 6, 1000000} {
		delay := Backoff(attempt, time.Second, 30*time.Second)
		if delay < time.Second/2 || delay > 30*time.Second {
			t.Fatal(delay)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := Wait(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
