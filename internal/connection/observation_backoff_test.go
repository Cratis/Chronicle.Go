// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package connection

import (
	"math"
	"testing"
	"time"
)

type backoffClock struct{ now time.Time }

func (c *backoffClock) Now() time.Time              { return c.now }
func (c *backoffClock) advance(delay time.Duration) { c.now = c.now.Add(delay) }

func testBackoff(jitter float64) (*ObservationBackoff, *backoffClock) {
	clock := &backoffClock{now: time.Unix(1_700_000_000, 0)}
	return NewObservationBackoff(2*time.Second, 60*time.Second, 3*time.Minute, clock.Now, func() float64 { return jitter }), clock
}

func endedWithoutWorking(b *ObservationBackoff, times int) []time.Duration {
	var delays []time.Duration
	for range times {
		b.Opened()
		delays = append(delays, b.Next())
	}
	return delays
}

func TestObservationBackoffDoublesUpToTheMaximum(t *testing.T) {
	b, _ := testBackoff(1)
	want := []time.Duration{2, 4, 8, 16, 32, 60, 60}
	for i, delay := range endedWithoutWorking(b, len(want)) {
		if delay != want[i]*time.Second {
			t.Fatalf("delay %d = %s, want %s", i+1, delay, want[i]*time.Second)
		}
	}
}

func TestObservationBackoffJittersIntoTheUpperHalf(t *testing.T) {
	for _, tc := range []struct {
		jitter float64
		want   time.Duration
	}{{0, time.Second}, {0.5, 1500 * time.Millisecond}, {-1, time.Second}, {2, 2 * time.Second}, {math.NaN(), time.Second}} {
		b, _ := testBackoff(tc.jitter)
		b.Opened()
		if delay := b.Next(); delay != tc.want {
			t.Errorf("jitter %v: delay = %s, want %s", tc.jitter, delay, tc.want)
		}
	}
}

func TestObservationBackoffStartsOverAfterADelivery(t *testing.T) {
	b, _ := testBackoff(1)
	endedWithoutWorking(b, 3)
	b.Opened()
	b.Delivered()
	if delay := b.Next(); delay != 2*time.Second {
		t.Fatalf("delay after delivery = %s, want 2s", delay)
	}
}

func TestObservationBackoffStartsOverAfterAStreamStayedOpen(t *testing.T) {
	b, clock := testBackoff(1)
	endedWithoutWorking(b, 3)
	b.Opened()
	clock.advance(3*time.Minute - time.Nanosecond)
	if delay := b.Next(); delay != 16*time.Second {
		t.Fatalf("delay just short of the healthy window = %s, want 16s", delay)
	}
	b.Opened()
	clock.advance(3 * time.Minute)
	if delay := b.Next(); delay != 2*time.Second {
		t.Fatalf("delay after a healthy stream = %s, want 2s", delay)
	}
}

func TestObservationBackoffNeverOverflows(t *testing.T) {
	b, _ := testBackoff(1)
	for _, delay := range endedWithoutWorking(b, 200) {
		if delay <= 0 || delay > 60*time.Second {
			t.Fatalf("delay = %s, want within (0, 60s]", delay)
		}
	}
}

func TestObservationBackoffUsesANonPositiveBaseAsItIs(t *testing.T) {
	clock := &backoffClock{}
	b := NewObservationBackoff(0, time.Minute, time.Minute, clock.Now, func() float64 { return 1 })
	if delay := b.Next(); delay != 0 {
		t.Fatalf("delay = %s, want 0", delay)
	}
}
