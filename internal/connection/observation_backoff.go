// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package connection

import "time"

const (
	// ObservationBaseDelay is the wait after a stream that had worked.
	ObservationBaseDelay = 2 * time.Second
	// ObservationMaxDelay is the longest wait between attempts.
	ObservationMaxDelay = 60 * time.Second
	// ObservationHealthyStream is how long an open stream must stay open to count as working.
	ObservationHealthyStream = 3 * time.Minute

	maxObservationExponent = 30
)

// ObservationBackoff decides how long one reactor or reducer waits before it
// opens its observation stream again. A fixed delay brings a herd of observers
// whose streams failed together back together, so each stream that ends without
// having worked doubles the wait up to the maximum, and every wait lands at
// random in the upper half of its ceiling. The wait starts over once a stream
// delivered something or stayed open for the healthy duration. A base delay of
// zero or less is returned as it is.
//
// One observer worker owns an ObservationBackoff; it is not safe for concurrent use.
type ObservationBackoff struct {
	base, maximum, healthy time.Duration
	now                    func() time.Time
	jitter                 func() float64
	endedWithoutWorking    int
	openedAt               time.Time
}

// NewObservationBackoff returns a backoff reading time from now and jitter in [0, 1] from jitter.
func NewObservationBackoff(base, maximum, healthy time.Duration, now func() time.Time, jitter func() float64) *ObservationBackoff {
	return &ObservationBackoff{base: base, maximum: maximum, healthy: healthy, now: now, jitter: jitter}
}

// Opened records that the stream is being opened, which starts its healthy window.
func (b *ObservationBackoff) Opened() { b.openedAt = b.now() }

// Delivered records that the open stream delivered something, which proves it worked.
func (b *ObservationBackoff) Delivered() { b.endedWithoutWorking = 0 }

// Next returns how long to wait before opening the stream again, now that it ended.
func (b *ObservationBackoff) Next() time.Duration {
	if b.base <= 0 {
		return b.base
	}
	if b.now().Sub(b.openedAt) >= b.healthy {
		b.endedWithoutWorking = 0
	}
	b.endedWithoutWorking = min(b.endedWithoutWorking+1, maxObservationExponent+1)
	ceiling := b.base
	for i := 1; i < b.endedWithoutWorking && ceiling < b.maximum; i++ {
		if ceiling > b.maximum/2 {
			ceiling = b.maximum
		} else {
			ceiling *= 2
		}
	}
	ceiling = min(ceiling, b.maximum)
	jitter := b.jitter()
	if !(jitter >= 0) {
		jitter = 0
	}
	jitter = min(jitter, 1)
	return time.Duration(float64(ceiling) * (0.5 + 0.5*jitter))
}
