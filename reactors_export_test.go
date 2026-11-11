// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"time"

	"github.com/cratis/chronicle.go/internal/connection"
)

// WithReactorRetryWaitForTest replaces the observer retry clock and pins the
// backoff jitter to its ceiling, so a first retry waits exactly the base delay.
func WithReactorRetryWaitForTest(wait func(context.Context, time.Duration) error) ClientOption {
	return func(c *clientConfig) {
		c.reactorRetryWait = wait
		c.observerBackoff = func() *connection.ObservationBackoff {
			return connection.NewObservationBackoff(connection.ObservationBaseDelay, connection.ObservationMaxDelay, connection.ObservationHealthyStream, time.Now, func() float64 { return 1 })
		}
	}
}
