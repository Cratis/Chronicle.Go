// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package connection

import (
	"context"
	"errors"
	"time"
)

// ErrStale identifies missing application heartbeats, independently of TCP health.
var ErrStale = errors.New("chronicle: kernel keep-alive is stale")

// Watch detects staleness or receiver failure. The caller cancels and joins the receiver.
func Watch(ctx context.Context, timeout time.Duration, heartbeat <-chan struct{}, result <-chan error) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			select {
			case err := <-result:
				return err
			default:
				return ctx.Err()
			}
		case err := <-result:
			return err
		case <-heartbeat:
			timer.Reset(timeout)
		case <-timer.C:
			// A concurrently queued heartbeat must not be discarded as stale.
			select {
			case <-heartbeat:
				timer.Reset(timeout)
			default:
				return ErrStale
			}
		}
	}
}
