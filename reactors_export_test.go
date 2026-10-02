// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"time"
)

// WithReactorRetryWaitForTest replaces only the observer retry clock.
func WithReactorRetryWaitForTest(wait func(context.Context, time.Duration) error) ClientOption {
	return func(c *clientConfig) { c.reactorRetryWait = wait }
}
