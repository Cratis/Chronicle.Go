// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

// WithMultiplexedObservation chooses whether reactors and reducers share one
// observation stream to the kernel. It is on by default and takes effect only
// when the kernel advertises the multiplexed-observation capability; against an
// older kernel, or when the compatibility check is skipped, every reactor and
// reducer observes on a stream of its own. The last option wins.
//
// Turn it off to observe with one stream per reactor or reducer even against a
// kernel that supports sharing.
func WithMultiplexedObservation(enabled bool) ClientOption {
	return func(c *clientConfig) { c.multiplexedObservationOff = !enabled }
}
