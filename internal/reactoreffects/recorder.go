// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package reactoreffects defines the internal scenario effect-recording boundary.
package reactoreffects

import "context"

// Recorder replaces execution only after production effect classification succeeds.
// Recording does not prove serialization or transport acceptance. Production
// runtimes do not implement this test-only extension.
type Recorder interface {
	RecordEffect(context.Context, any) error
}
