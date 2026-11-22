// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package constraints

// UniqueMode selects how a property constraint retains an owner's values.
// It is independent of scope and sequence selection, and does not apply to
// event-type constraints. The zero value is PerEventSource.
type UniqueMode int32

const (
	// PerEventSource keeps one value per source, releasing its previous value
	// when that source claims a replacement.
	PerEventSource UniqueMode = 0
	// PerValue retains every value claimed by a source until removal. The same
	// owner may reclaim a value; RemovedWith releases all its values within scope.
	PerValue UniqueMode = 1
)
