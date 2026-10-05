// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package observersource shares event-origin inference across observer kinds.
package observersource

import (
	"fmt"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
)

// Infer ignores unspecified origins. Multiple distinct named origins fail,
// matching C#'s MultipleEventStoresDefined; explicit sequences bypass inference.
func Infer(descriptors []events.Descriptor) (string, error) {
	source := ""
	for _, d := range descriptors {
		origin := d.SourceStore()
		if origin == "" {
			continue
		}
		if source != "" && source != origin {
			return "", fmt.Errorf("%w: observer handles multiple source stores", faults.ErrInvalidConfiguration)
		}
		source = origin
	}
	return source, nil
}
