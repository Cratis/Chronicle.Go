// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events

import (
	"fmt"

	"github.com/cratis/chronicle.go/internal/faults"
)

// Generic migration JSON is not an enum-aware codec. Refuse both endpoints,
// including identity migrations, before callbacks or wire definitions can escape.
func rejectEnumMigration(endpoints ...Descriptor) error {
	for _, endpoint := range endpoints {
		for _, field := range endpoint.Fields() {
			if field.IsEnum() {
				return fmt.Errorf("%w: migrations involving declared enum profiles are not supported", faults.ErrUnsupported)
			}
		}
	}
	return nil
}
