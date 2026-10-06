// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events

import (
	"fmt"

	"github.com/cratis/chronicle.go/internal/faults"
)

// Generic kernel migrations operate on JSON strings, not binary codecs. No
// operation (including identity/pass-through) has packaged binary qualification.
func rejectBinaryMigration(endpoints ...Descriptor) error {
	for _, endpoint := range endpoints {
		for _, field := range endpoint.Fields() {
			if field.ContainsBinary() {
				return fmt.Errorf("%w: migrations involving binary profiles are not supported", faults.ErrUnsupported)
			}
		}
	}
	return nil
}
