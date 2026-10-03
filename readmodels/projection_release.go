// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"fmt"

	"github.com/cratis/chronicle.go/internal/faults"
)

// Kernel 19.29.4 projection replay does not preserve per-property subjects.
// Collections cannot even infer a subject from the default PascalCase Id; keyed
// immediate/session reads substitute the source key for the event subject.
// History releases events first, but lacks a general model-release guarantee.
// Refuse every classified model on these routes, including namespace/global
// encryption and referenced/provider classifications in the frozen schema.
func projectionReleaseAdmission(d Descriptor) error {
	if len(d.definition.protected) != 0 {
		return fmt.Errorf("%w: protected projection replay release is not supported", faults.ErrUnsupported)
	}
	return nil
}
