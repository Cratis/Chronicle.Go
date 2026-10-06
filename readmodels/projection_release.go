// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"context"

	"github.com/cratis/chronicle.go/internal/kernelcapability"
)

// Projection replay, history, immediate and session reads of a classified model
// are released by the kernel with each value's original subject only from
// Chronicle 19.32.2 (Chronicle#4561). Earlier kernels substituted the source key
// for the subject, so a protected string could carry ciphertext and still pass
// shape validation. Refuse every classified model on these routes unless the
// connection reports such a kernel; skipped compatibility verification and
// low-level transports without a report are refused.
func (s *Service) projectionReleaseAdmission(ctx context.Context, d Descriptor) error {
	if len(d.definition.protected) == 0 {
		return nil
	}
	capabilities, err := kernelcapability.Of(ctx, s.conn)
	if err != nil {
		return err
	}
	return kernelcapability.Require(capabilities.ProtectedRelease, "protected projection replay release", kernelcapability.ProtectedReleaseVersion)
}
