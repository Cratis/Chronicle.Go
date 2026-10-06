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
// low-level transports without a report are refused. The report is a fast
// pre-check: the transport re-checks the generation that dispatches each RPC.
func (s *Service) projectionReleaseAdmission(ctx context.Context, d Descriptor) error {
	if len(d.definition.protected) == 0 {
		return nil
	}
	if err := kernelcapability.Precheck(ctx, s.conn, kernelcapability.NeedProtectedProjectionRead); err != nil {
		return err
	}
	// Authoritative check: the transport refuses dispatch on a generation
	// without the capability. An untracked context cannot carry it.
	if !kernelcapability.Add(ctx, kernelcapability.NeedProtectedProjectionRead) {
		return kernelcapability.NeedProtectedProjectionRead.Refusal()
	}
	return nil
}
