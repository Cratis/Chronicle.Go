// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/internal/wire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// maxWaitForAcceptedRegistration bounds how long a registration waits for the
// kernel to finish applying what it accepted; afterwards the kernel keeps applying it.
const maxWaitForAcceptedRegistration = 5 * time.Minute

// ErrRegistrationNotApplied identifies a registration the kernel accepted but could not apply.
var ErrRegistrationNotApplied = errors.New("chronicle: the kernel accepted the projection registration but could not apply it")

// registrant names this process in registrations, as the .NET client names its entry assembly.
func registrant() string {
	executable, err := os.Executable()
	if err != nil {
		return ""
	}
	return filepath.Base(executable)
}

// waitForAcceptedRegistration polls the kernel until it has applied what it
// accepted from the registrant. A kernel that does not accept registrations
// answers after applying them and has no status (UNIMPLEMENTED): nothing to wait for.
// UNAVAILABLE means the kernel is busy and the poll backs off and tries again.
func waitForAcceptedRegistration(ctx context.Context, conn grpc.ClientConnInterface, store, registrant string, maxWait time.Duration) error {
	if registrant == "" {
		return nil
	}
	client := contracts.NewProjectionsClient(conn)
	delay := 50 * time.Millisecond
	deadline := time.Now().Add(maxWait)
	for {
		result, err := client.GetRegistrationStatus(ctx, &contracts.GetRegistrationStatusRequest{EventStore: store, Registrant: registrant})
		switch {
		case status.Code(err) == codes.Unimplemented:
			return nil
		case status.Code(err) == codes.Unavailable:
			// Busy kernel: retry within the bound.
		case err != nil:
			return wire.RPCError(err)
		case result == nil:
			return nil
		case result.State == contracts.RegistrationState_Applied:
			return nil
		case result.State == contracts.RegistrationState_Failed:
			return fmt.Errorf("%w: %s: %s", ErrRegistrationNotApplied, registrant, result.Failure)
		}
		if time.Now().After(deadline) {
			// Still being applied; the kernel carries on.
			return nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		delay = min(delay*2, 2*time.Second)
	}
}
