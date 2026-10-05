// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observerruntime

import (
	"context"

	"github.com/cratis/chronicle.go/internal/faults"
)

// runStream has one receiver and one sender, no unbounded queue, and acknowledges
// only after the complete operation (including resource cleanup) has returned.
// Notification messages return nil and have no result handshake in the protocol.
func runStream[Request, Response any](ctx context.Context, receive func() (*Request, error), handle func(context.Context, *Request) (*Response, error), send func(*Response) error) error {
	for {
		request, err := receive()
		if err != nil {
			return err
		}
		if request == nil {
			return faults.ErrProtocol
		}
		response, err := handle(ctx, request)
		if err != nil {
			return err
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		if response == nil {
			continue
		}
		if err = send(response); err != nil {
			return err
		}
	}
}
