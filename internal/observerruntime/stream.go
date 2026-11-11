// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observerruntime

import (
	"context"
	"errors"
	"io"

	"github.com/cratis/chronicle.go/internal/connection"
	"github.com/cratis/chronicle.go/internal/faults"
)

// StreamError is an error the observation stream itself returned: opening it,
// receiving from it or sending on it. Errors from handlers and replay hooks are
// application errors and are never wrapped, so only a StreamError can tell the
// client that the kernel no longer knows its connection.
type StreamError struct{ err error }

func (e *StreamError) Error() string { return e.err.Error() }

// Unwrap returns the transport error.
func (e *StreamError) Unwrap() error { return e.err }

func streamError(err error) error {
	if err == nil {
		return nil
	}
	return &StreamError{err: err}
}

// SessionLost reports whether err is a stream error saying the kernel no
// longer knows the connection ID the stream was opened under. Only the exact
// StreamError type is inspected, so an application error can never end the
// client's connection, whatever it wraps or says.
func SessionLost(err error) bool {
	stream, ok := err.(*StreamError)
	return ok && connection.SessionLost(stream.err)
}

// runStream has one receiver and one sender, no unbounded queue, and acknowledges
// only after the complete operation (including resource cleanup) has returned.
// Notification messages return nil and have no result handshake in the protocol.
// delivered, when set, runs after each message the kernel delivers, before it is
// handled, which tells the reconnect backoff that the stream worked.
func runStream[Request, Response any](ctx context.Context, receive func() (*Request, error), delivered func(), handle func(context.Context, *Request) (*Response, error), send func(*Response) error) error {
	for {
		request, err := receive()
		if err != nil {
			return streamError(err)
		}
		if request == nil {
			return faults.ErrProtocol
		}
		if delivered != nil {
			delivered()
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
		if err = sendOnStream(send, receive, response); err != nil {
			return err
		}
	}
}

// sendOnStream sends message and returns any failure as a StreamError. When
// the kernel has ended the stream, grpc-go's send only says io.EOF; the status
// the kernel ended it with, such as an unknown connection, is what receive
// returns once the messages already in flight are drained. Those messages are
// never handled: the stream is over and the kernel redelivers them.
func sendOnStream[Request, Message any](send func(Message) error, receive func() (*Request, error), message Message) error {
	err := send(message)
	if err == nil {
		return nil
	}
	if errors.Is(err, io.EOF) {
		for {
			if _, received := receive(); received != nil {
				if errors.Is(received, io.EOF) {
					break
				}
				return streamError(received)
			}
		}
	}
	return streamError(err)
}
