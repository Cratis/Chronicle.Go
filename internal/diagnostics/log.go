// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package diagnostics emits SDK-owned, bounded operational records.
package diagnostics

import (
	"context"
	"io"
	"log/slog"
)

// Configuration is the module-private seam for binding detached declarations.
// The logger is borrowed; binding never changes an explicit artifact choice.
type Configuration struct{ Logger *slog.Logger }

// PanicError discards an application panic value without formatting or retaining it.
type PanicError struct{}

func (*PanicError) Error() string { return "chronicle: observer callback panicked" }

// Category inspects only exact trusted identities/types. It never calls Error,
// Is, As, Unwrap, GRPCStatus, or application-defined classification hooks.
func Category(err error) string {
	switch err {
	case nil:
		return "none"
	case context.Canceled:
		return "canceled"
	case context.DeadlineExceeded:
		return "deadline"
	case io.EOF:
		return "end_of_stream"
	}
	if _, ok := err.(*PanicError); ok {
		return "panic"
	}
	return "failure"
}

// Log accepts only SDK-defined messages, operations and stages. Do not pass
// artifact names, metadata, errors, or payloads as attributes. Handler calls are
// synchronous: callers must supply concurrency-safe, cooperative handlers.
// A handler panic is contained without recursively logging its panic value.
func Log(ctx context.Context, logger *slog.Logger, level slog.Level, message, operation, stage string, err error) {
	defer func() { _ = recover() }()
	if logger == nil {
		return
	}
	logger.LogAttrs(ctx, level, message, slog.String("operation", operation), slog.String("stage", stage), slog.String("category", Category(err)))
}
