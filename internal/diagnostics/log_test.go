// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package diagnostics

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type unsafeError struct{ calls *atomic.Int32 }

func (e *unsafeError) Error() string { e.calls.Add(1); return "secret-error" }
func (e *unsafeError) Unwrap() error { e.calls.Add(1); return nil }
func (e *unsafeError) Is(error) bool { e.calls.Add(1); return false }
func (e *unsafeError) As(any) bool   { e.calls.Add(1); return false }
func (e *unsafeError) GRPCStatus() *status.Status {
	e.calls.Add(1)
	return status.New(codes.Internal, "secret-status")
}
func (e *unsafeError) Format(s fmt.State, _ rune) {
	e.calls.Add(1)
	_, _ = io.WriteString(s, "secret-format")
}

type uncomparableError []string

func (uncomparableError) Error() string { panic("must not format") }

func TestDiagnosticsNeverInspectArbitraryErrors(t *testing.T) {
	var calls atomic.Int32
	for _, failure := range []error{&unsafeError{&calls}, status.Error(codes.Internal, "secret-status"), uncomparableError{"secret"}, &PanicError{}, context.Canceled, context.DeadlineExceeded, io.EOF, nil} {
		var output strings.Builder
		logger := slog.New(slog.NewTextHandler(&output, nil))
		Log(t.Context(), logger, slog.LevelError, "observer failed", "reactor", "observe", failure)
		if strings.Contains(output.String(), "secret") || output.Len() == 0 {
			t.Fatal("unsafe or missing record")
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("application hooks = %d", calls.Load())
	}
}

type panicHandler struct{ enabled bool }

func (h panicHandler) Enabled(context.Context, slog.Level) bool {
	if h.enabled {
		panic("secret-enabled")
	}
	return true
}
func (panicHandler) Handle(context.Context, slog.Record) error { panic("secret-handle") }
func (h panicHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h panicHandler) WithGroup(string) slog.Handler           { return h }
func TestLoggingHandlerPanicsDoNotEscape(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		Log(t.Context(), slog.New(panicHandler{enabled}), slog.LevelError, "observer failed", "reactor", "observe", &PanicError{})
	}
}
