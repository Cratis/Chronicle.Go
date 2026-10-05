// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package captures

import (
	"context"
	"strings"

	contracts "github.com/cratis/chronicle.go/contracts/captures"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc"
)

// ValidationError preserves capture-specific compiler/capability diagnostics,
// including failures returned in an otherwise successful command envelope.
// Save can persist an inactive definition before reporting these diagnostics.
type ValidationError struct {
	// Messages is a detached list of kernel diagnostic text. It may contain
	// declaration literals; treat it as sensitive and do not log it indiscriminately.
	Messages []string
}

func (e *ValidationError) Error() string {
	return "chronicle: capture declaration rejected or unsupported"
}

// Service submits explicit capture declarations through authoritative CDL RPCs.
// It is concurrency-safe, borrows its connection, never retries writes and never
// starts a capture. Saving is not activation or proof that a source can be read.
type Service struct {
	store  metadata.StoreName
	client contracts.CapturesClient
}

// New constructs a store-wide service without I/O.
func New(store metadata.StoreName, conn grpc.ClientConnInterface) (*Service, error) {
	if strings.TrimSpace(string(store)) == "" || conn == nil {
		return nil, invalid("store and connection required")
	}
	return &Service{store, contracts.NewCapturesClient(conn)}, nil
}

// Validate checks syntax and runtime capability without persisting or activating.
// References to missing external services or event types fail explicitly.
func (s *Service) Validate(ctx context.Context, d Definition) error {
	if d.text == "" {
		return invalid("capture definition required")
	}
	result, err := s.client.ValidateCaptureDeclaration(ctx, &contracts.ValidateCaptureDeclarationRequest{EventStore: string(s.store), Declaration: d.text})
	if err != nil {
		return err
	}
	if err = wire.CheckEnvelope(result); err != nil {
		return err
	}
	if result.Response == nil {
		return faults.ErrProtocol
	}
	return checkMessages(result.Response.Messages)
}

// Save submits an explicit definition under its stable ID, without activating.
// The kernel can save an inactive definition and then return ValidationError;
// retain d.ID() to correct or remove it. A transport error has an unknown outcome.
func (s *Service) Save(ctx context.Context, d Definition) error {
	if d.text == "" {
		return invalid("capture definition required")
	}
	result, err := s.client.SaveCapture(ctx, &contracts.SaveCaptureRequest{EventStore: string(s.store), Id: wire.Guid(metadata.CorrelationID(d.id)), Declaration: d.text})
	if err != nil {
		return err
	}
	if err = wire.CheckEnvelope(result); err != nil {
		return err
	}
	if result.Response == nil {
		return faults.ErrProtocol
	}
	if err = checkMessages(result.Response.Messages); err != nil {
		return err
	}
	if result.Response.Capture == nil || result.Response.Capture.Id != d.id.String() {
		return faults.ErrProtocol
	}
	return nil
}
func checkMessages(messages []*contracts.CaptureValidationMessage) error {
	if len(messages) == 0 {
		return nil
	}
	failure := &ValidationError{}
	for _, message := range messages {
		if message == nil {
			return faults.ErrProtocol
		}
		failure.Messages = append(failure.Messages, message.Message)
	}
	return failure
}
