// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/contracts/compliance"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/internal/faults"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestDecisionReadErrorsDoNotDiscloseTransportDiagnostics(t *testing.T) {
	const sensitive = "private-person-source-key"
	for _, failure := range []string{"fold", "cleanup", "both", "agreement", "tail", "release"} {
		t.Run(failure, func(t *testing.T) {
			f := newDecisionFixture(t, WithPII("name"))
			foldCause := status.Error(codes.Unimplemented, sensitive)
			cleanupCause := status.Error(codes.Canceled, sensitive)
			f.handle = func(_ context.Context, request any) (proto.Message, error) {
				switch req := request.(type) {
				case *contracts.GetDefinitionsRequest:
					if failure == "agreement" {
						return nil, foldCause
					}
				case *sequences.TailSequenceNumberRequest:
					if failure == "tail" {
						return nil, foldCause
					}
				case *contracts.GetInstanceByKeyRequest:
					if failure == "fold" || failure == "both" {
						return nil, foldCause
					}
				case *contracts.DehydrateSessionRequest:
					if failure == "cleanup" || failure == "both" {
						return nil, cleanupCause
					}
				case *compliance.ReleaseRequest:
					if failure == "release" {
						return nil, foldCause
					}
					return &compliance.ReleaseResponse{Payload: req.Payload}, nil
				}
				return nil, nil
			}
			read, err := f.reader.GetDetached(t.Context(), "source")
			if err == nil || !read.Token.IsZero() || read.Instance.Exists || strings.Contains(err.Error(), sensitive) {
				t.Fatalf("unsafe decision result: %+v %v", read, err)
			}
			if failure != "cleanup" && (!errors.Is(err, foldCause) || !errors.Is(err, faults.ErrUnsupported)) {
				t.Fatalf("lost fold/transport cause: %v", err)
			}
			if failure == "cleanup" || failure == "both" {
				if !errors.Is(err, cleanupCause) || !errors.Is(err, context.Canceled) {
					t.Fatalf("lost cleanup/cancellation cause: %v", err)
				}
				joined := errors.Unwrap(err).(interface{ Unwrap() []error })
				if strings.Contains(errors.Unwrap(err).Error(), sensitive) {
					t.Fatal("joined message disclosed diagnostics")
				}
				for _, member := range joined.Unwrap() {
					if strings.Contains(member.Error(), sensitive) {
						t.Fatal("joined member disclosed diagnostics")
					}
				}
			}
			if failure == "release" {
				var release *ReleaseError
				if !errors.As(err, &release) || !errors.Is(err, ErrRelease) || !errors.Is(release, foldCause) {
					t.Fatal("lost typed release cause")
				}
			}
			var rpc interface{ GRPCStatus() *status.Status }
			if !errors.As(err, &rpc) || !strings.Contains(rpc.GRPCStatus().Message(), sensitive) {
				t.Fatal("deliberate diagnostic inspection unavailable")
			}
			assertDecisionCleanup(t, f)
		})
	}
}
