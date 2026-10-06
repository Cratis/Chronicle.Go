// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package captures_test

import (
	"context"
	"errors"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/captures"
	contracts "github.com/cratis/chronicle.go/contracts/captures"
	"google.golang.org/grpc"
)

type captureRecordingConnection struct {
	invokes, streams int
	declaration      string
}

func (c *captureRecordingConnection) Invoke(_ context.Context, _ string, request, response any, _ ...grpc.CallOption) error {
	c.invokes++
	switch request := request.(type) {
	case *contracts.ValidateCaptureDeclarationRequest:
		c.declaration = request.Declaration
		result := response.(*contracts.CommandResult_ValidateCaptureDeclarationResponse)
		result.IsAuthorized = true
		result.Response = &contracts.ValidateCaptureDeclarationResponse{}
	case *contracts.SaveCaptureRequest:
		c.declaration = request.Declaration
		result := response.(*contracts.CommandResult_SaveCaptureResponse)
		result.IsAuthorized = true
		// This control records dispatch and returns capability messages, as
		// the pinned kernel does for unsupported webhook runtime sources.
		result.Response = &contracts.SaveCaptureResponse{Messages: []*contracts.CaptureValidationMessage{{Message: "synthetic capability diagnostic"}}}
	default:
		return errors.New("unexpected capture RPC")
	}
	return nil
}

func (c *captureRecordingConnection) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	c.streams++
	return nil, errors.New("unexpected capture stream")
}

func TestCaptureAuthorizationSubmissionRefusedBeforeDispatch(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, stop := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer stop()
	contexts := []struct {
		name string
		ctx  context.Context
	}{
		{"active", context.Background()}, {"canceled", canceled}, {"expired", expired},
	}
	for _, option := range syntheticAuthorizations() {
		d, err := new(captures.Builder).From(captures.Webhook("/synthetic-capture", option)).Key("id").Build("SyntheticCapture")
		if err != nil {
			t.Fatal(err)
		}
		auth, _ := d.Authorization()
		for _, tc := range contexts {
			t.Run(string(auth.Kind())+"/"+tc.name, func(t *testing.T) {
				conn := new(captureRecordingConnection)
				service, err := captures.New("synthetic-store", conn)
				if err != nil {
					t.Fatal(err)
				}
				for _, submit := range []func(context.Context, captures.Definition) error{service.Validate, service.Save} {
					err := submit(tc.ctx, d)
					if !errors.Is(err, chronicle.ErrUnsupported) {
						t.Fatal("authorization submission did not return ErrUnsupported")
					}
					if err.Error() != chronicle.ErrUnsupported.Error()+": capture source authorization cannot be submitted; the capture declaration transport carries CDL only" {
						t.Fatal("submission error is not fixed and value-free")
					}
					if conn.invokes != 0 || conn.streams != 0 {
						t.Fatal("authorization submission dispatched a remote call")
					}
				}
			})
		}
	}
}

func TestCaptureWithoutAuthorizationStillSubmitsCDL(t *testing.T) {
	d, err := new(captures.Builder).From(captures.Webhook("/synthetic-capture")).Key("id").Build("SyntheticCapture")
	if err != nil {
		t.Fatal(err)
	}
	for _, save := range []bool{false, true} {
		conn := new(captureRecordingConnection)
		service, err := captures.New("synthetic-store", conn)
		if err != nil {
			t.Fatal(err)
		}
		if save {
			var capability *captures.ValidationError
			if err := service.Save(context.Background(), d); !errors.As(err, &capability) {
				t.Fatal("unauthorized Save lost capability response")
			}
		} else if err := service.Validate(context.Background(), d); err != nil {
			t.Fatal(err)
		}
		if conn.invokes != 1 || conn.streams != 0 || conn.declaration != d.Declaration() {
			t.Fatal("unauthorized submission changed dispatch or CDL bytes")
		}
	}
}
