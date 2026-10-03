// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package compliance_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/compliance"
	contracts "github.com/cratis/chronicle.go/contracts/compliance"
	"github.com/cratis/chronicle.go/internal/faults"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

type lifecycleKernel struct {
	contracts.UnimplementedComplianceServer
	erase func(context.Context, *contracts.DeleteEncryptionKeyRequest) (*emptypb.Empty, error)
	allow func(context.Context, *contracts.AllowNewEncryptionKeyRequest) (*emptypb.Empty, error)
}

func (k *lifecycleKernel) DeleteEncryptionKey(ctx context.Context, r *contracts.DeleteEncryptionKeyRequest) (*emptypb.Empty, error) {
	return k.erase(ctx, r)
}
func (k *lifecycleKernel) AllowNewEncryptionKey(ctx context.Context, r *contracts.AllowNewEncryptionKeyRequest) (*emptypb.Empty, error) {
	return k.allow(ctx, r)
}

func managerFixture(t *testing.T, kernel *lifecycleKernel) (*compliance.Manager, context.Context) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	contracts.RegisterComplianceServer(server, kernel)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	conn, err := grpc.NewClient("passthrough:///compliance", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	manager, err := compliance.New("store-a", "tenant-a", conn)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return manager, ctx
}

func TestLifecycleUsesNamespaceScopedKernelContracts(t *testing.T) {
	erasures, authorizations := 0, 0
	manager, ctx := managerFixture(t, &lifecycleKernel{
		erase: func(_ context.Context, r *contracts.DeleteEncryptionKeyRequest) (*emptypb.Empty, error) {
			erasures++
			if r.EventStore != "store-a" || r.Namespace != "tenant-a" || r.Identifier != "subject" {
				t.Error("erasure coordinates changed")
			}
			return &emptypb.Empty{}, nil
		},
		allow: func(_ context.Context, r *contracts.AllowNewEncryptionKeyRequest) (*emptypb.Empty, error) {
			authorizations++
			if r.EventStore != "store-a" || r.Namespace != "tenant-a" || r.Identifier != "subject" {
				t.Error("authorization coordinates changed")
			}
			return &emptypb.Empty{}, nil
		},
	})
	if err := manager.ErasePII(ctx, "subject"); err != nil {
		t.Fatal(err)
	}
	if err := manager.DeleteEncryptionKeyFor(ctx, "subject"); err != nil {
		t.Fatal(err)
	}
	if err := manager.AllowNewEncryptionKeyFor(ctx, "subject"); err != nil {
		t.Fatal(err)
	}
	if erasures != 2 || authorizations != 1 {
		t.Fatal("lifecycle call count changed")
	}
}

func TestLifecycleFailuresPreserveIdentityWithoutSensitiveDiagnostics(t *testing.T) {
	for _, code := range []codes.Code{codes.Unimplemented, codes.PermissionDenied, codes.Internal, codes.Canceled, codes.DeadlineExceeded} {
		t.Run(code.String(), func(t *testing.T) {
			manager, ctx := managerFixture(t, &lifecycleKernel{
				erase: func(context.Context, *contracts.DeleteEncryptionKeyRequest) (*emptypb.Empty, error) {
					return nil, status.Error(code, "PRIVATE key lifecycle incomplete")
				},
				allow: func(context.Context, *contracts.AllowNewEncryptionKeyRequest) (*emptypb.Empty, error) {
					return nil, status.Error(code, "PRIVATE authorization incomplete")
				},
			})
			for _, operation := range []func(context.Context, string) error{manager.ErasePII, manager.AllowNewEncryptionKeyFor} {
				err := operation(ctx, "PRIVATE-subject")
				var lifecycle *compliance.LifecycleError
				if !errors.Is(err, compliance.ErrLifecycle) || !errors.As(err, &lifecycle) || status.Code(err) != code || strings.Contains(err.Error(), "PRIVATE") {
					t.Fatalf("unsafe lifecycle result: %v", err)
				}
				if code == codes.Unimplemented && !errors.Is(err, faults.ErrUnsupported) {
					t.Fatal("unsupported identity lost")
				}
			}
		})
	}
}

func TestLifecycleRejectsReservedKeysAndCancellationBeforeDispatch(t *testing.T) {
	manager, ctx := managerFixture(t, &lifecycleKernel{
		erase: func(context.Context, *contracts.DeleteEncryptionKeyRequest) (*emptypb.Empty, error) {
			t.Error("invalid erasure dispatched")
			return &emptypb.Empty{}, nil
		},
		allow: func(context.Context, *contracts.AllowNewEncryptionKeyRequest) (*emptypb.Empty, error) {
			t.Error("invalid authorization dispatched")
			return &emptypb.Empty{}, nil
		},
	})
	for _, operation := range []func(context.Context, string) error{manager.ErasePII, manager.AllowNewEncryptionKeyFor} {
		for _, id := range []string{"", "$chronicle-encrypted-value$subject$PRIVATE", "$chronicle-encrypted-value$namespace$", "$chronicle-encrypted-value$global$"} {
			var subject *compliance.InvalidSubjectError
			if err := operation(ctx, id); !errors.Is(err, faults.ErrInvalidConfiguration) || !errors.As(err, &subject) || subject.Reserved != (id != "") || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatal(err)
			}
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if err := operation(canceled, "subject"); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
}
