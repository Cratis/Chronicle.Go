// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/contracts/compliance"
	"github.com/cratis/chronicle.go/contracts/readmodelexplorer"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/internal/kernelcapability"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

type modelKernel struct {
	contracts.UnimplementedReadModelsServer
	compliance.UnimplementedComplianceServer
	readmodelexplorer.UnimplementedReadModelExplorerServer
	options   []readmodels.ServiceOption
	snapshots func(context.Context, *readmodelexplorer.AllSnapshotsForReadModelRequest) (*readmodelexplorer.QueryResult_IEnumerable_ReadModelSnapshotResponse, error)
	get       func(context.Context, *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error)
	release   func(context.Context, *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error)
	dehydrate func(context.Context, *contracts.DehydrateSessionRequest) (*emptypb.Empty, error)
	replay    func(context.Context, *contracts.GetAllInstancesRequest) (*contracts.GetAllInstancesResponse, error)
	// kernel, when set, is reported as the connection's kernel capabilities.
	kernel *kernelcapability.Capabilities
}

// capabilityConn reports fixed kernel capabilities over a fixture connection.
type capabilityConn struct {
	grpc.ClientConnInterface
	capabilities kernelcapability.Capabilities
}

func (c capabilityConn) KernelCapabilities(context.Context) (kernelcapability.Capabilities, error) {
	return c.capabilities, nil
}

// protectedReleaseKernel reports a kernel with the 19.32.2 replay release fixes.
var protectedReleaseKernel = &kernelcapability.Capabilities{MixedAllReplay: true, ProtectedRelease: true}

func (k *modelKernel) AllSnapshotsForReadModel(ctx context.Context, r *readmodelexplorer.AllSnapshotsForReadModelRequest) (*readmodelexplorer.QueryResult_IEnumerable_ReadModelSnapshotResponse, error) {
	return k.snapshots(ctx, r)
}

func (k *modelKernel) GetAllInstances(ctx context.Context, r *contracts.GetAllInstancesRequest) (*contracts.GetAllInstancesResponse, error) {
	return k.replay(ctx, r)
}

func (k *modelKernel) GetInstanceByKey(ctx context.Context, r *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
	return k.get(ctx, r)
}
func (k *modelKernel) Release(ctx context.Context, r *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
	if err := validateReleaseRequest(r); err != nil {
		return nil, err
	}
	return k.release(ctx, r)
}

// Mirror the kernel's rejection of undeclared document fields instead of
// accepting lineage/bookkeeping that the real schema manager cannot traverse.
func validateReleaseRequest(r *compliance.ReleaseRequest) error {
	var schema struct{ Properties map[string]json.RawMessage }
	var payload map[string]json.RawMessage
	if json.Unmarshal([]byte(r.Schema), &schema) != nil || json.Unmarshal([]byte(r.Payload), &payload) != nil {
		return status.Error(codes.InvalidArgument, "invalid release request")
	}
	for name := range payload {
		if _, declared := schema.Properties[name]; !declared {
			return status.Error(codes.InvalidArgument, "undeclared release property")
		}
	}
	return nil
}
func (k *modelKernel) DehydrateSession(ctx context.Context, r *contracts.DehydrateSessionRequest) (*emptypb.Empty, error) {
	return k.dehydrate(ctx, r)
}

func serviceFixture(t *testing.T, k *modelKernel, descriptors ...readmodels.Descriptor) (*readmodels.Service, context.Context) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	contracts.RegisterReadModelsServer(server, k)
	compliance.RegisterComplianceServer(server, k)
	readmodelexplorer.RegisterReadModelExplorerServer(server, k)
	served := make(chan struct{})
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-served })
	go serveServiceFixture(server, listener, served, t.Error)
	conn, err := grpc.NewClient("passthrough:///readmodels", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	catalog, err := readmodels.NewCatalog(descriptors...)
	if err != nil {
		t.Fatal(err)
	}
	var transport grpc.ClientConnInterface = conn
	if k.kernel != nil {
		transport = capabilityConn{conn, *k.kernel}
	}
	service, err := readmodels.New("store", "tenant-a", catalog, transport, k.options...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return service, ctx
}

func serveServiceFixture(server *grpc.Server, listener net.Listener, served chan struct{}, report func(...any)) {
	defer close(served)
	// Cleanup may stop the server before this goroutine starts serving.
	if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		report(err)
	}
}

type Person struct {
	ID         string            `json:"id"`
	Name       string            `json:"name"`
	Children   []PersonChild     `json:"children"`
	Optional   *[]string         `json:"optional"`
	Properties map[string]string `json:"properties"`
	Count      uint64            `json:"count"`
}
type PersonChild struct {
	Names []string `json:"names"`
}

func person(t *testing.T, options ...readmodels.ModelOption) readmodels.Model[Person] {
	t.Helper()
	model, err := readmodels.Define[Person](options...)
	if err != nil {
		t.Fatal(err)
	}
	return model
}
