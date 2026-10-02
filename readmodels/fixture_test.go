// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/contracts/compliance"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

type modelKernel struct {
	contracts.UnimplementedReadModelsServer
	compliance.UnimplementedComplianceServer
	get       func(context.Context, *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error)
	release   func(context.Context, *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error)
	dehydrate func(context.Context, *contracts.DehydrateSessionRequest) (*emptypb.Empty, error)
}

func (k *modelKernel) GetInstanceByKey(ctx context.Context, r *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
	return k.get(ctx, r)
}
func (k *modelKernel) Release(ctx context.Context, r *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
	return k.release(ctx, r)
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
	served := make(chan struct{})
	go func() {
		defer close(served)
		if err := server.Serve(listener); err != nil {
			t.Error(err)
		}
	}()
	conn, err := grpc.NewClient("passthrough:///readmodels", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
		server.Stop()
		_ = listener.Close()
		<-served
	})
	catalog, err := readmodels.NewCatalog(descriptors...)
	if err != nil {
		t.Fatal(err)
	}
	service, err := readmodels.New("store", "tenant-a", catalog, conn)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return service, ctx
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
