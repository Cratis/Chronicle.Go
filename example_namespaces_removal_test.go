// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"fmt"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/clients"
	"github.com/cratis/chronicle.go/contracts/eventstores"
	"github.com/cratis/chronicle.go/contracts/namespaces"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// removalExampleRPC is an offline interceptor, not a real removal or an
// authorization example. Applications use their authenticated connection.
func removalExampleRPC(_ context.Context, method string, request, reply any, _ *grpc.ClientConn, _ grpc.UnaryInvoker, _ ...grpc.CallOption) error {
	switch method {
	case clients.ConnectionService_CheckCompatibility_FullMethodName:
		reply.(*clients.CompatibilityResponse).IsCompatible = true
	case clients.ConnectionService_GetConnectedClients_FullMethodName:
		// Empty readiness probe response; this fixture does not start a session.
	case eventstores.EventStores_EnsureEventStore_FullMethodName:
		reply.(*eventstores.CommandResult).IsAuthorized = true
	case namespaces.Namespaces_EnsureNamespace_FullMethodName:
		reply.(*namespaces.CommandResult).IsAuthorized = true
	case namespaces.Namespaces_RemoveNamespace_FullMethodName:
		target := request.(*namespaces.RemoveNamespaceRequest)
		fmt.Println("removal acknowledged:", target.EventStore, target.Namespace)
		reply.(*namespaces.CommandResult).IsAuthorized = true
	default:
		return fmt.Errorf("unexpected example RPC %s", method)
	}
	return nil
}

func ExampleNamespaces_Remove() {
	// The interceptor handles every call locally: no kernel or network is used.
	conn, err := grpc.NewClient("passthrough:///offline", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(removalExampleRPC))
	if err != nil {
		panic(err)
	}
	defer func() { _ = conn.Close() }()
	client, err := chronicle.NewClient(chronicle.WithGRPCConnection(conn), chronicle.WithNoAuthentication(), chronicle.WithSkipKeepAlive())
	if err != nil {
		panic(err)
	}
	defer func() { _ = client.Close() }()
	ctx := context.Background()
	// A default-scoped handle avoids using the target namespace, but confers
	// no administrative permission. Kernel authorization still applies.
	store, err := client.EventStore(ctx, "orders")
	if err != nil {
		panic(err)
	}
	// The operator must stop ALL clients using tenant-retired before this
	// irreversible call. Neither SDK nor kernel enforces that prerequisite.
	if err := store.Namespaces().Remove(ctx, "tenant-retired"); err != nil {
		// An error does not prove rollback. Inspect state before retrying.
		panic(err)
	}
	// Output: removal acknowledged: orders tenant-retired
}
