// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"errors"
	"fmt"

	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc"
)

// collectionExampleTransport is an offline response fixture, not kernel replay
// or storage. The real-kernel specs establish the routing and state semantics.
type collectionExampleTransport struct{}

func (collectionExampleTransport) Invoke(_ context.Context, method string, request, response any, _ ...grpc.CallOption) error {
	if method != contracts.ReadModels_GetAllInstances_FullMethodName {
		return errors.New("unexpected RPC")
	}
	r := response.(*contracts.GetAllInstancesResponse)
	r.Instances = []string{`{"id":"account-1","name":"Open"}`}
	if request.(*contracts.GetAllInstancesRequest).EventCount != uint64(events.UnlimitedCount) {
		r.ProcessedEventsCount = 2
	}
	return nil
}
func (collectionExampleTransport) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("unexpected stream")
}

func ExampleReader_GetAll() {
	type Account struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	model, err := readmodels.Define[Account](readmodels.WithObserver(readmodels.Projection, "accounts"))
	if err != nil {
		panic(err)
	}
	catalog, err := readmodels.NewCatalog(model.Descriptor())
	if err != nil {
		panic(err)
	}
	// Production uses store.ReadModels(). This offline adapter owns the fixture's
	// simple source-key/empty-default producer evidence.
	service, err := readmodels.New("accounts", "Default", catalog, collectionExampleTransport{}, readmodels.WithProjectionReplayValidator(allowReplay))
	if err != nil {
		panic(err)
	}
	reader := readmodels.For(service, model)
	ctx := context.Background()
	current, err := reader.GetAll(ctx, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(current.Instances[0].Value.ID, current.ProcessedEventsCount)
	count := events.Count(2)
	historical, err := reader.GetAll(ctx, &count)
	if err != nil {
		panic(err)
	}
	fmt.Println(historical.ProcessedEventsCount, historical.Instances[0].LastHandled == nil)
	// Count two does not invent last-handled position one.
	// Output:
	// account-1 0
	// 2 true
}
