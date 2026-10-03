// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observation_test

import (
	"context"
	"errors"
	"fmt"

	jobcontracts "github.com/cratis/chronicle.go/contracts/jobs"
	contracts "github.com/cratis/chronicle.go/contracts/observation"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/jobs"
	"github.com/cratis/chronicle.go/observation"
	"google.golang.org/grpc"
)

// exampleKernel is an offline fixture. In an application use store.Observers()
// and store.Jobs(), which enforce connection and artifact registration barriers.
type exampleKernel struct{}

func (exampleKernel) Invoke(_ context.Context, method string, _ any, reply any, _ ...grpc.CallOption) error {
	switch method {
	case contracts.Observers_Replay_FullMethodName:
		reply.(*contracts.ReplayResponse).JobId = "00112233-4455-6677-8899-aabbccddeeff"
	case jobcontracts.Jobs_AllJobs_FullMethodName:
		reply.(*jobcontracts.QueryResult_IEnumerable_JobSummaryResponse).IsAuthorized = true
	case contracts.Observers_WaitForCompletion_FullMethodName:
		reply.(*contracts.WaitForObserverCompletionResponse).IsSuccess = true
	default:
		return fmt.Errorf("unexpected example RPC %s", method)
	}
	return nil
}
func (exampleKernel) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, fmt.Errorf("no example streams")
}

func ExampleService_Replay() {
	observers, err := observation.New("orders", "Default", exampleKernel{})
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	job, err := observers.Replay(ctx, "orders-summary", events.EventLog)
	if err != nil {
		panic(err)
	} // A lost reply may mean the replay was accepted. Do not blindly retry.
	fmt.Println("accepted job:", job.ID())
	_, err = job.WaitForCompletion(ctx, 0)
	var absent *jobs.AbsentError
	if errors.As(err, &absent) {
		fmt.Println("job absent: completion unknown")
	} else if err != nil {
		panic(err)
	}
	// Output:
	// accepted job: 00112233-4455-6677-8899-aabbccddeeff
	// job absent: completion unknown
}

func ExampleService_WaitForCompletion() {
	observers, err := observation.New("orders", "Default", exampleKernel{})
	if err != nil {
		panic(err)
	}
	// Normally obtain this immutable value from AppendManyWithMetadata(...).Completion().
	// These are the ORIGINAL append coordinates and each original input's position.
	completion, err := observation.NewCompletion("orders", "Default", events.EventLog,
		[]events.TypeRef{{ID: "OrderPlaced", Generation: 1}, {ID: "OrderPaid", Generation: 1}, {ID: "OrderPlaced", Generation: 1}},
		[]events.SequenceNumber{3, 5, 8})
	if err != nil {
		panic(err)
	}
	result, err := observers.WaitForCompletion(context.Background(), completion, 0)
	if err != nil {
		panic(err)
	}
	fmt.Println("processed:", result.IsSuccess(), "timed out:", result.TimedOut())
	fmt.Println("OrderPaid tail:", completion.Target().EventTypeTails["OrderPaid"])
	// Output:
	// processed: true timed out: false
	// OrderPaid tail: 5
}
