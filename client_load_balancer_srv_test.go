// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
)

type balancerSRVRecords []*net.SRV

func (r balancerSRVRecords) LookupSRV(context.Context, string, string, string) (string, []*net.SRV, error) {
	return "", r, nil
}

func TestLoadBalancerReceivesSRVPriorityWeightOrderWithoutMutation(t *testing.T) {
	records := balancerSRVRecords{
		{Target: "last.", Port: 35000, Priority: 2, Weight: 100},
		{Target: "second.", Port: 35001, Priority: 1, Weight: 1},
		{Target: "first.", Port: 35002, Priority: 1, Weight: 10},
	}
	want := []ServerAddress{{Host: "first", Port: 35002}, {Host: "second", Port: 35001}, {Host: "last", Port: 35000}}
	cause := errors.New("stop before dialing")
	var client *Client
	balancer := &testLoadBalancer{next: func(_ context.Context, candidates []ServerAddress) (ServerAddress, error) {
		// Reentry into the SDK lock is safe: selection runs outside it.
		client.mu.Lock()
		closed := client.closed
		client.mu.Unlock()
		if closed {
			t.Error("selection started after close")
		}
		if !reflect.DeepEqual(candidates, want) {
			t.Errorf("candidates=%v want=%v", candidates, want)
		}
		return ServerAddress{}, cause
	}}
	var err error
	client, err = NewClient(WithConnectionString("chronicle+srv://cluster"), WithSRVResolver(records), WithLoadBalancer(balancer))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err = client.Connect(t.Context()); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	if records[0].Target != "last." {
		t.Fatal("resolver records mutated")
	}
}
