// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package connection

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

type srvFunc func(context.Context, string, string, string) (string, []*net.SRV, error)

func (f srvFunc) LookupSRV(ctx context.Context, s, p, h string) (string, []*net.SRV, error) {
	return f(ctx, s, p, h)
}

func TestSRVRefreshAndPriorityWeightOrder(t *testing.T) {
	calls := 0
	resolver := srvFunc(func(_ context.Context, service, protocol, host string) (string, []*net.SRV, error) {
		calls++
		if service != "chronicle" || protocol != "tcp" || host != "cluster" {
			t.Fatal("wrong DNS query")
		}
		return "", []*net.SRV{{Target: "b.", Port: 2, Priority: 2}, {Target: "a.", Port: 1, Priority: 1, Weight: 3}, {Target: "c.", Port: 3, Priority: 1, Weight: 4}}, nil
	})
	for range 2 {
		addresses, err := Resolve(t.Context(), resolver, "cluster")
		if err != nil || !reflect.DeepEqual(addresses, []string{"c:3", "a:1", "b:2"}) {
			t.Fatalf("%v: %v", addresses, err)
		}
	}
	if calls != 2 {
		t.Fatal("stale DNS cache")
	}
}

func TestSRVEmptyAndCancellationFail(t *testing.T) {
	resolver := srvFunc(func(ctx context.Context, _, _, _ string) (string, []*net.SRV, error) { return "", nil, ctx.Err() })
	if _, err := Resolve(t.Context(), resolver, "cluster"); err == nil {
		t.Fatal("empty discovery succeeded")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := Resolve(ctx, resolver, "cluster"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestLeastConnectionsProbesAndReserves(t *testing.T) {
	var lowReservations, highReservations atomic.Int32
	server := func(count string, reservations *atomic.Int32) *httptest.Server {
		return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/connections/count":
				_, _ = w.Write([]byte(count))
			case "/connections/reserve":
				if r.Method != http.MethodPost {
					t.Error("reservation is not POST")
				}
				reservations.Add(1)
			default:
				t.Error("wrong route")
			}
		}))
	}
	low := server("1", &lowReservations)
	defer low.Close()
	high := server("9", &highReservations)
	defer high.Close()
	balancer := NewBalancer("least-connections", &tls.Config{InsecureSkipVerify: true})
	defer balancer.Close()
	address, err := balancer.Next(t.Context(), []string{strings.TrimPrefix(high.URL, "https://"), strings.TrimPrefix(low.URL, "https://")})
	if err != nil || address != strings.TrimPrefix(low.URL, "https://") || lowReservations.Load() != 1 || highReservations.Load() != 0 {
		t.Fatalf("selection %q: %v", address, err)
	}
}

func TestRoundRobinAndRandomSelectOnlyCandidates(t *testing.T) {
	for _, strategy := range []string{"round-robin", "random"} {
		balancer := NewBalancer(strategy, &tls.Config{})
		first, err := balancer.Next(t.Context(), []string{"a", "b"})
		if err != nil {
			t.Fatal(err)
		}
		second, err := balancer.Next(t.Context(), []string{"a", "b"})
		balancer.Close()
		if err != nil || (first != "a" && first != "b") || (second != "a" && second != "b") || (strategy == "round-robin" && first == second) {
			t.Fatalf("%s: %q %q %v", strategy, first, second, err)
		}
	}
}
