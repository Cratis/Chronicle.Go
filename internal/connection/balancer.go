// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package connection

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Balancer selects once per generation, not once per RPC. Only the supervisor calls Next.
type Balancer struct {
	client   *http.Client
	strategy string
	next     uint64
}

func NewBalancer(strategy string, config *tls.Config) *Balancer {
	return &Balancer{strategy: strategy, next: rand.Uint64(), client: &http.Client{
		Transport:     &http.Transport{TLSClientConfig: config.Clone(), Proxy: http.ProxyFromEnvironment},
		Timeout:       2 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("chronicle: probe redirect refused") },
	}}
}

func (b *Balancer) Close() { b.client.CloseIdleConnections() }

func (b *Balancer) Next(ctx context.Context, addresses []string) (string, error) {
	if len(addresses) == 0 || len(addresses) > 64 {
		return "", errors.New("chronicle: expected 1–64 endpoints")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if len(addresses) == 1 {
		return addresses[0], nil
	}
	switch b.strategy {
	case "round-robin":
		index := b.next % uint64(len(addresses))
		b.next++
		return addresses[index], nil
	case "random":
		return addresses[rand.IntN(len(addresses))], nil
	}
	if err := Wait(ctx, time.Duration(rand.Int64N(int64(250*time.Millisecond)))); err != nil {
		return "", err
	}
	counts := make([]int64, len(addresses))
	var workers sync.WaitGroup
	for i, address := range addresses {
		workers.Go(func() { counts[i] = b.count(ctx, address) })
	}
	workers.Wait()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	minimum := int64(math.MaxInt64)
	var candidates []int
	for i, count := range counts {
		if count < minimum {
			minimum, candidates = count, nil
		}
		if count == minimum {
			candidates = append(candidates, i)
		}
	}
	selected := addresses[candidates[rand.IntN(len(candidates))]]
	// Reservations are advisory, expire on the server after 30 seconds, and have
	// no release endpoint. Failure does not establish readiness; preflight still must.
	response, err := b.request(ctx, http.MethodPost, selected, "/connections/reserve")
	if err == nil {
		_ = response.Body.Close()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return selected, nil
}

func (b *Balancer) count(ctx context.Context, address string) int64 {
	response, err := b.request(ctx, http.MethodGet, address, "/connections/count")
	if err != nil {
		return math.MaxInt64
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return math.MaxInt64
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 64))
	if err != nil {
		return math.MaxInt64
	}
	count, err := strconv.ParseInt(strings.TrimSpace(string(body)), 10, 64)
	if err != nil || count < 0 {
		return math.MaxInt64
	}
	return count
}

func (b *Balancer) request(ctx context.Context, method, address, path string) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, method, "https://"+address+path, nil)
	if err != nil {
		return nil, err
	}
	return b.client.Do(request)
}

// Wait is a cancellation-aware delay, also driven by testing/synctest fake time.
func Wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

// Backoff returns capped exponential delay with downward jitter and no overflow.
func Backoff(attempt int, initial, maximum time.Duration) time.Duration {
	delay := initial
	for i := 1; i < attempt && delay < maximum; i++ {
		if delay > maximum/2 {
			delay = maximum
		} else {
			delay *= 2
		}
	}
	if delay > maximum {
		delay = maximum
	}
	return delay/2 + time.Duration(rand.Int64N(int64(delay-delay/2)+1))
}
