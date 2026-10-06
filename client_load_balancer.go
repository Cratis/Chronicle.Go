// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/cratis/chronicle.go/internal/diagnostics"
)

func validateLoadBalancer(config clientConfig, uri ConnectionString) error {
	if !config.loadBalancerSet {
		return nil
	}
	if nilValue(config.loadBalancer) || config.borrowedSet || uri.loadBalancer != "" {
		return fmt.Errorf("%w: nil or conflicting load balancer", ErrInvalidConfiguration)
	}
	return nil
}

func (c *Client) selectAddress(ctx context.Context, addresses []string) (address string, err error) {
	if c.config.loadBalancer == nil {
		return c.balancer.Next(ctx, addresses)
	}
	defer func() {
		if recover() != nil {
			address, err = "", &LoadBalancerError{Err: &diagnostics.PanicError{}}
		}
	}()
	candidates := make([]ServerAddress, len(addresses))
	for i, value := range addresses {
		candidate, parseErr := parseAddress(value)
		if parseErr != nil {
			return "", &LoadBalancerError{Err: parseErr}
		}
		candidates[i] = candidate
	}
	selected, err := c.config.loadBalancer.Next(ctx, slices.Clone(candidates))
	if err != nil {
		return "", &LoadBalancerError{Err: err}
	}
	if err := ctx.Err(); err != nil {
		return "", &LoadBalancerError{Err: err}
	}
	if !slices.Contains(candidates, selected) {
		return "", &LoadBalancerError{Err: errors.New("chronicle: load balancer returned a non-candidate")}
	}
	return selected.String(), nil
}
