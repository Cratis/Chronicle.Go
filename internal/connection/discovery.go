// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package connection

import (
	"context"
	"errors"
	"net"
	"slices"
	"strconv"
	"strings"
)

// SRVResolver resolves Chronicle service records. Implementations must honor cancellation.
type SRVResolver interface {
	LookupSRV(context.Context, string, string, string) (string, []*net.SRV, error)
}

// Resolve refreshes _chronicle._tcp records, preserving C# priority/weight order.
func Resolve(ctx context.Context, resolver SRVResolver, host string) ([]string, error) {
	_, records, err := resolver.LookupSRV(ctx, "chronicle", "tcp", host)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 || len(records) > 64 {
		return nil, errors.New("chronicle: SRV discovery requires 1–64 records")
	}
	records = slices.Clone(records)
	for _, record := range records {
		if record == nil || strings.TrimSuffix(record.Target, ".") == "" || record.Port == 0 {
			return nil, errors.New("chronicle: invalid SRV endpoint")
		}
	}
	slices.SortStableFunc(records, func(a, b *net.SRV) int {
		if a.Priority != b.Priority {
			return int(a.Priority) - int(b.Priority)
		}
		return int(b.Weight) - int(a.Weight)
	})
	addresses := make([]string, 0, len(records))
	for _, record := range records {
		addresses = append(addresses, net.JoinHostPort(strings.TrimSuffix(record.Target, "."), strconv.Itoa(int(record.Port))))
	}
	return addresses, nil
}

// Resolver uses system DNS unless an explicit name server is selected.
func Resolver(nameServer string) SRVResolver {
	if nameServer == "" {
		return net.DefaultResolver
	}
	return &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, nameServer)
	}}
}
