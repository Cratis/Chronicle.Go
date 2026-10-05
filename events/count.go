// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events

// Count is a number of events, not a sequence position or a freshness guarantee.
type Count uint64

// UnlimitedCount requests all matching events. It is distinct from an omitted
// read count, which may select already materialized state instead of a fold.
const UnlimitedCount Count = ^Count(0)
