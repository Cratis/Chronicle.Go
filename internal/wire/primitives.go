// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package wire

import (
	"encoding/binary"
	"time"

	"github.com/cratis/chronicle.go/contracts/bcl"
	"github.com/cratis/chronicle.go/metadata"
)

// Guid uses .NET's mixed-endian Guid byte layout, then fixed64 little-endian halves.
func Guid(id metadata.CorrelationID) *bcl.Guid {
	data := [16]byte(id)
	data[0], data[1], data[2], data[3] = data[3], data[2], data[1], data[0]
	data[4], data[5] = data[5], data[4]
	data[6], data[7] = data[7], data[6]
	return &bcl.Guid{Lo: binary.LittleEndian.Uint64(data[:8]), Hi: binary.LittleEndian.Uint64(data[8:])}
}

func Correlation(value *bcl.Guid) metadata.CorrelationID {
	var data [16]byte
	if value == nil {
		return metadata.CorrelationID(data)
	}
	binary.LittleEndian.PutUint64(data[:8], value.Lo)
	binary.LittleEndian.PutUint64(data[8:], value.Hi)
	data[0], data[1], data[2], data[3] = data[3], data[2], data[1], data[0]
	data[4], data[5] = data[5], data[4]
	data[6], data[7] = data[7], data[6]
	return metadata.CorrelationID(data)
}

// DateTimeOffset truncates sub-100ns precision and preserves the numeric offset.
func DateTimeOffset(value time.Time) string {
	return value.Truncate(100 * time.Nanosecond).Format("2006-01-02T15:04:05.0000000-07:00")
}
