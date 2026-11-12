// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package graphs

import "github.com/cratis/chronicle.go/serialization"

// contextScalars are the scalar event context properties a mapping can read,
// with the representation each one has. The kernel resolves them by these names.
var contextScalars = map[string]serialization.Scalar{
	"occurred":             serialization.String,
	"eventSourceId":        serialization.String,
	"EventSourceId":        serialization.String,
	"correlationId":        serialization.String,
	"eventSourceType":      serialization.String,
	"eventStreamType":      serialization.String,
	"eventStreamId":        serialization.String,
	"eventStore":           serialization.String,
	"namespace":            serialization.String,
	"subject":              serialization.String,
	"hash":                 serialization.String,
	"eventType.id":         serialization.String,
	"sequenceNumber":       serialization.Integer,
	"eventType.generation": serialization.Integer,
	"observationState":     serialization.Integer,
}

// assignable reports whether a value of the source representation can be stored
// in a property of the target representation.
func assignable(target, source serialization.Scalar) bool {
	return target == source || (target == serialization.Number && source == serialization.Integer)
}

func numeric(scalar serialization.Scalar) bool {
	return scalar == serialization.Integer || scalar == serialization.Number
}
