// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package conceptfixtures supplies real domain codecs shared by client and kernel tests.
package conceptfixtures

import (
	"encoding/json"
	"strconv"

	"github.com/cratis/fundamentals.go/concepts"
)

// AuthorID is a named UUID concept with explicitly forwarded codecs.
type AuthorID concepts.UUID

func (v AuthorID) ConceptValue() concepts.UUID      { return concepts.UUID(v) }
func (v AuthorID) MarshalJSON() ([]byte, error)     { return concepts.UUID(v).MarshalJSON() }
func (v *AuthorID) UnmarshalJSON(data []byte) error { return (*concepts.UUID)(v).UnmarshalJSON(data) }
func (v AuthorID) MarshalText() ([]byte, error)     { return concepts.UUID(v).MarshalText() }
func (v *AuthorID) UnmarshalText(data []byte) error { return (*concepts.UUID)(v).UnmarshalText(data) }

// Name is a named string concept.
type Name string

func (v Name) ConceptValue() string             { return string(v) }
func (v Name) MarshalJSON() ([]byte, error)     { return json.Marshal(string(v)) }
func (v *Name) UnmarshalJSON(data []byte) error { return json.Unmarshal(data, (*string)(v)) }
func (v Name) MarshalText() ([]byte, error)     { return []byte(v), nil }
func (v *Name) UnmarshalText(data []byte) error { *v = Name(data); return nil }

// Number is a named signed integer concept.
type Number int64

func (v Number) ConceptValue() int64              { return int64(v) }
func (v Number) MarshalJSON() ([]byte, error)     { return json.Marshal(int64(v)) }
func (v *Number) UnmarshalJSON(data []byte) error { return json.Unmarshal(data, (*int64)(v)) }
func (v Number) MarshalText() ([]byte, error)     { return []byte(strconv.FormatInt(int64(v), 10)), nil }
func (v *Number) UnmarshalText(data []byte) error {
	n, err := strconv.ParseInt(string(data), 10, 64)
	if err == nil {
		*v = Number(n)
	}
	return err
}

// Unsigned wraps a uint64 in an object: range checks must use the wire scalar.
type Unsigned struct{ Value uint64 }

func (v Unsigned) ConceptValue() uint64             { return v.Value }
func (v Unsigned) MarshalJSON() ([]byte, error)     { return json.Marshal(v.Value) }
func (v *Unsigned) UnmarshalJSON(data []byte) error { return json.Unmarshal(data, &v.Value) }
func (v Unsigned) MarshalText() ([]byte, error)     { return []byte(strconv.FormatUint(v.Value, 10)), nil }
func (v *Unsigned) UnmarshalText(data []byte) error {
	n, err := strconv.ParseUint(string(data), 10, 64)
	if err == nil {
		v.Value = n
	}
	return err
}

var (
	_ concepts.Concept[concepts.UUID] = AuthorID{}
	_ concepts.Concept[string]        = Name("")
	_ concepts.Concept[int64]         = Number(0)
	_ concepts.Concept[uint64]        = Unsigned{}
)
