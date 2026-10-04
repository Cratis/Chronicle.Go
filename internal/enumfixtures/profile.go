// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package enumfixtures supplies declared Go counterparts of the immutable
// Chronicle 19.29.4/Fundamentals 7.19.6 packaged enum capture.
package enumfixtures

import "github.com/cratis/chronicle.go/serialization"

type Plain int32
type NoZero int32
type Bits int32
type Int32Sample int32
type AllBits int32

type Scalar[T ~int32] struct{ Value T }
type NullableScalar[T ~int32] struct{ Value *T }
type ArrayValue[T ~int32] struct{ Values []T }

func Codecs() (*serialization.Codecs, error) {
	return serialization.NewCodecs(
		serialization.Enum(serialization.EnumMember[Plain]{Name: "Zero", Value: 0}, serialization.EnumMember[Plain]{Name: "One", Value: 1}, serialization.EnumMember[Plain]{Name: "Two", Value: 2}, serialization.EnumMember[Plain]{Name: "Min", Value: -2147483648}, serialization.EnumMember[Plain]{Name: "Max", Value: 2147483647}),
		serialization.Enum(serialization.EnumMember[NoZero]{Name: "One", Value: 1}),
		serialization.Flags(serialization.EnumMember[Bits]{Name: "None", Value: 0}, serialization.EnumMember[Bits]{Name: "A", Value: 1}, serialization.EnumMember[Bits]{Name: "B", Value: 2}, serialization.EnumMember[Bits]{Name: "AB", Value: 3}, serialization.EnumMember[Bits]{Name: "C", Value: 4}),
		serialization.Enum(serialization.EnumMember[Int32Sample]{Name: "Zero", Value: 0}, serialization.EnumMember[Int32Sample]{Name: "One", Value: 1}, serialization.EnumMember[Int32Sample]{Name: "Negative", Value: -1}, serialization.EnumMember[Int32Sample]{Name: "Min", Value: -2147483648}, serialization.EnumMember[Int32Sample]{Name: "Max", Value: 2147483647}),
		serialization.Flags(serialization.EnumMember[AllBits]{Name: "None", Value: 0}, serialization.EnumMember[AllBits]{Name: "A", Value: 1}, serialization.EnumMember[AllBits]{Name: "B", Value: 2}, serialization.EnumMember[AllBits]{Name: "C", Value: 4}, serialization.EnumMember[AllBits]{Name: "AB", Value: 3}, serialization.EnumMember[AllBits]{Name: "All", Value: -1}),
	)
}
