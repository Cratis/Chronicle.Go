// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
using Cratis.Concepts;
using System.Text.Json.Serialization;

// Unchanged declarations from Fundamentals.Go ContractTests/EnumJson/Program.cs,
// historical source profile d2accc4. Do not add All=-1 to historical Bits.
public enum Plain { Zero = 0, One = 1, Two = 2, Min = int.MinValue, Max = int.MaxValue }
public enum NoZero { One = 1 }
[Flags] public enum Bits { None = 0, A = 1, B = 2, AB = 3, C = 4 }
public enum ByteEnum : byte { Zero = 0, One = 1, Max = byte.MaxValue }
public enum UIntEnum : uint { Zero = 0, One = 1, High = 2147483648, Max = uint.MaxValue }
public enum LongEnum : long { Zero = 0, One = 1, Negative = -1, High = 2147483648, Low = -2147483649, Max = long.MaxValue }
public record PlainConcept(Plain Value) : ConceptAs<Plain>(Value);

// Separate Chronicle DTO controls, not additions to the historical corpus.
public enum Int32Sample { Zero = 0, One = 1, Negative = -1, Min = int.MinValue, Max = int.MaxValue }
[Flags] public enum AllBits { None = 0, A = 1, B = 2, C = 4, AB = 3, All = -1 }
public record Scalar<T>(T Value) where T : struct, Enum;
public record NullableScalar<T>(T? Value) where T : struct, Enum;
public record ArrayValue<T>(T[] Values) where T : struct, Enum;
public record Int32Defaults(Int32Sample Value = Int32Sample.One, Int32Sample? Optional = null, Int32Sample[]? Values = null);
public record NamingControl(Int32Sample URLValue, [property: JsonPropertyName("explicit_enum")] Int32Sample TaggedValue);
public record ConceptDefaultControl(PlainConcept? Value = null);
