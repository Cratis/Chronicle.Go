// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package consumer

import (
	sdk "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
)

type Conflict struct {
	Value string `chronicle:"pii;encrypted(details=\"CANARY_CLASSIFICATION_DETAILS\")"` // want "^CHR0053:"
}

type Reversed struct {
	Value string `json:",omitempty,omitzero" chronicle:"encrypted;pii"` // want "^CHR0053:"
}

type Valid struct {
	Confidential string `chronicle:"encrypted(details=\"pii;encrypted\")"`
	Personal     string `chronicle:"pii"`
	Empty        string `chronicle:""`
	Untagged     string
}

type Unregistered struct {
	Value string `chronicle:"pii;encrypted"`
}

type Lookalike struct {
	Value string `chronicle:"pii;encrypted"`
}

type abstentions struct {
	Ignored        string `json:"-" chronicle:"pii;encrypted"`
	Malformed      string `chronicle:"pii;encrypted("`
	InvalidArgs    string `chronicle:"pii;encrypted(scope=invalid)"`
	Unknown        string `chronicle:"pii;encrypted;unknown"`
	Duplicate      string `chronicle:"pii;encrypted;pii"`
	WithDetails    string `chronicle:"pii;encrypted;compliance-details(value=\"separate\")"`
	Unsupported    string `json:",string" chronicle:"pii;encrypted"`
	UnrelatedTag   string `other:"pii;encrypted"`
	NonString      int    `chronicle:"pii;encrypted"`
	Multiple, Pair string `chronicle:"pii;encrypted"`
	private        string `chronicle:"pii;encrypted"`
}

type ValueMethod struct {
	Value string `chronicle:"pii;encrypted"`
}

func (ValueMethod) MarshalJSON() ([]byte, error) { return []byte(`"value"`), nil }

type PointerMethod struct {
	Value string `chronicle:"pii;encrypted"`
}

func (*PointerMethod) MarshalJSON() ([]byte, error) { return []byte(`"value"`), nil }

type WithEmbedding struct {
	Valid
	Value string `chronicle:"pii;encrypted"`
}

type Generic[T any] struct {
	Value string `chronicle:"pii;encrypted"`
}

type WithOption struct {
	Value string `chronicle:"pii;encrypted"`
}

type ViaFunctionValue struct {
	Value string `chronicle:"pii;encrypted"`
}

type ViaWrapper struct {
	Value string `chronicle:"pii;encrypted"`
}

func Define[T any]() {}

func Declare[T any](registry *sdk.Registry) {
	_, _ = sdk.RegisterEvent[Conflict](registry)
	_, _ = sdk.RegisterEvent[Conflict](registry) // Repeated declaration of the same field.
	_, _ = events.Define[Reversed]()
	_, _ = events.Define[Valid]()
	_, _ = events.Define[abstentions]()
	_, _ = events.Define[ValueMethod]()
	_, _ = events.Define[PointerMethod]()
	_, _ = events.Define[WithEmbedding]()
	_, _ = events.Define[Generic[int]]()
	_, _ = events.Define[WithOption](events.WithID("WithOption"))
	_, _ = events.Define[T]()
	Define[Lookalike]()
	define := events.Define[ViaFunctionValue]
	_, _ = define()
	Wrapped[ViaWrapper]()
}

func Wrapped[T any]() { _, _ = events.Define[T]() }
