// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package modelconsumer

import (
	"time"

	sdk "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/readmodels"
)

type CANARYModel struct {
	CANARYValue string `json:"__subject"` // want "^CHR0035:"
}

type SingleUnderscore struct {
	Value string `json:"_subject,omitempty"` // want "^CHR0035:"
}

type Plural struct {
	Value string `json:"__subjects,omitzero"` // want "^CHR0035:"
}

type MixedCase struct {
	Value string "json:\"__SuBjEcT\"" // want "^CHR0035:"
}

type Siblings struct {
	ID      string
	Enabled bool `json:",omitempty"`
	Count   int
	Value   string `json:"__subject"` // want "^CHR0035:"
}

type Renamed struct {
	Value string `json:"subjectRef"`
}

func RegisterDoubleUnderscore() error {
	_, err := sdk.RegisterReadModel[CANARYModel](sdk.NewRegistry())
	return err
}

func RegisterSingleUnderscore() error {
	_, err := sdk.RegisterReadModel[SingleUnderscore](sdk.NewRegistry())
	return err
}

func RegisterPlural() error {
	_, err := sdk.RegisterReadModel[Plural](sdk.NewRegistry())
	return err
}

func RegisterMixedCase() error {
	_, err := sdk.RegisterReadModel[MixedCase](sdk.NewRegistry())
	return err
}

func RegisterSiblings() error {
	_, err := sdk.RegisterReadModel[Siblings](sdk.NewRegistry())
	return err
}

func RegisterRenamed() error {
	_, err := sdk.RegisterReadModel[Renamed](sdk.NewRegistry())
	return err
}

type MultipleReserved struct {
	First  string `json:"__subject"`  // want "^CHR0035:"
	Second string `json:"__subjects"` // want "^CHR0035:"
}

type NoReserved struct {
	Ignored  string `json:"-"`
	Default  string `json:""`
	NearMiss string `json:"___subject"`
	hidden   string `json:"__subject"`
	Boolean  bool   `json:"__subjects"`
}

type Unregistered struct {
	Value string `json:"__subject"`
}

type namedString string
type aliasedString = string

type NamedScalar struct {
	Value namedString `json:"__subject"`
}

type ScalarAlias struct {
	Value aliasedString `json:"__subject"`
}

type Nested struct {
	Value string `json:"__subject"`
	Child struct{ Count int }
}

type Embedded struct {
	Unregistered
	Value string `json:"__subject"`
}

type ValueMethod struct {
	Value string `json:"__subject"`
}

func (ValueMethod) MarshalJSON() ([]byte, error) { return []byte(`{}`), nil }

type PointerMethod struct {
	Value string `json:"__subject"`
}

func (*PointerMethod) Marker() {}

type Generic[T any] struct {
	Value string `json:"__subject"`
}

type ModelAlias = Unregistered

type ThroughWrapper struct {
	Value string `json:"__subject"`
}

type FunctionValue struct {
	Value string `json:"__subject"`
}

type WithOption struct {
	Value string `json:"__subject"`
}

type WithSpread struct {
	Value string `json:"__subject"`
}

type DefinedDirectly struct {
	Value string `json:"__subject"`
}

type Duplicate struct {
	First  string `json:"__subject"`
	Second string `json:"__subject"`
}

type FoldedDuplicate struct {
	ID    int
	Other int    `json:"id"`
	Value string `json:"__subject"`
}

type UnicodeDuplicate struct {
	AS    string
	Aſ    string
	Value string `json:"__subject"`
}

type UnsupportedOption struct {
	Other int    `json:",string"`
	Value string `json:"__subject"`
}

type OtherMetadata struct {
	Other string `chronicle:"pii"`
	Value string `json:"__subject"`
}

type PartialTag struct {
	Other string `json:"other" garbage`
	Value string `json:"__subject"`
}

type DuplicateTag struct {
	Value string `json:"__subject" json:"other"`
}

type KernelToken struct {
	Week  int
	Value string `json:"__subject"`
}

type MultipleNames struct {
	A, B  int
	Value string `json:"__subject"`
}

func RegisterReadModel[T any](r *sdk.Registry) { _, _ = sdk.RegisterReadModel[T](r) }

func Declare(r *sdk.Registry, options []readmodels.ModelOption) {
	_, _ = sdk.RegisterReadModel[CANARYModel](r) // The same tag is reported only once.
	_, _ = sdk.RegisterReadModel[MultipleReserved](nil)
	_, _ = sdk.RegisterReadModel[NoReserved](r)
	_, _ = sdk.RegisterReadModel[NamedScalar](r)
	_, _ = sdk.RegisterReadModel[ScalarAlias](r)
	_, _ = sdk.RegisterReadModel[Nested](r)
	_, _ = sdk.RegisterReadModel[Embedded](r)
	_, _ = sdk.RegisterReadModel[ValueMethod](r)
	_, _ = sdk.RegisterReadModel[PointerMethod](r)
	_, _ = sdk.RegisterReadModel[Generic[int]](r)
	_, _ = sdk.RegisterReadModel[ModelAlias](r)
	_, _ = sdk.RegisterReadModel[time.Time](r)
	RegisterReadModel[ThroughWrapper](r)
	register := sdk.RegisterReadModel[FunctionValue]
	_, _ = register(r)
	_, _ = sdk.RegisterReadModel[WithOption](r, readmodels.WithIdentifier("model"))
	_, _ = sdk.RegisterReadModel[WithSpread](r, options...)
	_, _ = readmodels.Define[DefinedDirectly]()
	_, _ = sdk.RegisterReadModel[Duplicate](r)
	_, _ = sdk.RegisterReadModel[FoldedDuplicate](r)
	_, _ = sdk.RegisterReadModel[UnicodeDuplicate](r)
	_, _ = sdk.RegisterReadModel[UnsupportedOption](r)
	_, _ = sdk.RegisterReadModel[OtherMetadata](r)
	_, _ = sdk.RegisterReadModel[PartialTag](r)
	_, _ = sdk.RegisterReadModel[DuplicateTag](r)
	_, _ = sdk.RegisterReadModel[KernelToken](r)
	_, _ = sdk.RegisterReadModel[MultipleNames](r)
	type BlockLocal struct {
		Value string `json:"__subject"`
	}
	_, _ = sdk.RegisterReadModel[BlockLocal](r)
}
