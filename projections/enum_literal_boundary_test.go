// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"sync/atomic"
	"testing"

	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type literalHookEnum int32

var literalEnumHookCalls atomic.Int32

func (literalHookEnum) MarshalJSON() ([]byte, error) {
	literalEnumHookCalls.Add(1)
	panic("enum JSON hook")
}

type literalHookView struct {
	Value  literalHookEnum
	Number int32
}

func TestEnumValueInvalidTargetNeverInvokesHook(t *testing.T) {
	c, err := serialization.NewCodecs(serialization.Enum(serialization.EnumMember[literalHookEnum]{Name: "One", Value: 1}))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"Missing", "Number"} {
		t.Run(path, func(t *testing.T) {
			literalEnumHookCalls.Store(0)
			b := projections.NewBuilder("enum-literal-target", mustModel[literalHookView](t, readmodels.WithCodecs(c)), projections.NoAutoMap())
			func() {
				defer func() {
					if recover() != nil {
						t.Error("invalid target panicked")
					}
				}()
				projections.From(b, mustEvent[IssueCreated](t), func(f *projections.FromBuilder[literalHookView, IssueCreated]) {
					projections.Value(f, projections.Path[literalHookView, literalHookEnum](path), literalHookEnum(1))
				})
			}()
			if calls := literalEnumHookCalls.Load(); calls != 0 {
				t.Fatalf("invalid target invoked hostile enum hook %d times", calls)
			}
			_, err := b.Build()
			enumBoundaryFailure(t, err, path)
		})
	}
}

type literalChild struct {
	ID     string `json:"id"`
	Number int32
	Note   *string
}
type literalParent struct {
	Children []literalChild
	Nested   *literalChild
}

func TestValueTargetValidationPreservesOrdinaryChildAndNestedLiterals(t *testing.T) {
	event := mustEvent[IssueCreated](t)
	b := projections.NewBuilder("ordinary-node-literals", mustModel[literalParent](t))
	define := func(child *projections.Builder[literalChild]) {
		projections.From(child, event, func(f *projections.FromBuilder[literalChild, IssueCreated]) {
			projections.Value(f, projections.Path[literalChild, int32]("Number"), int32(7))
			projections.Value(f, projections.Path[literalChild, *string]("Note"), nil)
		})
	}
	projections.Children(b, projections.Path[literalParent, []literalChild]("Children"), define)
	projections.Nested(b, projections.Path[literalParent, *literalChild]("Nested"), define)
	declaration, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	wire := mustCompile(t, declaration, event.Descriptor()).KernelDefinition()
	child, nested := wire.Children["Children"], wire.Nested["Nested"]
	if child == nil || nested == nil || len(child.From) != 1 || len(nested.From) != 1 {
		t.Fatal("missing ordinary node handlers")
	}
	for _, properties := range []map[string]string{child.From[0].Value.Properties, nested.From[0].Value.Properties} {
		if properties["Number"] != "$value(7)" || properties["Note"] != "$null" {
			t.Fatalf("ordinary literals changed: %v", properties)
		}
	}
}
