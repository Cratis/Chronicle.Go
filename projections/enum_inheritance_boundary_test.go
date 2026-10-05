// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/serialization"
)

type inheritanceEnum int32

// No model in this file contains an enum. The event profile alone activates
// enum validation; a negative declared value cannot safely become uint32.
type enumInheritanceEvent struct {
	ID     string
	Status inheritanceEnum
}
type enumInheritanceChild struct {
	ID     string `chronicle:"key"`
	Status uint32
}
type enumInheritanceOwner struct {
	ID    string
	Items []enumInheritanceChild
}
type enumInheritanceBoundOwner struct {
	ID    string
	Items []enumInheritanceChild `chronicle:"children(enumInheritanceEvent)"`
}
type enumInheritanceLeaf struct {
	ID     string
	Status uint32
}
type enumInheritanceMiddle struct {
	Inner *enumInheritanceLeaf
}
type enumInheritanceNestedOwner struct {
	ID    string
	Outer *enumInheritanceMiddle
}
type enumInheritanceBoundMiddle struct {
	Inner *enumInheritanceLeaf `chronicle:"nested"`
}
type enumInheritanceBoundNestedOwner struct {
	ID    string
	Outer *enumInheritanceBoundMiddle `chronicle:"nested"`
}
type enumInheritanceCollectionOwner struct {
	ID    string
	Items []enumInheritanceMiddle
}

func inheritanceEvent(t *testing.T) events.Type[enumInheritanceEvent] {
	t.Helper()
	codecs, err := serialization.NewCodecs(serialization.Enum(serialization.EnumMember[inheritanceEnum]{Name: "Negative", Value: -1}, serialization.EnumMember[inheritanceEnum]{Name: "Zero", Value: 0}))
	if err != nil {
		t.Fatal(err)
	}
	return mustEvent[enumInheritanceEvent](t, events.WithCodecs(codecs))
}

func enumAutoMode(name string) []projections.Option {
	switch name {
	case "enabled":
		return []projections.Option{projections.AutoMap()}
	case "disabled":
		return []projections.Option{projections.NoAutoMap()}
	default:
		return nil
	}
}

func TestEnumCollectionAutoMapValidatesEncodedMode(t *testing.T) {
	event := inheritanceEvent(t)
	for _, root := range []string{"default", "enabled", "disabled"} {
		for _, mode := range []string{"inherited", "enabled", "disabled"} {
			for _, join := range []bool{false, true} {
				name := "From"
				if join {
					name = "Join"
				}
				t.Run(root+"/"+mode+"/"+name, func(t *testing.T) {
					b := projections.NewBuilder("enum-child-mode", mustModel[enumInheritanceOwner](t), enumAutoMode(root)...)
					projections.Children(b, projections.Path[enumInheritanceOwner, []enumInheritanceChild]("Items"), func(child *projections.Builder[enumInheritanceChild]) {
						child.Configure(enumAutoMode(mode)...)
						if join {
							projections.Join(child, event, projections.Path[enumInheritanceChild, string]("ID"), nil)
						} else {
							projections.From(child, event, nil)
						}
					})
					declaration, err := b.Build()
					if mode != "disabled" {
						enumBoundaryFailure(t, err, "Status")
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					wire := mustCompile(t, declaration, event.Descriptor()).KernelDefinition()
					if wire.Children["Items"].AutoMap != contracts.AutoMap_Disabled {
						t.Fatal("explicit disabled mode changed")
					}
				})
			}
		}
	}
}

func TestEnumNestedAutoMapUsesContainingProjection(t *testing.T) {
	event := inheritanceEvent(t)
	for _, root := range []string{"default", "enabled", "disabled"} {
		for _, middle := range []string{"inherited", "enabled", "disabled"} {
			for _, leaf := range []string{"inherited", "enabled", "disabled"} {
				for _, join := range []bool{false, true} {
					name := "From"
					if join {
						name = "Join"
					}
					t.Run(root+"/"+middle+"/"+leaf+"/"+name, func(t *testing.T) {
						b := projections.NewBuilder("enum-nested-mode", mustModel[enumInheritanceNestedOwner](t), enumAutoMode(root)...)
						projections.Nested(b, projections.Path[enumInheritanceNestedOwner, *enumInheritanceMiddle]("Outer"), func(outer *projections.Builder[enumInheritanceMiddle]) {
							projections.Nested(outer, projections.Path[enumInheritanceMiddle, *enumInheritanceLeaf]("Inner"), func(inner *projections.Builder[enumInheritanceLeaf]) {
								if join {
									projections.Join(inner, event, projections.Path[enumInheritanceLeaf, string]("ID"), nil)
								} else {
									projections.From(inner, event, nil)
								}
							}, enumAutoMode(leaf)...)
						}, enumAutoMode(middle)...)
						declaration, err := b.Build()
						// Nested inheritance consults root Projection.AutoMap, not Outer.
						allowed := leaf == "disabled" || leaf == "inherited" && root == "disabled"
						if !allowed {
							enumBoundaryFailure(t, err, "Status")
							return
						}
						if err != nil {
							t.Fatal(err)
						}
						wire := mustCompile(t, declaration, event.Descriptor()).KernelDefinition()
						want := contracts.AutoMap_Inherit
						if leaf == "disabled" {
							want = contracts.AutoMap_Disabled
						}
						if wire.Nested["Outer"].Nested["Inner"].AutoMap != want {
							t.Fatal("nested wire mode changed")
						}
					})
				}
			}
		}
	}
}

func TestEnumNestedWithinCollectionUsesCollectionProjection(t *testing.T) {
	event := inheritanceEvent(t)
	for _, childMode := range []string{"inherited", "enabled", "disabled"} {
		t.Run(childMode, func(t *testing.T) {
			b := projections.NewBuilder("enum-collection-nested", mustModel[enumInheritanceCollectionOwner](t), projections.NoAutoMap())
			projections.Children(b, projections.Path[enumInheritanceCollectionOwner, []enumInheritanceMiddle]("Items"), func(child *projections.Builder[enumInheritanceMiddle]) {
				child.Configure(enumAutoMode(childMode)...)
				projections.Nested(child, projections.Path[enumInheritanceMiddle, *enumInheritanceLeaf]("Inner"), func(inner *projections.Builder[enumInheritanceLeaf]) { projections.From(inner, event, nil) })
			})
			_, err := b.Build()
			if childMode == "disabled" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			enumBoundaryFailure(t, err, "Status")
		})
	}
}

func TestEnumModelBoundNodeModesRemainExplicit(t *testing.T) {
	event := inheritanceEvent(t)
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	for _, root := range []string{"default", "disabled"} {
		for _, disabled := range []bool{false, true} {
			name := "default child"
			if disabled {
				name = "disabled child"
			}
			t.Run(root+"/"+name, func(t *testing.T) {
				options := enumAutoMode(root)
				if disabled {
					options = append(options, projections.WithNodes(projections.Node[enumInheritanceChild](projections.NoAutoMap())))
				}
				definition, err := projections.Compile(projections.ModelBound(mustModel[enumInheritanceBoundOwner](t), options...), catalog)
				if root != "disabled" && !disabled {
					enumBoundaryFailure(t, err, "Status")
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if definition.KernelDefinition().Children["Items"].AutoMap != contracts.AutoMap_Disabled {
					t.Fatal("model-bound mode was not explicit Disabled")
				}
			})
		}
	}
	// Model-bound nodes encode their immediate-parent rule explicitly; the final
	// validator must honor those wire overrides, not impose fluent inheritance.
	for _, root := range []string{"default", "disabled"} {
		for _, middle := range []string{"default", "disabled"} {
			t.Run("nested/"+root+"/"+middle, func(t *testing.T) {
				options := append(enumAutoMode(root), projections.WithNodes(projections.Node[enumInheritanceBoundMiddle](enumAutoMode(middle)...), projections.Node[enumInheritanceLeaf](projections.FromEvent(event))))
				definition, err := projections.Compile(projections.ModelBound(mustModel[enumInheritanceBoundNestedOwner](t), options...), catalog)
				if middle != "disabled" {
					enumBoundaryFailure(t, err, "Status")
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if definition.KernelDefinition().Nested["Outer"].Nested["Inner"].AutoMap != contracts.AutoMap_Disabled {
					t.Fatal("explicit nested override changed")
				}
			})
		}
	}
}

func TestEnumInheritedChildOverrideAndInvalidRegistration(t *testing.T) {
	event := inheritanceEvent(t)
	b := projections.NewBuilder("enum-child-override", mustModel[enumInheritanceOwner](t), projections.NoAutoMap())
	projections.Children(b, projections.Path[enumInheritanceOwner, []enumInheritanceChild]("Items"), func(child *projections.Builder[enumInheritanceChild]) {
		projections.From(child, event, func(f *projections.FromBuilder[enumInheritanceChild, enumInheritanceEvent]) {
			projections.Value(f, projections.Path[enumInheritanceChild, uint32]("Status"), uint32(7))
		})
	})
	declaration, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	if mustCompile(t, declaration, event.Descriptor()).KernelDefinition().Children["Items"].AutoMap != contracts.AutoMap_Inherit {
		t.Fatal("override changed wire mode")
	}
	// Source-enum-only failure through public model-bound Dial preparation.
	r := chronicle.NewRegistry()
	codecs, err := serialization.NewCodecs(serialization.Enum(serialization.EnumMember[inheritanceEnum]{Name: "Negative", Value: -1}, serialization.EnumMember[inheritanceEnum]{Name: "Zero", Value: 0}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[enumInheritanceEvent](r, events.WithCodecs(codecs)); err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[enumInheritanceBoundOwner](r)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.AddProjection(projections.ModelBound(model)); err != nil {
		t.Fatal(err)
	}
	enumRegistrationNoRPC(t, r, "Status", serialization.PreservePropertyNames)
}
