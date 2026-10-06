// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

// VariantOf groups explicitly registered models by identity type I. I is only a
// group marker; it needs no registration or inheritance relationship to a model.
// Each variant requires an EntersOn event and a key tag or explicit VariantKey.
func VariantOf[I any]() Option {
	return func(d *declaration) { d.variant = reflect.TypeFor[I]() }
}

// VariantKey selects the own-key property for fluent variants. Model-bound
// variants use chronicle:"key" instead. It never redirects sibling removal keys.
func VariantKey[M, V any](field Field[M, V]) Option {
	return func(d *declaration) {
		d.variantKey, d.variantKeyType = field.path, reflect.TypeFor[V]()
		if d.model.GoType() != field.owner {
			d.err = invalid("variant key owner mismatch")
		}
	}
}

// EntersOn declares a create-or-update handler for a variant. Other From handlers
// become update-only self-joins. Only key options are accepted; sibling removal
// still uses event-source identity, as in C#, even when this key is redirected.
func EntersOn[E any](event events.Type[E], options ...FromOption) Option {
	s := newSubscription(event.Descriptor(), options)
	return func(d *declaration) { d.entering = append(d.entering, s) }
}

// GlobalFor marks a shared-mapping type for variants of I. Only From property
// mappings are merged, overwriting existing writes. Keys, joins, children, All,
// removals and settings are not copied. Globals never register a read model.
// A From without explicit property mappings fails with GlobalFromHasNoProperties.
func GlobalFor[I any]() Option {
	return func(d *declaration) { d.globalFor = reflect.TypeFor[I]() }
}

// Global declares model-bound shared mappings without registering a read model.
// Include GlobalFor[I] in options, then pass the result to Registry.AddProjection.
// Fluent globals use a locally defined model and NewBuilder with GlobalFor[I].
func Global[M any](options ...Option) (Declaration, error) {
	model, err := readmodels.Define[M]()
	if err != nil {
		return Declaration{}, err
	}
	d := ModelBound(model, options...)
	if !d.IsGlobal() {
		return Declaration{}, invalid("Global requires GlobalFor")
	}
	return d, nil
}

// IsGlobal reports whether the declaration supplies shared variant mappings,
// rather than a separately registered projection/read model.
func (d Declaration) IsGlobal() bool { return d.data != nil && d.data.globalFor != nil }

// IsVariant reports whether the declaration belongs to a VariantOf group, even
// when it is the only declared member. The zero declaration is not a variant.
func (d Declaration) IsVariant() bool { return d.data != nil && d.data.variant != nil }

// GlobalHandlerPropertyNotOnVariant identifies a shared mapping whose serialized
// target is absent on a variant. It is wrapped in a DeclarationError.
type GlobalHandlerPropertyNotOnVariant struct {
	Global, Variant reflect.Type
	Property        string
}

func (e *GlobalHandlerPropertyNotOnVariant) Error() string {
	return fmt.Sprintf("global handler %s property %s is not on variant %s", e.Global, e.Property, e.Variant)
}

func (e *GlobalHandlerPropertyNotOnVariant) Unwrap() error { return invalid("global target missing") }

// GlobalFromHasNoProperties identifies a global From handler without explicit
// property mappings. Global AutoMap cannot supply these mappings because globals
// are not sent to the kernel. It is wrapped in a DeclarationError.
type GlobalFromHasNoProperties struct {
	Global reflect.Type
	Event  events.TypeRef
}

func (e *GlobalFromHasNoProperties) Error() string {
	return fmt.Sprintf("global handler %s From event %s,%d requires explicit property mappings", e.Global, e.Event.ID, e.Event.Generation)
}

func (e *GlobalFromHasNoProperties) Unwrap() error { return invalid("global From has no properties") }

// CompileGroup compiles one complete declaration graph atomically. Globals apply
// in declaration order (last writer wins); returned ordinary definitions are
// sorted by identifier. The optional current store resolves local log vs inbox.
// No subscription provisioning or callbacks are performed.
func CompileGroup(declarations []Declaration, catalog *events.Catalog, currentStore ...string) ([]Definition, error) {
	var compiled []Definition
	groups := map[reflect.Type][]int{}
	globals := map[reflect.Type][]Definition{}
	ids := map[string]bool{}
	models := map[reflect.Type]bool{}
	for _, declaration := range declarations {
		if declaration.data == nil {
			return nil, invalid("projection declaration required")
		}
		d := declaration.data
		if ids[d.id] || models[d.model.GoType()] {
			return nil, invalid("duplicate projection identity or model")
		}
		ids[d.id], models[d.model.GoType()] = true, true
		if d.variant != nil && d.globalFor != nil || d.variant == nil && (len(d.entering) > 0 || d.variantKey != "") {
			return nil, declarationFailure(d.id, Provenance{Offset: -1}, invalid("incompatible variant/global options"))
		}
		definition, err := compileOrdinary(declaration, catalog)
		if err != nil {
			return nil, err
		}
		if d.globalFor != nil {
			globals[d.globalFor] = append(globals[d.globalFor], definition)
			continue
		}
		if d.variant != nil {
			groups[d.variant] = append(groups[d.variant], len(compiled))
		}
		compiled = append(compiled, definition)
	}
	for _, declaration := range declarations {
		d := declaration.data
		if d.globalFor != nil && len(groups[d.globalFor]) == 0 {
			return nil, declarationFailure(d.id, Provenance{Offset: -1}, invalid("global handler has no registered variants"))
		}
	}
	// Follow input order, not map iteration: diagnostics and shadowing are stable.
	for _, definition := range compiled {
		d := definition.data
		if d.variant != nil {
			for _, global := range globals[d.variant] {
				if err := mergeGlobalHandler(d, global.data, catalog); err != nil {
					return nil, declarationFailure(d.id, Provenance{Directive: "GlobalFor", Offset: -1}, err)
				}
			}
			if err := lowerVariant(d); err != nil {
				return nil, declarationFailure(d.id, Provenance{Directive: "VariantOf", Offset: -1}, err)
			}
		}
		if err := inferSource(d, catalog, currentStore); err != nil {
			return nil, err
		}
		// Infer from the variant's own handlers before adding generated sibling
		// removals: those must not reroute local creation to a sibling's inbox.
		for _, sibling := range groups[d.variant] {
			other := compiled[sibling].data
			if other == d {
				continue
			}
			for _, entering := range other.entering {
				if slices.ContainsFunc(d.removals, func(r removalDefinition) bool { return !r.join && r.event == entering.event }) {
					continue
				}
				d.removals = append(d.removals, removalDefinition{event: entering.event, key: expression{kind: sourceExpression}, parent: expression{kind: sourceExpression}})
			}
		}
		if err := validateEnumGraph(d, catalog); err != nil {
			return nil, err
		}
		if err := validateBinaryGraph(d, catalog); err != nil {
			return nil, err
		}
		sortNode(&d.nodeDefinition)
	}
	slices.SortFunc(compiled, func(a, b Definition) int { return strings.Compare(a.Identifier(), b.Identifier()) })
	return compiled, nil
}

func mergeGlobalHandler(d, global *definition, catalog *events.Catalog) error {
	for _, source := range global.from {
		for _, w := range source.writes {
			if !slices.ContainsFunc(serialization.RootFields(d.model.Fields()), func(f serialization.Field) bool { return f.Path == w.path }) {
				return &GlobalHandlerPropertyNotOnVariant{Global: global.model.GoType(), Variant: d.model.GoType(), Property: w.path}
			}
			from := ensureFrom(&d.nodeDefinition, source.event)
			event, _ := catalog.LookupRef(source.event)
			// The shared type's Go field type is not the variant's declared type.
			// Validate serialized compatibility against the actual target instead.
			w.targetType = nil
			if err := addWrite(d, from, w, d.model.Fields(), event.Fields(), true); err != nil {
				return err
			}
		}
	}
	return nil
}

func ensureFrom(n *nodeDefinition, event events.TypeRef) *fromDefinition {
	for i := range n.from {
		if n.from[i].event == event {
			return &n.from[i]
		}
	}
	n.from = append(n.from, fromDefinition{event: event, key: expression{kind: sourceExpression}})
	return &n.from[len(n.from)-1]
}

func lowerVariant(d *definition) error {
	if len(d.entering) == 0 {
		return invalid("variant must declare EntersOn event")
	}
	if d.keyField == "" {
		return invalid("variant must declare a key property")
	}
	for _, entering := range d.entering {
		ensureFrom(&d.nodeDefinition, entering.event).key = entering.key
	}
	var froms []fromDefinition
	for _, from := range d.from {
		if slices.ContainsFunc(d.entering, func(e fromDefinition) bool { return e.event == from.event }) {
			froms = append(froms, from)
			continue
		}
		// C# replaces an existing join for this event, including its On and key.
		d.joins = slices.DeleteFunc(d.joins, func(j joinDefinition) bool { return j.event == from.event })
		from.parent = expression{}
		d.joins = append(d.joins, joinDefinition{fromDefinition: from, on: d.keyField})
	}
	d.from = froms
	return nil
}

func sortNode(n *nodeDefinition) {
	slices.SortFunc(n.from, func(a, b fromDefinition) int { return compareEvent(a.event, b.event) })
	slices.SortFunc(n.joins, func(a, b joinDefinition) int { return compareEvent(a.event, b.event) })
	slices.SortFunc(n.removals, func(a, b removalDefinition) int {
		if v := compareEvent(a.event, b.event); v != 0 {
			return v
		}
		if a.join == b.join {
			return 0
		}
		if a.join {
			return 1
		}
		return -1
	})
}
