// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"reflect"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/serialization"
)

// childOrdering is where the kernel places a new child in its collection.
// Without one, children are appended in event order.
type childOrdering struct {
	placement contracts.ChildrenPlacement
	sortBy    string
	sortType  reflect.Type
	direction contracts.SortDirection
}

// Prepend inserts each new child at the start of the collection instead of
// appending it. Only one ordering may be declared per children collection.
func Prepend() ChildOption {
	return orderChildren(&childOrdering{placement: contracts.ChildrenPlacement_Prepend})
}

// OrderBy keeps children sorted ascending by a property of the child. The
// kernel inserts each new child at its sorted position.
func OrderBy[C, V any](field Field[C, V]) ChildOption {
	return orderChildrenBy(field.path, field.owner, reflect.TypeFor[V](), contracts.SortDirection_Ascending)
}

// OrderByDescending keeps children sorted descending by a property of the child.
func OrderByDescending[C, V any](field Field[C, V]) ChildOption {
	return orderChildrenBy(field.path, field.owner, reflect.TypeFor[V](), contracts.SortDirection_Descending)
}

func orderChildrenBy(path string, owner, typ reflect.Type, direction contracts.SortDirection) ChildOption {
	ordering := &childOrdering{placement: contracts.ChildrenPlacement_Sorted, sortBy: path, sortType: typ, direction: direction}
	return func(c *childDeclaration) {
		if c.typ != owner {
			c.data.err = invalid("child ordering owner mismatch")
			return
		}
		orderChildren(ordering)(c)
	}
}

func orderChildren(ordering *childOrdering) ChildOption {
	return func(c *childDeclaration) {
		if c.ordering != nil {
			c.data.err = invalid("children ordering already defined")
			return
		}
		copied := *ordering
		c.ordering = &copied
	}
}

// checkChildOrdering validates the sort property against the child's fields.
func checkChildOrdering(child childDeclaration, local []serialization.Field) error {
	ordering := child.ordering
	if ordering == nil || ordering.placement != contracts.ChildrenPlacement_Sorted {
		return nil
	}
	field, ok := binaryMappingField(local, ordering.sortBy)
	if !ok || validateTarget(field, ordering.sortType) != nil || !field.Scalar.IsPrimitive() {
		return invalid("children sort property must be a scalar property of the child")
	}
	return nil
}

func (o *childOrdering) encode() *contracts.ChildrenOrderingDefinition {
	if o == nil {
		return nil
	}
	return &contracts.ChildrenOrderingDefinition{Placement: o.placement, SortBy: o.sortBy, Direction: o.direction}
}

// setOrdering applies the prepend, order-by or order-by-descending argument of a
// model-bound children directive. Several directives on one field must agree.
func (bc *boundChildContext) setOrdering(directive declarations.Directive) error {
	var ordering *childOrdering
	for _, name := range []string{"prepend", "order-by", "order-by-descending"} {
		a, ok := argument(directive, name)
		if !ok || name == "prepend" && a.Text != "true" {
			continue
		}
		if ordering != nil {
			return invalid("children declare more than one ordering")
		}
		switch name {
		case "prepend":
			ordering = &childOrdering{placement: contracts.ChildrenPlacement_Prepend}
		case "order-by":
			ordering = &childOrdering{placement: contracts.ChildrenPlacement_Sorted, sortBy: a.Text, direction: contracts.SortDirection_Ascending}
		default:
			ordering = &childOrdering{placement: contracts.ChildrenPlacement_Sorted, sortBy: a.Text, direction: contracts.SortDirection_Descending}
		}
	}
	if ordering == nil {
		return nil
	}
	if bc.child.ordering != nil && *bc.child.ordering != *ordering {
		return invalid("children disagree on ordering")
	}
	bc.child.ordering = ordering
	return nil
}
