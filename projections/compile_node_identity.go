// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"strings"

	"github.com/cratis/chronicle.go/serialization"
)

// explicitIdentityDirectives are the directives that write a child identity explicitly.
var explicitIdentityDirectives = map[string]bool{
	"set": true, "context": true, "value": true, "add": true, "subtract": true,
}

func (b *nodeBuilder) defaultIdentity() string {
	key := expression{kind: sourceExpression}
	if len(b.creators) > 0 {
		key = b.creators[0].key
	} else if len(b.d.subscriptions) > 0 {
		key = b.d.subscriptions[0].key
	}
	return discoverIdentity(b.fields, b.n.keyField, key)
}

func (b *nodeBuilder) hasExplicitIdentity() bool {
	for _, from := range b.froms {
		for _, w := range from.writes {
			if w.path == b.n.identifiedBy && explicitIdentityDirectives[w.provenance.Directive] {
				return true
			}
		}
	}
	return false
}

func isScalarIdentity(identity serialization.Field, found bool) bool {
	return found && !identity.Collection && !identity.Nullable && identity.Scalar.IsPrimitive()
}

func (b *nodeBuilder) addChildIdentity() error {
	if b.parentFields == nil || b.nested {
		return nil
	}
	if b.n.identifiedBy == "" {
		b.n.identifiedBy = b.defaultIdentity()
	}
	if b.n.identifiedBy == "$eventSourceId" {
		return nil
	}
	identity, ok := binaryMappingField(b.fields, b.n.identifiedBy)
	if !isScalarIdentity(identity, ok) {
		return invalid("child identity requires a non-nullable scalar field")
	}
	return b.addCreatorIdentities(b.hasExplicitIdentity())
}

func (b *nodeBuilder) addCreatorIdentities(explicitIdentity bool) error {
	for _, creator := range b.creators {
		if err := b.addCreatorIdentity(creator, explicitIdentity); err != nil {
			return err
		}
	}
	return nil
}

// usesConventionIdentity reports whether the identity convention should write the identity for the event.
func (b *nodeBuilder) usesConventionIdentity(from *fromDefinition, explicitIdentity bool) bool {
	n := b.n
	if !b.d.modelBound || n.noAuto || explicitIdentity {
		return false
	}
	if from.key.kind == compositeExpression || hasWrite(from.writes, n.identifiedBy) {
		return false
	}
	return !keyIsIdentityAlias(from.key, n.identifiedBy, n.keyField)
}

func keyIsIdentityAlias(key expression, identifiedBy, keyField string) bool {
	return strings.EqualFold(identifiedBy, key.text) && keyField != identifiedBy
}

func (b *nodeBuilder) addCreatorIdentity(creator subscription, explicitIdentity bool) error {
	n := b.n
	from := b.ensure(creator.event)
	if !b.usesConventionIdentity(from, explicitIdentity) {
		return nil
	}
	e := from.key
	if e.kind == sourceExpression {
		e = expression{kind: contextExpression, text: "EventSourceId"}
	}
	w := write{path: n.identifiedBy, expression: e, provenance: Provenance{FrontEnd: "convention", Path: n.identifiedBy, Directive: "child-identity", Offset: -1, Event: creator.event.Ref()}}
	return addWrite(b.c.result, from, w, b.fields, creator.event.Fields(), false)
}
