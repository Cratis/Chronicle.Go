// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"reflect"
	"strings"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/serialization"
)

// Dynamic dictionary key sources understood by the kernel. Each is a property
// path segment: <dictionary>.<source>.<path>.
const (
	eventContentKey = "$eventContent"
	eventContextKey = "$eventContext"
	causedByKey     = "$causedBy"
)

// CausedByProperty names one identity property of the event's causing user.
type CausedByProperty string

// Identity properties the kernel resolves for $causedBy(...) values and keys.
const (
	CausedBySubject  CausedByProperty = "subject"
	CausedByName     CausedByProperty = "name"
	CausedByUserName CausedByProperty = "userName"
)

func (p CausedByProperty) valid() bool {
	return p == CausedBySubject || p == CausedByName || p == CausedByUserName
}

// EntryKey selects the dictionary key from each projected event. Create it with
// KeyFromEvent, KeyFromContext or KeyFromCausedBy; the zero value is invalid.
type EntryKey struct {
	source, path string
}

// KeyFromEvent keys a dictionary entry by an event property ($eventContent).
// The path must exist on the projected event; Build/Compile validates it
// against that event's serialized fields, as the kernel does at registration.
func KeyFromEvent[E, K any](field Field[E, K]) EntryKey {
	return EntryKey{source: eventContentKey, path: field.path}
}

// KeyFromContext keys a dictionary entry by an admitted scalar kernel
// EventContext property ($eventContext), for example eventSourceId.
func KeyFromContext(path string) EntryKey {
	return EntryKey{source: eventContextKey, path: path}
}

// KeyFromCausedBy keys a dictionary entry by an identity property of the user
// who caused the event ($causedBy).
func KeyFromCausedBy(property CausedByProperty) EntryKey {
	return EntryKey{source: causedByKey, path: string(property)}
}

// Entry addresses the value stored under a dynamic key in a string-keyed map
// property. Pass the result as the target of Map, MapAs, Context, CausedBy,
// EventSourceID, Value, Add, Subtract, Increment, Decrement, Count, RemoveKey or
// their once variants. Entry targets are not model fields: using one as a key,
// identity, child collection or join field fails Build/Compile.
func Entry[M any, K ~string, V any](dictionary Field[M, map[K]V], key EntryKey) Field[M, V] {
	if key.source == "" || dictionary.path == "" {
		return Field[M, V]{path: invalidEntryPath, owner: dictionary.owner}
	}
	return Field[M, V]{path: dictionary.path + "." + key.source + "." + key.path, owner: dictionary.owner}
}

// RemoveKey removes the dictionary entry addressed by an Entry target with the
// kernel's $remove expression. A missing key is left unchanged by the kernel.
func RemoveKey[M, E, V any](b *FromBuilder[M, E], entry Field[M, V]) {
	b.add(entry.path, reflect.TypeFor[V](), nil, expression{kind: removeExpression}, "remove-key")
}

// invalidEntryPath is never a serialized path, so the ordinary unknown target
// diagnostic reports a zero EntryKey or dictionary descriptor.
const invalidEntryPath = "$invalidEntry"

// entryKey is the dynamic key of a dictionary entry write; zero means the write
// targets the model property itself.
type entryKey struct {
	source, path string
}

func (k entryKey) set() bool { return k.source != "" }

// encode returns the kernel property path for the write target.
func (k entryKey) encode(dictionary string) string {
	if !k.set() {
		return dictionary
	}
	return dictionary + "." + k.source + "." + k.path
}

// splitEntry separates a fluent Entry path into its dictionary and dynamic key.
func splitEntry(path string) (string, entryKey) {
	for _, source := range []string{eventContentKey, eventContextKey, causedByKey} {
		if dictionary, key, found := strings.Cut(path, "."+source+"."); found {
			return dictionary, entryKey{source: source, path: key}
		}
	}
	return path, entryKey{}
}

// entryTarget validates the dictionary and key of an entry write and returns the
// descriptor of its value. eventFields is nil for Every/All, which cannot key by
// event content because no single event is known.
func entryTarget(dictionary serialization.Field, key entryKey, eventFields []serialization.Field, global bool) (serialization.Field, error) {
	if dictionary.Collection || indirectType(dictionary.Type).Kind() != reflect.Map {
		return serialization.Field{}, invalid("dictionary entry requires a string-keyed map property")
	}
	value, ok := dictionary.Element()
	if !ok {
		return serialization.Field{}, invalid("dictionary entry requires a string-keyed map property")
	}
	value.Collection = false
	if err := validateEntryKey(key, eventFields, global); err != nil {
		return serialization.Field{}, err
	}
	return value, nil
}

func validateEntryKey(key entryKey, eventFields []serialization.Field, global bool) error {
	switch key.source {
	case eventContentKey:
		if global {
			return invalid("Every/All dictionary entries cannot be keyed by event content")
		}
		field, ok := binaryMappingField(eventFields, key.path)
		if !eventPropertyPath(key.path) || !ok || field.Collection || !field.Scalar.IsPrimitive() {
			return invalid("dictionary key requires a scalar property of the projected event")
		}
	case eventContextKey:
		if !declarations.Path(key.path) {
			return invalid("invalid context path")
		}
		field, err := resolveContext(key.path)
		if err != nil {
			return err
		}
		if !field.Scalar.IsPrimitive() {
			return invalid("dictionary key requires a scalar context property")
		}
	case causedByKey:
		if !CausedByProperty(key.path).valid() {
			return invalid("caused-by key must be subject, name or userName")
		}
	default:
		return invalid("unsupported dictionary key source")
	}
	return nil
}

// resolveWriteTarget looks up the target descriptor of a write, including the
// value descriptor of a dictionary entry. Remove is only valid for entries.
func resolveWriteTarget(fields []serialization.Field, w write, eventFields []serialization.Field, global bool) (serialization.Field, error) {
	target, ok := binaryMappingField(fields, w.path)
	if !ok {
		return target, invalid("unknown serialized model property")
	}
	if !w.entry.set() {
		if w.expression.kind == removeExpression {
			return target, invalid("remove requires a dictionary entry target")
		}
		return target, nil
	}
	if validateTarget(target, nil) != nil {
		return target, invalid("unsupported model property path")
	}
	value, err := entryTarget(target, w.entry, eventFields, global)
	if err != nil {
		return target, err
	}
	value.GoField = target.GoField
	return value, nil
}
