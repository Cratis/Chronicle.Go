// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events

import "reflect"

// WithSubjectResolver declares the compliance subject policy for T. Returning
// false falls back to the append source ID; an explicit append subject wins.
// The callback receives a value even when appending *T. It must be non-nil,
// match the declared event type and support concurrent calls. Last option wins.
// The resolver is borrowed by the descriptor and is invoked during serialization
// snapshots, not again at unit-of-work commit. Empty resolved subjects are invalid
// for append, like empty explicit subjects; false represents absence.
func WithSubjectResolver[T any](resolver func(T) (Subject, bool)) TypeOption {
	return func(c *typeConfig) {
		c.subjectType = reflect.TypeFor[T]()
		c.subject = nil
		if resolver != nil {
			c.subject = func(value any) (Subject, bool) {
				if typed, ok := value.(T); ok {
					return resolver(typed)
				}
				if typed, ok := value.(*T); ok && typed != nil {
					return resolver(*typed)
				}
				return "", false
			}
		}
	}
}

// ResolveSubject invokes the registered subject resolver for a matching value or
// non-nil pointer. False means no subject was resolved. It performs no I/O.
func (d Descriptor) ResolveSubject(value any) (Subject, bool) {
	if d.subject == nil {
		return "", false
	}
	return d.subject(value)
}
