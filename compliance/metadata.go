// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package compliance describes personal-data and confidentiality classifications
// and the kernel-owned lifecycle of erasable subject keys. It performs no local
// cryptography. Confidentiality keys are never PII keys and cannot be erased here.
package compliance

import "reflect"

// Scope selects a confidentiality key's ownership, independently of PII.
type Scope string

const (
	// Subject scopes confidentiality to the document's resolved subject.
	Subject Scope = "subject"
	// Namespace scopes confidentiality to the store and namespace.
	Namespace Scope = "namespace"
	// Global scopes confidentiality across the installation.
	Global Scope = "global"
)

// Classification is copied declaration metadata. PII and Encrypted are mutually
// exclusive. Details is the compliance rationale or encryption purpose; it is
// schema metadata, not a place for personal data or secrets. An empty Scope with
// Encrypted means Subject. Details alone can supply a declaring type's rationale.
type Classification struct {
	PII       bool
	Encrypted bool
	Scope     Scope
	Details   string
	// DetailsSet distinguishes an explicitly empty rationale from inheritance.
	// Nonempty Details and Encrypted imply presence without setting this flag.
	DetailsSet bool
}

// Target describes a type or serialized member to an explicitly registered
// metadata provider. Field is empty for types. Providers classify declared
// members, like C# PropertyInfo; use Property for a finite serialized-path override.
type Target struct {
	Type          reflect.Type
	DeclaringType reflect.Type
	Field         string
}

// Provider supplies classifications during schema compilation, never at runtime.
// It must be deterministic, concurrency-safe and free of I/O. A zero result means
// no metadata. Errors abort registration and must not contain personal data.
type Provider func(Target) (Classification, error)

// Declaration supplies type-wide, property-specific or provider classifications.
// Use For, Property or Using. Options copy declarations; no global type registry
// exists, so catalogs can classify the same Go type independently.
type Declaration struct {
	typ      reflect.Type
	path     string
	metadata Classification
	provider Provider
}

// For declares classification on T, including its occurrences in collections.
// Repeated declarations of the same type are rejected rather than overwritten.
func For[T any](metadata Classification) Declaration {
	return Declaration{typ: reflect.TypeFor[T](), metadata: metadata}
}

// Property classifies an exact serialized property path. Field tags are preferred
// when the declaration belongs to the model; duplicate property declarations fail.
func Property(path string, metadata Classification) Declaration {
	return Declaration{path: path, metadata: metadata}
}

// Using registers a provider in declaration order. Nil providers are invalid.
func Using(provider Provider) Declaration { return Declaration{provider: provider} }

// TargetType returns a type declaration's target, or nil.
func (d Declaration) TargetType() reflect.Type { return d.typ }

// Path returns a property declaration's serialized path, or empty.
func (d Declaration) Path() string { return d.path }

// Metadata returns the copied classification.
func (d Declaration) Metadata() Classification { return d.metadata }

// Provider returns the provider, or nil for a static declaration.
func (d Declaration) Provider() Provider { return d.provider }
