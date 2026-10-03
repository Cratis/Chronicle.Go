// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reactors

import (
	"reflect"

	"github.com/cratis/chronicle.go/internal/artifacts"
)

// Scope resolves borrowed dependencies for an observer operation and closes them
// after handling. The optional services package adapts Fundamentals.Go scopes.
type Scope = artifacts.Scope

// ScopeFactory opens an observer scope. Its provider is borrowed, never closed.
type ScopeFactory = artifacts.ScopeFactory

// Catalog optionally advertises resolvable service types for startup validation.
type Catalog = artifacts.Catalog

// DefaultScopeFactory supplies zero-container structs, pointers and empty slices.
func DefaultScopeFactory() ScopeFactory { return artifacts.DefaultScopeFactory() }

func nilLike(v any) bool { return artifacts.NilLike(v) }
func validateService(factory ScopeFactory, typ reflect.Type) error {
	return artifacts.ValidateService(factory, typ)
}
