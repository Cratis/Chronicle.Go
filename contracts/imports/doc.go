// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

//go:build chronicle_imports

// Package imports holds the generated contracts of the kernel's Imports service.
// The pinned Chronicle release (see contracts.ProtocolVersion) does not carry
// imports.proto yet, so no generated file is committed here and the package is
// built only with the chronicle_imports tag, against contracts generated
// locally for verification. Moving the pin to a release that carries
// imports.proto adds the generated files; this file then goes away.
package imports
