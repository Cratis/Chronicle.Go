// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package contracts supplies the canonical Chronicle descriptor and public generated RPC packages.
// Applications must reuse these packages instead of registering duplicate protobuf descriptors.
package contracts

import _ "embed"

//go:generate python3 ../scripts/generate-contracts.py

//go:embed chronicle.desc
var descriptor []byte

// DescriptorSet returns a copy of the unchanged upstream FileDescriptorSet.
func DescriptorSet() []byte { return append([]byte(nil), descriptor...) }
