// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Command chroniclevet checks Chronicle declarations through the Go vet protocol.
package main

import (
	"github.com/cratis/chronicle.go/tools/internal/eventprotection"
	"github.com/cratis/chronicle.go/tools/internal/migrationidentity"
	"golang.org/x/tools/go/analysis/unitchecker"
)

func main() { unitchecker.Main(eventprotection.Analyzer, migrationidentity.Analyzer) }
