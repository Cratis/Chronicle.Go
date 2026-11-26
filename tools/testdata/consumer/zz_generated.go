// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Code generated for the analyzer fixture. DO NOT EDIT.

package consumer

import "github.com/cratis/chronicle.go/events"

type GeneratedConflict struct {
	Value string `chronicle:"pii;encrypted"` // want "^CHR0053:"
}

func DeclareGenerated() { _, _ = events.Define[GeneratedConflict]() }
