// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

//go:build conflict

package consumer

import "github.com/cratis/chronicle.go/events"

type TaggedConflict struct {
	Value string `chronicle:"pii;encrypted"` // want "^CHR0053:"
}

func DeclareTagged() { _, _ = events.Define[TaggedConflict]() }
