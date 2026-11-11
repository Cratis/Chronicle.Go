// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"testing"
)

func TestSessionLostEndsTheGenerationAsErrSessionLost(t *testing.T) {
	g := &generation{}
	g.bind(t.Context())
	g.loseSession()
	if g.ctx.Err() == nil {
		t.Fatal("a lost session left the generation running")
	}
	if err := generationEnded(g, context.Canceled); !errors.Is(err, ErrSessionLost) {
		t.Fatalf("generation ended with %v, want ErrSessionLost", err)
	}
}

func TestLateSessionLostReportFromAnEndedGenerationChangesNothing(t *testing.T) {
	old, current := &generation{}, &generation{}
	old.bind(t.Context())
	current.bind(t.Context())
	stale := errors.New("keep-alive ended")
	old.cancel()
	old.loseSession()
	if err := generationEnded(old, stale); err != stale {
		t.Fatalf("ended generation reported %v, want its original end %v", err, stale)
	}
	if current.ctx.Err() != nil {
		t.Fatal("a report from an ended generation ended the current one")
	}
}
