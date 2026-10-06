// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

type binaryMappedEvent struct{ Payload []byte }
type binaryStringEvent struct{ Payload string }
type binaryLowerEvent struct {
	Payload string `json:"payload"`
}
type binaryMappedModel struct {
	ID      string
	Payload []byte
}
type binaryStringModel struct {
	ID      string
	Payload string
}
type binaryKeyModel struct {
	ID      []byte
	Payload []byte
}

func binaryMappingFailure(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatalf("binary mapping should refuse before dispatch: %v", err)
	}
}

func TestBinaryProjectionMappingsRequireSameRepresentation(t *testing.T) {
	good := projections.NewBuilder("binary", mustModel[binaryMappedModel](t))
	projections.From(good, mustEvent[binaryMappedEvent](t), nil)
	if _, err := good.Build(); err != nil {
		t.Fatal(err)
	}
	toBinary := projections.NewBuilder("to-binary", mustModel[binaryMappedModel](t))
	projections.From(toBinary, mustEvent[binaryStringEvent](t), nil)
	_, err := toBinary.Build()
	binaryMappingFailure(t, err)
	toString := projections.NewBuilder("to-string", mustModel[binaryStringModel](t))
	projections.From(toString, mustEvent[binaryMappedEvent](t), nil)
	_, err = toString.Build()
	binaryMappingFailure(t, err)
	lower := projections.NewBuilder("case", mustModel[binaryMappedModel](t))
	projections.From(lower, mustEvent[binaryLowerEvent](t), nil)
	_, err = lower.Build()
	binaryMappingFailure(t, err)
}

func TestBinaryExplicitProjectionMappingsAndExpressionsRefuse(t *testing.T) {
	event := mustEvent[binaryMappedEvent](t)
	for name, configure := range map[string]func(*projections.FromBuilder[binaryMappedModel, binaryMappedEvent]){
		"literal": func(f *projections.FromBuilder[binaryMappedModel, binaryMappedEvent]) {
			projections.Value(f, projections.Path[binaryMappedModel, []byte]("Payload"), []byte{1})
		},
		"context": func(f *projections.FromBuilder[binaryMappedModel, binaryMappedEvent]) {
			projections.Context(f, projections.Path[binaryMappedModel, []byte]("Payload"), "eventStore")
		},
		"source": func(f *projections.FromBuilder[binaryMappedModel, binaryMappedEvent]) {
			projections.EventSourceID(f, projections.Path[binaryMappedModel, []byte]("Payload"))
		},
		"arithmetic": func(f *projections.FromBuilder[binaryMappedModel, binaryMappedEvent]) {
			projections.Increment(f, projections.Path[binaryMappedModel, []byte]("Payload"))
		},
	} {
		t.Run(name, func(t *testing.T) {
			b := projections.NewBuilder("binary-"+name, mustModel[binaryMappedModel](t), projections.NoAutoMap())
			projections.From(b, event, configure)
			_, err := b.Build()
			binaryMappingFailure(t, err)
		})
	}
	b := projections.NewBuilder("binary-as-string", mustModel[binaryStringModel](t), projections.NoAutoMap())
	projections.From(b, event, func(f *projections.FromBuilder[binaryStringModel, binaryMappedEvent]) {
		projections.MapAs(f, projections.Path[binaryStringModel, string]("Payload"), projections.Path[binaryMappedEvent, []byte]("Payload"))
	})
	_, err := b.Build()
	binaryMappingFailure(t, err)
}

func TestBinaryProjectionKeysAndJoinsRefuse(t *testing.T) {
	b := projections.NewBuilder("binary-key", mustModel[binaryMappedModel](t), projections.NoAutoMap())
	projections.From(b, mustEvent[binaryMappedEvent](t), nil, projections.UsingKey(projections.Path[binaryMappedEvent, []byte]("Payload")))
	_, err := b.Build()
	binaryMappingFailure(t, err)
	join := projections.NewBuilder("binary-join", mustModel[binaryMappedModel](t), projections.NoAutoMap())
	projections.Join(join, mustEvent[binaryMappedEvent](t), projections.Path[binaryMappedModel, []byte]("Payload"), nil)
	_, err = join.Build()
	binaryMappingFailure(t, err)
	_, err = readmodels.Define[binaryKeyModel]()
	binaryMappingFailure(t, err)
}
