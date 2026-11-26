// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projectionconsumer

import (
	"fmt"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	fluent "github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

type Changed struct{ Value string }

type RequiredView struct {
	ID   string `json:"id"`
	Note string `json:"note"`
}

type OptionalView struct {
	ID   string  `json:"id"`
	Note *string `json:"note"`
}

func ClearRequired() error {
	event, err := events.Define[Changed]()
	if err != nil {
		return fmt.Errorf("define event: %v", err)
	}
	model, err := readmodels.Define[RequiredView]()
	if err != nil {
		return fmt.Errorf("define model: %v", err)
	}
	b := projections.NewBuilder("notes", model, projections.NoAutoMap())
	projections.From(b, event, func(from *projections.FromBuilder[RequiredView, Changed]) {
		projections.Clear(from, projections.Path[RequiredView, string]("note")) // want "^CHR0048:"
	})
	_, err = b.Build()
	return err
}

func ClearOptional() error {
	event, err := events.Define[Changed]()
	if err != nil {
		return fmt.Errorf("define event: %v", err)
	}
	model, err := readmodels.Define[OptionalView]()
	if err != nil {
		return fmt.Errorf("define model: %v", err)
	}
	b := projections.NewBuilder("notes", model, projections.NoAutoMap())
	projections.From(b, event, func(from *projections.FromBuilder[OptionalView, Changed]) {
		projections.Clear(from, projections.Path[OptionalView, *string]("note"))
	})
	_, err = b.Build()
	return err
}

func UnprovenTargets(b *projections.Builder[RequiredView], event events.Type[Changed]) {
	projections.From(b, event, func(from *projections.FromBuilder[RequiredView, Changed]) {
		projections.Clear(from, projections.Path[RequiredView, string]("missing"))
	})
	projections.From(b, event, func(from *projections.FromBuilder[RequiredView, Changed]) {
		projections.Clear(from, projections.Path[RequiredView, string]("Note"))
	})
	projections.From(b, event, func(from *projections.FromBuilder[RequiredView, Changed]) {
		projections.Clear(from, projections.Path[RequiredView, string]("id"))
	})
	projections.From(b, event, func(from *projections.FromBuilder[RequiredView, Changed]) {
		projections.Clear(from, projections.Path[RequiredView, string]("not\x65"))
	})
}

func DescriptorMismatch(b *projections.Builder[OptionalView], event events.Type[Changed]) {
	projections.From(b, event, func(from *projections.FromBuilder[OptionalView, Changed]) {
		projections.Clear(from, projections.Path[OptionalView, string]("note"))
	})
}

type text string
type textAlias = string
type requiredAlias = RequiredView

func NamedDescriptors(b *projections.Builder[RequiredView], event events.Type[Changed]) {
	projections.From(b, event, func(from *projections.FromBuilder[RequiredView, Changed]) {
		projections.Clear(from, projections.Path[RequiredView, text]("note"))
	})
	projections.From(b, event, func(from *projections.FromBuilder[RequiredView, Changed]) {
		projections.Clear(from, projections.Path[RequiredView, textAlias]("note"))
	})
	projections.From(b, event, func(from *projections.FromBuilder[requiredAlias, Changed]) {
		projections.Clear(from, projections.Path[requiredAlias, string]("note"))
	})
}

func DynamicTargets(b *projections.Builder[RequiredView], event events.Type[Changed]) {
	const path = "note"
	projections.From(b, event, func(from *projections.FromBuilder[RequiredView, Changed]) {
		projections.Clear(from, projections.Path[RequiredView, string](path))
	})
	descriptor := projections.Path[RequiredView, string]("note")
	projections.From(b, event, func(from *projections.FromBuilder[RequiredView, Changed]) {
		projections.Clear(from, descriptor)
	})
}

func IndirectOrConditional(b *projections.Builder[RequiredView], event events.Type[Changed], enabled bool) {
	callback := func(from *projections.FromBuilder[RequiredView, Changed]) {
		projections.Clear(from, projections.Path[RequiredView, string]("note"))
	}
	projections.From(b, event, callback)
	projections.From(b, event, func(from *projections.FromBuilder[RequiredView, Changed]) {
		if enabled {
			projections.Clear(from, projections.Path[RequiredView, string]("note"))
		}
	})
}

func OtherBuilder(b *projections.Builder[RequiredView], event events.Type[Changed], other *projections.FromBuilder[RequiredView, Changed]) {
	projections.From(b, event, func(from *projections.FromBuilder[RequiredView, Changed]) {
		projections.Clear(other, projections.Path[RequiredView, string]("note"))
	})
}

func LookalikeAndAlias(b *projections.Builder[RequiredView], event events.Type[Changed]) {
	lookalike := struct {
		Clear func(*projections.FromBuilder[RequiredView, Changed], projections.Field[RequiredView, string])
	}{}
	projections.From(b, event, func(from *projections.FromBuilder[RequiredView, Changed]) {
		lookalike.Clear(from, projections.Path[RequiredView, string]("note"))
	})
	fluent.From(b, event, func(from *fluent.FromBuilder[RequiredView, Changed]) {
		fluent.Clear(from, fluent.Path[RequiredView, string]("note"))
	})
}

func ExplicitTypeArguments(b *projections.Builder[RequiredView], event events.Type[Changed]) {
	projections.From[RequiredView, Changed](b, event, func(from *projections.FromBuilder[RequiredView, Changed]) {
		projections.Clear(from, projections.Path[RequiredView, string]("note"))
	})
	projections.From(b, event, func(from *projections.FromBuilder[RequiredView, Changed]) {
		projections.Clear[RequiredView, Changed, string](from, projections.Path[RequiredView, string]("note"))
	})
}

type ImplicitView struct {
	ID   string `json:"id"`
	Note string
}

type EmbeddedView struct {
	RequiredView
}

type ExtraTagView struct {
	ID   string `json:"id"`
	Note string `json:"note,omitempty"`
}

type DuplicateView struct {
	ID    string `json:"id"`
	Note  string `json:"note"`
	Other string `json:"nOte"`
}

type RenamedIdentityView struct {
	ID string `json:"note"`
}

func UnsupportedShapes(
	implicit *projections.Builder[ImplicitView],
	embedded *projections.Builder[EmbeddedView],
	extraTag *projections.Builder[ExtraTagView],
	duplicate *projections.Builder[DuplicateView],
	identity *projections.Builder[RenamedIdentityView],
	event events.Type[Changed],
) {
	projections.From(implicit, event, func(from *projections.FromBuilder[ImplicitView, Changed]) {
		projections.Clear(from, projections.Path[ImplicitView, string]("note"))
	})
	projections.From(embedded, event, func(from *projections.FromBuilder[EmbeddedView, Changed]) {
		projections.Clear(from, projections.Path[EmbeddedView, string]("note"))
	})
	projections.From(extraTag, event, func(from *projections.FromBuilder[ExtraTagView, Changed]) {
		projections.Clear(from, projections.Path[ExtraTagView, string]("note"))
	})
	projections.From(duplicate, event, func(from *projections.FromBuilder[DuplicateView, Changed]) {
		projections.Clear(from, projections.Path[DuplicateView, string]("note"))
	})
	projections.From(identity, event, func(from *projections.FromBuilder[RenamedIdentityView, Changed]) {
		projections.Clear(from, projections.Path[RenamedIdentityView, string]("note"))
	})
}
