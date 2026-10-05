// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

func TestEnumReboundExplicitPropertyExpressionsRefuseBeforeRPC(t *testing.T) {
	for _, handler := range []string{"From", "Join", "Every"} {
		t.Run(handler, func(t *testing.T) {
			codecs := projectionEnumCodecs(t, false)
			r := chronicle.NewRegistry()
			event, err := chronicle.RegisterEvent[enumPolicyLiteralEvent](r, events.WithCodecs(codecs))
			if err != nil {
				t.Fatal(err)
			}
			model, err := chronicle.RegisterReadModel[enumPolicyLiteralModel](r, readmodels.WithCodecs(codecs))
			if err != nil {
				t.Fatal(err)
			}
			b := projections.NewBuilder("enum-rebound-expression", model, projections.NoAutoMap())
			define := func(from *projections.FromBuilder[enumPolicyLiteralModel, enumPolicyLiteralEvent]) {
				projections.Map(from, projections.Path[enumPolicyLiteralModel, projectionEnum]("TRUE"), projections.Path[enumPolicyLiteralEvent, projectionEnum]("TRUE"))
			}
			switch handler {
			case "From":
				projections.From(b, event, define)
			case "Join":
				projections.Join(b, event, projections.Path[enumPolicyLiteralModel, string]("ID"), define)
			case "Every":
				projections.From(b, event, nil)
				projections.Every(b, func(every *projections.EveryBuilder[enumPolicyLiteralModel]) {
					projections.EveryMap(every, projections.Path[enumPolicyLiteralModel, projectionEnum]("TRUE"), "TRUE")
				})
			}
			declaration, err := b.Build()
			if err != nil {
				t.Fatal(err)
			}
			if err := r.AddProjection(declaration); err != nil {
				t.Fatal(err)
			}
			enumRegistrationNoRPC(t, r, "true", serialization.LegacyGoCamelCase)
		})
	}
}
