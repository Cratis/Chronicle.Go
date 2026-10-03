// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/internal/derivedfixtures"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/readmodels"
)

func TestDerivedModelDuplicatesFailBeforeIDNormalizationAndDelivery(t *testing.T) {
	codecs, err := derivedfixtures.Codecs()
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[derivedModel](readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	d := model.Descriptor()
	for name, data := range map[string]string{
		"duplicate root before ID alias rewrite": `{"_id":"source","Member":{"_derivedTypeId":"secret"},"Member":{"_derivedTypeId":"robot"}}`,
		"nested parent before ID alias rewrite":  `{"_id":"source","Member":{"children":[{"_derivedTypeId":"robot","_derivedTypeId":"human"}],"children":[],"_derivedTypeId":"human"}}`,
		"duplicate unknown map key":              `{"_id":"source","unknown":{"secret":{},"secret":{}}}`,
		"duplicate inside unknown arrays":        `{"_id":"source","unknown":[[{"secret":1,"secret":2}]]}`,
		"duplicate family inside array":          `{"_id":"source","Members":[{"_derivedTypeId":"robot","_derivedTypeId":"human"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			value, err := d.Unmarshal([]byte(data))
			if !errors.Is(err, faults.ErrProtocol) || value != nil || strings.Contains(err.Error(), "secret") {
				t.Fatalf("descriptor delivered ambiguous data: %v", err)
			}
			kernel := &modelKernel{get: func(context.Context, *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
				return &contracts.GetInstanceByKeyResponse{ReadModel: data, LastHandledEventSequenceNumber: 7}, nil
			}}
			service, ctx := serviceFixture(t, kernel, d)
			raw, err := service.Get(ctx, model.Identifier(), "source")
			if !errors.Is(err, faults.ErrProtocol) || raw.Exists || raw.Value != nil || raw.LastHandled != nil {
				t.Fatalf("service delivered ambiguous data: %v", err)
			}
			got, err := readmodels.For(service, model).Get(ctx, "source")
			if !errors.Is(err, faults.ErrProtocol) || got.Exists || got.Value.ID != "" || got.Value.Member != nil || got.LastHandled != nil {
				t.Fatalf("typed reader delivered ambiguous data: %v", err)
			}
			released, err := service.Release(ctx, model.Identifier(), []byte(data))
			if err == nil || released != nil || strings.Contains(err.Error(), "secret") {
				t.Fatalf("release delivered ambiguous data: %v", err)
			}
		})
	}
	value, err := d.Unmarshal([]byte(`{"_id":"source","Member":{"children":[{"count":7,"_derivedTypeId":"robot"}],"_derivedTypeId":"human"}}`))
	if err != nil {
		t.Fatal(err)
	}
	got := value.(*derivedModel)
	if got.ID != "source" || got.Member.(*derivedfixtures.HumanValue).Children[0] != (derivedfixtures.RobotValue{Count: 7}) {
		t.Fatal("valid family or ID alias lost")
	}
}
