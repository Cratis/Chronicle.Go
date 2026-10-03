// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"context"
	"errors"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"google.golang.org/protobuf/proto"
)

func TestDecisionSinkDefaultRequiresActualServerProfileBeforeFolding(t *testing.T) {
	for _, kind := range []SinkType{SQL, InMemory} {
		t.Run(string(kind), func(t *testing.T) {
			f := newDecisionFixture(t)
			resolved, err := f.model.Descriptor().WithDefaultSinkType(kind)
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := NewCatalog(resolved)
			if err != nil {
				t.Fatal(err)
			}
			f.reader.reader.service.catalog = catalog
			// The server still holds MongoDB. Local shape admission is not proof
			// of provider availability or server agreement with a new default.
			if !f.reader.Admit().IsAdmitted {
				t.Fatal("local shape admission changed")
			}
			folds := 0
			f.handle = func(_ context.Context, request any) (proto.Message, error) {
				if _, ok := request.(*contracts.GetInstanceByKeyRequest); ok {
					folds++
				}
				return nil, nil
			}
			read, err := f.reader.GetDetached(t.Context(), "source")
			var refused *DecisionReadRefused
			if !errors.As(err, &refused) || refused.Reason != DecisionDefinitionMismatch || folds != 0 || !read.Token.IsZero() {
				t.Fatalf("wrong server sink folded: %+v %v folds=%d", read, err, folds)
			}
			assertDecisionCleanup(t, f)
		})
	}
}
