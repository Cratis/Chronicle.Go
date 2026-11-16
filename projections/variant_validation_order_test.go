// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
)

func TestCompileGroupReportsVariantFailuresInInputOrder(t *testing.T) {
	created := mustEvent[IssueCreated](t)
	renamed := mustEvent[TitleChanged](t)
	catalog, err := events.NewCatalog(created.Descriptor(), renamed.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	first := projections.ModelBound(mustModel[PullRequestItem](t), projections.WithIdentifier("z-first"), projections.VariantOf[WorkItem]())
	second := projections.ModelBound(mustModel[BacklogItem](t), projections.WithIdentifier("a-second"), projections.VariantOf[WorkItem]())
	_, err = projections.CompileGroup([]projections.Declaration{first, second}, catalog)
	var failure *projections.DeclarationError
	if !errors.As(err, &failure) {
		t.Fatalf("expected declaration failure, got %v", err)
	}
	if failure.Artifact != "z-first" {
		t.Fatalf("first failure = %+v, want z-first", failure)
	}
}

func TestCompileGroupChecksOrphanGlobalsBeforeLoweringVariants(t *testing.T) {
	created := mustEvent[IssueCreated](t)
	catalog, err := events.NewCatalog(created.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	type UnregisteredGroup struct{}
	global, err := projections.Global[FluentShared](projections.GlobalFor[UnregisteredGroup](), projections.WithIdentifier("orphan"))
	if err != nil {
		t.Fatal(err)
	}
	variant := projections.ModelBound(mustModel[PullRequestItem](t), projections.VariantOf[WorkItem]())
	_, err = projections.CompileGroup([]projections.Declaration{variant, global}, catalog)
	var failure *projections.DeclarationError
	if !errors.As(err, &failure) {
		t.Fatalf("expected declaration failure, got %v", err)
	}
	if failure.Artifact != "orphan" {
		t.Fatalf("first failure = %+v, want orphan", failure)
	}
}
