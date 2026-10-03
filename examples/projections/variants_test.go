// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package main

import (
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/protobuf/proto"
)

func TestVariantExamplesCompileAndMatch(t *testing.T) {
	for _, fluent := range []bool{false, true} {
		registry, err := variantRegistry(fluent)
		if err != nil {
			t.Fatal(err)
		}
		client, err := chronicle.NewClient(chronicle.WithRegistry(registry))
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
	}
	opened, err := events.Define[IssueOpened]()
	if err != nil {
		t.Fatal(err)
	}
	submitted, err := events.Define[PullRequestOpened]()
	if err != nil {
		t.Fatal(err)
	}
	renamed, err := events.Define[TitleChanged]()
	if err != nil {
		t.Fatal(err)
	}
	backlog, err := readmodels.Define[BacklogItem]()
	if err != nil {
		t.Fatal(err)
	}
	pullRequest, err := readmodels.Define[PullRequestItem]()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(opened.Descriptor(), submitted.Descriptor(), renamed.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	bound, err := modelBoundVariants(backlog, pullRequest, opened, submitted)
	if err != nil {
		t.Fatal(err)
	}
	fluent, err := fluentVariants(backlog, pullRequest, opened, submitted, renamed)
	if err != nil {
		t.Fatal(err)
	}
	a, err := projections.CompileGroup(bound, catalog)
	if err != nil {
		t.Fatal(err)
	}
	b, err := projections.CompileGroup(fluent, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != 2 || len(b) != 2 {
		t.Fatal("expected two variants")
	}
	for i := range a {
		if !proto.Equal(a[i].KernelDefinition(), b[i].KernelDefinition()) {
			t.Fatal("variant example frontend drift")
		}
	}
}
