// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package main

import (
	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

// variant-models:start
type WorkItem struct{}
type IssueOpened struct {
	Title string `json:"title"`
}
type PullRequestOpened struct {
	URL string `json:"url"`
}
type TitleChanged struct {
	Title string `json:"title"`
}

type BacklogItem struct {
	ID    string `json:"id" chronicle:"key"`
	Title string `json:"title"`
}
type PullRequestItem struct {
	ID    string `json:"id" chronicle:"key"`
	Title string `json:"title"`
	URL   string `json:"url"`
}
type SharedWorkItem struct {
	Title string `json:"title" chronicle:"set(TitleChanged)"`
}

// variant-models:end

// variant-model-bound:start
func modelBoundVariants(backlog readmodels.Model[BacklogItem], pullRequest readmodels.Model[PullRequestItem], opened events.Type[IssueOpened], submitted events.Type[PullRequestOpened]) ([]projections.Declaration, error) {
	shared, err := projections.Global[SharedWorkItem](projections.GlobalFor[WorkItem]())
	if err != nil {
		return nil, err
	}
	return []projections.Declaration{
		projections.ModelBound(backlog, projections.VariantOf[WorkItem](), projections.EntersOn(opened)),
		projections.ModelBound(pullRequest, projections.VariantOf[WorkItem](), projections.EntersOn(submitted)),
		shared,
	}, nil
}

// variant-model-bound:end

// variant-fluent:start
func fluentVariants(backlog readmodels.Model[BacklogItem], pullRequest readmodels.Model[PullRequestItem], opened events.Type[IssueOpened], submitted events.Type[PullRequestOpened], renamed events.Type[TitleChanged]) ([]projections.Declaration, error) {
	// Explicit shared From mappings are the C# fluent equivalent of GlobalFor.
	first := projections.NewBuilder("", backlog, projections.VariantOf[WorkItem](), projections.EntersOn(opened))
	projections.From(first, renamed, func(from *projections.FromBuilder[BacklogItem, TitleChanged]) {
		projections.Map(from, projections.Path[BacklogItem, string]("title"), projections.Path[TitleChanged, string]("title"))
	})
	second := projections.NewBuilder("", pullRequest, projections.VariantOf[WorkItem](), projections.EntersOn(submitted))
	projections.From(second, renamed, func(from *projections.FromBuilder[PullRequestItem, TitleChanged]) {
		projections.Map(from, projections.Path[PullRequestItem, string]("title"), projections.Path[TitleChanged, string]("title"))
	})
	a, err := first.Build()
	if err != nil {
		return nil, err
	}
	b, err := second.Build()
	if err != nil {
		return nil, err
	}
	return []projections.Declaration{a, b}, nil
}

// variant-fluent:end

func variantRegistry(fluent bool) (*chronicle.Registry, error) {
	r := chronicle.NewRegistry()
	opened, err := chronicle.RegisterEvent[IssueOpened](r)
	if err != nil {
		return nil, err
	}
	submitted, err := chronicle.RegisterEvent[PullRequestOpened](r)
	if err != nil {
		return nil, err
	}
	renamed, err := chronicle.RegisterEvent[TitleChanged](r)
	if err != nil {
		return nil, err
	}
	backlog, err := chronicle.RegisterReadModel[BacklogItem](r)
	if err != nil {
		return nil, err
	}
	pullRequest, err := chronicle.RegisterReadModel[PullRequestItem](r)
	if err != nil {
		return nil, err
	}
	var declarations []projections.Declaration
	if fluent {
		declarations, err = fluentVariants(backlog, pullRequest, opened, submitted, renamed)
	} else {
		declarations, err = modelBoundVariants(backlog, pullRequest, opened, submitted)
	}
	if err != nil {
		return nil, err
	}
	for _, declaration := range declarations {
		if err := r.AddProjection(declaration); err != nil {
			return nil, err
		}
	}
	return r, nil
}
