//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/google/uuid"
)

type VariantWorkItem struct{}
type VariantIssueOpened struct {
	Title string `json:"title"`
}
type VariantPullRequestOpened struct {
	URL string `json:"url"`
}
type VariantTitleChanged struct {
	Title string `json:"title"`
}
type VariantBacklog struct {
	ID       string    `json:"id" chronicle:"key"`
	Title    string    `json:"title"`
	Updated  time.Time `json:"updated" chronicle:"every(context=occurred)"`
	Observed time.Time `json:"observed" chronicle:"all(context=occurred)"`
}
type VariantPullRequest struct {
	ID       string    `json:"id" chronicle:"key"`
	Title    string    `json:"title"`
	URL      string    `json:"url"`
	Updated  time.Time `json:"updated" chronicle:"every(context=occurred)"`
	Observed time.Time `json:"observed" chronicle:"all(context=occurred)"`
}
type VariantShared struct {
	Title string `json:"title" chronicle:"set(VariantTitleChanged)"`
}

func TestKernelVariantsEnterLeaveAndDoNotResurrect(t *testing.T) {
	fixture := newKernelFixture(t)
	r := chronicle.NewRegistry()
	issue, err := chronicle.RegisterEvent[VariantIssueOpened](r)
	if err != nil {
		t.Fatal(err)
	}
	pull, err := chronicle.RegisterEvent[VariantPullRequestOpened](r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = chronicle.RegisterEvent[VariantTitleChanged](r); err != nil {
		t.Fatal(err)
	}
	backlog, err := chronicle.RegisterReadModel[VariantBacklog](r)
	if err != nil {
		t.Fatal(err)
	}
	pr, err := chronicle.RegisterReadModel[VariantPullRequest](r)
	if err != nil {
		t.Fatal(err)
	}
	global, err := projections.Global[VariantShared](projections.GlobalFor[VariantWorkItem]())
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []projections.Declaration{
		projections.ModelBound(backlog, projections.VariantOf[VariantWorkItem](), projections.EntersOn(issue)),
		projections.ModelBound(pr, projections.VariantOf[VariantWorkItem](), projections.EntersOn(pull)), global,
	} {
		if err := r.AddProjection(d); err != nil {
			t.Fatal(err)
		}
	}
	store, err := fixture.client(r).EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	if len(store.Projections()) != 2 || len(store.ReadModels().Catalog().Descriptors()) != 2 {
		t.Fatal("global became standalone model")
	}
	source := events.SourceID(uuid.NewString())
	backlogReader := readmodels.For(store.ReadModels(), backlog)
	prReader := readmodels.For(store.ReadModels(), pr)
	appendSuccessfully(t, fixture.ctx, store, source, VariantTitleChanged{Title: "before-entry"})
	appendSuccessfully(t, fixture.ctx, store, source, VariantIssueOpened{Title: "first"})
	awaitProjection(t, fixture.ctx, backlogReader, readmodels.Key(source), func(m VariantBacklog) bool {
		return m.Title == "before-entry" && !m.Updated.IsZero() && m.Observed.Equal(m.Updated)
	})
	if state, err := prReader.Get(fixture.ctx, readmodels.Key(source)); err != nil || state.Exists {
		t.Fatalf("inactive variant created: %+v %v", state, err)
	}
	appendSuccessfully(t, fixture.ctx, store, source, VariantTitleChanged{Title: "shared"})
	awaitProjection(t, fixture.ctx, backlogReader, readmodels.Key(source), func(m VariantBacklog) bool { return m.Title == "shared" })
	appendSuccessfully(t, fixture.ctx, store, source, VariantPullRequestOpened{URL: "https://example.test/pull/1"})
	awaitProjection(t, fixture.ctx, prReader, readmodels.Key(source), func(m VariantPullRequest) bool { return m.URL != "" })
	awaitProjectionAbsent(t, fixture.ctx, backlogReader, readmodels.Key(source))
	appendSuccessfully(t, fixture.ctx, store, source, VariantTitleChanged{Title: "after-leaving"})
	awaitProjection(t, fixture.ctx, prReader, readmodels.Key(source), func(m VariantPullRequest) bool { return m.Title == "after-leaving" })
	if state, err := backlogReader.Get(fixture.ctx, readmodels.Key(source)); err != nil || state.Exists {
		t.Fatalf("variant resurrected: %+v %v", state, err)
	}
	// Its entering event may deliberately bring it back. The latest shared
	// join still wins over the entering event's local title, exactly as in C#.
	appendSuccessfully(t, fixture.ctx, store, source, VariantIssueOpened{Title: "returned"})
	awaitProjection(t, fixture.ctx, backlogReader, readmodels.Key(source), func(m VariantBacklog) bool { return m.Title == "after-leaving" })
	awaitProjectionAbsent(t, fixture.ctx, prReader, readmodels.Key(source))
}

func awaitProjectionAbsent[M any](t *testing.T, parent context.Context, reader *readmodels.Reader[M], key readmodels.Key) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := reader.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if !state.Exists {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("variant was not removed: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

type ProjectionQueryIssue struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

func TestKernelProjectionQueryAndPreview(t *testing.T) {
	kernelProjectionQuery(t, true)
}

func TestKernelProjectionQueryInfersSchema(t *testing.T) {
	kernelProjectionQuery(t, false)
}

func kernelProjectionQuery(t *testing.T, explicitModel bool) {
	t.Helper()
	fixture := newKernelFixture(t)
	r := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[VariantIssueOpened](r); err != nil {
		t.Fatal(err)
	}
	declaration := "projection PreviewIssues"
	if explicitModel {
		if _, err := chronicle.RegisterReadModel[ProjectionQueryIssue](r, readmodels.WithIdentifier("ProjectionQueryIssue")); err != nil {
			t.Fatal(err)
		}
		declaration += " => ProjectionQueryIssue"
	}
	declaration += "\n  from VariantIssueOpened\n    title = title"
	store, err := fixture.client(r).EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	appendSuccessfully(t, fixture.ctx, store, events.SourceID(uuid.NewString()), VariantIssueOpened{Title: "preview"})
	for _, query := range []func(context.Context, string, ...events.SequenceID) (projections.QueryResult, error){store.QueryProjection, store.PreviewProjection} {
		result, err := query(fixture.ctx, declaration)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.ReadModelEntries) != 1 {
			t.Fatalf("preview: %+v", result)
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(result.ReadModelEntries[0]), &entry); err != nil || entry["title"] != "preview" {
			t.Fatalf("entry: %+v %v; schema: %s", entry, err, result.Schema)
		}
	}
	_, err = store.QueryProjection(fixture.ctx, "not a valid projection declaration")
	var failure *projections.QueryError
	if !errors.As(err, &failure) || len(failure.Errors) == 0 {
		t.Fatalf("query diagnostics: %v", err)
	}
	other, err := store.QueryProjection(fixture.ctx, declaration, "empty-query-sequence")
	if err != nil || len(other.ReadModelEntries) != 0 {
		t.Fatalf("sequence selection: %+v %v", other, err)
	}
}
