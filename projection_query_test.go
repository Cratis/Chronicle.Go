// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/projections"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestProjectionQueryPreviewTransportAndFailureBoundaries(t *testing.T) {
	kernel := &supervisedKernel{projections: &projectionKernel{preview: func(ctx context.Context, r *contracts.PreviewProjectionRequest) (*contracts.OneOf_ProjectionPreview_ProjectionDeclarationParsingErrors, error) {
		if r.EventStore != "store" || r.Namespace != "tenant" {
			t.Error("query coordinates lost")
		}
		switch r.Declaration {
		case "query":
			if r.EventSequenceId != "event-log" {
				t.Error("missing default sequence")
			}
			return &contracts.OneOf_ProjectionPreview_ProjectionDeclarationParsingErrors{Value0: &contracts.ProjectionPreview{ReadModelEntries: []string{`{"name":"Ada"}`}, ReadModel: &contracts.ReadModelDefinition{Schema: `{"type":"object"}`}}}, nil
		case "invalid":
			return &contracts.OneOf_ProjectionPreview_ProjectionDeclarationParsingErrors{Value1: &contracts.ProjectionDeclarationParsingErrors{Errors: []*contracts.ProjectionDeclarationSyntaxError{{Message: "private declaration", Line: 2, Column: 3}}}}, nil
		case "both":
			return &contracts.OneOf_ProjectionPreview_ProjectionDeclarationParsingErrors{Value0: &contracts.ProjectionPreview{}, Value1: &contracts.ProjectionDeclarationParsingErrors{}}, nil
		case "empty":
			return &contracts.OneOf_ProjectionPreview_ProjectionDeclarationParsingErrors{}, nil
		case "bad-json":
			return &contracts.OneOf_ProjectionPreview_ProjectionDeclarationParsingErrors{Value0: &contracts.ProjectionPreview{ReadModelEntries: []string{"broken"}}}, nil
		case "unsupported":
			return nil, status.Error(codes.Unimplemented, "old kernel")
		case "cancel":
			<-ctx.Done()
			return nil, ctx.Err()
		default:
			if r.EventSequenceId != "custom" {
				t.Error("explicit sequence lost")
			}
			return &contracts.OneOf_ProjectionPreview_ProjectionDeclarationParsingErrors{Value0: &contracts.ProjectionPreview{}}, nil
		}
	}}}
	client, ctx := supervisionClient(t, kernel)
	store, err := client.EventStore(ctx, "store", WithNamespace("tenant"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.QueryProjection(ctx, "query")
	if err != nil || len(result.ReadModelEntries) != 1 || result.Schema == "" {
		t.Fatalf("query: %+v %v", result, err)
	}
	if _, err := store.PreviewProjection(ctx, "preview", "custom"); err != nil {
		t.Fatal(err)
	}
	for _, declaration := range []string{"both", "empty", "bad-json"} {
		if _, err := store.QueryProjection(ctx, declaration); !errors.Is(err, ErrProtocol) {
			t.Fatalf("%s: %v", declaration, err)
		}
	}
	_, err = store.QueryProjection(ctx, "invalid")
	var failure *projections.QueryError
	if !errors.As(err, &failure) || !errors.Is(err, ErrInvalidConfiguration) || failure.Errors[0].Line != 2 || failure.Errors[0].Column != 3 || err.Error() == "private declaration" {
		t.Fatalf("parsing: %v", err)
	}
	if _, err := store.QueryProjection(ctx, "unsupported"); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.QueryProjection(canceled, "cancel"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := store.QueryProjection(ctx, "query", ""); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal(err)
	}
}
