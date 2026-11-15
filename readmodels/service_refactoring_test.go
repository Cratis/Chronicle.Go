// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc"
)

func TestServiceConstructorRejectsMissingInputs(t *testing.T) {
	catalog, err := readmodels.NewCatalog(person(t).Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	var nilConn *grpc.ClientConn
	for _, tc := range []struct {
		name      string
		store     metadata.StoreName
		namespace metadata.Namespace
		catalog   *readmodels.Catalog
		conn      grpc.ClientConnInterface
	}{
		{"blank store", " ", "tenant", catalog, collectionExampleTransport{}},
		{"blank namespace", "store", "\t", catalog, collectionExampleTransport{}},
		{"nil catalog", "store", "tenant", nil, collectionExampleTransport{}},
		{"nil transport", "store", "tenant", catalog, nil},
		{"typed nil transport", "store", "tenant", catalog, nilConn},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, err := readmodels.New(tc.store, tc.namespace, tc.catalog, tc.conn)
			if service != nil || !errors.Is(err, chronicle.ErrInvalidConfiguration) {
				t.Fatalf("New = %v, %v; want nil, ErrInvalidConfiguration", service, err)
			}
		})
	}
}

func TestServiceOptionsStopAtFirstFailure(t *testing.T) {
	catalog, err := readmodels.NewCatalog(person(t).Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("option failure")
	for _, tc := range []struct {
		name   string
		option readmodels.ServiceOption
		want   error
	}{
		{"nil option", nil, chronicle.ErrInvalidConfiguration},
		{"failed option", func(*readmodels.Service) error { return failure }, failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, err := readmodels.New("store", "tenant", catalog, collectionExampleTransport{}, tc.option, func(*readmodels.Service) error {
				t.Error("option after failure executed")
				return nil
			})
			if service != nil || !errors.Is(err, tc.want) {
				t.Fatalf("New = %v, %v; want nil, %v", service, err, tc.want)
			}
		})
	}
}

func TestPassiveInstanceReadsPreserveReleaseAndFailureBoundaries(t *testing.T) {
	model := person(t, readmodels.WithObserver(readmodels.Reducer, "reducer"), readmodels.WithSink(readmodels.Sink{Type: readmodels.NoSink}))
	failure := errors.New("passive failure")
	position := events.SequenceNumber(17)
	for _, tc := range []struct {
		name    string
		install func(readmodels.PassiveReader) readmodels.ServiceOption
		result  readmodels.Instance[json.RawMessage]
		err     error
		want    error
	}{
		{"raw present", readmodels.WithPassiveReader, readmodels.Instance[json.RawMessage]{Exists: true, Value: json.RawMessage(`{"count":3}`), LastHandled: &position}, nil, nil},
		{"released present", readmodels.WithReleasedPassiveReader, readmodels.Instance[json.RawMessage]{Exists: true, Value: json.RawMessage(`{"count":3}`), LastHandled: &position}, nil, nil},
		{"absent", readmodels.WithPassiveReader, readmodels.Instance[json.RawMessage]{LastHandled: &position}, nil, nil},
		{"failed", readmodels.WithPassiveReader, readmodels.Instance[json.RawMessage]{Exists: true, LastHandled: &position}, failure, failure},
		{"invalid raw", readmodels.WithPassiveReader, readmodels.Instance[json.RawMessage]{Exists: true, Value: json.RawMessage(`[]`)}, nil, chronicle.ErrInvalidConfiguration},
		{"invalid released", readmodels.WithReleasedPassiveReader, readmodels.Instance[json.RawMessage]{Exists: true, Value: json.RawMessage(`[]`)}, nil, chronicle.ErrProtocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			callback := func(_ context.Context, d readmodels.Descriptor, key readmodels.Key) (readmodels.Instance[json.RawMessage], error) {
				if d.Identifier() != model.Identifier() || key != "key" {
					t.Error("passive read coordinates changed")
				}
				return tc.result, tc.err
			}
			service, ctx := serviceFixture(t, &modelKernel{options: []readmodels.ServiceOption{tc.install(callback)}}, model.Descriptor())
			got, err := service.Get(ctx, model.Identifier(), "key")
			assertPassiveReadResult(t, got, err, tc.want)
			if tc.want != nil {
				return
			}
			if !reflect.DeepEqual(got, tc.result) {
				t.Fatalf("passive result = %+v, want %+v", got, tc.result)
			}
		})
	}
}

func assertPassiveReadResult(t *testing.T, got readmodels.Instance[json.RawMessage], err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("passive read error = %v, want %v", err, want)
	}
	if want == nil {
		return
	}
	if !reflect.DeepEqual(got, readmodels.Instance[json.RawMessage]{}) {
		t.Fatalf("failed passive read published %+v", got)
	}
}

func TestPassiveInstanceRequiresReader(t *testing.T) {
	model := person(t, readmodels.WithObserver(readmodels.Reducer, "reducer"), readmodels.WithSink(readmodels.Sink{Type: readmodels.NoSink}))
	service, ctx := serviceFixture(t, &modelKernel{}, model.Descriptor())
	got, err := service.Get(ctx, model.Identifier(), "key")
	assertPassiveReadResult(t, got, err, chronicle.ErrUnsupported)
}
