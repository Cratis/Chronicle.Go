// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package seeding declares global and namespace-specific event seeds. The kernel,
// not this package, tracks which entries have already been appended.
package seeding

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/contracts/seeding"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/protobuf/proto"
)

// Seeder declares seed data during client preparation, before connection work.
// Seed must be synchronous, perform no I/O, and not retain the builder. Returning
// an error aborts preparation; no subset is registered. Separate clients may
// invoke the same seeder concurrently.
type Seeder interface {
	Seed(*Builder) error
}

// Func adapts a plain function to Seeder. Captured dependencies remain borrowed.
type Func func(*Builder) error

// Seed invokes f. A nil function returns ErrInvalidConfiguration.
func (f Func) Seed(builder *Builder) error {
	if f == nil {
		return fmt.Errorf("%w: nil seeder function", faults.ErrInvalidConfiguration)
	}
	return f(builder)
}

// Builder collects declarations against a frozen event catalog. Only Prepare
// constructs usable builders. Builders and their scopes are synchronous and must
// not be shared across goroutines or retained after Seed returns. Errors from
// fluent calls accumulate and are returned by Prepare, even if ignored by Seed.
type Builder struct {
	state     *builderState
	namespace metadata.Namespace
	scoped    bool
}

type builderState struct {
	catalog *events.Catalog
	request *seeding.SeedEventsRequest
	err     error
	sealed  bool
}

// For adds typed events for source in this builder's scope. The root scope is
// global, including future namespaces; it is not the client's default namespace.
// Empty collections still validate E against the catalog. Values are serialized
// immediately, so later mutation cannot alter the prepared snapshot.
func For[E any](builder *Builder, source events.SourceID, values ...E) *Builder {
	if builder == nil || builder.state == nil {
		return builder
	}
	typ := reflect.TypeFor[E]()
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	found := false
	for _, descriptor := range builder.state.catalog.Descriptors() {
		if descriptor.GoType() == typ {
			found = true
			break
		}
	}
	if !found {
		builder.fail(fmt.Errorf("%w: seed event type %s", faults.ErrNotRegistered, typ))
		return builder
	}
	if strings.TrimSpace(string(source)) == "" {
		builder.fail(fmt.Errorf("%w: blank seed source", faults.ErrInvalidConfiguration))
		return builder
	}
	if err := compliance.ValidateSubject(string(source)); err != nil {
		builder.fail(err)
		return builder
	}
	for _, value := range values {
		builder.ForEventSource(source, value)
	}
	return builder
}

// ForNamespace returns a new scoped view without changing the receiver. An
// explicit default namespace is local; only the unscoped root is global.
func (b *Builder) ForNamespace(namespace metadata.Namespace) *Builder {
	if b == nil || b.state == nil {
		return b
	}
	if strings.TrimSpace(string(namespace)) == "" {
		b.fail(fmt.Errorf("%w: blank seed namespace", faults.ErrInvalidConfiguration))
	}
	return &Builder{state: b.state, namespace: namespace, scoped: true}
}

// ForEventSource adds heterogeneous registered event values, in input order.
// Non-nil pointers are supported. Payloads and static tags use the client's
// event descriptors and naming policy; event-source IDs are preserved verbatim.
func (b *Builder) ForEventSource(source events.SourceID, values ...any) *Builder {
	if b == nil || b.state == nil || b.state.sealed || b.state.err != nil {
		return b
	}
	if strings.TrimSpace(string(source)) == "" {
		b.fail(fmt.Errorf("%w: blank seed source", faults.ErrInvalidConfiguration))
		return b
	}
	// The seed wire has no separate subject; the kernel uses the source ID.
	if err := compliance.ValidateSubject(string(source)); err != nil {
		b.fail(err)
		return b
	}
	for _, value := range values {
		descriptor, ok := b.state.catalog.Lookup(value)
		if !ok {
			b.fail(fmt.Errorf("%w: seed event %T", faults.ErrNotRegistered, value))
			return b
		}
		content, err := descriptor.Marshal(value)
		if err != nil {
			b.fail(fmt.Errorf("serialize seed %s: %w", descriptor.Ref().ID, err))
			return b
		}
		entry := &seeding.SeedingEntry{EventSourceId: string(source), EventTypeId: string(descriptor.Ref().ID), Content: string(content)}
		for _, tag := range descriptor.Tags() {
			entry.Tags = append(entry.Tags, string(tag))
		}
		request := b.state.request
		if !b.scoped {
			addEntry(&request.GlobalByEventType, &request.GlobalByEventSource, entry)
			continue
		}
		var group *seeding.NamespacedSeedEntries
		for _, candidate := range request.NamespacedEntries {
			if candidate.Namespace == string(b.namespace) {
				group = candidate
				break
			}
		}
		if group == nil {
			group = &seeding.NamespacedSeedEntries{Namespace: string(b.namespace)}
			request.NamespacedEntries = append(request.NamespacedEntries, group)
		}
		addEntry(&group.ByEventType, &group.ByEventSource, entry)
	}
	return b
}

func (b *Builder) fail(err error) {
	if !b.state.sealed && b.state.err == nil {
		b.state.err = err
	}
}

// Definition is an immutable serialized seed batch, safe for concurrent use.
// Its zero value is an empty batch. Prepare is the declaration boundary; no
// callback or caller-owned event remains in the resulting definition.
type Definition struct{ request *seeding.SeedEventsRequest }

// IsEmpty reports whether the batch contains any entries.
func (d Definition) IsEmpty() bool {
	return d.request == nil || len(d.request.GlobalByEventType)+len(d.request.NamespacedEntries) == 0
}

// Contract returns an independent kernel request bound to store. It preserves
// both C# groupings and their first-seen ordering; callers own the returned data.
func (d Definition) Contract(store metadata.StoreName) *seeding.SeedEventsRequest {
	request := &seeding.SeedEventsRequest{}
	if d.request != nil {
		request = proto.Clone(d.request).(*seeding.SeedEventsRequest)
	}
	request.EventStore = string(store)
	return request
}

// Prepare runs seeders once, in order, and snapshots their complete batch. It
// performs no I/O. Invalid declarations, serialization errors, callback errors
// and panics fail the entire batch. Catalog must be non-nil. Nil seeders fail.
func Prepare(catalog *events.Catalog, seeders ...Seeder) (definition Definition, err error) {
	if catalog == nil {
		return Definition{}, fmt.Errorf("%w: nil seed catalog", faults.ErrInvalidConfiguration)
	}
	state := &builderState{catalog: catalog, request: &seeding.SeedEventsRequest{}}
	defer func() {
		state.sealed = true
		if recover() != nil {
			definition = Definition{}
			err = fmt.Errorf("%w: seeder panicked", faults.ErrInvalidConfiguration)
		}
	}()
	builder := &Builder{state: state}
	for _, seeder := range seeders {
		if seeder == nil || (reflect.ValueOf(seeder).Kind() == reflect.Pointer && reflect.ValueOf(seeder).IsNil()) {
			return Definition{}, fmt.Errorf("%w: nil seeder", faults.ErrInvalidConfiguration)
		}
		if err := seeder.Seed(builder); err != nil {
			return Definition{}, err
		}
		if state.err != nil {
			return Definition{}, state.err
		}
	}
	return Definition{request: state.request}, nil
}

func addEntry(byType *[]*seeding.EventTypeSeedEntries, bySource *[]*seeding.EventSourceSeedEntries, entry *seeding.SeedingEntry) {
	var typeGroup *seeding.EventTypeSeedEntries
	for _, group := range *byType {
		if group.EventTypeId == entry.EventTypeId {
			typeGroup = group
			break
		}
	}
	if typeGroup == nil {
		typeGroup = &seeding.EventTypeSeedEntries{EventTypeId: entry.EventTypeId}
		*byType = append(*byType, typeGroup)
	}
	typeGroup.Entries = append(typeGroup.Entries, entry)
	var sourceGroup *seeding.EventSourceSeedEntries
	for _, group := range *bySource {
		if group.EventSourceId == entry.EventSourceId {
			sourceGroup = group
			break
		}
	}
	if sourceGroup == nil {
		sourceGroup = &seeding.EventSourceSeedEntries{EventSourceId: entry.EventSourceId}
		*bySource = append(*bySource, sourceGroup)
	}
	sourceGroup.Entries = append(sourceGroup.Entries, entry)
}
