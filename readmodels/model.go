// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package readmodels provides explicit model declarations and namespace-bound,
// presence-aware reads. Models describe kernel-owned state, not local projections.
package readmodels

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
	"github.com/google/uuid"
)

// Identifier is the persisted, store-wide model identity, independent of its sink container.
type Identifier string

// Key identifies a model instance. Values are preserved verbatim.
type Key string

// Generation identifies a schema generation, starting at one.
type Generation uint32

// SinkType identifies a kernel sink provider.
type SinkType string

const (
	// MongoDB is the default sink.
	MongoDB SinkType = "MongoDB"
	// SQL selects the kernel's SQL sink.
	SQL SinkType = "SQL"
	// InMemory selects the kernel's in-memory sink.
	InMemory SinkType = "InMemory"
	// NoSink selects immediate projection rather than materialized state.
	NoSink SinkType = "None"
)

// Sink selects kernel-owned storage. ConfigurationID is a canonical UUID string;
// empty means the all-zero default configuration. No database driver is required.
type Sink struct {
	// Type selects one of the supported kernel sink providers.
	Type SinkType
	// ConfigurationID selects a server-managed connection configuration.
	ConfigurationID string
}

// ObserverType identifies the definition that produces this model.
type ObserverType uint8

const (
	// Projection is the C# default observer type.
	Projection ObserverType = 2
	// Reducer identifies a reducer-backed materialized model. In-process folds and
	// reducer sessions are not supported by this package.
	Reducer ObserverType = 1
)

// Descriptor is immutable model metadata. Its zero value is invalid.
type Descriptor struct{ definition *definition }
type definition struct {
	typ    reflect.Type
	plan   *serialization.Plan
	origin *serialization.Plan
	config modelConfig
	schema string
}

// Identifier returns the stable persisted identity.
func (d Descriptor) Identifier() Identifier { return d.definition.config.identifier }

// Generation returns the schema generation.
func (d Descriptor) Generation() Generation { return d.definition.config.generation }

// GoType returns the declared, non-pointer struct type.
func (d Descriptor) GoType() reflect.Type {
	if d.definition == nil {
		return nil
	}
	return d.definition.typ
}

// ContainerName returns the sink container, not the model identifier.
func (d Descriptor) ContainerName() string { return d.definition.config.container }

// DisplayName returns the human-facing name.
func (d Descriptor) DisplayName() string { return d.definition.config.display }

// Schema returns the shared serialization plan's schema with explicit read-model metadata.
func (d Descriptor) Schema() string { return d.definition.schema }

// Sink returns a copy of the sink selection.
func (d Descriptor) Sink() Sink { return d.definition.config.sink }

// Indexes returns copied serialized property paths, in declaration order.
func (d Descriptor) Indexes() []string { return append([]string(nil), d.definition.config.indexes...) }

// Observer returns the producer kind and identifier. An empty identifier means no producer was declared.
func (d Descriptor) Observer() (ObserverType, string) {
	return d.definition.config.observer, d.definition.config.observerID
}

// EventSequence returns the sequence used by immediate reads and session cleanup.
func (d Descriptor) EventSequence() events.SequenceID { return d.definition.config.sequence }

// Marshal serializes a value or non-nil pointer of this model's type. The caller
// must not mutate it during the call. This does not apply encryption.
func (d Descriptor) Marshal(value any) ([]byte, error) { return d.definition.plan.Marshal(value) }

// Model is a typed, immutable declaration, normally returned by chronicle.RegisterReadModel.
type Model[T any] struct{ descriptor Descriptor }

// Descriptor returns the immutable untyped model metadata.
func (m Model[T]) Descriptor() Descriptor { return m.descriptor }

// Identifier returns the persisted model identity.
func (m Model[T]) Identifier() Identifier { return m.descriptor.Identifier() }

// ModelOption configures a declaration. Scalars are last-wins; property path options
// accumulate and reject duplicates. Slices are copied.
// Nil options and invalid final configurations fail before registry admission.
type ModelOption func(*modelConfig)
type modelConfig struct {
	identifier         Identifier
	identifierExplicit bool
	containerExplicit  bool
	generation         Generation
	container, display string
	sink               Sink
	observer           ObserverType
	observerID         string
	sequence           events.SequenceID
	sequenceExplicit   bool
	indexes            []string
	pii                []string
	subject            string
}

// WithIdentifier overrides the default full Go import path plus type name. Use an
// explicit C# full name to share a persisted model across languages.
func WithIdentifier(id Identifier) ModelOption {
	return func(c *modelConfig) { c.identifier, c.identifierExplicit = id, true }
}

// WithGeneration selects a positive schema generation (default one).
func WithGeneration(generation Generation) ModelOption {
	return func(c *modelConfig) { c.generation = generation }
}

// WithContainerName overrides the case-preserving pluralized simple type name,
// corresponding to C# ReadModelNameAttribute. It does not change the identifier.
func WithContainerName(name string) ModelOption {
	return func(c *modelConfig) { c.container, c.containerExplicit = name, true }
}

// WithDisplayName overrides the simple type name used for display.
func WithDisplayName(name string) ModelOption { return func(c *modelConfig) { c.display = name } }

// WithSink replaces the default MongoDB/default-configuration sink.
func WithSink(sink Sink) ModelOption { return func(c *modelConfig) { c.sink = sink } }

// WithObserver associates a registered producer. It does not register that producer.
func WithObserver(kind ObserverType, id string) ModelOption {
	return func(c *modelConfig) { c.observer, c.observerID = kind, id }
}

// WithEventSequence selects the sequence for immediate reads (default event-log).
func WithEventSequence(sequence events.SequenceID) ModelOption {
	return func(c *modelConfig) { c.sequence, c.sequenceExplicit = sequence, true }
}

// WithIndexes declares nested serialized property paths, including paths through
// collection items. Calls accumulate paths in declaration order. Unknown paths and
// duplicates within or across calls fail declaration with ErrInvalidConfiguration.
func WithIndexes(paths ...string) ModelOption {
	copy := append([]string(nil), paths...)
	return func(c *modelConfig) { c.indexes = append(c.indexes, copy...) }
}

// WithPII marks scalar string properties as personal data in the model schema.
// Paths use serialized names and accumulate across calls. Duplicate paths within or
// across calls fail declaration with ErrInvalidConfiguration.
// Container/type-wide PII and encryption classifications
// remain unsupported; the shared serializer rejects unsupported chronicle tags.
func WithPII(paths ...string) ModelOption {
	copy := append([]string(nil), paths...)
	return func(c *modelConfig) { c.pii = append(c.pii, copy...) }
}

// WithSubjectProperty selects a top-level serialized string property for Release.
// Empty/unset values fall back to the model's ID field, as in C#.
func WithSubjectProperty(path string) ModelOption { return func(c *modelConfig) { c.subject = path } }

// Define compiles T without registration or I/O. T must be a named, non-pointer
// struct accepted by serialization.Compile. Generic instantiations require explicit
// WithIdentifier and WithContainerName options. Most callers use RegisterReadModel.
func Define[T any](options ...ModelOption) (Model[T], error) {
	typ := reflect.TypeFor[T]()
	if typ.Kind() != reflect.Struct || typ.Name() == "" {
		return Model[T]{}, invalid("model must be a named non-pointer struct")
	}
	config := modelConfig{identifier: Identifier(typ.PkgPath() + "." + typ.Name()), generation: 1,
		container: pluralize(typ.Name()), display: typ.Name(), sink: Sink{Type: MongoDB}, observer: Projection, sequence: events.EventLog}
	for _, option := range options {
		if option == nil {
			return Model[T]{}, invalid("nil model option")
		}
		option(&config)
	}
	if strings.Contains(typ.Name(), "[") && (!config.identifierExplicit || !config.containerExplicit) {
		return Model[T]{}, invalid("generic models require an explicit identifier and container name")
	}
	if strings.TrimSpace(string(config.identifier)) == "" || strings.TrimSpace(config.container) == "" || strings.TrimSpace(config.display) == "" || config.generation == 0 || strings.TrimSpace(string(config.sequence)) == "" {
		return Model[T]{}, invalid("nonblank model identity, container, display, sequence and positive generation required")
	}
	if config.observer != Projection && config.observer != Reducer {
		return Model[T]{}, invalid("unknown observer type")
	}
	switch config.sink.Type {
	case MongoDB, SQL, InMemory, NoSink:
	default:
		return Model[T]{}, fmt.Errorf("%w: unknown sink type", faults.ErrUnsupported)
	}
	if config.sink.ConfigurationID == "" {
		config.sink.ConfigurationID = uuid.Nil.String()
	}
	id, err := uuid.Parse(config.sink.ConfigurationID)
	if err != nil || id.String() != config.sink.ConfigurationID {
		return Model[T]{}, invalid("sink configuration requires a canonical UUID")
	}
	if config.sink.Type == NoSink && strings.TrimSpace(config.observerID) == "" {
		return Model[T]{}, invalid("passive models require a nonblank observer identifier")
	}
	if config.observer == Reducer && config.sink.Type == NoSink {
		return Model[T]{}, fmt.Errorf("%w: passive reducers require an in-process fold", faults.ErrUnsupported)
	}
	plan, err := serialization.CompileReadModel(typ)
	if err != nil {
		return Model[T]{}, err
	}
	schema, err := modelSchema(plan.Schema(), config)
	if err != nil {
		return Model[T]{}, err
	}
	descriptor := Descriptor{definition: &definition{typ: typ, plan: plan, origin: plan, config: config, schema: schema}}
	for _, path := range config.pii {
		if path == idProperty(descriptor) {
			return Model[T]{}, invalid("PII cannot protect the model key")
		}
	}
	return Model[T]{descriptor: descriptor}, nil
}

func invalid(message string) error {
	return fmt.Errorf("%w: %s", faults.ErrInvalidConfiguration, message)
}
