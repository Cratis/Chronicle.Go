// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package readmodels provides explicit model declarations and namespace-bound,
// presence-aware reads. Models describe kernel-owned state, not local projections.
package readmodels

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/declarations"
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
	// NoSink selects on-demand projection or in-process reduction.
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
	// Reducer identifies a reducer-backed model. Passive instances fold locally;
	// reducer sessions are not supported.
	Reducer ObserverType = 1
)

// Descriptor is immutable model metadata. Its zero value is invalid.
type Descriptor struct{ definition *definition }
type definition struct {
	typ       reflect.Type
	plan      *serialization.Plan
	origin    *serialization.Plan
	config    modelConfig
	schema    string
	protected map[string]bool
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

// RebindJSON returns a detached serialized snapshot with this descriptor's names.
// The original descriptor must describe the same Go type. Explicit nulls and
// omissions survive; no user callbacks or encryption are invoked.
func (d Descriptor) RebindJSON(data []byte, original Descriptor) ([]byte, error) {
	if d.definition == nil || original.definition == nil {
		return nil, invalid("matching model descriptors required")
	}
	return original.definition.plan.RebindJSON(data, d.definition.plan)
}

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
	codecs             *serialization.Codecs
	identifier         Identifier
	identifierExplicit bool
	containerExplicit  bool
	generation         Generation
	container, display string
	sink               Sink
	sinkExplicit       bool
	observer           ObserverType
	observerExplicit   bool
	passive            bool
	observerID         string
	sequence           events.SequenceID
	sequenceExplicit   bool
	indexes            []string
	pii                []string
	protection         []compliance.Declaration
	subject            string
}

// WithCodecs selects an immutable explicit derived codec set. The last option
// wins; nil clears it. Classification providers are frozen at declaration.
func WithCodecs(codecs *serialization.Codecs) ModelOption {
	if codecs != nil {
		owned := *codecs
		codecs = &owned
	}
	return func(c *modelConfig) { c.codecs = codecs }
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
func WithSink(sink Sink) ModelOption {
	return func(c *modelConfig) { c.sink, c.sinkExplicit = sink, true }
}

// WithObserver associates a registered producer. It does not register that producer.
func WithObserver(kind ObserverType, id string) ModelOption {
	return func(c *modelConfig) { c.observer, c.observerID, c.observerExplicit = kind, id, true }
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

// WithPII marks properties as personal data in the model schema.
// Paths use serialized names and accumulate across calls. Duplicate paths within or
// across calls fail declaration with ErrInvalidConfiguration.
// Composite values propagate to leaves; explicitly classified collections retain
// their container classification. Tags and WithProtection use the same pipeline.
func WithPII(paths ...string) ModelOption {
	copy := append([]string(nil), paths...)
	return func(c *modelConfig) { c.pii = append(c.pii, copy...) }
}

// WithProtection adds copied type/property classifications and providers. Tags
// need no option. Invalid or conflicting metadata fails before any connection I/O.
func WithProtection(declarations ...compliance.Declaration) ModelOption {
	owned := append([]compliance.Declaration(nil), declarations...)
	return func(c *modelConfig) { c.protection = append(c.protection, owned...) }
}

// WithSubjectProperty selects a top-level serialized scalar property for Release.
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
	config, err := configureModel(typ, options)
	if err != nil {
		return Model[T]{}, err
	}
	definition, err := compileModelDefinition(typ, config)
	if err != nil {
		return Model[T]{}, err
	}
	return Model[T]{descriptor: Descriptor{definition: definition}}, nil
}

func configureModel(typ reflect.Type, options []ModelOption) (modelConfig, error) {
	config := modelConfig{identifier: Identifier(typ.PkgPath() + "." + typ.Name()), generation: 1,
		container: pluralize(typ.Name()), display: typ.Name(), sink: Sink{Type: MongoDB}, observer: Projection, sequence: events.EventLog}
	for _, option := range options {
		if option == nil {
			return modelConfig{}, invalid("nil model option")
		}
		option(&config)
	}
	if err := config.validateDeclaration(typ); err != nil {
		return modelConfig{}, err
	}
	if err := config.normalizeSink(); err != nil {
		return modelConfig{}, err
	}
	return config, nil
}

func (config modelConfig) validateDeclaration(typ reflect.Type) error {
	if config.needsExplicitIdentity(typ) {
		return invalid("generic models require an explicit identifier and container name")
	}
	if !config.validMetadata() {
		return invalid("nonblank model identity, container, display, sequence and positive generation required")
	}
	if !config.validObserver() {
		return invalid("unknown observer type")
	}
	return nil
}

func (config modelConfig) needsExplicitIdentity(typ reflect.Type) bool {
	return strings.Contains(typ.Name(), "[") && (!config.identifierExplicit || !config.containerExplicit)
}

func (config modelConfig) validMetadata() bool {
	return strings.TrimSpace(string(config.identifier)) != "" && strings.TrimSpace(config.container) != "" && strings.TrimSpace(config.display) != "" && config.generation != 0 && strings.TrimSpace(string(config.sequence)) != ""
}

func (config modelConfig) validObserver() bool {
	return config.observer == Projection || config.observer == Reducer
}

func (config *modelConfig) normalizeSink() error {
	if !supportedSink(config.sink.Type) {
		return fmt.Errorf("%w: unknown sink type", faults.ErrUnsupported)
	}
	if config.sink.ConfigurationID == "" {
		config.sink.ConfigurationID = uuid.Nil.String()
	}
	if !canonicalSinkConfiguration(config.sink.ConfigurationID) {
		return invalid("sink configuration requires a canonical UUID")
	}
	if config.needsObserverIdentifier() {
		return invalid("passive models require a nonblank observer identifier")
	}
	return nil
}

func supportedSink(kind SinkType) bool {
	switch kind {
	case MongoDB, SQL, InMemory, NoSink:
		return true
	default:
		return false
	}
}

func canonicalSinkConfiguration(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id.String() == value
}

func (config modelConfig) needsObserverIdentifier() bool {
	return config.sink.Type == NoSink && strings.TrimSpace(config.observerID) == "" && !config.passive
}

func compileModelDefinition(typ reflect.Type, config modelConfig) (*definition, error) {
	plan, err := serialization.CompileReadModelWith(typ, serialization.Config{Codecs: config.codecs})
	if err != nil {
		return nil, err
	}
	if err := plan.ValidateRole(declarations.Model); err != nil {
		return nil, err
	}
	if err := collectIndexes(plan, &config, typ); err != nil {
		return nil, err
	}
	if err := collectSubject(plan, &config); err != nil {
		return nil, err
	}
	config.protection, err = plan.FreezeProtection(config.protection...)
	if err != nil {
		return nil, err
	}
	schema, err := modelSchema(plan, config)
	if err != nil {
		return nil, err
	}
	protected, err := serialization.ProtectionRoots(schema)
	if err != nil {
		return nil, err
	}
	return &definition{typ: typ, plan: plan, origin: plan, config: config, schema: schema, protected: protected}, nil
}

func invalid(message string) error {
	return fmt.Errorf("%w: %s", faults.ErrInvalidConfiguration, message)
}
