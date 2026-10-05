// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package webhooks declares kernel-owned outgoing HTTP event webhooks. It never
// makes HTTP requests or logs targets, headers or authorization credentials.
package webhooks

import (
	"context"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"

	contracts "github.com/cratis/chronicle.go/contracts/observation/webhooks"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// ID is a stable webhook identity within a store.
type ID string

// Definition is an immutable webhook. Define supplies C# defaults; zero is invalid.
// Formatting is redacted, including %#v. KernelDefinition explicitly exposes secrets.
type Definition struct{ value *contracts.WebhookDefinition }

// Option configures a definition. Scalar options are last-wins; event filters
// accumulate as a set. Options are reusable; nil options fail definition creation.
type Option func(*configuration)
type configuration struct {
	value *contracts.WebhookDefinition
	types []events.TypeRef
}

// WithEventTypes adds exact generation filters. No filters selects all current catalog types.
func WithEventTypes(types ...events.TypeRef) Option {
	copy := slices.Clone(types)
	return func(c *configuration) { c.types = append(c.types, copy...) }
}

// WithEventSequence overrides the default event-log.
func WithEventSequence(sequence events.SequenceID) Option {
	return func(c *configuration) { c.value.EventSequenceId = string(sequence) }
}

// WithReplayable sets replay eligibility (default true).
func WithReplayable(value bool) Option {
	return func(c *configuration) { c.value.IsReplayable = value }
}

// WithActive sets delivery activity (default true).
func WithActive(value bool) Option { return func(c *configuration) { c.value.IsActive = value } }

// WithHeader sets one header, replacing the same exact key. Values are never logged.
func WithHeader(key, value string) Option {
	return func(c *configuration) { c.value.Target.Headers[key] = value }
}

// WithBasicAuth replaces authorization with HTTP Basic credentials.
func WithBasicAuth(username, password string) Option {
	return func(c *configuration) {
		c.value.Target.Authorization = &contracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization{Value0: &contracts.BasicAuthorization{Username: username, Password: password}}
	}
}

// WithBearerToken replaces authorization with the supplied bearer token.
func WithBearerToken(token string) Option {
	return func(c *configuration) {
		c.value.Target.Authorization = &contracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization{Value1: &contracts.BearerTokenAuthorization{Token: token}}
	}
}

// WithOAuth replaces authorization with client-credentials OAuth. Acquisition is kernel-owned.
func WithOAuth(authority, clientID, clientSecret string) Option {
	return func(c *configuration) {
		c.value.Target.Authorization = &contracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization{Value2: &contracts.OAuthAuthorization{Authority: authority, ClientId: clientID, ClientSecret: clientSecret}}
	}
}

// Define validates and snapshots a webhook. It preserves exact registered event
// generations and targets. URLs must be absolute HTTP(S), without userinfo;
// credentials belong in authorization options. It performs no I/O.
func Define(catalog *events.Catalog, id ID, target string, options ...Option) (Definition, error) {
	if catalog == nil || strings.TrimSpace(string(id)) == "" {
		return Definition{}, invalid("catalog and webhook ID required")
	}
	u, err := url.Parse(target)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil {
		return Definition{}, invalid("absolute HTTP(S) target without userinfo required")
	}
	c := configuration{value: &contracts.WebhookDefinition{Identifier: string(id), EventSequenceId: string(events.EventLog), IsReplayable: true, IsActive: true, Target: &contracts.WebhookTarget{Url: target, Headers: map[string]string{}}}}
	for _, option := range options {
		if option == nil {
			return Definition{}, invalid("nil webhook option")
		}
		option(&c)
	}
	if strings.TrimSpace(c.value.EventSequenceId) == "" {
		return Definition{}, invalid("sequence required")
	}
	for key, value := range c.value.Target.Headers {
		if strings.TrimSpace(key) == "" || strings.ContainsAny(key+value, "\r\n") {
			return Definition{}, invalid("invalid HTTP header")
		}
	}
	if len(c.types) == 0 {
		for _, d := range catalog.Descriptors() {
			if !d.IsHistorical() {
				c.types = append(c.types, d.Ref())
			}
		}
	}
	seen := map[events.TypeRef]bool{}
	for _, ref := range c.types {
		if strings.TrimSpace(string(ref.ID)) == "" || ref.Generation == 0 {
			return Definition{}, invalid("event ID and positive generation required")
		}
		if !seen[ref] {
			c.value.EventTypes = append(c.value.EventTypes, &contracts.EventType{Id: string(ref.ID), Generation: uint32(ref.Generation)})
			seen[ref] = true
		}
	}
	return Definition{c.value}, nil
}

// Identifier returns the persisted identity.
func (d Definition) Identifier() ID {
	if d.value == nil {
		return ""
	}
	return ID(d.value.Identifier)
}

// EventSequence returns the selected sequence.
func (d Definition) EventSequence() events.SequenceID {
	if d.value == nil {
		return ""
	}
	return events.SequenceID(d.value.EventSequenceId)
}

// EventTypes returns detached exact generation filters.
func (d Definition) EventTypes() []events.TypeRef {
	if d.value == nil {
		return nil
	}
	result := make([]events.TypeRef, 0, len(d.value.EventTypes))
	for _, event := range d.value.EventTypes {
		result = append(result, events.TypeRef{ID: events.TypeID(event.Id), Generation: events.Generation(event.Generation)})
	}
	return result
}

// TargetURL returns the target URL. It can contain sensitive query parameters.
func (d Definition) TargetURL() string {
	if d.value == nil {
		return ""
	}
	return d.value.Target.Url
}

// Headers returns a detached map that may contain secrets. Do not log it.
func (d Definition) Headers() map[string]string {
	if d.value == nil {
		return nil
	}
	return maps.Clone(d.value.Target.Headers)
}

// IsActive reports whether delivery is enabled.
func (d Definition) IsActive() bool { return d.value != nil && d.value.IsActive }

// IsReplayable reports whether the kernel may replay this webhook.
func (d Definition) IsReplayable() bool { return d.value != nil && d.value.IsReplayable }

// KernelDefinition returns a detached wire definition, including secret values.
// Do not log it. Mutating it never changes the definition.
func (d Definition) KernelDefinition() *contracts.WebhookDefinition {
	if d.value == nil {
		return nil
	}
	return proto.Clone(d.value).(*contracts.WebhookDefinition)
}

// registrationDefinition preserves protobuf-net's DefaultValue(true) semantics.
// Proto3 omits false; C# initializes absent fields to true. Emit explicit zero
// occurrences on the fresh outgoing snapshot only, never on a mutable public copy.
// See Cratis/Chronicle#4394. The authoritative schema remains unchanged.
func (d Definition) registrationDefinition() *contracts.WebhookDefinition {
	value := d.KernelDefinition()
	var explicit []byte
	if !value.IsReplayable {
		explicit = protowire.AppendVarint(protowire.AppendTag(explicit, 5, protowire.VarintType), 0)
	}
	if !value.IsActive {
		explicit = protowire.AppendVarint(protowire.AppendTag(explicit, 6, protowire.VarintType), 0)
	}
	value.ProtoReflect().SetUnknown(explicit)
	return value
}

// Format redacts all configuration for every fmt verb, including %#v.
func (d Definition) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("webhooks.Definition{redacted}"))
}

// Service is a concurrent-safe, store-wide service borrowing its connection and
// catalog. Contexts control each call; writes are never retried by this service.
type Service struct {
	store   metadata.StoreName
	catalog *events.Catalog
	client  contracts.WebhooksClient
}

// New constructs a service without I/O. Namespace selection does not scope definitions.
func New(store metadata.StoreName, catalog *events.Catalog, conn grpc.ClientConnInterface) (*Service, error) {
	if strings.TrimSpace(string(store)) == "" || catalog == nil || conn == nil {
		return nil, invalid("store, catalog and connection required")
	}
	return &Service{store, catalog, contracts.NewWebhooksClient(conn)}, nil
}

// Register defines and submits a webhook using the selected event catalog.
func (s *Service) Register(ctx context.Context, id ID, target string, options ...Option) error {
	d, err := Define(s.catalog, id, target, options...)
	if err != nil {
		return err
	}
	return s.RegisterDefinition(ctx, d)
}

// RegisterDefinition submits a frozen definition. Success is command acceptance,
// not delivery confirmation. Envelope failures remain inspectable, unlike C#'s unchecked result.
func (s *Service) RegisterDefinition(ctx context.Context, d Definition) error {
	if d.value == nil {
		return invalid("webhook definition required")
	}
	result, err := s.client.AddWebhooks(ctx, &contracts.AddWebhooksRequest{EventStore: string(s.store), Webhooks: []*contracts.WebhookDefinition{d.registrationDefinition()}})
	if err != nil {
		return err
	}
	return wire.CheckEnvelope(result)
}

// GetAll returns detached definitions. Authorization is deliberately absent,
// matching C#: the server exposes its kind but not the original credentials.
// Read-back definitions are not credential-preserving update templates.
func (s *Service) GetAll(ctx context.Context) ([]Definition, error) {
	result, err := s.client.GetWebhooks(ctx, &contracts.GetWebhooksRequest{EventStore: string(s.store)})
	if err != nil {
		return nil, err
	}
	if err = wire.CheckEnvelope(result); err != nil {
		return nil, err
	}
	definitions := make([]Definition, 0, len(result.Data))
	for _, item := range result.Data {
		if item == nil || item.Identifier == "" {
			return nil, faults.ErrProtocol
		}
		value := &contracts.WebhookDefinition{Identifier: item.Identifier, EventSequenceId: item.EventSequenceId, IsReplayable: item.IsReplayable, IsActive: item.IsActive, Target: &contracts.WebhookTarget{Url: item.Url, Headers: maps.Clone(item.Headers)}}
		for _, event := range item.EventTypes {
			if event == nil || event.Id == "" || event.Generation == 0 {
				return nil, faults.ErrProtocol
			}
			value.EventTypes = append(value.EventTypes, proto.Clone(event).(*contracts.EventType))
		}
		definitions = append(definitions, Definition{value})
	}
	return definitions, nil
}

// Remove deletes the named webhook through the kernel's removal contract. This
// supplements C# IWebhooks (which currently exposes only Register and GetAll).
func (s *Service) Remove(ctx context.Context, id ID) error {
	if strings.TrimSpace(string(id)) == "" {
		return invalid("webhook ID required")
	}
	result, err := s.client.RemoveWebhooks(ctx, &contracts.RemoveWebhooksRequest{EventStore: string(s.store), Webhooks: []string{string(id)}})
	if err != nil {
		return err
	}
	return wire.CheckEnvelope(result)
}
func invalid(message string) error {
	return fmt.Errorf("%w: %s", faults.ErrInvalidConfiguration, message)
}
