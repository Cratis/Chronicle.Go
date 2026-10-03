// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package externalservices declares kernel-owned HTTP and database integrations.
// No HTTP client, database driver, credential acquisition or background worker is
// installed in the SDK. Secrets are submitted to the kernel, never logged here.
package externalservices

import (
	"context"
	"fmt"
	"maps"
	"net/url"
	"strings"

	contracts "github.com/cratis/chronicle.go/contracts/externalservices"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// Definition is immutable. Its zero value is invalid. Formatting is redacted;
// KernelDefinition deliberately exposes an owned wire copy containing secrets.
type Definition struct {
	value *contracts.ExternalServiceDefinition
}

// Database describes connection configuration, not a client-side connection.
// Port zero requests the kernel's default. Options are copied by MSSQL/PostgreSQL.
// Treat all fields as sensitive; formatting is redacted.
type Database struct {
	// Host names the database server.
	Host string
	// Port is zero or a TCP port in 1..65535.
	Port int
	// Name is the database name.
	Name string
	// Username is the authentication user.
	Username string
	// Password is the authentication secret.
	Password string
	// Options contains provider-specific connection settings.
	Options map[string]string
}

// Format redacts database credentials and options for every fmt verb.
func (d Database) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("externalservices.Database{redacted}"))
}

// Option configures a service definition. Endpoint/auth/scalar choices are
// last-wins; headers and options replace exact keys. Nil options are invalid.
type Option func(*configuration)
type configuration struct {
	endpoint *contracts.ExternalServiceEndpoint
	http     *contracts.HttpEndpointConfiguration
	database *contracts.DatabaseEndpointConfiguration
	invalid  bool
}

// HTTP selects an HTTP endpoint. URLs must be absolute HTTP(S) without userinfo.
func HTTP(target string) Option {
	return func(c *configuration) {
		c.endpoint.Type = contracts.ExternalServiceEndpointType_Http
		c.http.Url = target
	}
}

// MSSQL selects a Microsoft SQL Server endpoint, copying its options.
func MSSQL(database Database) Option {
	return databaseOption(contracts.ExternalServiceEndpointType_MsSql, database)
}

// PostgreSQL selects a PostgreSQL endpoint, copying its options.
func PostgreSQL(database Database) Option {
	return databaseOption(contracts.ExternalServiceEndpointType_PostgreSql, database)
}
func databaseOption(kind contracts.ExternalServiceEndpointType, d Database) Option {
	d.Options = maps.Clone(d.Options)
	return func(c *configuration) {
		c.invalid = d.Port < 0 || d.Port > 65535
		c.endpoint.Type = kind
		options := maps.Clone(c.database.Options)
		if options == nil {
			options = map[string]string{}
		}
		maps.Copy(options, d.Options)
		c.database = &contracts.DatabaseEndpointConfiguration{Host: d.Host, Port: int32(d.Port), Database: d.Name, Username: d.Username, Password: d.Password, Options: options}
	}
}

// WithHeader adds or replaces an HTTP header.
func WithHeader(key, value string) Option {
	return func(c *configuration) { c.http.Headers[key] = value }
}

// WithOption adds or replaces a database provider option.
func WithOption(key, value string) Option {
	return func(c *configuration) {
		if c.database.Options == nil {
			c.database.Options = map[string]string{}
		}
		c.database.Options[key] = value
	}
}

// WithBasicAuth selects HTTP Basic credentials, replacing previous authorization.
func WithBasicAuth(username, password string) Option {
	return func(c *configuration) {
		c.http.Authorization = &contracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization{Value0: &contracts.BasicAuthorization{Username: username, Password: password}}
	}
}

// WithBearerToken selects bearer authorization, replacing previous authorization.
func WithBearerToken(token string) Option {
	return func(c *configuration) {
		c.http.Authorization = &contracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization{Value1: &contracts.BearerTokenAuthorization{Token: token}}
	}
}

// WithOAuth selects kernel-owned client-credentials OAuth.
func WithOAuth(authority, clientID, clientSecret string) Option {
	return func(c *configuration) {
		c.http.Authorization = &contracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization{Value2: &contracts.OAuthAuthorization{Authority: authority, ClientId: clientID, ClientSecret: clientSecret}}
	}
}

// Define snapshots a service using name for both ID and display name, like C#.
// Exactly the final endpoint kind is emitted; irrelevant HTTP/database fields
// are omitted. It validates configuration without connecting to the endpoint.
func Define(name string, options ...Option) (Definition, error) {
	if strings.TrimSpace(name) == "" {
		return Definition{}, invalid("service name required")
	}
	c := configuration{endpoint: &contracts.ExternalServiceEndpoint{}, http: &contracts.HttpEndpointConfiguration{Headers: map[string]string{}}, database: &contracts.DatabaseEndpointConfiguration{Options: map[string]string{}}}
	for _, option := range options {
		if option == nil {
			return Definition{}, invalid("nil service option")
		}
		option(&c)
	}
	if c.endpoint.Type == contracts.ExternalServiceEndpointType_Http {
		u, err := url.Parse(c.http.Url)
		if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil {
			return Definition{}, invalid("absolute HTTP(S) URL without userinfo required")
		}
		for key, value := range c.http.Headers {
			if strings.TrimSpace(key) == "" || strings.ContainsAny(key+value, "\r\n") {
				return Definition{}, invalid("invalid HTTP header")
			}
		}
		c.endpoint.Http = c.http
	} else {
		if c.invalid {
			return Definition{}, invalid("invalid database port")
		}
		if strings.TrimSpace(c.database.Host) == "" || strings.TrimSpace(c.database.Database) == "" {
			return Definition{}, invalid("database host and name required")
		}
		c.endpoint.Database = c.database
	}
	return Definition{&contracts.ExternalServiceDefinition{Id: name, Name: name, Endpoint: c.endpoint}}, nil
}

// Name returns the persisted service name, also its identity.
func (d Definition) Name() string {
	if d.value == nil {
		return ""
	}
	return d.value.Name
}

// KernelDefinition returns a detached wire snapshot containing credentials.
// Never log this explicit export; mutations cannot change the definition.
func (d Definition) KernelDefinition() *contracts.ExternalServiceDefinition {
	if d.value == nil {
		return nil
	}
	return proto.Clone(d.value).(*contracts.ExternalServiceDefinition)
}

// Format redacts the entire definition, including targets and headers.
func (d Definition) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("externalservices.Definition{redacted}"))
}

// Service is concurrent-safe and store-wide. It borrows its connection, has no
// background work and does not retry dispatched writes. Each RPC uses its caller's context.
type Service struct {
	store  metadata.StoreName
	client contracts.ExternalServicesClient
}

// New constructs a service without I/O.
func New(store metadata.StoreName, conn grpc.ClientConnInterface) (*Service, error) {
	if strings.TrimSpace(string(store)) == "" || conn == nil {
		return nil, invalid("store and connection required")
	}
	return &Service{store, contracts.NewExternalServicesClient(conn)}, nil
}

// Register defines and submits a service. Success acknowledges the definition,
// not connectivity to the declared HTTP/database endpoint.
func (s *Service) Register(ctx context.Context, name string, options ...Option) error {
	d, err := Define(name, options...)
	if err != nil {
		return err
	}
	return s.RegisterDefinition(ctx, d)
}

// RegisterDefinition submits an immutable snapshot, preserving command envelope failures.
func (s *Service) RegisterDefinition(ctx context.Context, d Definition) error {
	if d.value == nil {
		return invalid("service definition required")
	}
	result, err := s.client.AddExternalServices(ctx, &contracts.AddExternalServicesRequest{EventStore: string(s.store), ExternalServices: []*contracts.ExternalServiceDefinition{d.KernelDefinition()}})
	if err != nil {
		return err
	}
	return wire.CheckEnvelope(result)
}
func invalid(message string) error {
	return fmt.Errorf("%w: %s", faults.ErrInvalidConfiguration, message)
}
