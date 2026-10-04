// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/identities"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/registration"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

type identityKernel struct {
	contracts.UnimplementedIdentitiesServer
	mu                       sync.Mutex
	reads, commands          int
	pre, post                *contracts.QueryResult_IEnumerable_IdentityDetailsResponse
	command                  *contracts.CommandResult
	queryError, commandError error
	requests                 []*contracts.RenameIdentityRequest
	coordinates              []*contracts.GetIdentitiesRequest
	wire                     func(proto.Message, []byte) []byte
}

func (k *identityKernel) GetIdentities(_ context.Context, req *contracts.GetIdentitiesRequest) (*contracts.QueryResult_IEnumerable_IdentityDetailsResponse, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.reads++
	k.coordinates = append(k.coordinates, req)
	if k.queryError != nil {
		return nil, k.queryError
	}
	if k.reads == 1 {
		return k.pre, nil
	}
	return k.post, nil
}
func (k *identityKernel) RenameIdentity(_ context.Context, req *contracts.RenameIdentityRequest) (*contracts.CommandResult, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.commands++
	k.requests = append(k.requests, req)
	return k.command, k.commandError
}
func identityRow(subject, name string) *contracts.IdentityDetailsResponse {
	return &contracts.IdentityDetailsResponse{Subject: subject, Id: subject, Name: name, UserName: "username"}
}
func identityQuery(rows ...*contracts.IdentityDetailsResponse) *contracts.QueryResult_IEnumerable_IdentityDetailsResponse {
	return &contracts.QueryResult_IEnumerable_IdentityDetailsResponse{IsAuthorized: true, Data: rows}
}
func identityHappyKernel() *identityKernel {
	return &identityKernel{pre: identityQuery(identityRow("subject", "old")), post: identityQuery(identityRow("subject", "new")), command: &contracts.CommandResult{IsAuthorized: true}}
}

// Explicit false is encoded by protobuf-net's required authorization field.
type identityServerCodec struct{ kernel *identityKernel }

func (identityServerCodec) Name() string { return "proto" }
func (c identityServerCodec) Marshal(value any) ([]byte, error) {
	m := value.(proto.Message)
	data, err := proto.Marshal(m)
	if err != nil {
		return nil, err
	}
	var authorized bool
	switch m := m.(type) {
	case *contracts.CommandResult:
		authorized = m.IsAuthorized
	case *contracts.QueryResult_IEnumerable_IdentityDetailsResponse:
		authorized = m.IsAuthorized
	}
	if !authorized {
		data = append(data, 0x10, 0)
	}
	if c.kernel.wire != nil {
		data = c.kernel.wire(m, data)
	}
	return data, nil
}
func (identityServerCodec) Unmarshal(data []byte, value any) error {
	return proto.Unmarshal(data, value.(proto.Message))
}

func identityConnection(t *testing.T, kernel *identityKernel) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.ForceServerCodec(identityServerCodec{kernel}))
	contracts.RegisterIdentitiesServer(server, kernel)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	conn, err := grpc.NewClient("passthrough:///identity", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	return conn
}
func identityStore(t *testing.T, raw grpc.ClientConnInterface, options ...ClientOption) (*EventStore, *generation) {
	t.Helper()
	client, err := NewClient(append([]ClientOption{WithNoAuthentication()}, options...)...)
	if err != nil {
		t.Fatal(err)
	}
	store := testDefinitionStore(t, &EventStore{client: client, name: "store", namespace: "namespace", catalog: client.catalog})
	ctx, cancel := context.WithCancel(client.life)
	g := &generation{client: client, number: 1, ctx: ctx, cancel: cancel, raw: raw}
	g.transport = &generationTransport{generation: g}
	client.current = g
	identityMarkBarrier(g, registrationKey(store.name, store.namespace, store.definitions.root.revision), nil)
	t.Cleanup(func() {
		cancel()
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	return store, g
}
func identityMarkBarrier(g *generation, key string, failure error) {
	g.registrations.For(key).Run(context.Background(), g.number, registration.Policy{MaxAttempts: 1, AttemptTimeout: 1000000000}, func(error) bool { return false }, func(context.Context) ([]ArtifactRegistration, error) { return nil, failure })
}

func TestIdentityRenameObservedWireSequence(t *testing.T) {
	k := identityHappyKernel()
	// Unrelated empty identities and existing empty names are valid source rows.
	k.pre.Data = append(k.pre.Data, &contracts.IdentityDetailsResponse{}, identityRow("unrelated", ""))
	k.post.Data = append(k.post.Data, &contracts.IdentityDetailsResponse{})
	store, _ := identityStore(t, identityConnection(t, k))
	result, err := store.Identities().Rename(t.Context(), "subject", identities.Name("new"))
	if err != nil || result.Disposition != IdentityRenameObserved || !result.Acknowledged || result.RequestCorrelationID == ([16]byte{}) || result.ResponseCorrelationID != nil {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.reads != 2 || k.commands != 1 || k.requests[0].Subject != "subject" || k.requests[0].Name != "new" || k.requests[0].Namespace != "namespace" || k.requests[0].EventStore != "store" {
		t.Fatal("wrong command/counts")
	}
	for _, req := range k.coordinates {
		if req.Namespace != "namespace" || req.EventStore != "store" {
			t.Fatal("wrong query coordinates")
		}
	}
}

func TestIdentityRenamePreReadSelection(t *testing.T) {
	cases := []struct {
		name     string
		rows     []*contracts.IdentityDetailsResponse
		category error
	}{
		{"empty", nil, identities.ErrNotFound},
		{"duplicate", []*contracts.IdentityDetailsResponse{identityRow("subject", "old"), identityRow("subject", "old")}, ErrProtocol},
		{"wrong_id", []*contracts.IdentityDetailsResponse{{Subject: "subject", Id: "alias"}}, ErrProtocol},
		{"aliases", []*contracts.IdentityDetailsResponse{{Subject: "Subject", Id: "Subject", Name: "subject", UserName: "subject", OnBehalfOf: &contracts.Identity{Subject: "subject"}}}, identities.ErrNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			k := identityHappyKernel()
			k.pre = identityQuery(tc.rows...)
			s, _ := identityStore(t, identityConnection(t, k))
			r, err := s.Identities().Rename(t.Context(), "subject", "new")
			if r.Disposition != IdentityRenameNotDispatched || !errors.Is(err, tc.category) || k.commands != 0 || k.reads != 1 {
				t.Fatalf("result=%+v error=%v reads=%d commands=%d", r, err, k.reads, k.commands)
			}
			if tc.category == identities.ErrNotFound {
				var missing *identities.NotFoundError
				if !errors.As(err, &missing) {
					t.Fatal("missing typed error")
				}
			}
		})
	}
}

func TestIdentityRenamePostReadKeepsAcknowledgment(t *testing.T) {
	for _, tc := range []struct {
		name string
		post *contracts.QueryResult_IEnumerable_IdentityDetailsResponse
	}{
		{"missing", identityQuery()}, {"duplicate", identityQuery(identityRow("subject", "new"), identityRow("subject", "new"))}, {"mismatch", identityQuery(identityRow("subject", "different"))}, {"denied_data", &contracts.QueryResult_IEnumerable_IdentityDetailsResponse{Data: []*contracts.IdentityDetailsResponse{identityRow("subject", "new")}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := identityHappyKernel()
			k.post = tc.post
			s, _ := identityStore(t, identityConnection(t, k))
			r, err := s.Identities().Rename(t.Context(), "subject", "new")
			if !r.Acknowledged || r.Disposition != IdentityRenameUnknown || !errors.Is(err, ErrIdentityRenameUnknown) || k.reads != 2 || k.commands != 1 {
				t.Fatalf("result=%+v error=%v", r, err)
			}
		})
	}
}

func TestIdentityRenameCommandEnvelopes(t *testing.T) {
	for _, tc := range []struct {
		name        string
		command     *contracts.CommandResult
		disposition IdentityRenameDisposition
	}{
		{"explicit_false", &contracts.CommandResult{}, IdentityRenameRefused},
		{"warning", &contracts.CommandResult{IsAuthorized: true, ValidationResults: []*contracts.ValidationResult{{Severity: contracts.ValidationResultSeverity_Warning, Message: "secret"}}}, IdentityRenameRefused},
		{"exception", &contracts.CommandResult{IsAuthorized: true, ExceptionMessages: []string{"secret"}}, IdentityRenameUnknown},
		{"exception_over_refusal", &contracts.CommandResult{ExceptionStackTrace: "secret"}, IdentityRenameUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := identityHappyKernel()
			k.command = tc.command
			s, _ := identityStore(t, identityConnection(t, k))
			r, err := s.Identities().Rename(t.Context(), "subject", "new")
			if err == nil || r.Acknowledged || r.Disposition != tc.disposition || k.reads != 1 || k.commands != 1 {
				t.Fatalf("result=%+v error=%v", r, err)
			}
		})
	}
}

func TestIdentityRenameWirePresenceFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"absent", func([]byte) []byte { return nil }},
		{"duplicate_true", func(b []byte) []byte { return append(b, 0x10, 1) }},
		{"contradictory", func(b []byte) []byte { return append(b, 0x10, 0) }},
		{"non_boolean", func([]byte) []byte { return []byte{0x10, 2} }},
		{"wrong_type", func([]byte) []byte { return []byte{0x12, 0} }},
		{"unknown", func(b []byte) []byte {
			return protowire.AppendVarint(protowire.AppendTag(b, 99, protowire.VarintType), 1)
		}},
		{"malformed", func([]byte) []byte { return []byte{0x10, 0x80} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := identityHappyKernel()
			k.wire = func(m proto.Message, data []byte) []byte {
				if _, ok := m.(*contracts.CommandResult); ok {
					return tc.mutate(data)
				}
				return data
			}
			s, _ := identityStore(t, identityConnection(t, k))
			r, err := s.Identities().Rename(t.Context(), "subject", "new")
			if r.Acknowledged || r.Disposition != IdentityRenameUnknown || !errors.Is(err, ErrIdentityRenameUnknown) || k.reads != 1 || k.commands != 1 {
				t.Fatalf("result=%+v error=%v", r, err)
			}
		})
	}
}

func TestIdentityRenamePreservesPaddedUTF8SubjectAndName(t *testing.T) {
	k := identityHappyKernel()
	k.pre = identityQuery(identityRow(" café ", ""))
	k.post = identityQuery(identityRow(" café ", " Jane 😀 "))
	s, _ := identityStore(t, identityConnection(t, k))
	r, err := s.Identities().Rename(t.Context(), " café ", " Jane 😀 ")
	if err != nil || r.Disposition != IdentityRenameObserved || k.requests[0].Subject != " café " || k.requests[0].Name != " Jane 😀 " {
		t.Fatal("accepted strings normalized", r, err)
	}
}

func TestIdentityRenameFailedQueryDataIsNotAbsence(t *testing.T) {
	k := identityHappyKernel()
	k.pre.IsAuthorized = false
	s, _ := identityStore(t, identityConnection(t, k))
	r, err := s.Identities().Rename(t.Context(), "subject", "new")
	if r.Disposition != IdentityRenameNotDispatched || err == nil || errors.Is(err, identities.ErrNotFound) || k.reads != 1 || k.commands != 0 {
		t.Fatalf("result=%+v error=%v", r, err)
	}
}
