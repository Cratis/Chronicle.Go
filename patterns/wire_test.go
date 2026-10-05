// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package patterns

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"net"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/patterns"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

// Hand-derived C# protobuf-net fixtures (not captured output), from QueryResult
// and ValidationResult at 2e31b0dfb. 10 00 is explicitly denied, not omitted.
func TestCSharpScalarDefaultWireGoldens(t *testing.T) {
	cases := []struct {
		name, hex  string
		authorized bool
		severity   *int32
	}{
		{"omitted-authorized-default", "", true, nil},
		{"explicit-authorized", "1001", true, nil},
		{"explicit-denied", "1000", false, nil},
		{"omitted-severity-default-error", "10011a03120178", true, new(int32(3))},
		{"explicit-unknown-severity", "10011a050800120178", true, new(int32(0))},
		{"future-severity", "10011a050863120178", true, new(int32(99))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := hex.DecodeString(tc.hex)
			if err != nil {
				t.Fatal(err)
			}
			for _, reply := range []proto.Message{&contracts.QueryResult_IEnumerable_BehaviorPatternDetailsResponse{}, &contracts.QueryResult_IEnumerable_PatternScopeResponse{}} {
				if err := (queryCodec{}).Unmarshal(data, reply); err != nil {
					t.Fatal(err)
				}
				var auth bool
				var validation []*contracts.ValidationResult
				switch r := reply.(type) {
				case *contracts.QueryResult_IEnumerable_BehaviorPatternDetailsResponse:
					auth, validation = r.IsAuthorized, r.ValidationResults
				case *contracts.QueryResult_IEnumerable_PatternScopeResponse:
					auth, validation = r.IsAuthorized, r.ValidationResults
				}
				if auth != tc.authorized {
					t.Fatalf("authorized=%v", auth)
				}
				if tc.severity != nil && (len(validation) != 1 || int32(validation[0].Severity) != *tc.severity) {
					t.Fatalf("severity=%v", validation)
				}
			}
		})
	}
}

// Fields 5/6 have C# zero defaults, unlike IsAuthorized. Explicit zero must not
// acquire a client limit. A hand-written hex golden detects accidental changes.
func TestCSharpRequestZeroWireAndExplicitLimits(t *testing.T) {
	zero := &contracts.MatchingPatternsRequest{EventStore: "s", Namespace: "n", GroupingKey: "g"}
	data, err := (queryCodec{}).Marshal(zero)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := hex.DecodeString("0a017312016e1a0167")
	if !bytes.Equal(data, want) {
		t.Fatalf("request=%x, want=%x", data, want)
	}
	zero.MinimumConfidence = 0.5
	zero.MaximumResults = 23
	data, err = (queryCodec{}).Marshal(zero)
	if err != nil {
		t.Fatal(err)
	}
	want, _ = hex.DecodeString("0a017312016e1a016729000000000000e03f3017")
	if !bytes.Equal(data, want) {
		t.Fatalf("explicit request=%x, want=%x", data, want)
	}
}

func TestNilReplyAndMalformedCodec(t *testing.T) {
	if _, err := decodePatterns(nil); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
	for _, data := range [][]byte{{0xff}, {0x1a, 0xff}} {
		if err := (queryCodec{}).Unmarshal(data, &contracts.QueryResult_IEnumerable_BehaviorPatternDetailsResponse{}); err == nil {
			t.Fatal("malformed bytes accepted")
		}
	}
}

// rawCodec returns hand-derived bytes through actual gRPC framing. It is test
// only and proves the facade selects queryCodec rather than the generated proto3
// defaults, including the security-significant explicit false/omitted distinction.
type rawCodec struct{ response []byte }

func (c rawCodec) Name() string                { return "proto" }
func (c rawCodec) Marshal(any) ([]byte, error) { return c.response, nil }
func (c rawCodec) Unmarshal(data []byte, value any) error {
	return proto.Unmarshal(data, value.(proto.Message))
}

type rawServer struct {
	contracts.UnimplementedPatternsServer
}

func (rawServer) PatternsForScope(context.Context, *contracts.PatternsForScopeRequest) (*contracts.QueryResult_IEnumerable_BehaviorPatternDetailsResponse, error) {
	return &contracts.QueryResult_IEnumerable_BehaviorPatternDetailsResponse{}, nil
}

func TestDefaultCodecThroughActualGRPC(t *testing.T) {
	for _, tc := range []struct {
		name  string
		bytes []byte
		fails bool
	}{{"omitted-true", nil, false}, {"explicit-false", []byte{0x10, 0}, true}, {"validation-default-error", []byte{0x10, 1, 0x1a, 3, 0x12, 1, 'x'}, true}} {
		t.Run(tc.name, func(t *testing.T) {
			listener := bufconn.Listen(1024 * 1024)
			server := grpc.NewServer(grpc.ForceServerCodec(rawCodec{response: tc.bytes}))
			contracts.RegisterPatternsServer(server, rawServer{})
			done := make(chan struct{})
			t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
			go func() {
				defer close(done)
				if err := server.Serve(listener); err != nil && err != grpc.ErrServerStopped {
					t.Error(err)
				}
			}()
			conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := conn.Close(); err != nil {
					t.Error(err)
				}
			})
			s, err := New("store", "ns", conn)
			if err != nil {
				t.Fatal(err)
			}
			result, err := s.GetPatternsForScope(t.Context(), "scope")
			if tc.fails {
				var envelope *EnvelopeError
				if !errors.As(err, &envelope) || result.Data != nil {
					t.Fatalf("failure=%+v %v", result, err)
				}
			} else if err != nil || result.Data == nil || len(result.Data) != 0 {
				t.Fatalf("empty=%+v %v", result, err)
			}
		})
	}
}
