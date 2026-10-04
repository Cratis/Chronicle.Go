// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package webhooks_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/observation/webhooks"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/webhooks"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

type authenticationFirst struct{ Value string }
type authenticationSecond struct{ Value string }

// This compares real Go Register requests with actual packaged C# Register
// requests. Both collaborators are local; neither is kernel/delivery evidence.
func TestRegisterMatchesActualAuthenticationCapture(t *testing.T) {
	data, err := os.ReadFile("../captures/testdata/authentication/profile.json")
	if err != nil {
		t.Fatal("authentication capture unavailable")
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != "5912663e5b7ec32ccee825391815af894bb63abe73bbc61757ba7c8b4e9152ee" {
		t.Fatal("authentication capture SHA256")
	}
	var profile struct {
		Cases []struct {
			Surface   string `json:"surface"`
			Operation string `json:"operation"`
			ID        string `json:"id"`
			Evidence  string `json:"evidence"`
			Result    struct {
				Method      string `json:"method"`
				BytesBase64 string `json:"bytesBase64"`
			} `json:"result"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &profile); err != nil {
		t.Fatal("authentication capture JSON")
	}
	captured := map[string][]byte{}
	for _, item := range profile.Cases {
		if item.Surface != "outgoing" || item.ID == "oauth-unavailable" {
			continue
		}
		if item.Operation != "Webhooks.Register" || item.Evidence != "public-Register-captured" || item.Result.Method != contracts.Webhooks_AddWebhooks_FullMethodName {
			t.Fatal("authentication capture request classification")
		}
		if _, duplicate := captured[item.ID]; duplicate {
			t.Fatal("duplicate authentication capture")
		}
		wire, err := base64.StdEncoding.Strict().DecodeString(item.Result.BytesBase64)
		if err != nil {
			t.Fatal("authentication capture bytes")
		}
		captured[item.ID] = wire
	}
	if len(captured) != 6 {
		t.Fatal("authentication capture request count")
	}
	first, err := events.Define[authenticationFirst](events.WithID("capture-first"), events.WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	second, err := events.Define[authenticationSecond](events.WithID("capture-second"), events.WithGeneration(3))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(first.Descriptor(), second.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"none-default", "basic-default", "bearer-selected", "basic-then-bearer", "none-false", "bearer-selected-false"} {
		t.Run(id, func(t *testing.T) {
			wire, ok := captured[id]
			if !ok {
				t.Fatal("missing actual Register capture")
			}
			conn := new(authenticationRecordingConnection)
			service, err := webhooks.New("synthetic-store", catalog, conn)
			if err != nil {
				t.Fatal(err)
			}
			var options []webhooks.Option
			if strings.HasPrefix(id, "basic") {
				options = append(options, webhooks.WithBasicAuth("synthetic-user", "synthetic-password"))
			}
			if strings.Contains(id, "bearer") {
				options = append(options, webhooks.WithBearerToken("synthetic-token"))
			}
			if strings.Contains(id, "selected") {
				options = append(options, webhooks.WithEventTypes(second.Ref()), webhooks.WithHeader("X-Synthetic", "fixed-header"))
			}
			if strings.HasSuffix(id, "false") {
				options = append(options, webhooks.WithActive(false), webhooks.WithReplayable(false))
			}
			if err := service.Register(t.Context(), "synthetic-webhook", "https://synthetic.invalid/receive", options...); err != nil {
				t.Fatal(err)
			}
			if conn.calls != 1 {
				t.Fatal("Register dispatch count")
			}
			var actual, expected contracts.AddWebhooksRequest
			if proto.Unmarshal(conn.wire, &actual) != nil || proto.Unmarshal(wire, &expected) != nil || len(actual.Webhooks) != 1 || len(expected.Webhooks) != 1 {
				t.Fatal("Register request decoding")
			}
			goFlags := authenticationRequestFlags(t, conn.wire)
			clrFlags := authenticationRequestFlags(t, wire)
			for _, field := range []protowire.Number{5, 6} {
				if strings.HasSuffix(id, "false") {
					if len(goFlags[field]) != 1 || goFlags[field][0] != 0 || len(clrFlags[field]) != 1 || clrFlags[field][0] != 0 {
						t.Fatal("explicit false presence")
					}
				} else if len(goFlags[field]) != 1 || goFlags[field][0] != 1 || len(clrFlags[field]) != 0 {
					t.Fatal("C# omitted true versus Go explicit true")
				}
			}
			// Apply only the observed protobuf-net DefaultValue(true) omission. A
			// generic proto3 decode alone would incorrectly change true to false.
			if len(clrFlags[5]) == 0 {
				expected.Webhooks[0].IsReplayable = true
			}
			if len(clrFlags[6]) == 0 {
				expected.Webhooks[0].IsActive = true
			}
			if !proto.Equal(&actual, &expected) {
				t.Fatal("Register differs from actual package request (payload redacted)")
			}
		})
	}
}

type authenticationRecordingConnection struct {
	calls int
	wire  []byte
}

func (c *authenticationRecordingConnection) Invoke(_ context.Context, method string, request, response any, _ ...grpc.CallOption) error {
	c.calls++
	if method != contracts.Webhooks_AddWebhooks_FullMethodName {
		return errors.New("unexpected local RPC")
	}
	value, ok := request.(*contracts.AddWebhooksRequest)
	if !ok {
		return errors.New("unexpected request type")
	}
	var err error
	c.wire, err = proto.Marshal(value)
	if err != nil {
		return err
	}
	result, ok := response.(*contracts.CommandResult)
	if !ok {
		return errors.New("unexpected response type")
	}
	result.IsAuthorized = true
	return nil
}
func (*authenticationRecordingConnection) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("unexpected local stream")
}
func authenticationRequestFlags(t *testing.T, data []byte) map[protowire.Number][]uint64 {
	t.Helper()
	var definition []byte
	for len(data) > 0 {
		number, kind, n := protowire.ConsumeTag(data)
		if n < 0 {
			t.Fatal("request tag")
		}
		data = data[n:]
		if number == 2 && kind == protowire.BytesType {
			if definition != nil {
				t.Fatal("duplicate webhook")
			}
			definition, n = protowire.ConsumeBytes(data)
		} else {
			n = protowire.ConsumeFieldValue(number, kind, data)
		}
		if n < 0 {
			t.Fatal("request field")
		}
		data = data[n:]
	}
	if definition == nil {
		t.Fatal("missing webhook")
	}
	flags := map[protowire.Number][]uint64{}
	for len(definition) > 0 {
		number, kind, n := protowire.ConsumeTag(definition)
		if n < 0 {
			t.Fatal("definition tag")
		}
		definition = definition[n:]
		if number == 5 || number == 6 {
			if kind != protowire.VarintType {
				t.Fatal("flag wire type")
			}
			value, size := protowire.ConsumeVarint(definition)
			if size < 0 {
				t.Fatal("flag value")
			}
			flags[number] = append(flags[number], value)
		}
		n = protowire.ConsumeFieldValue(number, kind, definition)
		if n < 0 {
			t.Fatal("definition field")
		}
		definition = definition[n:]
	}
	return flags
}
