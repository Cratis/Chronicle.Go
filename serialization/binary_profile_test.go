// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/serialization"
)

type binaryNested struct{ Inner []byte }
type binaryEvent struct {
	Payload  []byte
	Optional *[]byte
	Chunks   [][]byte
	Nested   binaryNested
}

func binaryPlan(t *testing.T, policy serialization.NamingPolicy) *serialization.Plan {
	t.Helper()
	p, err := serialization.Compile(reflect.TypeFor[binaryEvent](), policy)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func binaryPolicy(name string) serialization.NamingPolicy {
	if strings.Contains(name, "CamelCase") {
		return serialization.CamelCase
	}
	return serialization.PreservePropertyNames
}

func populateBinarySnapshot(t *testing.T, value reflect.Value, data json.RawMessage) {
	t.Helper()
	if string(data) == "null" {
		value.SetZero()
		return
	}
	if value.Kind() == reflect.Pointer {
		value.Set(reflect.New(value.Type().Elem()))
		populateBinarySnapshot(t, value.Elem(), data)
		return
	}
	if value.Kind() == reflect.Slice && value.Type().Elem() == reflect.TypeFor[byte]() {
		var s struct{ DeclaredType, Hex string }
		if err := json.Unmarshal(data, &s); err != nil {
			t.Fatal(err)
		}
		if s.DeclaredType != "System.Byte[]" {
			t.Fatal("snapshot lost binary CLR identity")
		}
		bytes, err := hex.DecodeString(s.Hex)
		if err != nil {
			t.Fatal(err)
		}
		value.SetBytes(bytes)
		return
	}
	if value.Kind() == reflect.Slice {
		var items []json.RawMessage
		if err := json.Unmarshal(data, &items); err != nil {
			t.Fatal(err)
		}
		value.Set(reflect.MakeSlice(value.Type(), len(items), len(items)))
		for i, item := range items {
			populateBinarySnapshot(t, value.Index(i), item)
		}
		return
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != value.NumField() {
		t.Fatal("incomplete typed binary snapshot")
	}
	for i := range value.NumField() {
		populateBinarySnapshot(t, value.Field(i), fields[value.Type().Field(i).Name])
	}
}

func TestBinaryPackagedWritesMatchCSharp(t *testing.T) {
	writes := 0
	for _, profile := range loadBinaryCapture(t).Profiles {
		p := binaryPlan(t, binaryPolicy(profile.NamingPolicy))
		for _, c := range profile.Cases {
			if c.Operation != "EventSerializer.Serialize" {
				continue
			}
			writes++
			t.Run(profile.NamingPolicy+"/"+c.ID, func(t *testing.T) {
				var value any = &binaryEvent{}
				plan := p
				if c.ID == "null" {
					value = &struct {
						Payload  []byte
						Optional *[]byte
						Chunks   [][]byte
						Nested   *binaryNested
					}{}
					var err error
					plan, err = serialization.Compile(reflect.TypeOf(value).Elem(), binaryPolicy(profile.NamingPolicy))
					if err != nil {
						t.Fatal(err)
					}
				}
				populateBinarySnapshot(t, reflect.ValueOf(value).Elem(), c.Input)
				got, err := plan.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				assertEnumCaptureString(t, c.Result.Output, string(got))
			})
		}
	}
	if writes != 22 {
		t.Fatal("incomplete write matrix")
	}
}

func TestBinaryReadsFollowQualifiedProfile(t *testing.T) {
	reads, narrowed := 0, 0
	for _, profile := range loadBinaryCapture(t).Profiles {
		p := binaryPlan(t, binaryPolicy(profile.NamingPolicy))
		for _, c := range profile.Cases {
			if c.Operation != "EventSerializer.Deserialize" {
				continue
			}
			reads++
			t.Run(profile.NamingPolicy+"/"+c.ID, func(t *testing.T) {
				var input string
				if err := json.Unmarshal(c.Input, &input); err != nil {
					t.Fatal(err)
				}
				var value binaryEvent
				err := p.Unmarshal([]byte(input), &value)
				narrow := strings.HasSuffix(c.ID, "/leading-space") || strings.HasSuffix(c.ID, "/trailing-newline") || c.ID == "chunks/null-element"
				if narrow {
					narrowed++
					if c.Result.Status != "accepted" {
						t.Fatal("narrowing no longer matches capture")
					}
				}
				if narrow || c.Result.Status == "error" {
					if !errors.Is(err, chronicle.ErrProtocol) {
						t.Fatalf("invalid binary must fail with protocol error: %v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				var want binaryEvent
				populateBinarySnapshot(t, reflect.ValueOf(&want).Elem(), c.Result.Output)
				if !reflect.DeepEqual(value, want) {
					t.Fatalf("typed read differs from C#: %#v != %#v", value, want)
				}
				got, err := p.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				assertEnumCaptureString(t, c.Result.Reserialize.Output, string(got))
			})
		}
	}
	if reads != 102 || narrowed != 14 {
		t.Fatal("incomplete read classifications")
	}
}
