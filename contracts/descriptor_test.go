// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package contracts_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/contracts"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestCanonicalDescriptorIsEmbeddedAndCopied(t *testing.T) {
	original := contracts.DescriptorSet()
	var set descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(original, &set); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, file := range set.File {
		// Google's well-known types carry canonical Go options upstream. Only
		// Chronicle/BCL files must remain free of managed Go package overrides.
		if !strings.HasPrefix(file.GetName(), "google/protobuf/") && file.GetOptions().GetGoPackage() != "" {
			t.Fatalf("managed Go options changed canonical descriptor %s", file.GetName())
		}
		if file.GetPackage() == "Cratis.Chronicle.Contracts.Clients" {
			for _, service := range file.Service {
				if service.GetName() == "ConnectionService" {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatal("descriptor does not contain the real connection protocol")
	}
	copy := contracts.DescriptorSet()
	copy[0] ^= 0xff
	if !bytes.Equal(original, contracts.DescriptorSet()) {
		t.Fatal("descriptor mutation leaked")
	}
}
