//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"os"
	"path/filepath"
	"testing"
)

func writeBinaryEvidence(t *testing.T, directory, name string, data []byte) {
	t.Helper()
	if os.Getenv("CHRONICLE_CAPTURE_BINARY_EVIDENCE") != "1" {
		return
	}
	if err := os.WriteFile(filepath.Join(directory, name), data, 0600); err != nil {
		t.Fatal(err)
	}
}

func binaryEvidenceDirectory(t *testing.T) string {
	t.Helper()
	if os.Getenv("CHRONICLE_CAPTURE_BINARY_EVIDENCE") != "1" {
		return ""
	}
	if directory := os.Getenv("CHRONICLE_BINARY_EVIDENCE_DIR"); directory != "" {
		if err := os.MkdirAll(directory, 0750); err != nil {
			t.Fatal(err)
		}
		return directory
	}
	return t.TempDir()
}

func TestBinaryEvidenceCaptureOptIn(t *testing.T) {
	t.Setenv("CHRONICLE_CAPTURE_BINARY_EVIDENCE", "1")
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "temporary", true: "explicit directory"}[explicit], func(t *testing.T) {
			t.Setenv("CHRONICLE_BINARY_EVIDENCE_DIR", "")
			if explicit {
				t.Setenv("CHRONICLE_BINARY_EVIDENCE_DIR", filepath.Join(t.TempDir(), "evidence"))
			}
			directory := binaryEvidenceDirectory(t)
			writeBinaryEvidence(t, directory, "reply.json", []byte(`{}`))
			data, err := os.ReadFile(filepath.Join(directory, "reply.json"))
			if err != nil || string(data) != `{}` {
				t.Fatalf("opt-in evidence: %s %v", data, err)
			}
		})
	}
}

func TestBinaryEvidenceCaptureIsOffByDefault(t *testing.T) {
	t.Setenv("CHRONICLE_CAPTURE_BINARY_EVIDENCE", "")
	t.Setenv("CHRONICLE_BINARY_EVIDENCE_DIR", filepath.Join(t.TempDir(), "disabled"))
	if directory := binaryEvidenceDirectory(t); directory != "" {
		t.Fatal("default capture allocated a directory")
	}
	if _, err := os.Stat(os.Getenv("CHRONICLE_BINARY_EVIDENCE_DIR")); !os.IsNotExist(err) {
		t.Fatalf("disabled capture created a directory: %v", err)
	}
	directory := t.TempDir()
	writeBinaryEvidence(t, directory, "reply.json", []byte(`{}`))
	if _, err := os.Stat(filepath.Join(directory, "reply.json")); !os.IsNotExist(err) {
		t.Fatalf("default run retained binary evidence: %v", err)
	}
}
