//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// rawStorageScan is what the kernel actually persisted for one event store,
// read from MongoDB inside the disposable integration container rather than
// through a releasing kernel route.
type rawStorageScan struct {
	// Documents counts every scanned document, so an empty scan is visible.
	Documents int `json:"documents"`
	// Hits maps each needle to the database/collection pairs containing it.
	Hits map[string][]string `json:"hits"`
}

// scanRawStorage searches every database of store for each needle. It requires
// CHRONICLE_INTEGRATION_CONTAINER (the kernel container name or ID) and never
// silently skips. Needles are synthetic fixture values, never real data.
func scanRawStorage(t *testing.T, store string, needles ...string) rawStorageScan {
	t.Helper()
	container := os.Getenv("CHRONICLE_INTEGRATION_CONTAINER")
	if container == "" {
		t.Fatal("set CHRONICLE_INTEGRATION_CONTAINER to the kernel container; raw storage assertions never silently skip")
	}
	encoded, err := json.Marshal(needles)
	if err != nil {
		t.Fatal(err)
	}
	const script = `
const store = process.env.RAW_STORE, needles = JSON.parse(process.env.RAW_NEEDLES);
const result = { documents: 0, hits: {} };
for (const d of db.adminCommand({ listDatabases: 1 }).databases) {
  if (d.name !== store && !d.name.startsWith(store + "+")) continue;
  const database = db.getSiblingDB(d.name);
  for (const c of database.getCollectionNames()) {
    database.getCollection(c).find().forEach(doc => {
      result.documents++;
      const text = EJSON.stringify(doc);
      for (const needle of needles) {
        if (text.includes(needle)) (result.hits[needle] ??= []).push(d.name + "/" + c);
      }
    });
  }
}
print(JSON.stringify(result));`
	command := exec.CommandContext(t.Context(), "docker", "exec", "-e", "RAW_STORE="+store, "-e", "RAW_NEEDLES="+string(encoded), container, "mongosh", "--quiet", "--eval", script) // #nosec G204 -- test-owned container and fixed script.
	output, err := command.Output()
	if err != nil {
		t.Fatalf("raw storage scan: %v", err)
	}
	var scan rawStorageScan
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &scan); err != nil {
		t.Fatalf("raw storage scan output: %v", err)
	}
	return scan
}

// assertStoredWithout fails when any secret is persisted in plaintext. A
// control value that is stored unprotected must be found, so a scan that
// reaches no document cannot pass.
func assertStoredWithout(t *testing.T, store, control string, secrets ...string) {
	t.Helper()
	scan := scanRawStorage(t, store, append([]string{control}, secrets...)...)
	if scan.Documents == 0 || len(scan.Hits[control]) == 0 {
		t.Fatalf("raw storage scan found no control value %q in %d documents", control, scan.Documents)
	}
	for _, secret := range secrets {
		if locations := scan.Hits[secret]; len(locations) != 0 {
			t.Errorf("plaintext %q persisted in %v", secret, locations)
		}
	}
}

type storedChildStringProtection struct {
	Collection string   `json:"collection"`
	Key        string   `json:"key"`
	Children   string   `json:"children"`
	Property   string   `json:"property"`
	Plaintexts []string `json:"plaintexts"`
}

const storedChildStringProtectionScript = `
const store = process.env.RAW_STORE, target = JSON.parse(process.env.RAW_PROTECTED_CHILD);
let documents = 0;
for (const d of db.adminCommand({listDatabases: 1}).databases.filter(d => d.name.startsWith(store + "+"))) {
  const document = db.getSiblingDB(d.name).getCollection(target.collection).findOne({_id: target.key});
  if (document === null) continue;
  documents++;
  const items = document[target.children];
  if (!Array.isArray(items) || items.length !== 1) quit(43);
  const stored = items[0][target.property];
  if (typeof stored !== "string" || stored.length === 0 || target.plaintexts.includes(stored)) quit(44);
  const text = JSON.stringify(document);
  if (target.plaintexts.some(value => text.includes(value))) quit(45);
}
if (documents !== 1) quit(46);`

// assertStoredChildStringProtected inspects only the selected sink document,
// without going through the SDK's release path or printing stored values.
func assertStoredChildStringProtected(t *testing.T, f *kernelFixture, target storedChildStringProtection) {
	t.Helper()
	container := os.Getenv("CHRONICLE_INTEGRATION_CONTAINER")
	if container == "" {
		t.Fatal("raw storage assertion requires CHRONICLE_INTEGRATION_CONTAINER")
	}
	encoded, err := json.Marshal(target)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "exec", "-e", "RAW_STORE="+string(f.storeName), "-e", "RAW_PROTECTED_CHILD="+string(encoded), container, "mongosh", "--quiet", "--eval", storedChildStringProtectionScript)
	if err := cmd.Run(); err != nil {
		t.Fatalf("raw MongoDB protected child assertion failed: %v", err)
	}
}
