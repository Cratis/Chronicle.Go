// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type capabilityBinaryPayload struct {
	ID       string
	Payload  []byte
	Optional *[]byte
	Nested   struct{ Payload []byte }
}
type capabilityBinaryUnique struct {
	Payload []byte `chronicle:"unique"`
}
type capabilityBinarySubject struct {
	Payload []byte `chronicle:"subject"`
}
type capabilityBinaryIndex struct {
	Payload []byte `chronicle:"index"`
}
type capabilityBinaryPII struct {
	Payload []byte `chronicle:"pii"`
}
type capabilityBinaryEncrypted struct {
	Payload []byte `chronicle:"encrypted"`
}
type capabilityBinaryKey struct {
	Payload []byte `chronicle:"key"`
}
type capabilityBinaryIdentity struct {
	Payload []byte `json:"id"`
}
type capabilityBinaryComposite struct{ Payload []byte }
type capabilityBinaryBase struct{ ID string }

func TestBinaryCapabilitiesRefuseBeforeIO(t *testing.T) {
	event, err := events.Define[capabilityBinaryPayload]()
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[capabilityBinaryPayload]()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	build := func(options []projections.Option, configure func(*projections.Builder[capabilityBinaryPayload])) error {
		b := projections.NewBuilder("binary-capability", model, append([]projections.Option{projections.NoAutoMap()}, options...)...)
		configure(b)
		_, err := b.Build()
		return err
	}
	upgrade, err := events.Define[capabilityBinaryPayload](events.WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := events.DefineGeneration[capabilityBinaryBase](upgrade, 1)
	if err != nil {
		t.Fatal(err)
	}
	migration := func(configure func(*events.MigrationBuilder[capabilityBinaryPayload, capabilityBinaryBase])) error {
		_, err := events.DefineMigration(upgrade, previous, events.Migration[capabilityBinaryPayload, capabilityBinaryBase]{
			Upcast: configure, Downcast: func(*events.MigrationBuilder[capabilityBinaryBase, capabilityBinaryPayload]) {},
		})
		return err
	}
	cases := []struct {
		name  string
		check func() error
	}{
		{"ambiguous event plan alias first", func() error { _, err := events.Define[binaryAliasFirst](); return err }},
		{"ambiguous event plan alias last", func() error { _, err := events.Define[binaryAliasLast](); return err }},
		{"ambiguous model plan alias first", func() error { _, err := readmodels.Define[binaryAliasFirst](); return err }},
		{"ambiguous model plan alias last", func() error { _, err := readmodels.Define[binaryAliasLast](); return err }},
		{"event naming plan alias first", func() error {
			event, err := events.Define[binaryKeyAliasFirst]()
			if err != nil {
				return err
			}
			_, err = event.Descriptor().WithNamingPolicy(serialization.CamelCase)
			return err
		}},
		{"event naming plan alias last", func() error {
			event, err := events.Define[binaryKeyAliasLast]()
			if err != nil {
				return err
			}
			_, err = event.Descriptor().WithNamingPolicy(serialization.CamelCase)
			return err
		}},
		{"unique tag", func() error { _, err := events.Define[capabilityBinaryUnique](); return err }},
		{"unique builder", func() error {
			_, err := constraints.UniqueValues("binary").On(event.Descriptor(), "Payload").Build()
			return err
		}},
		{"constraint naming rebind", binaryConstraintNamingAdmission[binaryNamedAliasFirst]},
		{"unique object", func() error {
			_, err := constraints.UniqueValues("binary").On(event.Descriptor(), "Nested").Build()
			return err
		}},
		{"event subject", func() error { _, err := events.Define[capabilityBinarySubject](); return err }},
		{"model subject", func() error { _, err := readmodels.Define[capabilityBinarySubject](); return err }},
		{"subject property", func() error {
			_, err := readmodels.Define[capabilityBinaryPayload](readmodels.WithSubjectProperty("Payload"))
			return err
		}},
		{"index tag", func() error { _, err := readmodels.Define[capabilityBinaryIndex](); return err }},
		{"indexes option", func() error {
			_, err := readmodels.Define[capabilityBinaryPayload](readmodels.WithIndexes("Payload"))
			return err
		}},
		{"indexed object", func() error {
			_, err := readmodels.Define[capabilityBinaryPayload](readmodels.WithIndexes("Nested"))
			return err
		}},
		{"key tag", func() error {
			m, err := readmodels.Define[capabilityBinaryKey]()
			if err != nil {
				return err
			}
			_, err = projections.Compile(projections.ModelBound(m, projections.FromEvent(event)), catalog)
			return err
		}},
		{"key", func() error {
			return build(nil, func(b *projections.Builder[capabilityBinaryPayload]) {
				projections.From(b, event, nil, projections.UsingKey(projections.Path[capabilityBinaryPayload, []byte]("Payload")))
			})
		}},
		{"composite key", func() error {
			return build(nil, func(b *projections.Builder[capabilityBinaryPayload]) {
				key := projections.UsingCompositeKey(func(k *projections.CompositeKeyBuilder[capabilityBinaryComposite, capabilityBinaryPayload]) {
					projections.KeyPart(k, projections.Path[capabilityBinaryComposite, []byte]("Payload"), projections.Path[capabilityBinaryPayload, []byte]("Payload"))
				})
				projections.From(b, event, nil, key)
			})
		}},
		{"variant key", func() error {
			return build([]projections.Option{projections.VariantOf[capabilityBinaryBase](), projections.VariantKey(projections.Path[capabilityBinaryPayload, []byte]("Payload")), projections.EntersOn(event)}, func(b *projections.Builder[capabilityBinaryPayload]) { projections.From(b, event, nil) })
		}},
		{"PII tag", func() error { _, err := events.Define[capabilityBinaryPII](); return err }},
		{"encrypted tag", func() error { _, err := events.Define[capabilityBinaryEncrypted](); return err }},
		{"PII option", func() error {
			_, err := readmodels.Define[capabilityBinaryPayload](readmodels.WithProtection(compliance.Property("Payload", compliance.Classification{PII: true})))
			return err
		}},
		{"encrypted option", func() error {
			_, err := events.Define[capabilityBinaryPayload](events.WithProtection(compliance.Property("Payload", compliance.Classification{Encrypted: true})))
			return err
		}},
		{"identity", func() error {
			m, err := readmodels.Define[capabilityBinaryIdentity]()
			if err != nil {
				return err
			}
			b := projections.NewBuilder("binary-identity", m, projections.NoAutoMap())
			projections.From(b, event, nil)
			_, err = b.Build()
			return err
		}},
		{"literal", func() error {
			return build(nil, func(b *projections.Builder[capabilityBinaryPayload]) {
				projections.From(b, event, func(f *projections.FromBuilder[capabilityBinaryPayload, capabilityBinaryPayload]) {
					projections.Value(f, projections.Path[capabilityBinaryPayload, []byte]("Payload"), []byte{1})
				})
			})
		}},
		{"nullable literal", func() error {
			return build(nil, func(b *projections.Builder[capabilityBinaryPayload]) {
				projections.From(b, event, func(f *projections.FromBuilder[capabilityBinaryPayload, capabilityBinaryPayload]) {
					projections.Value(f, projections.Path[capabilityBinaryPayload, *[]byte]("Optional"), nil)
				})
			})
		}},
		{"join", func() error {
			return build(nil, func(b *projections.Builder[capabilityBinaryPayload]) {
				projections.Join(b, event, projections.Path[capabilityBinaryPayload, []byte]("Payload"), nil)
			})
		}},
		{"correlation", func() error {
			return build(nil, func(b *projections.Builder[capabilityBinaryPayload]) {
				projections.Join(b, event, projections.Path[capabilityBinaryPayload, string]("ID"), nil, projections.UsingKey(projections.Path[capabilityBinaryPayload, []byte]("Payload")))
			})
		}},
		{"context", func() error {
			return build(nil, func(b *projections.Builder[capabilityBinaryPayload]) {
				projections.From(b, event, func(f *projections.FromBuilder[capabilityBinaryPayload, capabilityBinaryPayload]) {
					projections.Context(f, projections.Path[capabilityBinaryPayload, []byte]("Payload"), "subject")
				})
			})
		}},
		{"source", func() error {
			return build(nil, func(b *projections.Builder[capabilityBinaryPayload]) {
				projections.From(b, event, func(f *projections.FromBuilder[capabilityBinaryPayload, capabilityBinaryPayload]) {
					projections.EventSourceID(f, projections.Path[capabilityBinaryPayload, []byte]("Payload"))
				})
			})
		}},
		{"migration identity", func() error {
			return migration(func(*events.MigrationBuilder[capabilityBinaryPayload, capabilityBinaryBase]) {})
		}},
		{"migration rename", func() error {
			return migration(func(b *events.MigrationBuilder[capabilityBinaryPayload, capabilityBinaryBase]) {
				b.RenamedFrom("Payload", "ID")
			})
		}},
		{"migration default", func() error {
			return migration(func(b *events.MigrationBuilder[capabilityBinaryPayload, capabilityBinaryBase]) {
				b.DefaultValue("Payload", "AQ==")
			})
		}},
		{"migration split", func() error {
			return migration(func(b *events.MigrationBuilder[capabilityBinaryPayload, capabilityBinaryBase]) {
				b.Split("Payload", "ID", "=", 0)
			})
		}},
		{"migration combine", func() error {
			return migration(func(b *events.MigrationBuilder[capabilityBinaryPayload, capabilityBinaryBase]) {
				b.Combine("Payload", "", "ID")
			})
		}},
		{"migration value map", func() error {
			return migration(func(b *events.MigrationBuilder[capabilityBinaryPayload, capabilityBinaryBase]) {
				b.MapValues("Payload", "ID")
			})
		}},
		{"migration shared map", func() error {
			_, err := events.DefineMigration(upgrade, previous, events.Migration[capabilityBinaryPayload, capabilityBinaryBase]{
				Upcast:    func(*events.MigrationBuilder[capabilityBinaryPayload, capabilityBinaryBase]) {},
				Downcast:  func(*events.MigrationBuilder[capabilityBinaryBase, capabilityBinaryPayload]) {},
				MapValues: func(b *events.ValueMapBuilder[capabilityBinaryPayload, capabilityBinaryBase]) { b.For("Payload", "ID") },
			})
			return err
		}},
		{"explicit copy", func() error {
			return build(nil, func(b *projections.Builder[capabilityBinaryPayload]) {
				projections.From(b, event, func(f *projections.FromBuilder[capabilityBinaryPayload, capabilityBinaryPayload]) {
					projections.Map(f, projections.Path[capabilityBinaryPayload, []byte]("Payload"), projections.Path[capabilityBinaryPayload, []byte]("Payload"))
				})
			})
		}},
	}
	// These APIs only define/compile metadata: no client/connection is created.
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.check(); !errors.Is(err, chronicle.ErrUnsupported) && !errors.Is(err, chronicle.ErrInvalidConfiguration) {
				t.Fatalf("binary capability admitted before I/O: %v", err)
			}
		})
	}
}
