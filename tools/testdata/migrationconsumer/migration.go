// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package migrationconsumer

import (
	"fmt"

	ev "github.com/cratis/chronicle.go/events"
)

type V1 struct{ Name string }
type V2 struct{ Name string }

func Mismatch(mapping ev.Migration[V2, V1]) (previous, upgrade ev.TypeRef, compileErr, setupErr error) {
	up, err := ev.Define[V2](ev.WithID("CANARY_MIGRATION_UP"), ev.WithGeneration(2))
	if err != nil {
		return ev.TypeRef{}, ev.TypeRef{}, nil, fmt.Errorf("define upgrade: %w", err)
	}
	prev, err := ev.Define[V1](ev.WithID(`CANARY_MIGRATION_PREVIOUS`))
	if err != nil {
		return ev.TypeRef{}, ev.TypeRef{}, nil, fmt.Errorf("define previous: %w", err)
	}
	declaration, err := ev.DefineMigration(up, prev, mapping) // want "^CHR0037:"
	if err != nil {
		return ev.TypeRef{}, ev.TypeRef{}, nil, fmt.Errorf("author migration: %w", err)
	}
	catalog, err := ev.NewCatalog(up.Descriptor(), prev.Descriptor())
	if err != nil {
		return ev.TypeRef{}, ev.TypeRef{}, nil, fmt.Errorf("create catalog: %w", err)
	}
	_, err = catalog.WithMigrations([]ev.MigrationDeclaration{declaration}, false)
	return declaration.Previous().Ref(), declaration.Upgrade().Ref(), err, nil
}

func ReversedBindings(mapping ev.Migration[V2, V1]) error {
	var declaration ev.MigrationDeclaration
	prev, err := ev.Define[V1](ev.WithID(`CANARY_MIGRATION_PREVIOUS`))
	if err != nil {
		return err
	}
	up, upgradeErr := ev.Define[V2](ev.WithGeneration(0x2), ev.WithID("CANARY_MIGRATION_UP"))
	if upgradeErr != nil {
		return upgradeErr
	}
	declaration, err = ev.DefineMigration[V2, V1](up, prev, mapping) // want "^CHR0037:"
	_ = declaration
	return err
}

func DerivedHistory(mapping ev.Migration[V2, V1]) error {
	up, err := ev.Define[V2](ev.WithID("registered"), ev.WithGeneration(2))
	if err != nil {
		return err
	}
	prev, err := ev.DefineGeneration[V1](up, 1)
	if err != nil {
		return err
	}
	declaration, err := ev.DefineMigration(up, prev, mapping)
	if err != nil {
		return err
	}
	catalog, err := ev.NewCatalog(up.Descriptor(), prev.Descriptor())
	if err != nil {
		return err
	}
	_, err = catalog.WithMigrations([]ev.MigrationDeclaration{declaration}, false)
	return err
}

func SameDecodedID(mapping ev.Migration[V2, V1]) error {
	up, err := ev.Define[V2](ev.WithID("sam\x65"), ev.WithGeneration(2))
	if err != nil {
		return err
	}
	prev, err := ev.Define[V1](ev.WithID(`same`))
	if err != nil {
		return err
	}
	_, err = ev.DefineMigration(up, prev, mapping)
	return err
}

const fixedID = "previous"

func ConstantID(mapping ev.Migration[V2, V1]) error {
	up, err := ev.Define[V2](ev.WithID("upgrade"))
	if err != nil {
		return err
	}
	prev, err := ev.Define[V1](ev.WithID(fixedID))
	if err != nil {
		return err
	}
	_, err = ev.DefineMigration(up, prev, mapping)
	return err
}

func DynamicID(id ev.TypeID, mapping ev.Migration[V2, V1]) error {
	up, err := ev.Define[V2](ev.WithID("upgrade"))
	if err != nil {
		return err
	}
	prev, err := ev.Define[V1](ev.WithID(id))
	if err != nil {
		return err
	}
	_, err = ev.DefineMigration(up, prev, mapping)
	return err
}

func DefaultID(mapping ev.Migration[V2, V1]) error {
	up, err := ev.Define[V2](ev.WithID("upgrade"))
	if err != nil {
		return err
	}
	prev, err := ev.Define[V1](ev.WithGeneration(1))
	if err != nil {
		return err
	}
	_, err = ev.DefineMigration(up, prev, mapping)
	return err
}

func RepeatedOption(mapping ev.Migration[V2, V1]) error {
	up, err := ev.Define[V2](ev.WithID("upgrade"))
	if err != nil {
		return err
	}
	prev, err := ev.Define[V1](ev.WithID("one"), ev.WithID("two"))
	if err != nil {
		return err
	}
	_, err = ev.DefineMigration(up, prev, mapping)
	return err
}

func UnknownOption(option ev.TypeOption, mapping ev.Migration[V2, V1]) error {
	up, err := ev.Define[V2](ev.WithID("upgrade"), option)
	if err != nil {
		return err
	}
	prev, err := ev.Define[V1](ev.WithID("previous"))
	if err != nil {
		return err
	}
	_, err = ev.DefineMigration(up, prev, mapping)
	return err
}

func IgnoredError(mapping ev.Migration[V2, V1]) error {
	up, _ := ev.Define[V2](ev.WithID("upgrade"))
	prev, err := ev.Define[V1](ev.WithID("previous"))
	if err != nil {
		return err
	}
	_, err = ev.DefineMigration(up, prev, mapping)
	return err
}

func ExistingHandle(up ev.Type[V2], mapping ev.Migration[V2, V1]) error {
	up, err := ev.Define[V2](ev.WithID("upgrade")) // Only err is newly defined.
	if err != nil {
		return err
	}
	prev, err := ev.Define[V1](ev.WithID("previous"))
	if err != nil {
		return err
	}
	_, err = ev.DefineMigration(up, prev, mapping)
	return err
}

func ExistingError(err error, mapping ev.Migration[V2, V1]) error {
	up, err := ev.Define[V2](ev.WithID("upgrade")) // Only up is newly defined.
	if err != nil {
		return err
	}
	prev, err := ev.Define[V1](ev.WithID("previous"))
	if err != nil {
		return err
	}
	_, err = ev.DefineMigration(up, prev, mapping)
	return err
}

func CapturedHandle(change func(*ev.Type[V2]), mapping ev.Migration[V2, V1]) error {
	up, err := ev.Define[V2](ev.WithID("upgrade"))
	if err != nil {
		return err
	}
	change(&up)
	prev, err := ev.Define[V1](ev.WithID("previous"))
	if err != nil {
		return err
	}
	_, err = ev.DefineMigration(up, prev, mapping)
	return err
}

func DifferentHandle(other ev.Type[V2], mapping ev.Migration[V2, V1]) error {
	up, err := ev.Define[V2](ev.WithID("upgrade"))
	if err != nil {
		return err
	}
	prev, err := ev.Define[V1](ev.WithID("previous"))
	if err != nil {
		return err
	}
	_, err = ev.DefineMigration(other, prev, mapping)
	_ = up
	return err
}

func DifferentBlock(mapping ev.Migration[V2, V1]) error {
	up, err := ev.Define[V2](ev.WithID("upgrade"))
	if err != nil {
		return err
	}
	{
		prev, err := ev.Define[V1](ev.WithID("previous"))
		if err != nil {
			return err
		}
		_, err = ev.DefineMigration(up, prev, mapping)
		return err
	}
}

func DefineMigration(up ev.Type[V2], prev ev.Type[V1], mapping ev.Migration[V2, V1]) (ev.MigrationDeclaration, error) {
	return ev.DefineMigration(up, prev, mapping)
}

func Wrapper(mapping ev.Migration[V2, V1]) error {
	up, err := ev.Define[V2](ev.WithID("upgrade"))
	if err != nil {
		return err
	}
	prev, err := ev.Define[V1](ev.WithID("previous"))
	if err != nil {
		return err
	}
	_, err = DefineMigration(up, prev, mapping)
	return err
}

func Indirect(mapping ev.Migration[V2, V1]) error {
	define := ev.DefineMigration[V2, V1]
	up, err := ev.Define[V2](ev.WithID("upgrade"))
	if err != nil {
		return err
	}
	prev, err := ev.Define[V1](ev.WithID("previous"))
	if err != nil {
		return err
	}
	_, err = define(up, prev, mapping)
	return err
}

func Generic[T any](mapping ev.Migration[T, V1]) error {
	up, err := ev.Define[T](ev.WithID("upgrade"))
	if err != nil {
		return err
	}
	prev, err := ev.Define[V1](ev.WithID("previous"))
	if err != nil {
		return err
	}
	_, err = ev.DefineMigration(up, prev, mapping)
	return err
}

func ReversedGuard(mapping ev.Migration[V2, V1]) error {
	up, err := ev.Define[V2](ev.WithID("upgrade"))
	if nil != err {
		return err
	}
	prev, err := ev.Define[V1](ev.WithID("previous"))
	if err != nil {
		return err
	}
	_, err = ev.DefineMigration(up, prev, mapping)
	return err
}

func ShadowedNil(nil error, mapping ev.Migration[V2, V1]) error {
	up, err := ev.Define[V2](ev.WithID("upgrade"))
	if err != nil {
		return err
	}
	prev, err := ev.Define[V1](ev.WithID("previous"))
	if err != nil {
		return err
	}
	_, err = ev.DefineMigration(up, prev, mapping)
	return err
}
