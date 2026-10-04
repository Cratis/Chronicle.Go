// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/identities"
)

// This compiling example requires a running development kernel and an identity
// already stored by an ordinary actor append. Development defaults are not a
// production TLS/authentication configuration.
func ExampleIdentityManager_Rename() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := chronicle.Dial(ctx, chronicle.WithDevelopmentDefaults())
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = client.Close() }()
	store, err := client.EventStore(ctx, "example") // Establish readiness separately.
	if err != nil {
		fmt.Println(err)
		return
	}
	result, err := store.Identities().Rename(ctx, "person-42", identities.Name("Jane Austen"))
	switch {
	case errors.Is(err, identities.ErrNotFound):
		fmt.Println("No stored identity for that exact subject")
	case errors.Is(err, chronicle.ErrIdentityRenameUnknown):
		fmt.Println("Do not retry blindly; acknowledged:", result.Acknowledged)
	case err != nil:
		fmt.Println(err)
	default:
		fmt.Println("Requested name observed")
	}
}
