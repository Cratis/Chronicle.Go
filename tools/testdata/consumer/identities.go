// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package consumer

import (
	sdk "github.com/cratis/chronicle.go"
	ev "github.com/cratis/chronicle.go/events"
)

type PIIIdentity struct {
	Subject ev.SourceID `chronicle:"pii"` // want "^CHR0034:"
}

type LocalID = ev.SourceID

type EncryptedIdentity struct {
	Key LocalID `json:",omitzero" chronicle:"encrypted(scope=namespace,details=\"CANARY_IDENTITY_DETAILS\")"` // want "^CHR0052:"
}

type Other ev.SourceID
type SourceID string

type ValidIdentity struct {
	Key      LocalID
	Name     string   `chronicle:"pii"`
	Alias    Other    `chronicle:"encrypted"`
	External SourceID `chronicle:"pii"`
}

type CombinedIdentity struct {
	Key ev.SourceID `chronicle:"pii;encrypted"`
}

type nestedIdentity struct {
	Key ev.SourceID
}

type quietIdentities struct {
	ID        string         `chronicle:"pii"`
	Pointer   *ev.SourceID   `chronicle:"encrypted"`
	Slice     []ev.SourceID  `chronicle:"pii"`
	Nested    nestedIdentity `chronicle:"pii"`
	Duplicate ev.SourceID    `chronicle:"pii;pii"`
	Details   ev.SourceID    `chronicle:"pii;compliance-details(value=\"separate\")"`
	Other     ev.SourceID    `chronicle:"pii;subject"`
	Scope     ev.SourceID    `chronicle:"encrypted(scope=invalid)"`
	Ignored   ev.SourceID    `json:"-" chronicle:"pii"`
	Renamed   ev.SourceID    `json:"renamed" chronicle:"encrypted"`
	Option    ev.SourceID    `json:",string" chronicle:"pii"`
}

type UnregisteredIdentity struct {
	Key ev.SourceID `chronicle:"pii"`
}

type IdentityWithOption struct {
	Key ev.SourceID `chronicle:"encrypted"`
}

func DeclareIdentities(registry *sdk.Registry) {
	_, _ = sdk.RegisterEvent[PIIIdentity](registry)
	_, _ = ev.Define[PIIIdentity]() // Repeated declaration of the same field.
	_, _ = ev.Define[EncryptedIdentity]()
	_, _ = ev.Define[ValidIdentity]()
	_, _ = ev.Define[CombinedIdentity]()
	_, _ = ev.Define[quietIdentities]()
	_, _ = ev.Define[IdentityWithOption](ev.WithID("identity"))
}
