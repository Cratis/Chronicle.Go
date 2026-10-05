// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package identities

// Unknown returns a fresh copy of Chronicle's canonical unknown-actor identity.
func Unknown() Identity {
	return Identity{Subject: "3321cf62-db16-425e-8173-99fcfefe11dd", Name: "[Unknown]", UserName: "[Unknown]"}
}

// System returns a fresh copy of Chronicle's canonical system-actor identity.
func System() Identity {
	return Identity{Subject: "5d032c92-9d5e-41eb-947a-ee5314ed0032", Name: "[System]", UserName: "[System]"}
}
