// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package chronicle is the Go client for Cratis Chronicle. Register event structs
// explicitly, construct a Client, obtain a registered EventStore, and append via
// its EventLog. Client, store and sequence handles are concurrency-safe.
//
// NewClient is lazy; Dial performs TLS/OAuth and structural compatibility preflight.
// Close cancels and joins owned work. Use WithDevelopmentDefaults only for local
// self-signed kernels. Production TLS verifies server identity by default.
//
// Append distinguishes known domain rejection from transport failures with an
// unknown commit outcome. Always inspect both the operation error and result.Err;
// never blindly retry an ambiguous write. See ExampleRegisterEvent to declare events.
package chronicle
