// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package metadata

// RootCausation configures an explicit client-captured Root audit link. Empty
// application fields use "N/A". These facts are audit data, not authorization.
// Never include credentials, secrets or personal data in application fields.
// Machine and process facts are separate opt-ins; arguments/environment are
// never captured. SoftwareCommit describes the application, not the Go SDK.
type RootCausation struct {
	// SoftwareVersion is the application's version.
	SoftwareVersion string
	// SoftwareCommit is the application's source revision.
	SoftwareCommit string
	// ProgramIdentifier identifies the application.
	ProgramIdentifier string
	// IncludeMachineName requests the host name; unavailability fails capture.
	IncludeMachineName bool
	// IncludeProcessID requests the numeric process ID, never process arguments.
	IncludeProcessID bool
}
