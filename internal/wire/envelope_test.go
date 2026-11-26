// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package wire

import (
	"errors"
	"reflect"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/eventsources"
)

func TestCheckEnvelopeAllowsOnlyInformationalAndWarningValidation(t *testing.T) {
	cases := map[string]struct {
		severity contracts.ValidationResultSeverity
		fatal    bool
	}{
		"negative":    {-1, true},
		"unknown":     {contracts.ValidationResultSeverity_Unknown, true},
		"information": {contracts.ValidationResultSeverity_Information, false},
		"warning":     {contracts.ValidationResultSeverity_Warning, false},
		"error":       {contracts.ValidationResultSeverity_Error, true},
		"future":      {42, true},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			members := []string{"source"}
			reply := &contracts.CommandResult{IsAuthorized: true, ValidationResults: []*contracts.ValidationResult{{Severity: c.severity, Message: "diagnostic", Members: members}}}
			err := CheckEnvelope(reply)
			if !c.fatal && err == nil {
				return
			}
			var failure *EnvelopeError
			if !c.fatal || !errors.As(err, &failure) {
				t.Fatalf("error = %v, want fatal validation = %t", err, c.fatal)
			}
			want := []ValidationResult{{Severity: int32(c.severity), Message: "diagnostic", Members: members}}
			if !reflect.DeepEqual(failure.ValidationResults, want) {
				t.Fatalf("diagnostics = %+v, want %+v", failure.ValidationResults, want)
			}
		})
	}
}
