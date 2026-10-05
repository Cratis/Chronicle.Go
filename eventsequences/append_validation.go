// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"fmt"
	"strings"
	"time"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
)

func validateAppendMetadata(subject *events.Subject, occurred *time.Time, named []events.NamedTag) error {
	if subject != nil {
		if err := compliance.ValidateSubject(string(*subject)); err != nil {
			return err
		}
	}
	for _, tag := range named {
		if strings.TrimSpace(tag.Name) == "" {
			return fmt.Errorf("%w: empty named tag name", faults.ErrInvalidConfiguration)
		}
	}
	if occurred != nil && (occurred.Year() < 1 || occurred.Year() > 9999) {
		return fmt.Errorf("%w: occurrence outside .NET date range", faults.ErrInvalidConfiguration)
	}
	return nil
}
