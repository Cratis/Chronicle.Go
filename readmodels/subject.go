// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"strings"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/serialization"
)

func collectSubject(plan *serialization.Plan, config *modelConfig) error {
	subject := ""
	for _, field := range plan.Fields() {
		directives, err := declarations.Parse(declarations.V1, field.Tag)
		if err != nil {
			return err
		}
		for _, directive := range directives {
			if directive.Name != "subject" {
				continue
			}
			if subject != "" {
				return invalid("only one subject declaration is allowed")
			}
			if strings.Contains(field.Path, ".") || field.Collection || field.Scalar == serialization.NotScalar {
				return invalid("subject requires a top-level scalar")
			}
			subject = field.Path
		}
	}
	if config.subject == "" {
		config.subject = subject
	}
	return nil
}

// Release subject fallback is deliberately separate from the root schema key.
// C# uses case-insensitive Go/CLR Id, not the projection's Key marker.
func releaseIDProperty(d Descriptor) string {
	for _, field := range serialization.RootFields(d.definition.plan.Fields()) {
		if strings.EqualFold(field.GoField, "id") {
			return field.Path
		}
	}
	return ""
}
