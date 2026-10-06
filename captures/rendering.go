// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package captures

import (
	"fmt"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/internal/faults"
)

func renderScope(scope Scope, indent int, write func(int, string)) error {
	if len(scope.mappings) > 0 {
		write(indent, "map")
	}
	for _, mapping := range scope.mappings {
		if mapping.err != nil {
			return mapping.err
		}
		if len(mapping.lines) == 0 {
			return invalid("map operation required")
		}
		for _, line := range mapping.lines {
			write(indent+2, line)
		}
	}
	for _, rule := range scope.appends {
		if !validEventID(string(rule.event)) {
			return fmt.Errorf("%w: capture event ID must fit CDL's uppercase-leading single identifier grammar", faults.ErrUnsupported)
		}
		if rule.when.err != nil {
			return rule.when.err
		}
		if rule.when.text == "" {
			return invalid("append condition required")
		}
		write(indent, "append "+string(rule.event))
		write(indent+2, "when "+rule.when.text)
		keys := make([]string, 0, len(rule.assignments))
		for key := range rule.assignments {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			value := rule.assignments[key]
			if !validPath(key) || !line(value) {
				return invalid("invalid assignment path or expression")
			}
			write(indent+2, key+" = "+value)
		}
	}
	return nil
}
func line(value string) bool {
	return strings.TrimSpace(value) != "" && !strings.ContainsAny(value, "\r\n\x00")
}
func validPath(value string) bool {
	for _, part := range strings.Split(value, ".") {
		if part == "" {
			return false
		}
		for i, r := range part {
			letter := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
			continuation := i > 0 && (r == '_' || (r >= '0' && r <= '9'))
			if !letter && !continuation {
				return false
			}
		}
	}
	return true
}
func invalid(message string) error {
	return fmt.Errorf("%w: %s", faults.ErrInvalidConfiguration, message)
}
