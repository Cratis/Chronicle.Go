// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package declarations

import "strings"

func eventDeclaration(d Directive) bool {
	if d.Name == "subject" {
		return len(d.Args) == 0
	}
	for _, a := range d.Args {
		switch a.Name {
		case "name":
			if a.Value.Kind != String || strings.TrimSpace(a.Value.Text) == "" {
				return false
			}
		case "message":
			if a.Value.Kind != String {
				return false
			}
		case "sequences":
			if a.Value.Kind != List {
				return false
			}
			for _, item := range a.Value.Args {
				if item.Value.Kind != String || strings.TrimSpace(item.Value.Text) == "" {
					return false
				}
			}
		default:
			return false
		}
	}
	return true
}
