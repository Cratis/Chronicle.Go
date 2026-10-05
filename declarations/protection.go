// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package declarations

func protectionDeclaration(d Directive) bool {
	switch d.Name {
	case "pii":
		return len(d.Args) == 0
	case "compliance-details":
		return len(d.Args) == 1 && d.Args[0].Name == "value" && d.Args[0].Value.Kind == String
	case "encrypted":
		for _, a := range d.Args {
			switch a.Name {
			case "scope":
				if a.Value.Kind != Name || (a.Value.Text != "subject" && a.Value.Text != "namespace" && a.Value.Text != "global") {
					return false
				}
			case "details":
				if a.Value.Kind != String {
					return false
				}
			default:
				return false
			}
		}
		return true
	default:
		return false
	}
}
