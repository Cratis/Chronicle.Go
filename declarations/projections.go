// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package declarations

func projectionArgument(operation string, a Argument) bool {
	switch a.Name {
	case "from":
		return (operation == "add" || operation == "subtract" || operation == "join") && a.Value.Kind == Name && Path(a.Value.Text)
	case "on":
		return operation == "join" && a.Value.Kind == Name && Path(a.Value.Text)
	case "identified-by":
		return operation == "children" && a.Value.Kind == Name && Path(a.Value.Text)
	case "key":
		return (operation == "children" || operation == "remove" || operation == "remove-join" || operation == "increment" || operation == "decrement" || operation == "count") && keyExpression(a.Value)
	case "parent-key":
		return (operation == "children" || operation == "remove") && keyExpression(a.Value)
	default:
		return false
	}
}

func keyExpression(v Value) bool {
	if v.Kind == Name {
		return Path(v.Text)
	}
	if v.Kind != Call {
		return false
	}
	switch v.Text {
	case "context":
		return len(v.Args) == 1 && v.Args[0].Name == "" && v.Args[0].Value.Kind == Name && Path(v.Args[0].Value.Text)
	case "value":
		return len(v.Args) == 1 && v.Args[0].Name == "" && v.Args[0].Value.Kind != Name && v.Args[0].Value.Kind != Call && v.Args[0].Value.Kind != List && v.Args[0].Value.Kind != Null
	case "composite":
		if len(v.Args) == 0 {
			return false
		}
		for _, part := range v.Args {
			if !Path(part.Name) || !keyExpression(part.Value) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
