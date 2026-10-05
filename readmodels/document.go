// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"bytes"

	"github.com/cratis/chronicle.go/internal/jsonstructure"
)

// Check the complete token stream before any map decode can collapse duplicate
// members. Unknown properties obey the same bounded, unambiguous JSON shape.
func validDocument(data []byte) bool {
	data = bytes.TrimSpace(data)
	return len(data) > 0 && data[0] == '{' && jsonstructure.Validate(data) == nil
}
