// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package administration_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/internal/administration"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUnknownOutcomeFormattingDoesNotExposeCause(t *testing.T) {
	cause := status.Error(codes.Internal, "private-token-and-payload-marker")
	err := administration.MutationError("stop job", cause)
	for _, text := range []string{err.Error(), fmt.Sprint(err), fmt.Sprintf("%+v", err), fmt.Sprintf("%s", err)} {
		if text != "chronicle: stop job outcome unknown" || strings.Contains(text, "private-token-and-payload-marker") {
			t.Fatal("sensitive diagnostic in error text", text)
		}
	}
	var unknown *administration.OutcomeUnknownError
	if !errors.As(err, &unknown) || !errors.Is(err, cause) || unknown.Unwrap() != cause || status.Code(err) != codes.Internal {
		t.Fatal("cause identity lost", err)
	}
}
