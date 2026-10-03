// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

// decisionReadError keeps transport, codec and joined cleanup diagnostics out of
// the public message. Causes intentionally remain available to errors.Is/As;
// callers inspecting them must account for sensitive server-supplied content.
type decisionReadError struct {
	cause   error
	cleanup bool
}

func (e *decisionReadError) Error() string {
	if e.cleanup {
		return "chronicle: decision session cleanup failed"
	}
	return "chronicle: decision read failed"
}

func (e *decisionReadError) Unwrap() error { return e.cause }

func decisionReadFailure(err error) error {
	if err == nil {
		return nil
	}
	if _, safe := err.(*decisionReadError); safe {
		return err
	}
	return &decisionReadError{cause: err}
}
