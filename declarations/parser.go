// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package declarations parses versioned Chronicle field declarations. It is shared
// by runtime reflection and declaration tooling; it never evaluates expressions.
package declarations

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/cratis/chronicle.go/internal/faults"
)

// Version identifies the decoded struct-tag grammar, independently of SDK versions.
type Version uint8

// V1 is the supported grammar. Pass it explicitly to Parse.
const V1 Version = 1

// DeclarationError locates a failed declaration. Offset is a byte offset within
// the decoded chronicle tag (or -1 for typed declarations). Cause is inspectable;
// Error deliberately omits literal values and EventReference to avoid disclosures.
type DeclarationError struct {
	Artifact       string
	GoField        string
	Path           string
	Directive      string
	Offset         int
	EventReference string
	Message        string
	Cause          error
}

func (e *DeclarationError) Error() string {
	return fmt.Sprintf("chronicle declaration %s field %s (%s), %s at %d: %s", e.Artifact, e.GoField, e.Path, e.Directive, e.Offset, e.Message)
}

// Unwrap preserves the failure category without including it in default text.
func (e *DeclarationError) Unwrap() error { return e.Cause }

// Kind describes a parsed value; numbers retain exact JSON text.
type Kind uint8

const (
	Name    Kind = iota // Name is an identifier, alias or serialized path.
	String              // String is a decoded JSON string.
	Number              // Number is an exact JSON number.
	Boolean             // Boolean is true or false.
	Null                // Null is an explicit clear, never a Go zero value.
	Call                // Call is a named expression with ordered arguments.
	List                // List is an ordered list of values (sequence restrictions).
)

// Value is an owned syntax tree. Text contains the name or scalar; Args is used
// for Call and List. Callers may mutate parsed trees without affecting another parse.
type Value struct {
	Kind   Kind
	Text   string
	Args   []Argument
	Offset int
}

// Argument is positional when Name is empty, otherwise a named argument.
type Argument struct {
	Name   string
	Value  Value
	Offset int
}

// Directive is one named, optionally parameterized declaration.
type Directive struct {
	Name   string
	Args   []Argument
	Offset int
}

// Parse parses decoded tag contents. Whitespace outside JSON strings is ignored.
// Limits (16 KiB, 32 nesting levels) bound malformed-input work. Unknown directives
// are parsed for tooling; Validate rejects unsupported directives at registration.
func Parse(version Version, tag string) ([]Directive, error) {
	p := parser{text: tag}
	if version != V1 {
		return nil, p.fail("unsupported grammar version")
	}
	if len(tag) > 16384 || !utf8.ValidString(tag) {
		return nil, p.fail("tag too large or invalid UTF-8")
	}
	var result []Directive
	p.space()
	for p.pos < len(tag) {
		start := p.pos
		name := p.name()
		if name == "" || strings.ContainsAny(name, ".@") {
			return nil, p.fail("expected directive name")
		}
		p.directive = name
		d := Directive{Name: name, Offset: start}
		p.space()
		if p.take('(') {
			args, err := p.arguments(1)
			if err != nil {
				return nil, err
			}
			d.Args = args
		}
		result = append(result, d)
		p.space()
		if p.pos == len(tag) {
			break
		}
		if !p.take(';') {
			return nil, p.fail("expected directive separator")
		}
		p.space()
		if p.pos == len(tag) {
			return nil, p.fail("missing directive after separator")
		}
	}
	return result, nil
}

type parser struct {
	text      string
	pos       int
	directive string
}

func (p *parser) fail(message string) error {
	return &DeclarationError{Directive: p.directive, Offset: p.pos, Message: message, Cause: faults.ErrInvalidConfiguration}
}
func (p *parser) space() {
	for p.pos < len(p.text) {
		r, n := utf8.DecodeRuneInString(p.text[p.pos:])
		if !unicode.IsSpace(r) {
			break
		}
		p.pos += n
	}
}
func (p *parser) take(c byte) bool {
	if p.pos < len(p.text) && p.text[p.pos] == c {
		p.pos++
		return true
	}
	return false
}
func (p *parser) name() string {
	start := p.pos
	for p.pos < len(p.text) {
		r, size := utf8.DecodeRuneInString(p.text[p.pos:])
		if !nameCharacter(r) {
			break
		}
		p.pos += size
	}
	return p.text[start:p.pos]
}
func nameCharacter(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-' || r == '.' || r == '@'
}
func (p *parser) arguments(depth int) ([]Argument, error) {
	if depth > 32 {
		return nil, p.fail("expression nesting limit exceeded")
	}
	var args []Argument
	names := map[string]bool{}
	named := false
	p.space()
	if p.take(')') {
		return args, nil
	}
	for {
		p.space()
		start := p.pos
		name := p.name()
		p.space()
		if name != "" && p.take('=') {
			if names[name] {
				return nil, p.fail("duplicate argument")
			}
			names[name] = true
			named = true
		} else {
			name = ""
			p.pos = start
			if named {
				return nil, p.fail("positional argument after named argument")
			}
		}
		v, err := p.value(depth)
		if err != nil {
			return nil, err
		}
		args = append(args, Argument{Name: name, Value: v, Offset: start})
		p.space()
		if p.take(')') {
			return args, nil
		}
		if !p.take(',') {
			return nil, p.fail("expected comma or closing parenthesis")
		}
	}
}
func (p *parser) value(depth int) (Value, error) {
	p.space()
	v := Value{Offset: p.pos}
	if p.pos == len(p.text) {
		return v, p.fail("expected value")
	}
	if p.take('[') {
		if depth > 32 {
			return v, p.fail("expression nesting limit exceeded")
		}
		v.Kind = List
		p.space()
		if p.take(']') {
			return v, nil
		}
		for {
			item, err := p.value(depth + 1)
			if err != nil {
				return v, err
			}
			v.Args = append(v.Args, Argument{Value: item, Offset: item.Offset})
			p.space()
			if p.take(']') {
				return v, nil
			}
			if !p.take(',') {
				return v, p.fail("expected comma or closing bracket")
			}
		}
	}
	if p.text[p.pos] == '"' {
		start := p.pos
		p.pos++
		for p.pos < len(p.text) {
			c := p.text[p.pos]
			p.pos++
			if c == '\\' {
				if p.pos < len(p.text) {
					p.pos++
				}
				continue
			}
			if c == '"' {
				if err := json.Unmarshal([]byte(p.text[start:p.pos]), &v.Text); err != nil {
					return v, p.fail("invalid JSON string")
				}
				v.Kind = String
				return v, nil
			}
		}
		return v, p.fail("unterminated string")
	}
	start := p.pos
	v.Text = p.name()
	if v.Text == "" {
		return v, p.fail("expected scalar, reference or expression")
	}
	p.space()
	if p.take('(') {
		v.Kind = Call
		args, err := p.arguments(depth + 1)
		v.Args = args
		return v, err
	}
	switch v.Text {
	case "null":
		v.Kind = Null
	case "true", "false":
		v.Kind = Boolean
	default:
		// JSON numbers can contain an exponent sign, which is not a name character.
		if p.pos < len(p.text) && p.text[p.pos] == '+' && strings.HasSuffix(strings.ToLower(v.Text), "e") {
			p.pos++
			p.name()
			v.Text = p.text[start:p.pos]
		}
		if (v.Text[0] >= '0' && v.Text[0] <= '9') || v.Text[0] == '-' {
			if !json.Valid([]byte(v.Text)) {
				return v, p.fail("invalid JSON number")
			}
			if _, err := strconv.ParseFloat(v.Text, 64); err != nil {
				return v, p.fail("number out of range")
			}
			v.Kind = Number
		} else {
			v.Kind = Name
		}
	}
	return v, nil
}

// Role is the artifact role in which a field declaration occurs.
type Role uint8

const (
	Model Role = iota // Model admits implemented model-bound projection declarations.
	Event             // Event admits unique and subject declarations, not projection mappings.
	Any               // Any validates shared serialization metadata before its artifact role is known.
)

// Validate checks supported directives and operation-specific arguments. It does
// not resolve event references or paths, which require a frozen registry.
func Validate(role Role, directives []Directive) error {
	for _, d := range directives {
		failure := func(message string, unsupported bool) error {
			cause := error(faults.ErrInvalidConfiguration)
			if unsupported {
				cause = errors.Join(cause, faults.ErrUnsupported)
			}
			return &DeclarationError{Directive: d.Name, Offset: d.Offset, Message: message, Cause: cause}
		}
		if d.Name == "pii" || d.Name == "encrypted" {
			return failure("security declarations require the compliance implementation", true)
		}
		if d.Name == "unique" || d.Name == "subject" {
			if role != Event && role != Any {
				return failure("event directive on a non-event artifact", true)
			}
			if !eventDeclaration(d) {
				return failure("invalid event declaration arguments", false)
			}
			continue
		}
		if role != Model && role != Any {
			return failure("directive is not supported on this artifact role", true)
		}
		switch d.Name {
		case "key", "no-auto", "not-projected", "nested", "index":
			if len(d.Args) != 0 {
				return failure("directive takes no arguments", false)
			}
		case "every", "all":
			if len(d.Args) > 1 {
				return failure("specify either from or context", false)
			}
			for _, a := range d.Args {
				if (a.Name != "from" && a.Name != "context") || a.Value.Kind != Name || !Path(a.Value.Text) {
					return failure("serialized property or context path required", false)
				}
			}
		case "add", "subtract", "increment", "decrement", "count", "clear", "children", "join", "remove", "remove-join":
			if len(d.Args) == 0 || d.Args[0].Name != "" || !eventReference(d.Args[0].Value) {
				return failure("event reference required", false)
			}
			for _, a := range d.Args[1:] {
				if !projectionArgument(d.Name, a) {
					return failure("unknown or invalid projection argument", false)
				}
			}
		case "set", "context", "value":
			if len(d.Args) == 0 || d.Args[0].Name != "" {
				return failure("event reference required", false)
			}
			if !eventReference(d.Args[0].Value) {
				return failure("invalid event reference", false)
			}
			for i, a := range d.Args[1:] {
				expected := "from"
				if d.Name == "value" {
					expected = "value"
				}
				if a.Name != expected || i > 0 {
					return failure("unknown or duplicate argument", false)
				}
				if expected == "from" && (a.Value.Kind != Name || !Path(a.Value.Text)) {
					return failure("serialized property path required", false)
				}
				if expected == "value" && (a.Value.Kind == Name || a.Value.Kind == Call || a.Value.Kind == List) {
					return failure("JSON scalar required", false)
				}
			}
			if d.Name == "value" && len(d.Args) != 2 {
				return failure("value argument required", false)
			}
		default:
			return failure("unknown or not yet supported directive", true)
		}
	}
	return nil
}

// Path reports whether text is an unambiguous serialized object-property path.
// Kernel expression syntax cannot represent arbitrary JSON property names.
func Path(text string) bool {
	for _, part := range strings.Split(text, ".") {
		if part == "" {
			return false
		}
		for i, r := range part {
			if !unicode.IsLetter(r) && r != '_' && (i == 0 || !unicode.IsDigit(r)) {
				return false
			}
		}
	}
	return true
}
func eventReference(v Value) bool {
	if v.Kind == Name {
		return Path(strings.TrimPrefix(v.Text, "@")) && !strings.Contains(v.Text, ".")
	}
	if v.Kind != Call || (v.Text != "go" && v.Text != "id") || len(v.Args) == 0 || v.Args[0].Name != "" || v.Args[0].Value.Kind != String || v.Args[0].Value.Text == "" {
		return false
	}
	if len(v.Args) == 1 {
		return true
	}
	if v.Text != "id" || len(v.Args) != 2 || v.Args[1].Name != "" || v.Args[1].Value.Kind != Number {
		return false
	}
	n, err := strconv.ParseUint(v.Args[1].Value.Text, 10, 32)
	return err == nil && n > 0
}
