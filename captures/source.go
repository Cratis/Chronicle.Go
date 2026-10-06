// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package captures

// Source is an immutable, redacted source declaration. Zero is invalid. Copies
// are safe for concurrent use. JSON import/export fails with ErrUnsupported.
type Source struct {
	kind, name, route, poll string
	authorization           SourceAuthorization
	invalidAuthorization    bool
}

// API refers to an external HTTP service by name. Poll uses kernel units s/m/h/d;
// an empty interval or route is preserved for server validation, like C#.
func API(service, route, poll string) Source {
	return Source{kind: "api", name: service, route: route, poll: poll}
}

// Webhook declares an inbound path and optional authorization. Nil options,
// blank credential values and multiple authorization options fail at Build.
// Authorization is never embedded in CDL; Validate/Save refuse authorized
// definitions with ErrUnsupported. The pinned kernel cannot run webhook sources.
func Webhook(path string, options ...WebhookOption) Source {
	var configuration webhookConfiguration
	for _, option := range options {
		if option == nil {
			configuration.invalid = true
			continue
		}
		option(&configuration)
	}
	return Source{kind: "webhook", name: path, authorization: configuration.authorization, invalidAuthorization: configuration.invalid}
}

// MessageTopic declares a topic. The pinned kernel cannot run message sources.
func MessageTopic(topic string) Source { return Source{kind: "message", name: topic} }

// Authorization returns an immutable value and whether authorization was
// supplied, not whether the source is valid. Blank values or multiple options
// are rejected by Build. No options (including the zero source) means absent,
// not an explicitly supplied None.
func (s Source) Authorization() (SourceAuthorization, bool) {
	return s.authorization, s.authorization.kind != ""
}

// Authorization returns an immutable value and whether authorization was
// supplied on the source. The zero definition has no authorization.
func (d Definition) Authorization() (SourceAuthorization, bool) {
	return d.authorization, d.authorization.kind != ""
}
