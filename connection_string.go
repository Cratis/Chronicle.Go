// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

const developmentClient = "chronicle-dev-client"
const developmentSecret = "chronicle-dev-secret"

// ServerAddress is a Chronicle endpoint. Port defaults to 35000 during parsing.
type ServerAddress struct {
	// Host is a DNS name or unbracketed IP address.
	Host string
	// Port is the TCP port.
	Port uint16
}

// String returns host:port, with IPv6 brackets when required.
func (a ServerAddress) String() string { return net.JoinHostPort(a.Host, strconv.Itoa(int(a.Port))) }

// ConnectionString is an immutable parsed URI. Its zero value is not a usable endpoint.
// String and GoString never expose credentials. Parsing does not resolve DNS or connect.
type ConnectionString struct {
	addresses                                []ServerAddress
	clientID, secret, apiKey                 string
	srv, noAuth, explicitCredentials         bool
	skipTLS, tlsSpecified, skipCompatibility bool
	unsupported                              []string
}

// Addresses returns a defensive copy of the configured endpoints.
func (s ConnectionString) Addresses() []ServerAddress {
	return append([]ServerAddress(nil), s.addresses...)
}

// IsSRV reports whether DNS SRV discovery was requested (not implemented in slice 1).
func (s ConnectionString) IsSRV() bool { return s.srv }

// String renders only scheme and addresses; credentials and query values are omitted.
func (s ConnectionString) String() string {
	scheme := "chronicle"
	if s.srv {
		scheme += "+srv"
	}
	addresses := make([]string, len(s.addresses))
	for i, address := range s.addresses {
		addresses[i] = address.String()
	}
	return scheme + "://" + strings.Join(addresses, ",")
}

// GoString is the credential-safe representation used by fmt with %#v.
func (s ConnectionString) GoString() string { return s.String() }

// ParseConnectionString parses Chronicle's multi-host grammar, including encoded
// credentials and IPv6. Unknown options, partial credentials and conflicting auth
// modes fail without echoing the input. Unsupported known modes parse for diagnostics
// but are rejected by NewClient. Unlike C#, certificate validation defaults to enabled.
func ParseConnectionString(value string) (ConnectionString, error) {
	s := ConnectionString{clientID: developmentClient, secret: developmentSecret}
	scheme, rest, ok := strings.Cut(value, "://")
	scheme = strings.ToLower(scheme)
	if !ok || (scheme != "chronicle" && scheme != "chronicle+srv") {
		return s, invalidURI("scheme")
	}
	s.srv = scheme == "chronicle+srv"
	if strings.Contains(rest, "#") {
		return s, invalidURI("fragment")
	}
	authority, query, _ := strings.Cut(rest, "?")
	authority = strings.TrimSuffix(authority, "/")
	if strings.Contains(authority, "/") {
		return s, invalidURI("path")
	}
	if at := strings.LastIndex(authority, "@"); at >= 0 {
		credentials := authority[:at]
		authority = authority[at+1:]
		id, secret, hasSecret := strings.Cut(credentials, ":")
		var err error
		s.clientID, err = url.PathUnescape(id)
		if err != nil {
			return s, invalidURI("credentials")
		}
		s.secret, err = url.PathUnescape(secret)
		if err != nil || !hasSecret || s.clientID == "" || s.secret == "" || strings.Contains(credentials, "@") {
			return s, invalidURI("credentials")
		}
		s.explicitCredentials = true
	}
	for _, host := range strings.Split(authority, ",") {
		address, err := parseAddress(host)
		if err != nil {
			return s, err
		}
		s.addresses = append(s.addresses, address)
	}
	seen := make(map[string]bool)
	for _, entry := range strings.Split(query, "&") {
		if entry == "" {
			continue
		}
		name, value, _ := strings.Cut(entry, "=")
		name, err := url.PathUnescape(name)
		if err != nil {
			return s, invalidURI("query")
		}
		value, err = url.PathUnescape(value)
		if err != nil {
			return s, invalidURI("query")
		}
		name = strings.ToLower(name)
		if seen[name] {
			return s, invalidURI("duplicate option")
		}
		seen[name] = true
		switch name {
		case "auth":
			if !strings.EqualFold(value, "none") {
				return s, invalidURI("authentication mode")
			}
			s.noAuth = true
		case "apikey":
			if value == "" {
				return s, invalidURI("empty API key")
			}
			s.apiKey = value
		case "skiptlsvalidation", "skipcompatibilitycheck":
			value = strings.ToLower(strings.TrimSpace(value))
			if value != "true" && value != "false" {
				return s, invalidURI("boolean option")
			}
			if name == "skiptlsvalidation" {
				s.skipTLS, s.tlsSpecified = value == "true", true
			} else {
				s.skipCompatibility = value == "true"
			}
		case "certificatepath", "certificatepassword", "loadbalancer", "srvnameserver", "disabletls":
			s.unsupported = append(s.unsupported, name)
		default:
			return s, invalidURI("unknown option")
		}
	}
	if (s.noAuth && (s.explicitCredentials || s.apiKey != "")) || (s.explicitCredentials && s.apiKey != "") {
		return s, invalidURI("conflicting authentication")
	}
	return s, nil
}

func invalidURI(part string) error {
	return fmt.Errorf("%w: connection string %s", ErrInvalidConfiguration, part)
}

func parseAddress(value string) (ServerAddress, error) {
	host, port := value, "35000"
	if strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]") {
		host = strings.TrimSuffix(strings.TrimPrefix(value, "["), "]")
		if net.ParseIP(host) == nil {
			return ServerAddress{}, invalidURI("IPv6 host")
		}
	} else if strings.Contains(value, ":") {
		var err error
		host, port, err = net.SplitHostPort(value)
		if err != nil {
			return ServerAddress{}, invalidURI("host or port")
		}
		if strings.HasPrefix(value, "[") && net.ParseIP(host) == nil {
			return ServerAddress{}, invalidURI("IPv6 host")
		}
	}
	if host == "" || strings.ContainsAny(host, " @/?#%\\\t\r\n[]") {
		return ServerAddress{}, invalidURI("host")
	}
	p, err := strconv.ParseUint(port, 10, 16)
	if err != nil || p == 0 {
		return ServerAddress{}, invalidURI("port")
	}
	return ServerAddress{Host: host, Port: uint16(p)}, nil
}
