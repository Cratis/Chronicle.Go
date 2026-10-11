// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"fmt"
	"math"
)

type optionRule func(config clientConfig) error

type uriRule func(config clientConfig, uri ConnectionString) error

var optionRules = []optionRule{
	checkConnectionHooks,
	checkLogger,
	func(config clientConfig) error { return config.naming.Validate() },
	checkScopeFactory,
	checkNilDependencies,
	checkStatsHandlerPresent,
	checkStatsHandlerBorrowed,
	checkBorrowedAuthentication,
}

var uriRules = []uriRule{
	validateLoadBalancer,
	checkUnsupportedURIOptions,
	checkBorrowedEndpoints,
	checkMessageSizes,
	checkKeepAlive,
	checkLifecycle,
	checkAuthenticationConflicts,
}

// rejectIf returns an invalid configuration error carrying the message when the condition holds.
func rejectIf(condition bool, message string) error {
	if condition {
		return fmt.Errorf("%w: %s", ErrInvalidConfiguration, message)
	}
	return nil
}

// setButNil reports whether an option was set to a nil value.
func setButNil(set bool, value any) bool {
	return set && nilValue(value)
}

// validateConfigOptions checks the options that do not depend on the parsed connection string.
func validateConfigOptions(config clientConfig) error {
	for _, rule := range optionRules {
		if err := rule(config); err != nil {
			return err
		}
	}
	return nil
}

// validateConfigForURI checks the options that interact with the parsed connection string.
func validateConfigForURI(config clientConfig, uri ConnectionString) error {
	for _, rule := range uriRules {
		if err := rule(config, uri); err != nil {
			return err
		}
	}
	return nil
}

func checkConnectionHooks(config clientConfig) error {
	for _, hooks := range [][]ConnectionHook{config.connectedHooks, config.disconnectedHooks} {
		for _, hook := range hooks {
			if err := rejectIf(hook == nil, "nil connection hook"); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkLogger(config clientConfig) error {
	return rejectIf(config.loggerSet && config.logger == nil, "nil logger")
}

func checkScopeFactory(config clientConfig) error {
	return rejectIf(setButNil(config.reactorServicesSet, config.reactorServices), "nil scope factory")
}

func checkNilDependencies(config clientConfig) error {
	nilTLS := config.tlsSet && config.tls == nil
	nilTransportOrResolver := setButNil(config.borrowedSet, config.borrowed) || setButNil(config.resolverSet, config.resolver)
	nilToken := setButNil(config.tokenSet, config.tokenSource)
	return rejectIf(nilTLS || nilTransportOrResolver || nilToken, "nil TLS, transport, resolver or token source")
}

func checkStatsHandlerPresent(config clientConfig) error {
	return rejectIf(setButNil(config.grpcStatsHandlerSet, config.grpcStatsHandler), "nil gRPC stats handler")
}

func checkStatsHandlerBorrowed(config clientConfig) error {
	return rejectIf(config.grpcStatsHandlerSet && config.borrowedSet, "gRPC stats handler conflicts with borrowed connection")
}

func checkBorrowedAuthentication(config clientConfig) error {
	unauthenticated := !config.uriSet && !config.tokenSet && !config.noAuth
	return rejectIf(config.borrowed != nil && unauthenticated, "borrowed connection requires explicit authentication")
}

func checkUnsupportedURIOptions(_ clientConfig, uri ConnectionString) error {
	if uri.apiKey != "" || len(uri.unsupported) > 0 {
		return fmt.Errorf("%w: API keys and certificate/plaintext URI options; use WithTLS for PEM material", ErrUnsupported)
	}
	return nil
}

func checkBorrowedEndpoints(config clientConfig, uri ConnectionString) error {
	selectsEndpoints := uri.srv || len(uri.addresses) > 1 || uri.loadBalancer != ""
	return rejectIf(config.borrowed != nil && selectsEndpoints, "borrowed channels own endpoint selection")
}

func checkMessageSizes(config clientConfig, _ ConnectionString) error {
	return rejectIf(messageSizesInvalid(config), "message sizes must be between 1 and 2147483647 bytes")
}

func checkKeepAlive(config clientConfig, _ ConnectionString) error {
	return rejectIf(config.skipKeepAlive && config.keepAliveTimeoutSet, "SkipKeepAlive conflicts with an explicit keepalive timeout")
}

func checkLifecycle(config clientConfig, _ ConnectionString) error {
	return rejectIf(lifecycleInvalid(config), "invalid lifecycle timeout or retry policy")
}

func checkAuthenticationConflicts(config clientConfig, uri ConnectionString) error {
	noAuthConflict := config.noAuth && (uri.explicitCredentials || config.tokenSource != nil)
	tokenConflict := config.tokenSource != nil && (uri.noAuth || uri.explicitCredentials)
	return rejectIf(noAuthConflict || tokenConflict, "conflicting authentication options")
}

func sizeOutOfRange(set bool, size int64) bool {
	return set && (size < 1 || size > math.MaxInt32)
}

func messageSizesInvalid(config clientConfig) bool {
	return sizeOutOfRange(config.maxSendMessageSizeSet, int64(config.maxSendMessageSize)) ||
		sizeOutOfRange(config.maxReceiveMessageSizeSet, int64(config.maxReceiveMessageSize))
}

func lifecycleInvalid(config clientConfig) bool {
	policy := config.registrationRetry
	return config.connectTimeout <= 0 || config.keepAliveTimeout <= 0 || retryPolicyInvalid(policy)
}

func retryPolicyInvalid(policy RegistrationRetry) bool {
	return policy.MaxAttempts < 1 || policy.MaxAttempts > 100 || policy.InitialDelay <= 0 || policy.MaximumDelay < policy.InitialDelay || policy.AttemptTimeout <= 0
}
