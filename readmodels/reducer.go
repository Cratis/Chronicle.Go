// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"context"
	"encoding/json"

	"github.com/cratis/chronicle.go/events"
)

// Passive declares an on-demand reducer model. Register its reducer before
// NewClient; for projections use projections.Passive on the producer instead.
func Passive() ModelOption { return func(c *modelConfig) { c.passive = true } }

// ValidateProducer checks deferred passive-model requirements at registry freeze.
func (d Descriptor) ValidateProducer() error {
	if d.definition.config.passive && (d.definition.config.observer != Reducer || d.definition.config.observerID == "" || d.definition.config.sink.Type != NoSink) {
		return invalid("passive model requires a registered reducer; use the projection passive option for projections")
	}
	return nil
}

// BindReducer associates a model with exactly one reducer at registry freeze.
// Existing projection ownership and explicit conflicting sequences are errors.
// A passive model uses no sink; inactive materialized reducers retain their sink.
func BindReducer(d Descriptor, id string, sequence events.SequenceID, passive bool) (Descriptor, error) {
	if d.GoType() == nil || id == "" || sequence == "" {
		return Descriptor{}, invalid("model and reducer identity required")
	}
	config := d.definition.config
	if (config.observerExplicit && config.observer != Reducer) || (config.observerID != "" && (config.observer != Reducer || config.observerID != id)) {
		return Descriptor{}, invalid("model has a conflicting producer")
	}
	if config.sequenceExplicit && config.sequence != sequence {
		return Descriptor{}, invalid("model and reducer event sequences conflict")
	}
	passive = passive || config.passive || config.sink.Type == NoSink
	if passive {
		if (config.sinkExplicit && config.sink.Type != NoSink) || config.sink.ConfigurationID != "00000000-0000-0000-0000-000000000000" {
			return Descriptor{}, invalid("passive reducer cannot use a configured sink")
		}
		config.sink.Type = NoSink
	}
	config.observer, config.observerID, config.sequence = Reducer, id, sequence
	copy := *d.definition
	copy.config = config
	return Descriptor{definition: &copy}, nil
}

// PassiveReader is the store's in-process reducer seam. It returns raw state;
// Service performs protected-value release before returning it to the caller.
type PassiveReader func(context.Context, Descriptor, Key) (Instance[json.RawMessage], error)

// ServiceOption configures an immutable read service at construction.
type ServiceOption func(*Service) error

// WithPassiveReader installs the store's reducer reader. The callback is borrowed
// and must support concurrent calls and cancellation. Repeated options fail.
func WithPassiveReader(reader PassiveReader) ServiceOption {
	return func(s *Service) error {
		if reader == nil || s.passive != nil {
			return invalid("one non-nil passive reader required")
		}
		s.passive = reader
		return nil
	}
}
