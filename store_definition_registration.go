// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import "context"

func (s *EventStore) definitionStage(ctx context.Context, g *generation, root *definitionRoot, stage string, full bool, run func(context.Context) error) error {
	policy := s.client.config.registrationRetry
	policy.MaxAttempts = 1
	outcome := g.registrations.For(definitionStageKey(s.name, root.revision, stage, full)).Run(ctx, g.number, policy, retryRegistration, func(ctx context.Context) ([]ArtifactRegistration, error) {
		if !s.definitionCurrent(root) {
			return nil, errDefinitionSuperseded
		}
		err := run(ctx)
		return []ArtifactRegistration{{Name: stage, Failure: err}}, err
	})
	return outcome.Failure
}

func (s *EventStore) cumulativeRegistration(g *generation, root *definitionRoot) bool {
	if root.previous == 0 {
		return true
	}
	for _, stage := range []string{"read-models", "projections"} {
		acknowledged := false
		for _, full := range []bool{false, true} {
			previous := g.registrations.For(definitionStageKey(s.name, root.previous, stage, full)).Snapshot()
			acknowledged = acknowledged || previous.IsSuccess()
			current := g.registrations.For(definitionStageKey(s.name, root.revision, stage, full)).Snapshot()
			if current.HasRun && !current.IsSuccess() {
				return true
			}
		}
		if !acknowledged {
			return true
		}
	}
	return false
}
