// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package main

import (
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

// scoreboard-events:start
type PointsScored struct {
	Player string `json:"player"`
	Amount int64  `json:"amount"`
}

type PlayerRetired struct {
	Player string `json:"player"`
}

type RoundPlayed struct {
	RoundID string `json:"roundId"`
	Number  int32  `json:"number"`
}

// scoreboard-events:end

// scoreboard-fluent:start
type Round struct {
	ID     string `json:"id"`
	Number int32  `json:"number"`
}

type Scoreboard struct {
	ID          string           `json:"id" chronicle:"key"`
	Points      map[string]int64 `json:"points"`
	ScoredBy    map[string]int64 `json:"scoredBy"`
	StartedAt   time.Time        `json:"startedAt"`
	StartedBy   string           `json:"startedBy"`
	LastScorer  string           `json:"lastScorer"`
	Rounds      []Round          `json:"rounds"`
	LatestFirst []Round          `json:"latestFirst"`
}

func scoreboard(model readmodels.Model[Scoreboard], scored events.Type[PointsScored], retired events.Type[PlayerRetired], played events.Type[RoundPlayed]) (projections.Declaration, error) {
	points := projections.Path[Scoreboard, map[string]int64]("points")
	builder := projections.NewBuilder("scoreboards", model)
	projections.From(builder, scored, func(from *projections.FromBuilder[Scoreboard, PointsScored]) {
		player := projections.KeyFromEvent(projections.Path[PointsScored, string]("player"))
		projections.Add(from, projections.Entry(points, player), projections.Path[PointsScored, int64]("amount"))
		projections.Count(from, projections.Entry(projections.Path[Scoreboard, map[string]int64]("scoredBy"), projections.KeyFromCausedBy(projections.CausedByUserName)))
		projections.ContextOnce(from, projections.Path[Scoreboard, time.Time]("startedAt"), "occurred")
		projections.CausedByOnce(from, projections.Path[Scoreboard, string]("startedBy"), projections.CausedBySubject)
		projections.CausedBy(from, projections.Path[Scoreboard, string]("lastScorer"), projections.CausedByName)
	})
	projections.From(builder, retired, func(from *projections.FromBuilder[Scoreboard, PlayerRetired]) {
		projections.RemoveKey(from, projections.Entry(points, projections.KeyFromEvent(projections.Path[PlayerRetired, string]("player"))))
	})
	rounds := func(child *projections.Builder[Round]) {
		projections.From(child, played, nil, projections.UsingKey(projections.Path[RoundPlayed, string]("roundId")))
	}
	projections.Children(builder, projections.Path[Scoreboard, []Round]("rounds"), rounds,
		projections.OrderBy(projections.Path[Round, int32]("number")))
	projections.Children(builder, projections.Path[Scoreboard, []Round]("latestFirst"), rounds, projections.Prepend())
	return builder.Build()
}

// scoreboard-fluent:end

// scoreboard-model-bound:start
type BoundScoreboard struct {
	ID        string    `json:"id" chronicle:"key"`
	StartedAt time.Time `json:"startedAt" chronicle:"context(PointsScored,from=occurred,once=true)"`
	Rounds    []Round   `json:"rounds" chronicle:"children(RoundPlayed,key=roundId,order-by-descending=number)"`
}

// scoreboard-model-bound:end

func scoreboardDeclarations() (*chronicle.Registry, error) {
	registry := chronicle.NewRegistry()
	scored, err := chronicle.RegisterEvent[PointsScored](registry)
	if err != nil {
		return nil, err
	}
	retired, err := chronicle.RegisterEvent[PlayerRetired](registry)
	if err != nil {
		return nil, err
	}
	played, err := chronicle.RegisterEvent[RoundPlayed](registry)
	if err != nil {
		return nil, err
	}
	model, err := chronicle.RegisterReadModel[Scoreboard](registry)
	if err != nil {
		return nil, err
	}
	declaration, err := scoreboard(model, scored, retired, played)
	if err != nil {
		return nil, err
	}
	if err = registry.AddProjection(declaration); err != nil {
		return nil, err
	}
	bound, err := chronicle.RegisterReadModel[BoundScoreboard](registry)
	if err != nil {
		return nil, err
	}
	return registry, registry.AddProjection(projections.ModelBound(bound, projections.WithIdentifier("bound-scoreboards")))
}
