---
title: Dictionaries, set-once values and child ordering
description: Key read-model dictionary entries by event content, context or the causing user, remove entries, set values only once and keep children in a chosen order.
---

Chronicle can maintain a `map[string]V` property one entry at a time, keep the
first value it saw for a property, and insert children at a chosen position. The
kernel does this work during projection; your Go code only declares it. These
operations need Chronicle 19.150.0 or later
([Cratis/Chronicle#5775](https://github.com/Cratis/Chronicle/pull/5775)).

The examples use these events:

```go
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
```

## Fluent declarations

`projections.Entry` turns a string-keyed map property and a key source into a
target that every write builder accepts: `Map`, `MapAs`, `Context`, `CausedBy`,
`EventSourceID`, `Value`, `Add`, `Subtract`, `Increment`, `Decrement`, `Count`,
`RemoveKey` and the `*Once` variants.

```go
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
```

| Key source | Kernel path | Use it for |
| --- | --- | --- |
| `KeyFromEvent(field)` | `points.$eventContent.player` | A scalar property of the projected event. It must exist on that event (names are case-sensitive) |
| `KeyFromContext(path)` | `points.$eventContext.eventSourceId` | An admitted scalar `EventContext` property |
| `KeyFromCausedBy(property)` | `points.$causedBy.userName` | `CausedBySubject`, `CausedByName` or `CausedByUserName` of the causing user |

`RemoveKey` emits `$remove` and deletes the entry; it accepts only an `Entry`
target. `Every` and `All` mappings accept context and caused-by keys, but not
event-content keys, because no single event is known there.

The `*Once` builders wrap the value in `$setOnce(...)`: Chronicle assigns it only
while the property, or the dictionary entry, has no value yet. Use them to keep
the first timestamp or author. `MapOnce`, `MapAsOnce`, `ContextOnce`,
`CausedByOnce`, `EventSourceIDOnce` and `ValueOnce` work in `From` and `Join`;
`EveryMapOnce` and `EveryContextOnce` work in `Every` and `All`. A null value
cannot be set once.

`CausedBy` and `EveryCausedBy` assign the causing user's subject, name or user
name to an unformatted string property with `$causedBy(...)`.

Children are appended in event order unless you pass one ordering option to
`Children`. `Prepend()` inserts each new child first. `OrderBy(field)` and
`OrderByDescending(field)` keep the collection sorted by a scalar child property.
Declaring two orderings for one collection fails `Build`.

## Model-bound declarations

Tags express set-once values and child ordering. Dictionary entries are fluent-only.

```go
type BoundScoreboard struct {
    ID        string    `json:"id" chronicle:"key"`
    StartedAt time.Time `json:"startedAt" chronicle:"context(PointsScored,from=occurred,once=true)"`
    Rounds    []Round   `json:"rounds" chronicle:"children(RoundPlayed,key=roundId,order-by-descending=number)"`
}
```

| Tag argument | Behavior |
| --- | --- |
| `set(E,once=true)`, `context(E,from=occurred,once=true)`, `value(E,value="new",once=true)` | Wrap the mapping in `$setOnce(...)` |
| `children(E,prepend=true)` | Insert new children first |
| `children(E,order-by=number)` / `children(E,order-by-descending=number)` | Keep children sorted by a child property |

Several `children` directives on one field must declare the same ordering.

## Projection or reducer?

Prefer a projection when every change is one of these declarative operations:
mapping, arithmetic, counting, keeping the first value, adding or removing a keyed
dictionary entry, and inserting a child at a fixed or sorted position. The kernel
evaluates these operations itself, so the read model stays current without a Go
process folding events.
Write a [reducer](../reducers.md) only when the next state depends on control
flow over the previous state that no projection operation can express, for
example a rule that compares several properties before choosing what to change.
