---
title: Query established behavior
description: Ask contextual and usual-action questions without inventing behavior when Chronicle has no answer.
---

Use `store.Patterns()` to ask what a scope usually does. Answers are server
inference from established behavior, **not promises of future actions**. An empty
answer means nothing is established for that question; do not fill it with local
inference. This experimental v0.x API starts no miner, training runtime or scheduler.

This reference assumes a connected `*chronicle.EventStore`; see
[Get started](clients/go/getting-started.md) for connection setup. The facade
borrows the store's client and preserves its authentication, registration and
cancellation barriers. Its constructor performs no I/O.

## Queries

| Method | Question and result |
| --- | --- |
| `GetPatterns(ctx, key, facets, options)` | Which established patterns describe this context? Server order: most specific, then most confident |
| `GetUsualActions(ctx, key, facets, options)` | What usually happens here? Server-ranked answers, at most one per action |
| `GetPatternsAt(ctx, key, moment, alsoConstraining, options)` | What usually happens at this moment? Calls `GetUsualActions`, adding/replacing `Day` and `TimeBucket` |
| `GetPatternsForScope(ctx, key)` | Every pattern established for this scope, without a client threshold or limit |
| `GetScopes(ctx)` | Scope IDs and optional server display labels in the selected namespace |

All methods take a caller context and return `(patterns.QueryResult[T], error)`.
`Data` is a caller-owned slice in server order, with immutable `FacetSet` values.
`CorrelationID` preserves the response's .NET Guid as an RFC UUID; an omitted
Guid means the all-zero Guid. There is no response cache, including across stores
or namespaces. The response has no total count or resolved threshold/limit.

`GroupingKey` is a string scope, usually a user identity, **not a UUID or an
event-source ID**. Empty is the C# unspecified sentinel and is forwarded rather
than answered locally. Scope IDs, `InitiatorId`, `OnBehalfOf` and other actor
facets are query facts, not credentials or authorization grants. Ordinary
outgoing metadata and context correlation follow existing client conventions;
identity context is not turned into a new patterns authorization claim.

## Criteria and defaults

`NewFacetSet(map[FacetName]FacetValue)` copies its input. Empty values mean an
unspecified fact and are dropped, like C# `FacetSet`; `"0"` is a specified fact.
Unknown facet names are forwarded unchanged. A zero `FacetSet` constrains nothing.
`With` replaces a fact and returns a new immutable set; `Facts` returns a copy.
Do not mutate input maps or option pointers concurrently with their initial copy.

`QueryOptions` has `MinimumConfidence *Confidence` and `MaximumResults *int32`.
Nil and explicit zero both send zero and **both request server defaults**. They
cannot request a zero confidence threshold or zero answers. The wire does not
retain a separate omitted-versus-zero scalar criterion, and Go does not invent one.

At the pinned server revision, confidence is resolved as
`requested > 0 ? requested : configuration.MinimumConfidence`, and limits as
`requested > 0 ? requested : 10`. The SDK sends your values rather than duplicating
those defaults. Negative values, out-of-range confidence, NaN and infinities are
forwarded: the C# confidence concept/request has no range or finiteness validator.
This is compatibility behavior, not a recommendation to send invalid estimates.

`moment` is `*time.Time`: nil means local now, whereas a pointer to zero time
means an explicit year-1 moment. Moments must fit .NET DateTimeOffset's year,
whole-minute offset and ±14-hour offset bounds. Sub-100ns precision is truncated.
Day and bucket use the moment's **own offset**, never UTC normalization:

| Bucket | Local time interval |
| --- | --- |
| `EarlyMorning` | 05:00–08:00 |
| `Morning` | 08:00–11:00 |
| `Midday` | 11:00–14:00 |
| `Afternoon` | 14:00–17:00 |
| `Evening` | 17:00–22:00 |
| `Night` | 22:00–05:00 |

Intervals include the start and exclude the end. Other facets in
`alsoConstraining` survive; supplied `Day` and `TimeBucket` are replaced.

## Reading an answer

This excerpt assumes `ctx` and a connected `store`. The
[complete compiling example](https://github.com/Cratis/Chronicle.Go/blob/main/example_patterns_test.go)
includes disposable-development connection setup; it requires a kernel to run.

```go
moment := time.Date(2024, 1, 15, 9, 30, 0, 0, time.FixedZone("", 2*3600))
facets := patterns.NewFacetSet(map[patterns.FacetName]patterns.FacetValue{
    patterns.AggregateType: "Order",
})
result, err := store.Patterns().GetPatternsAt(ctx, "demo-scope", &moment, facets, patterns.QueryOptions{})
if err != nil {
    return err
}
for _, answer := range result.Data {
    fmt.Println(answer.Action(), answer.Confidence, answer.Occurrences)
}
```

The excerpt needs `fmt`, `time` and `github.com/cratis/chronicle.go/patterns` imports.
No rows means no established behavior, not a failed query or a guarantee that
nothing will happen. `Action()` reads `CommandType`, or returns the empty sentinel
for a context-only pattern. Each answer also preserves support, recency weight,
occurrences, first/last seen times and the server's string facet-key ID.
Specificity is derived from its canonical facets, as in the C# client.
Dates retain the response's offset and 100ns precision. Go additionally preserves
scope display labels and response correlation, which the C# facade discards.

## Errors and capability evidence

RPC, cancellation, deadline and malformed replies remain errors; no error becomes
an empty successful answer. `errors.Is` recognizes standard context errors,
`patterns.ErrProtocol`, and `patterns.ErrUnsupported`. An unavailable RPC returns
`*patterns.UnsupportedError`; the SDK never substitutes inference or another query.
Transport/pre-dispatch failures return `*patterns.CallError`, retaining the original
cause graph through `Unwrap` without formatting raw server/local text.

`*patterns.EnvelopeError` preserves failed authorization, every validation
severity (including unknown numeric values), messages and response correlation.
Like C#, **any** validation result fails the query, even Information or Warning.
Error strings omit diagnostic text; deliberately inspect fields or unwrapped
causes when needed, and treat them as potentially sensitive. Stack traces are
not copied into public results.

The facade uses an isolated wire codec to preserve C# default-true authorization
and default-Error validation severity when scalar fields are absent. Explicit
false and explicit Unknown stay distinct from omission. It does not edit generated
contracts or register a global codec. A missing top-level reply or malformed
required date is a protocol error; missing repeated data is an empty enumerable,
indistinguishable on the wire from an explicit empty one.

`TestKernelPatternsQueries` exercises all five workflows against a fresh namespace
on **19.29.4-development**, through the selected store and real TLS/OAuth transport.
Each returned an actual empty typed answer. This proves query availability and
honest empty handling, **not** nonempty mining, ranking or prediction accuracy.
No synthetic provider or direct storage mutation populated those answers.
Hand-derived C# request/response and raw protobuf fixtures are regression evidence,
not captured .NET serializer output. See the [parity map](parity.md#patterns-queries)
for the pinned source and exact evidence boundary.
