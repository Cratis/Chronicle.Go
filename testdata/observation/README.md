# Observer information wire fixtures

These hex files were emitted by **protobuf-net 3.4.30**, using `Serializer.Serialize`
and the unmodified `ObserverInformation`, `ObserverType`, `ObserverOwner`,
`ObserverRunningState` and `Events/EventType` contract sources from Chronicle
**ae5e00a8** (`Source/Kernel/Contracts`). They are not Go re-serializations or
hand-assembled bytes. The serializer version is pinned in that revision's
`Directory.Packages.props`.

The .NET 10 generator populated two `ObserverInformation` instances:

| Field | `replayable.hex` | `once-only.hex` |
| --- | --- | --- |
| Id | `replayable` | `once-only` |
| EventSequenceId | `custom` | `custom` |
| Type | `Reactor` | `Reactor` |
| Owner | `Client` | `Client` |
| IsSubscribed | `true` | `true` |
| IsReplayable | initializer default (`true`) | `false` |

All other properties retain their contract defaults. Each was serialized directly
with `Serializer.Serialize(stream, information)`. For `list.hex`, `IsSubscribed`
was reset to `false` on both instances, matching `Grpc/Observation/ObserverInformationConverters.cs`
(which never supplies it), and the two instances were serialized as
`IEnumerable<ObserverInformation>` in the table's order.

`[DefaultValue(true)]` on field 10 means ordinary replayable observers **omit**
that field, whereas non-replayable observers emit `50 00` (field 10, zero).
A proto3 bool loses this distinction after decoding. The List/Get receive codec
therefore inspects raw presence without altering generated code or other RPCs.
`TestObserverSnapshotsDecodeCSharpWirePresence` serves these bytes verbatim over
bufconn, including List's unavailable subscription versus Get's live subscription.
