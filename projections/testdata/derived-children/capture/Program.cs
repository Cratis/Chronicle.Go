// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Captures the packaged C# model-bound definition for a derived children
// collection and the typed derived-child payload under both naming policies.
// The NuGet package internalizes ModelBoundProjectionBuilder and the contract
// types, so reflection constructs the builder and normalizes only the JSON
// carrier: event-keyed dictionaries become protobuf JSON key/value arrays.
using System.Collections;
using System.Reflection;
using System.Text.Json;
using Cratis.Chronicle.Events;
using Cratis.Chronicle.Keys;
using Cratis.Chronicle.Projections.ModelBound;
using Cratis.Serialization;

if (args.Length != 1)
{
    throw new ArgumentException("usage: DerivedChildrenCapture <new-output-directory>");
}
var target = args[0];
if (Directory.Exists(target))
{
    throw new IOException($"{target} already exists; compare a fresh capture instead of overwriting goldens");
}
Directory.CreateDirectory(target);
foreach (var naming in new INamingPolicy[] { new DefaultNamingPolicy(), new CamelCaseNamingPolicy() })
{
    var type = typeof(FromEventAttribute<>).Assembly.GetType("Cratis.Chronicle.Projections.ModelBound.ModelBoundProjectionBuilder")!;
    var builder = Activator.CreateInstance(type, naming, DispatchProxy.Create<IEventTypes, EventTypes>(), null)!;
    var definition = type.GetMethod("Build", [typeof(Type)])!.Invoke(builder, [typeof(Catalog)])!;
    var options = new JsonSerializerOptions { WriteIndented = true, PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
    File.WriteAllText(Path.Combine(target, naming.GetType().Name + ".definition.json"), JsonSerializer.Serialize(Normalize(definition), options) + "\n");
    var serializer = new JsonSerializerOptions { PropertyNamingPolicy = naming.JsonPropertyNamingPolicy, PropertyNameCaseInsensitive = true };
    serializer.Converters.Add(new DerivedTypeJsonConverterFactory(new ChildTypes()));
    var payload = JsonSerializer.Serialize(new Catalog("order", [new Line("line", "Ada")]), serializer);
    File.WriteAllText(Path.Combine(target, naming.GetType().Name + ".child.json"), payload + "\n");
    var decoded = JsonSerializer.Deserialize<Catalog>(payload, serializer)!;
    if (decoded.Items.Single() is not Line { ItemId: "line", Name: "Ada" })
    {
        throw new InvalidOperationException("child decode failed");
    }
    Console.WriteLine(naming.GetType().Name + " captured definition and typed child payload");
}

static object? Normalize(object? value)
{
    if (value is null || value is string || value is bool || value.GetType().IsPrimitive)
    {
        return value;
    }
    if (value is Enum)
    {
        return Convert.ToInt32(value);
    }
    if (value is IDictionary dictionary)
    {
        if (value.GetType().GetGenericArguments()[0] == typeof(string))
        {
            var result = new SortedDictionary<string, object?>(StringComparer.Ordinal);
            foreach (DictionaryEntry entry in dictionary)
            {
                result[(string)entry.Key] = Normalize(entry.Value);
            }
            return result;
        }
        var entries = new List<object>();
        foreach (DictionaryEntry entry in dictionary)
        {
            entries.Add(new { key = Normalize(entry.Key), value = Normalize(entry.Value) });
        }
        return entries;
    }
    if (value is IEnumerable sequence)
    {
        return sequence.Cast<object>().Select(Normalize).ToArray();
    }
    return value.GetType().GetProperties().ToDictionary(p => JsonNamingPolicy.CamelCase.ConvertName(p.Name), p => Normalize(p.GetValue(value)));
}

public class EventTypes : DispatchProxy
{
    protected override object? Invoke(MethodInfo? method, object?[]? args) => method!.Name switch
    {
        "GetEventTypeFor" => new EventType(((Type)args![0]!).Name, EventTypeGeneration.First),
        "HasFor" => true,
        _ => throw new NotSupportedException(method.Name)
    };
}

public interface Child;

[DerivedType("line", typeof(Child))]
public record Line(
    [Key] string ItemId,
    [SetFrom<ItemAdded>(nameof(ItemAdded.Name)), Join<ItemRenamed>(on: nameof(ItemId), eventPropertyName: nameof(ItemRenamed.Name))] string Name) : Child;

public record ItemAdded(string ItemId, string OrderId, string Name);
public record ItemRemoved(string ItemId, string OrderId);
public record ItemRenamed(string Name);

public record Catalog(
    [Key] string Id,
    [ChildrenFrom<ItemAdded>(key: nameof(ItemAdded.ItemId), parentKey: nameof(ItemAdded.OrderId))]
    [RemovedWith<ItemRemoved>(key: nameof(ItemRemoved.ItemId), parentKey: nameof(ItemRemoved.OrderId))]
    Child[] Items);

public class ChildTypes : IDerivedTypes
{
    public IEnumerable<Type> TypesWithDerivatives => [typeof(Child)];
    public bool HasDerivatives(Type type) => type == typeof(Child);
    public bool IsDerivedType(Type type) => type == typeof(Line);
    public Type GetDerivedTypeFor(Type target, DerivedTypeId id) => id.Value == "line" ? typeof(Line) : throw new ArgumentException("unknown derived type", nameof(id));
    public Type GetTargetTypeFor(Type type) => typeof(Child);
}
