// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#:package Cratis.Chronicle@19.153.0
#:package Cratis.Fundamentals@7.33.1
#:property EnableNETAnalyzers=false
#:property RunAnalyzers=false
#:property PublishAot=false

using System.Collections;
using System.Globalization;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using Cratis.Chronicle;
using Cratis.Chronicle.Compliance;
using Cratis.Chronicle.Events;
using Cratis.Chronicle.Json;
using Cratis.Chronicle.Schemas;
using Cratis.Concepts;
using Cratis.Json;
using Cratis.Serialization;

if (args.Length != 1) throw new ArgumentException("Usage: dotnet run --file capture.cs -- OUTPUT.json (must not exist)");
if (!Environment.Version.ToString().StartsWith("11.0.", StringComparison.Ordinal)) throw new InvalidOperationException("Requires the .NET 11.0 runtime");
CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
CultureInfo.CurrentUICulture = CultureInfo.InvariantCulture;

var profiles = new List<object>();
foreach (var naming in new INamingPolicy[] { new DefaultNamingPolicy(), new CamelCaseNamingPolicy() })
{
    var options = Isolation.ClientOptions(naming);
    var derived = new EmptyDerived();
    var runner = new Runner(
        naming.GetType() == typeof(DefaultNamingPolicy) ? "default" : "camelCase",
        Isolation.Serializer(options, derived),
        new JsonSchemaGenerator(new EmptyMetadata(), naming, derived),
        new ExpandoObjectConverter(new TypeFormats()));
    foreach (var probe in Probes.All(naming)) await runner.Run(probe);
    profiles.Add(new
    {
        namingPolicy = naming.GetType().FullName,
        eventOptions = new { converters = options.Converters.Select(c => c.GetType().FullName).ToArray() },
        cases = runner.Cases
    });
}
if (ForbiddenRegistry.Calls != 0) throw new InvalidOperationException("Registry access invalidated capture");

var fixture = new
{
    profile = "chronicle-19.153.0_fundamentals-7.33.1_net-11.0.0",
    goAdmission = "not-implemented; capture only, no Go API approved",
    runtime = new { version = Environment.Version.ToString(), culture = CultureInfo.CurrentCulture.Name },
    isolation = new
    {
        options = "packaged ChronicleClient.InitializeJsonSerializationOptions via reflection on an uninitialized client; no client constructor",
        serializer = "actual EventSerializer constructor with Serialize and Deserialize(Type, JsonObject)",
        schemas = "JsonSchemaGenerator.Generate(Type) and GenerateForReadModel(Type) with empty compliance metadata and derived types",
        expando = "packaged ExpandoObjectConverter ToExpandoObject and ToJsonObject for each schema API; in memory, no BSON or kernel. Built with the one-argument constructor, whose options are Cratis.Json.Globals.JsonSerializerOptions from Fundamentals (Source/Infrastructure/Json/ExpandoObjectConverter.cs:26,40,46 at the pinned revision), NOT the configured client options used by Serialize/Deserialize; the DotNET client itself does not use this converter",
        registryCalls = ForbiddenRegistry.Calls,
        network = "no client connection constructed"
    },
    assemblies = new[] { typeof(ChronicleClient).Assembly, typeof(DefaultNamingPolicy).Assembly, typeof(JsonSerializer).Assembly, typeof(JsonSchema).Assembly, typeof(ExpandoObjectConverter).Assembly }
        .Distinct().Select(a => new { name = a.FullName, informationalVersion = a.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion }),
    profiles
};
using var stream = new FileStream(args[0], FileMode.CreateNew, FileAccess.Write);
JsonSerializer.Serialize(stream, fixture, new JsonSerializerOptions { WriteIndented = true, PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
stream.WriteByte((byte)'\n');
Console.WriteLine("Captured dictionary-key observations for both naming policies; runtime " + Environment.Version);

public enum Mode { Zero = 0, One = 1, Alias = 1, Two = 2 }
public record NameKey(string Value) : ConceptAs<string>(Value);
public record PairKey(string First, int Second);
public sealed class StringKeyed { public Dictionary<string, int> Values { get; init; } = new(); }
public sealed class GuidKeyed { public Dictionary<Guid, string> Values { get; init; } = new(); }
public sealed class EnumKeyed { public Dictionary<Mode, string> Values { get; init; } = new(); }
public sealed class ConceptKeyed { public Dictionary<NameKey, string> Values { get; init; } = new(); }
public sealed class RecordKeyed { public Dictionary<PairKey, string> Values { get; init; } = new(); }
public sealed class NestedKeyed { public Dictionary<string, Dictionary<string, int>> Values { get; init; } = new(); }
public sealed class NullableKeyed { public Dictionary<string, string?> Values { get; init; } = new(); }

public record Probe(Type Type, (string Id, object Value)[] Values, (string Id, string Json)[] Raws, (string Label, object Key)[] Lookups);

public sealed record Case(
    string Id, string DeclaredType, string InputId, string Stage, string Method, string SchemaAPI,
    string[] DependsOn, string? ValueDescription, string? Input, string Status, string? Output, string? Category, int? ConstructedKeyCount);

public static class Probes
{
    static readonly Guid Known = new("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    const string Escaped = "q\"uote\\\u00e9\u2603";

    public static Probe[] All(INamingPolicy naming)
    {
        string V(string template) => template.Replace("$V", naming.JsonPropertyNamingPolicy?.ConvertName("Values") ?? "Values");
        string Pair(string first, int second, bool spaced) => JsonSerializer.Serialize(
            "{\"" + Name(naming, "First") + "\":" + JsonSerializer.Serialize(first) + "," + (spaced ? " " : "") + "\"" + Name(naming, "Second") + "\":" + second + "}");
        (string, string)[] common = [("missing", "{}")];
        return
        [
            new(typeof(StringKeyed),
            [
                ("empty", new StringKeyed()),
                ("ordinal-case", new StringKeyed { Values = { ["a"] = 1, ["A"] = 2 } }),
                ("escaped", new StringKeyed { Values = { [Escaped] = 1, ["line\nbreak"] = 2 } }),
                ("null-dictionary", new StringKeyed { Values = null! })
            ],
            [
                ("missing", "{}"), ("null", V("{\"$V\":null}")), ("empty", V("{\"$V\":{}}")),
                ("duplicate-spelling", V("{\"$V\":{\"a\":1,\"a\":2}}"))
            ],
            [("a", "a"), ("A", "A"), ("escaped", Escaped), ("line-break", "line\nbreak"), ("absent", "absent")]),
            new(typeof(GuidKeyed),
            [("zero-and-nonzero", new GuidKeyed { Values = { [Guid.Empty] = "zero", [Known] = "nonzero" } })],
            [("missing", "{}"), ("spellings-collide", V("{\"$V\":{\"" + Known.ToString("D") + "\":\"x\",\"" + Known.ToString("D").ToUpperInvariant() + "\":\"y\"}}"))],
            [("zero", Guid.Empty), ("known", Known)]),
            new(typeof(EnumKeyed),
            [
                ("declared", new EnumKeyed { Values = { [Mode.One] = "one", [Mode.Two] = "two" } }),
                ("unknown-member", new EnumKeyed { Values = { [Mode.One] = "one", [(Mode)99] = "unknown" } })
            ],
            [
                ("missing", "{}"), ("names-collide", V("{\"$V\":{\"One\":\"x\",\"Alias\":\"y\"}}")),
                ("numeric-name", V("{\"$V\":{\"1\":\"x\"}}")), ("unknown-name", V("{\"$V\":{\"Nope\":\"x\"}}"))
            ],
            [("one", Mode.One), ("alias", Mode.Alias), ("two", Mode.Two), ("unknown", (Mode)99)]),
            new(typeof(ConceptKeyed),
            [("escaped", new ConceptKeyed { Values = { [new NameKey(Escaped)] = "a", [new NameKey("\u00fcn\u00ef")] = "b" } })],
            common,
            [("escaped", new NameKey(Escaped)), ("unicode", new NameKey("\u00fcn\u00ef"))]),
            new(typeof(RecordKeyed),
            [
                ("two-distinct", new RecordKeyed { Values = { [new PairKey("a", 1)] = "x", [new PairKey("a", 2)] = "y" } }),
                ("escaped", new RecordKeyed { Values = { [new PairKey(Escaped, 1)] = "x", [new PairKey("\u00e9\u2603", 2)] = "y" } })
            ],
            [
                ("missing", "{}"),
                ("spellings-collide", V("{\"$V\":{" + Pair("a", 1, false) + ":\"x\"," + Pair("a", 1, true) + ":\"y\"}}"))
            ],
            [("a1", new PairKey("a", 1)), ("a2", new PairKey("a", 2)), ("escaped", new PairKey(Escaped, 1)), ("unicode", new PairKey("\u00e9\u2603", 2))]),
            new(typeof(NestedKeyed),
            [("nested", new NestedKeyed { Values = { ["outer"] = new() { ["inner"] = 1 }, ["other"] = new() { ["inner"] = 2 } } })],
            common,
            [("outer", "outer"), ("other", "other"), ("absent", "absent")]),
            new(typeof(NullableKeyed),
            [("null-value", new NullableKeyed { Values = { ["present"] = "v", ["absent-value"] = null } })],
            common,
            [("present", "present"), ("absent-value", "absent-value")])
        ];
    }

    static string Name(INamingPolicy naming, string name) => naming.JsonPropertyNamingPolicy?.ConvertName(name) ?? name;
}

public sealed class Runner(string policy, EventSerializer serializer, JsonSchemaGenerator generator, ExpandoObjectConverter converter)
{
    sealed record Result(string Status, string? Output, string? Category, object? Value);

    readonly Dictionary<string, Result> _results = [];

    public List<Case> Cases { get; } = [];

    public async Task Run(Probe probe)
    {
        var type = probe.Type.Name;
        var schemas = new Dictionary<string, string>();
        foreach (var api in new[] { "Generate", "GenerateForReadModel" })
        {
            schemas[api] = await Step(type, "type", "schema", api, [], () =>
            {
                var schema = api == "Generate" ? generator.Generate(probe.Type) : generator.GenerateForReadModel(probe.Type);
                return Task.FromResult<(string, object?)>((schema.ToJson(), schema));
            });
        }
        foreach (var (id, value) in probe.Values)
        {
            var input = "value:" + id;
            var dictionary = value.GetType().GetProperty("Values")!.GetValue(value) as IDictionary;
            var serialize = await Step(type, input, "serialize", "", [], async () => ((await serializer.Serialize(value)).ToJsonString(), null),
                description: "constructed " + type + " value " + id, constructedKeyCount: dictionary?.Count);
            await Pipeline(probe, input, serialize, schemas, _results.TryGetValue(serialize, out var written) ? written.Output : null);
        }
        foreach (var (id, json) in probe.Raws) await Pipeline(probe, "raw:" + id, null, schemas, json);
    }

    async Task Pipeline(Probe probe, string input, string? dependency, Dictionary<string, string> schemas, string? text)
    {
        var type = probe.Type.Name;
        var parse = await Step(type, input, "parse", "", dependency is null ? [] : [dependency], () =>
            Task.FromResult<(string, object?)>((text!, JsonNode.Parse(text!)!.AsObject())), text);
        var read = await Step(type, input, "deserialize", "", [parse], async () =>
        {
            var value = await serializer.Deserialize(probe.Type, JsonNode.Parse(text!)!.AsObject());
            return (probe.Type.Name + " deserialized", value);
        }, text);
        await Step(type, input, "lookup", "", [read], () => Task.FromResult<(string, object?)>((Lookup(probe, _results[read].Value!), null)));
        await Step(type, input, "rewrite", "", [read], async () => ((await serializer.Serialize(_results[read].Value!)).ToJsonString(), null));
        foreach (var (api, schemaId) in schemas)
        {
            var schemaValue = _results[schemaId].Value as JsonSchema;
            var expando = await Step(type, input, "expando-read", api, [parse, schemaId], () =>
            {
                var value = converter.ToExpandoObject(JsonNode.Parse(text!)!.AsObject(), schemaValue!);
                return Task.FromResult<(string, object?)>((JsonSerializer.Serialize(value), value));
            }, text);
            await Step(type, input, "expando-write", api, [expando], () =>
                Task.FromResult<(string, object?)>((converter.ToJsonObject((System.Dynamic.ExpandoObject)_results[expando].Value!, schemaValue!).ToJsonString(), null)));
        }
    }

    static string Lookup(Probe probe, object value)
    {
        var dictionary = (IDictionary?)value.GetType().GetProperty("Values")!.GetValue(value);
        var lookups = probe.Lookups.Select(l => new
        {
            key = l.Label,
            found = dictionary?.Contains(l.Key) ?? false,
            value = dictionary is not null && dictionary.Contains(l.Key) ? JsonSerializer.Serialize(dictionary[l.Key]) : null
        });
        return JsonSerializer.Serialize(new { keyCount = dictionary?.Count, lookups });
    }

    async Task<string> Step(string type, string input, string stage, string api, string[] dependsOn, Func<Task<(string Output, object? Value)>> action,
        string? text = null, string? description = null, int? constructedKeyCount = null)
    {
        var id = policy + "/" + type + "/" + input + "/" + stage + (api.Length == 0 ? "" : "/" + api);
        var blocked = dependsOn.Any(d => _results[d].Status != "ok");
        Result result;
        if (blocked) result = new("not-attempted", null, null, null);
        else
        {
            try
            {
                var (output, value) = await action();
                result = new("ok", stage == "parse" ? text : output, null, value);
                if (stage == "parse") result = result with { Value = null };
            }
            catch (Exception error) { result = new("error", null, Category(error), null); }
        }
        _results[id] = result;
        Cases.Add(new(id, type, input, stage, Method(stage, api), api, dependsOn, description, blocked ? null : text, result.Status, result.Output, result.Category, constructedKeyCount));
        return id;
    }

    static string Method(string stage, string api) => stage switch
    {
        "serialize" or "rewrite" => "EventSerializer.Serialize",
        "parse" => "JsonNode.Parse",
        "deserialize" => "EventSerializer.Deserialize",
        "lookup" => "IDictionary.Contains+indexer",
        "schema" => "JsonSchemaGenerator." + api,
        "expando-read" => "ExpandoObjectConverter.ToExpandoObject",
        _ => "ExpandoObjectConverter.ToJsonObject"
    };

    // Exception messages can echo keys, so only the type chain is recorded.
    static string Category(Exception error)
    {
        var chain = new List<string>();
        for (var current = error is TargetInvocationException { InnerException: not null } wrapped ? wrapped.InnerException : error; current is not null; current = current.InnerException)
            chain.Add(current.GetType().FullName!);
        return string.Join(">", chain);
    }
}

public static class Isolation
{
    public static JsonSerializerOptions ClientOptions(INamingPolicy naming)
    {
        // Run the real packaged client initializer without constructing a connection
        // or triggering discovery, as the naming and enum captures do.
        const BindingFlags flags = BindingFlags.Instance | BindingFlags.NonPublic;
        var client = (ChronicleClient)RuntimeHelpers.GetUninitializedObject(typeof(ChronicleClient));
        var options = new ChronicleOptions { AutoDiscoverAndRegister = false };
        typeof(ChronicleClient).GetField("<Options>k__BackingField", flags)!.SetValue(client, options);
        typeof(ChronicleClient).GetField("_namingPolicy", flags)!.SetValue(client, naming);
        typeof(ChronicleClient).GetMethod("InitializeJsonSerializationOptions", flags)!.Invoke(client, null);
        return options.JsonSerializerOptions;
    }

    public static EventSerializer Serializer(JsonSerializerOptions options, IDerivedTypes derived) =>
        (EventSerializer)typeof(EventSerializer).GetConstructors().Single().Invoke([
            DispatchProxy.Create<IClientArtifactsProvider, EmptyArtifacts>(), null,
            DispatchProxy.Create<IEventTypes, ForbiddenRegistry>(), options, derived]);
}

public class EmptyArtifacts : DispatchProxy
{
    protected override object? Invoke(MethodInfo? method, object?[]? args) =>
        method!.ReturnType == typeof(IEnumerable<Type>) ? Array.Empty<Type>() :
        throw new InvalidOperationException("Unexpected artifacts operation: " + method.Name);
}

public class ForbiddenRegistry : DispatchProxy
{
    public static int Calls { get; private set; }

    protected override object? Invoke(MethodInfo? method, object?[]? args)
    {
        Calls++;
        throw new InvalidOperationException("Capture attempted event registry access");
    }
}

public sealed class EmptyDerived : IDerivedTypes
{
    public IEnumerable<Type> TypesWithDerivatives => [];
    public bool HasDerivatives(Type type) => false;
    public bool IsDerivedType(Type type) => false;
    public Type GetDerivedTypeFor(Type targetType, DerivedTypeId id) => throw new InvalidOperationException("No derivatives configured");
    public Type GetTargetTypeFor(Type derivedType) => throw new InvalidOperationException("No derivatives configured");
}

public sealed class EmptyMetadata : IComplianceMetadataResolver
{
    public bool HasMetadataFor(Type type) => false;
    public bool HasMetadataFor(PropertyInfo property) => false;
    public IEnumerable<ComplianceMetadata> GetMetadataFor(Type type) => [];
    public IEnumerable<ComplianceMetadata> GetMetadataFor(PropertyInfo property) => [];
}
